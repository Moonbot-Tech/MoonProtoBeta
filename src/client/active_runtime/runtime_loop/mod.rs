//! Runtime-owner loop and command handlers for `MoonClient`.
//!
//! The loop driver lives here; its two halves are split out:
//!   - `handlers` — draining the command channel and dispatching each
//!     [`RuntimeCommand`], plus scheduling the async Engine API requests.
//!   - `pending`  — the in-flight request state and the per-tick `poll_*`
//!     helpers that drain it.

use super::commands::{
    RuntimeCommand, RuntimeCommandKind, RuntimeTradeCommandKind, StratRuntimeCommand,
    UiRuntimeCommand,
};
use super::*;
use crate::client::init::{RuntimeInitMachine, RuntimeInitPoll};
use std::collections::VecDeque;
use std::sync::RwLock;

mod handlers;
mod pending;

use handlers::*;
use pending::*;

pub(super) fn runtime_loop(
    mut client: Client,
    mut dispatcher: crate::events::EventDispatcher,
    rx: mpsc::Receiver<RuntimeCommand>,
    event_sink: MoonEventSink,
    snapshot: Arc<RwLock<Option<MoonClientSnapshot>>>,
    connect: ConnectConfig,
    ready_tx: Option<mpsc::Sender<Result<(), ConnectError>>>,
) {
    let api_pending = Arc::clone(&client.pending_api.api_pending);
    let mut pending = RuntimePending::default();
    let mut startup = Some(RuntimeInitMachine::new(connect, &mut dispatcher));
    let startup_started_at = Instant::now();
    let mut deferred_commands = VecDeque::new();
    let mut dispatch_buffers = InlineDispatchBuffers::default();
    loop {
        let (stop, changed) = if startup.is_some() {
            drain_commands_during_startup(&rx, &mut deferred_commands)
        } else {
            drain_deferred_and_live_commands(
                &mut client,
                &mut dispatcher,
                &rx,
                &mut pending,
                &mut deferred_commands,
            )
        };
        if changed {
            publish_snapshot(&dispatcher, &snapshot);
        }
        if stop {
            break;
        }

        if !run_protocol_step(&mut client, &mut dispatcher, &mut dispatch_buffers) {
            break;
        }

        let state_changed = if let Some(startup_machine) = startup.as_mut() {
            match startup_machine.poll(&mut client, &mut dispatcher) {
                RuntimeInitPoll::Pending { changed } => changed,
                RuntimeInitPoll::Ready(_result) => {
                    if client.trades_storage_scope_intent().is_some() {
                        sync_runtime_trade_storage_scope(&client, &mut dispatcher);
                        schedule_auto_candles_snapshot(&mut client, &mut pending);
                    }
                    // Carry server/account identity (BaseCheck/AuthCheck) into the
                    // published snapshot so `MoonClient` consumers can read it
                    // without holding the low-level client. Set once Init has
                    // resolved both checks; reconnect-with-reinit re-runs this.
                    dispatcher.set_session_identity(
                        client.server_info().clone(),
                        client.auth_info().cloned(),
                    );
                    publish_snapshot(&dispatcher, &snapshot);
                    client.fire_lifecycle(LifecycleEvent::InitStepCompleted {
                        step: "StartupSnapshot",
                        elapsed_ms: startup_started_at.elapsed().as_millis() as u64,
                    });
                    publish_queued_events(&mut dispatcher, &event_sink);
                    client.fire_lifecycle(LifecycleEvent::InitStepCompleted {
                        step: "StartupEvents",
                        elapsed_ms: startup_started_at.elapsed().as_millis() as u64,
                    });
                    client.fire_lifecycle(LifecycleEvent::Ready);
                    if let Some(tx) = ready_tx.as_ref() {
                        let _ = tx.send(Ok(()));
                    }
                    startup = None;
                    true
                }
                RuntimeInitPoll::Failed(err) => {
                    client.fire_lifecycle(LifecycleEvent::ConnectFailed { error: err.clone() });
                    if let Some(tx) = ready_tx.as_ref() {
                        let _ = tx.send(Err(err));
                    }
                    break;
                }
            }
        } else {
            let candles_changed = poll_auto_candles(&mut client, &mut pending, &mut dispatcher);
            if !pending.auto_candles_requested && client.trades_storage_scope_intent().is_some() {
                schedule_auto_candles_snapshot(&mut client, &mut pending);
            }
            let coin_card_changed = poll_coin_card_candles(
                &mut pending.coin_card_candles,
                &mut dispatcher,
                &api_pending,
            );
            let transfer_assets_changed =
                poll_transfer_assets(&mut pending, &mut dispatcher, &api_pending);
            let account_changed = poll_account_refreshes(
                &mut pending.account_refreshes,
                &mut dispatcher,
                &api_pending,
            );
            poll_engine_actions(&mut pending.engine_actions, &mut dispatcher, &api_pending);
            candles_changed || coin_card_changed || transfer_assets_changed || account_changed
        };
        if state_changed && startup.is_none() {
            publish_snapshot(&dispatcher, &snapshot);
        }

        if startup.is_none() {
            let events = take_queued_events_and_publish_snapshot(&mut dispatcher, &snapshot);
            // Snapshot was published before events were emitted, while the
            // runtime still held the state that produced those events. Event
            // delivery itself runs after state apply and snapshot publish, not
            // inline inside user callbacks.
            emit_domain_events(events, &event_sink);
        }

        let (stop, changed) = if startup.is_some() {
            drain_commands_during_startup(&rx, &mut deferred_commands)
        } else {
            drain_deferred_and_live_commands(
                &mut client,
                &mut dispatcher,
                &rx,
                &mut pending,
                &mut deferred_commands,
            )
        };
        if changed {
            publish_snapshot(&dispatcher, &snapshot);
        }
        if stop {
            break;
        }
    }
}

#[derive(Default)]
struct InlineDispatchBuffers {
    event_buf: Vec<crate::events::Event>,
    payload_buf: Vec<(Command, Vec<u8>)>,
    active_actions_buf: Vec<crate::events::ActiveAction>,
}

fn run_protocol_step(
    client: &mut Client,
    dispatcher: &mut crate::events::EventDispatcher,
    buffers: &mut InlineDispatchBuffers,
) -> bool {
    let mut mode = RunMode::Dispatcher {
        dispatcher,
        on_event: DispatcherEventFn::Queue,
        event_buf: std::mem::take(&mut buffers.event_buf),
        payload_buf: std::mem::take(&mut buffers.payload_buf),
        active_actions_buf: std::mem::take(&mut buffers.active_actions_buf),
    };
    let keep_running = (ProtocolCore { client }).run_step(&mut mode);
    let RunMode::Dispatcher {
        event_buf,
        payload_buf,
        active_actions_buf,
        ..
    } = mode
    else {
        unreachable!("inline runtime must use RunMode::Dispatcher");
    };
    buffers.event_buf = event_buf;
    buffers.payload_buf = payload_buf;
    buffers.active_actions_buf = active_actions_buf;
    keep_running
}

pub(super) fn publish_queued_events(
    dispatcher: &mut crate::events::EventDispatcher,
    event_sink: &MoonEventSink,
) -> bool {
    let events = dispatcher.take_queued_events();
    let changed = !events.is_empty();
    emit_domain_events(events, event_sink);
    changed
}

pub(super) fn take_queued_events_and_publish_snapshot(
    dispatcher: &mut crate::events::EventDispatcher,
    snapshot: &RwLock<Option<MoonClientSnapshot>>,
) -> Vec<crate::events::Event> {
    let events = dispatcher.take_queued_events();
    if !events.is_empty() {
        publish_snapshot(dispatcher, snapshot);
    }
    events
}

pub(super) fn emit_domain_events(events: Vec<crate::events::Event>, event_sink: &MoonEventSink) {
    for event in events {
        event_sink.emit_domain(event);
    }
}

pub(super) fn publish_snapshot(
    dispatcher: &crate::events::EventDispatcher,
    snapshot: &RwLock<Option<MoonClientSnapshot>>,
) {
    let next = Arc::new(dispatcher.snapshot());
    let mut guard = snapshot.write().unwrap();
    let revision = guard
        .as_ref()
        .map(|snapshot| snapshot.revision().saturating_add(1))
        .unwrap_or(1);
    *guard = Some(MoonClientSnapshot::new(revision, next));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::engine_api::ServerInfo;
    use crate::commands::market::{BaseCurrency, ExchangeCode};
    use crate::commands::trade::TradeCommand;

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            mask_ver: TransportMode::V0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
        }
    }

    fn ready_client() -> Client {
        let mut client = Client::new(dummy_cfg());
        client.testing_set_domain_ready(true);
        client.set_server_info(ServerInfo {
            exchange_code: Some(ExchangeCode::FGate),
            base_currency_code: Some(BaseCurrency::IDR),
            ..Default::default()
        });
        client
    }

    #[test]
    fn moon_trade_new_order_derives_route_and_builds_delphi_wire() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();

        let queued = handle_trade_action(
            &mut client,
            &mut dispatcher,
            RuntimeTradeCommandKind::NewOrder {
                params: NewOrderParams::new("DOGEUSDT", OrderSide::Short, 12.5, 0.25)
                    .with_strategy_id(42),
                request_uid: 0xCAFE_BABE,
            },
        )
        .expect("BaseCheck route is present");

        assert!(queued);
        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        match TradeCommand::parse(&high[0].data).expect("valid new order") {
            TradeCommand::NewOrder(cmd) => {
                assert_eq!(cmd.market.market_name, "DOGEUSDT");
                assert_eq!(cmd.market.base.uid, 0xCAFE_BABE);
                assert_eq!(cmd.market.currency, 17);
                assert_eq!(cmd.market.platform, 9);
                assert!(cmd.is_short);
                assert_eq!(cmd.price, 12.5);
                assert_eq!(cmd.strat_id, 42);
                assert_eq!(cmd.order_size, 0.25);
            }
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn moon_trade_returns_route_error_before_base_check_fields() {
        let mut client = Client::new(dummy_cfg());
        client.testing_set_domain_ready(true);
        let mut dispatcher = crate::events::EventDispatcher::new();

        let err = handle_trade_action(
            &mut client,
            &mut dispatcher,
            RuntimeTradeCommandKind::Penalty {
                market_name: "DOGEUSDT".to_string(),
            },
        )
        .expect_err("new Client has no BaseCheck route");

        assert!(err.missing_exchange_code);
        assert!(err.missing_base_currency_code);
        let (sliced, high, low) = client.take_send_queues_for_test();
        assert!(sliced.is_empty() && high.is_empty() && low.is_empty());
    }

    #[test]
    fn auto_candles_snapshot_is_one_shot_for_current_trades_scope() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();
        let mut pending = RuntimePending::default();

        handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::SubscribeAllTrades(false),
            &mut pending,
        );
        assert!(pending.auto_candles_requested);
        assert_eq!(pending.auto_candles.len(), 1);

        handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::SubscribeAllTrades(false),
            &mut pending,
        );
        assert_eq!(
            pending.auto_candles.len(),
            1,
            "same trades scope must not schedule duplicate full candles requests"
        );

        handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::UnsubscribeAllTrades,
            &mut pending,
        );
        assert!(!pending.auto_candles_requested);
        assert!(pending.auto_candles.is_empty());
        assert!(pending.auto_candles_apply.is_empty());
        assert!(pending.auto_candles_scope.is_none());
    }

    #[test]
    fn init_time_trades_scope_schedules_auto_candles_when_runtime_starts() {
        let mut client = ready_client();
        client.subscribe_all_trades(false);
        let mut dispatcher = crate::events::EventDispatcher::new();
        let mut pending = RuntimePending::default();

        sync_runtime_trade_storage_scope(&client, &mut dispatcher);
        schedule_auto_candles_snapshot(&mut client, &mut pending);

        assert!(pending.auto_candles_requested);
        assert_eq!(pending.auto_candles.len(), 1);
    }

    #[test]
    fn auto_candles_timeout_cleans_chunk_collector_and_allows_retry() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();
        let mut pending = RuntimePending::default();

        handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::SubscribeAllTrades(false),
            &mut pending,
        );
        let uid = pending.auto_candles[0].uid;
        assert!(client.pending_api.pending_candles.contains_key(&uid));
        pending.auto_candles[0].deadline = Instant::now() - std::time::Duration::from_millis(1);

        assert!(poll_auto_candles(
            &mut client,
            &mut pending,
            &mut dispatcher
        ));
        assert!(!pending.auto_candles_requested);
        assert!(pending.auto_candles.is_empty());
        assert!(!client.pending_api.pending_candles.contains_key(&uid));
        match dispatcher.take_queued_events().as_slice() {
            [crate::events::Event::CandlesSnapshot(crate::state::CandlesSnapshotEvent::Failed {
                request_uid: Some(failed_uid),
                error,
            })] => {
                assert_eq!(*failed_uid, uid);
                assert!(error.contains("timed out"));
            }
            other => panic!("unexpected events: {other:?}"),
        }

        schedule_auto_candles_snapshot(&mut client, &mut pending);
        assert!(pending.auto_candles_requested);
        assert_eq!(pending.auto_candles.len(), 1);
    }

    #[test]
    fn auto_candles_scope_change_drops_old_chunk_collector() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();
        let mut pending = RuntimePending::default();

        handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::SubscribeAllTrades(false),
            &mut pending,
        );
        let old_uid = pending.auto_candles[0].uid;
        assert!(client.pending_api.pending_candles.contains_key(&old_uid));

        handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::SubscribeTradesFor {
                want_mm: false,
                markets: vec!["BTCUSDT".to_string()],
            },
            &mut pending,
        );

        assert!(!client.pending_api.pending_candles.contains_key(&old_uid));
        assert!(pending.auto_candles_requested);
        assert_eq!(pending.auto_candles.len(), 1);
        let new_uid = pending.auto_candles[0].uid;
        assert!(client.pending_api.pending_candles.contains_key(&new_uid));
    }

    #[test]
    fn transfer_assets_batch_emits_completion_after_all_kinds_finish() {
        let mut pending = RuntimePending::default();
        pending
            .transfer_assets_batches
            .push(PendingTransferAssetsBatch {
                id: 7,
                remaining: 3,
                updated: 0,
                failed: 0,
            });
        let mut dispatcher = crate::events::EventDispatcher::new();

        finish_transfer_assets_batch_item(&mut pending, &mut dispatcher, Some(7), true);
        assert!(dispatcher.take_queued_events().is_empty());
        finish_transfer_assets_batch_item(&mut pending, &mut dispatcher, Some(7), false);
        assert!(dispatcher.take_queued_events().is_empty());
        finish_transfer_assets_batch_item(&mut pending, &mut dispatcher, Some(7), true);

        assert!(matches!(
            dispatcher.take_queued_events().as_slice(),
            [crate::events::Event::TransferAssets(
                crate::state::TransferAssetsEvent::RefreshCompleted {
                    request_id: 7,
                    requested: 3,
                    updated: 2,
                    failed: 1,
                    ..
                }
            )]
        ));
        assert!(pending.transfer_assets_batches.is_empty());
    }

    #[test]
    fn published_snapshots_have_monotonic_revisions() {
        let dispatcher = crate::events::EventDispatcher::new();
        let snapshot = RwLock::new(None);

        publish_snapshot(&dispatcher, &snapshot);
        let first = snapshot.read().unwrap().clone().expect("first snapshot");
        assert_eq!(first.revision(), 1);

        publish_snapshot(&dispatcher, &snapshot);
        let second = snapshot.read().unwrap().clone().expect("second snapshot");
        assert_eq!(second.revision(), 2);
        assert_eq!(second.orders().len(), first.orders().len());
    }
}
