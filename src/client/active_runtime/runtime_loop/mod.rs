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
use parking_lot::RwLock;
use std::collections::VecDeque;

mod handlers;
mod pending;

use handlers::*;
use pending::*;

pub(super) fn runtime_loop(
    mut client: Client,
    mut dispatcher: crate::events::EventDispatcher,
    rx: &mpsc::Receiver<RuntimeCommand>,
    event_sink: MoonEventSink,
    snapshot: Arc<RwLock<Option<MoonClientSnapshot>>>,
    connect: ConnectConfig,
    ready_tx: Option<mpsc::Sender<Result<(), ConnectError>>>,
    deferred_commands: &mut VecDeque<RuntimeCommand>,
) {
    let api_pending = Arc::clone(&client.pending_api.api_pending);
    let mut pending = RuntimePending::default();
    let mut startup = Some(RuntimeInitMachine::new(connect, &mut dispatcher));
    let startup_started_at = Instant::now();
    let mut dispatch_buffers = InlineDispatchBuffers::default();
    loop {
        #[cfg(any(test, feature = "diagnostics"))]
        let command_drain_start = Instant::now();
        let (stop, changed) = if startup.is_some() {
            drain_commands_during_startup(rx, deferred_commands)
        } else {
            drain_deferred_and_live_commands(
                &mut client,
                &mut dispatcher,
                rx,
                &mut pending,
                deferred_commands,
            )
        };
        #[cfg(any(test, feature = "diagnostics"))]
        client
            .metrics
            .protocol_metrics
            .record_profile_phase_labeled(
                ProfilePhase::RuntimeCommandDrain,
                command_drain_start.elapsed(),
                u8::MAX,
                u8::MAX,
                0,
            );
        if changed {
            publish_snapshot_profiled(&client, &dispatcher, &snapshot);
        }
        if stop {
            break;
        }

        if !run_protocol_step(&mut client, &mut dispatcher, &mut dispatch_buffers) {
            break;
        }

        let state_changed = if let Some(startup_machine) = startup.as_mut() {
            #[cfg(any(test, feature = "diagnostics"))]
            let init_poll_start = Instant::now();
            #[cfg(any(test, feature = "diagnostics"))]
            let (init_cmd, init_api_method) = startup_machine.profile_source();
            let init_poll = startup_machine.poll(&mut client, &mut dispatcher);
            #[cfg(any(test, feature = "diagnostics"))]
            client
                .metrics
                .protocol_metrics
                .record_profile_phase_labeled(
                    ProfilePhase::InitStep,
                    init_poll_start.elapsed(),
                    init_cmd,
                    init_api_method,
                    0,
                );
            match init_poll {
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
                    publish_snapshot_profiled(&client, &dispatcher, &snapshot);
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
            #[cfg(any(test, feature = "diagnostics"))]
            let pending_start = Instant::now();
            #[cfg(any(test, feature = "diagnostics"))]
            let auto_candles_start = Instant::now();
            let candles_changed = poll_auto_candles(&mut client, &mut pending, &mut dispatcher);
            #[cfg(any(test, feature = "diagnostics"))]
            client
                .metrics
                .protocol_metrics
                .record_profile_phase_labeled(
                    ProfilePhase::PendingAutoCandles,
                    auto_candles_start.elapsed(),
                    u8::MAX,
                    u8::MAX,
                    pending.auto_candles.len() + pending.auto_candles_apply.len(),
                );
            if !pending.auto_candles_requested && client.trades_storage_scope_intent().is_some() {
                schedule_auto_candles_snapshot(&mut client, &mut pending);
            }
            #[cfg(any(test, feature = "diagnostics"))]
            let coin_card_start = Instant::now();
            let coin_card_changed = poll_coin_card_candles(
                &mut pending.coin_card_candles,
                &mut dispatcher,
                &api_pending,
            );
            #[cfg(any(test, feature = "diagnostics"))]
            client
                .metrics
                .protocol_metrics
                .record_profile_phase_labeled(
                    ProfilePhase::PendingCoinCard,
                    coin_card_start.elapsed(),
                    u8::MAX,
                    u8::MAX,
                    pending.coin_card_candles.len(),
                );
            #[cfg(any(test, feature = "diagnostics"))]
            let transfer_assets_start = Instant::now();
            let transfer_assets_changed =
                poll_transfer_assets(&mut pending, &mut dispatcher, &api_pending);
            #[cfg(any(test, feature = "diagnostics"))]
            client
                .metrics
                .protocol_metrics
                .record_profile_phase_labeled(
                    ProfilePhase::PendingTransferAssets,
                    transfer_assets_start.elapsed(),
                    u8::MAX,
                    u8::MAX,
                    pending.transfer_assets.len() + pending.transfer_assets_batches.len(),
                );
            #[cfg(any(test, feature = "diagnostics"))]
            let account_start = Instant::now();
            let account_changed = poll_account_refreshes(
                &mut pending.account_refreshes,
                &mut dispatcher,
                &api_pending,
            );
            #[cfg(any(test, feature = "diagnostics"))]
            client
                .metrics
                .protocol_metrics
                .record_profile_phase_labeled(
                    ProfilePhase::PendingAccount,
                    account_start.elapsed(),
                    u8::MAX,
                    u8::MAX,
                    pending.account_refreshes.len(),
                );
            #[cfg(any(test, feature = "diagnostics"))]
            let engine_actions_start = Instant::now();
            poll_engine_actions(&mut pending.engine_actions, &mut dispatcher, &api_pending);
            #[cfg(any(test, feature = "diagnostics"))]
            client
                .metrics
                .protocol_metrics
                .record_profile_phase_labeled(
                    ProfilePhase::PendingEngineActions,
                    engine_actions_start.elapsed(),
                    u8::MAX,
                    u8::MAX,
                    pending.engine_actions.len(),
                );
            #[cfg(any(test, feature = "diagnostics"))]
            client
                .metrics
                .protocol_metrics
                .record_profile_phase_labeled(
                    ProfilePhase::RuntimePending,
                    pending_start.elapsed(),
                    u8::MAX,
                    u8::MAX,
                    pending.auto_candles.len()
                        + pending.auto_candles_apply.len()
                        + pending.coin_card_candles.len()
                        + pending.account_refreshes.len()
                        + pending.transfer_assets.len()
                        + pending.engine_actions.len(),
                );
            candles_changed || coin_card_changed || transfer_assets_changed || account_changed
        };
        if state_changed && startup.is_none() {
            publish_snapshot_profiled(&client, &dispatcher, &snapshot);
        }

        if startup.is_none() {
            let events =
                take_queued_events_and_publish_snapshot(&client, &mut dispatcher, &snapshot);
            // Snapshot was published before events were emitted, while the
            // runtime still held the state that produced those events. Event
            // delivery itself runs after state apply and snapshot publish, not
            // inline inside user callbacks.
            emit_domain_events(events, &event_sink);
        }

        #[cfg(any(test, feature = "diagnostics"))]
        let command_drain_start = Instant::now();
        let (stop, changed) = if startup.is_some() {
            drain_commands_during_startup(rx, deferred_commands)
        } else {
            drain_deferred_and_live_commands(
                &mut client,
                &mut dispatcher,
                rx,
                &mut pending,
                deferred_commands,
            )
        };
        #[cfg(any(test, feature = "diagnostics"))]
        client
            .metrics
            .protocol_metrics
            .record_profile_phase_labeled(
                ProfilePhase::RuntimeCommandDrain,
                command_drain_start.elapsed(),
                u8::MAX,
                u8::MAX,
                0,
            );
        if changed {
            publish_snapshot_profiled(&client, &dispatcher, &snapshot);
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
    let mut mode = RunMode::with_buffers(
        dispatcher,
        std::mem::take(&mut buffers.event_buf),
        std::mem::take(&mut buffers.payload_buf),
        std::mem::take(&mut buffers.active_actions_buf),
    );
    let keep_running = (ProtocolCore { client }).run_step(&mut mode);
    let (event_buf, payload_buf, active_actions_buf) = mode.into_buffers();
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
    client: &Client,
    dispatcher: &mut crate::events::EventDispatcher,
    snapshot: &RwLock<Option<MoonClientSnapshot>>,
) -> Vec<crate::events::Event> {
    let events = dispatcher.take_queued_events();
    if !events.is_empty() {
        publish_snapshot_profiled(client, dispatcher, snapshot);
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
    let mut guard = snapshot.write();
    let revision = guard
        .as_ref()
        .map(|snapshot| snapshot.revision().saturating_add(1))
        .unwrap_or(1);
    *guard = Some(MoonClientSnapshot::new(revision, next));
}

fn publish_snapshot_profiled(
    client: &Client,
    dispatcher: &crate::events::EventDispatcher,
    snapshot: &RwLock<Option<MoonClientSnapshot>>,
) {
    #[cfg(not(any(test, feature = "diagnostics")))]
    let _ = client;
    #[cfg(any(test, feature = "diagnostics"))]
    let snapshot_start = Instant::now();
    publish_snapshot(dispatcher, snapshot);
    #[cfg(any(test, feature = "diagnostics"))]
    client
        .metrics
        .protocol_metrics
        .record_profile_phase_labeled(
            ProfilePhase::SnapshotPublish,
            snapshot_start.elapsed(),
            u8::MAX,
            u8::MAX,
            0,
        );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::engine_api::ServerInfo;
    use crate::commands::market::{BaseCurrency, ExchangeCode};
    use crate::commands::strategy_serializer::{FieldValue, StrategyFields, StrategySnapshot};
    use crate::commands::trade::{
        BaseCommandHeader, CanonicalOrderState, DelphiBool, OrderCommandPayload, OrderDescription,
        OrderImage, OrderWorkerStatus, StopSettings, TradeCommand, ORDER_SECTION_ALL_MASK,
    };

    fn dummy_cfg() -> ClientConfig {
        ClientConfig {
            server_ip: "127.0.0.1".to_string(),
            server_port: 3000,
            master_key: [0; 16],
            mac_key: [0; 16],
            transport_mode: TransportMode::V0,
            client_id: 0,
            ntp_host: None,
            refresh: RefreshConfig {
                update_markets_every: None,
                check_tags_every: None,
            },
            market_history: crate::state::MarketHistorySizing::default(),
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

    fn seed_runtime_order(
        dispatcher: &mut crate::events::EventDispatcher,
        uid: u64,
        status: OrderWorkerStatus,
        revision: u64,
    ) {
        let desc = OrderDescription::for_test("DOGEUSDT", false, false);
        let mut state = CanonicalOrderState::default();
        state.0[0] = status.to_byte();
        let command = TradeCommand::OrderImage(OrderImage {
            header: BaseCommandHeader {
                cmd_id: 41,
                ver: crate::commands::registry::CURRENT_PROTO_CMD_VER,
                uid,
            },
            state_rev: revision,
            desc: desc.clone(),
            section_mask: ORDER_SECTION_ALL_MASK,
            state: state.clone(),
        });
        let mut events = Vec::new();
        let mut repairs = Vec::new();
        dispatcher.orders_mut().apply_protocol(
            command,
            1_000,
            1,
            2,
            0.0,
            &|_| true,
            &mut events,
            &mut repairs,
        );
        assert!(repairs.is_empty());
        assert!(dispatcher.orders().get(uid).is_some());
    }

    fn write_str8(out: &mut Vec<u8>, value: &str) {
        out.push(value.len() as u8);
        out.extend_from_slice(value.as_bytes());
    }

    fn apply_comment_strategy_schema(dispatcher: &mut crate::events::EventDispatcher) {
        use flate2::write::DeflateEncoder;
        use flate2::Compression;
        use std::io::Write;

        let mut body = Vec::new();
        body.push(crate::commands::strategy_schema::SCHEMA_FORMAT_VERSION);
        body.push(1); // kind_count
        body.push(1); // kind ordinal
        write_str8(&mut body, "Kind1");
        body.extend_from_slice(&1u16.to_le_bytes()); // field_count
        write_str8(&mut body, "Comment");
        body.push(crate::commands::strategy_serializer::TID_STRING);
        body.push(0);
        body.push(1); // visible for kind 1

        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&body).unwrap();
        let data = encoder.finish().unwrap();

        let mut payload = Vec::new();
        payload.push(8); // TStratSchema
        payload.extend_from_slice(&crate::commands::registry::CURRENT_PROTO_CMD_VER.to_le_bytes());
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&(data.len() as u32).to_le_bytes());
        payload.extend_from_slice(&data);

        let mut out = Vec::new();
        dispatcher.dispatch_into(Command::Strat, &payload, 0, &mut out);
        assert!(out.iter().any(|ev| {
            matches!(
                ev,
                crate::events::Event::Strat(crate::state::StratEvent::SchemaApplied {
                    kind_count: 1,
                    field_count: 1,
                    ..
                })
            )
        }));
    }

    #[test]
    fn moon_trade_new_order_builds_v4_start_command() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();

        let changed = handle_trade_action(
            &mut client,
            &mut dispatcher,
            RuntimeTradeCommandKind::NewOrder {
                params: NewOrderParams::new("DOGEUSDT", OrderSide::Short, 12.5, 0.25)
                    .with_strategy_id(42)
                    .with_planned_sell_price(15.0)
                    .with_market_stop(true),
                request_uid: 0xCAFE_BABE,
            },
        )
        .expect("v4 start command does not need legacy route bytes");

        assert!(!changed);
        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        match TradeCommand::parse(&high[0].data).expect("valid new order") {
            TradeCommand::OrderCommand(cmd) => match cmd.payload {
                OrderCommandPayload::Start {
                    market_name,
                    is_short,
                    use_market_stop,
                    strategy_id,
                    size,
                    price,
                    planned_sell_price,
                } => {
                    assert_eq!(cmd.header.uid, 0xCAFE_BABE);
                    assert_eq!(market_name, "DOGEUSDT");
                    assert!(is_short && use_market_stop);
                    assert_eq!(strategy_id, 42);
                    assert_eq!(size, 0.25);
                    assert_eq!(price, 12.5);
                    assert_eq!(planned_sell_price, 15.0);
                }
                other => panic!("unexpected order payload: {other:?}"),
            },
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn runtime_update_stops_sends_for_tracked_server_order() {
        let uid = 0x5151;
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();
        seed_runtime_order(&mut dispatcher, uid, OrderWorkerStatus::BuySet, 1);

        let stops = StopSettings {
            stop_loss_on: DelphiBool::TRUE,
            sl_level: 12.5,
            use_take_profit: DelphiBool::TRUE,
            take_profit: 15.0,
            ..StopSettings::default()
        };
        let mut pending = RuntimePending::default();
        assert!(handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::OrderAction(RuntimeCommandKind::UpdateStops { uid, stops }),
            &mut pending
        ));

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        match TradeCommand::parse(&high[0].data).expect("valid stops update") {
            TradeCommand::OrderCommand(cmd) => match cmd.payload {
                OrderCommandPayload::Stops { order_id, stops } => {
                    assert_eq!(order_id, uid);
                    assert!(bool::from(stops.stop_loss_on));
                    assert_eq!(stops.sl_level, 12.5);
                    assert!(bool::from(stops.use_take_profit));
                    assert_eq!(stops.take_profit, 15.0);
                    assert!(
                        bool::from(stops.take_profit_changed),
                        "runtime derives the TP latch before sending"
                    );
                }
                other => panic!("unexpected order payload: {other:?}"),
            },
            other => panic!("unexpected trade command: {other:?}"),
        }
    }

    #[test]
    fn runtime_update_vstop_sends_for_tracked_server_order() {
        let uid = 0x5252;
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();
        seed_runtime_order(&mut dispatcher, uid, OrderWorkerStatus::SellSet, 1);

        let mut pending = RuntimePending::default();
        assert!(handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::OrderAction(RuntimeCommandKind::UpdateVStop {
                uid,
                params: VStopParams::percent(12.5, 100.0),
            }),
            &mut pending
        ));

        let (_, high, _) = client.take_send_queues_for_test();
        assert_eq!(high.len(), 1);
        match TradeCommand::parse(&high[0].data).expect("valid VStop update") {
            TradeCommand::OrderCommand(cmd) => match cmd.payload {
                OrderCommandPayload::VStop {
                    order_id,
                    enabled,
                    fixed,
                    level,
                    volume,
                } => {
                    assert_eq!(order_id, uid);
                    assert!(enabled);
                    assert!(!fixed);
                    assert_eq!(level, 12.5);
                    assert_eq!(volume, 100.0);
                }
                other => panic!("unexpected order payload: {other:?}"),
            },
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
    fn post_connect_strategy_sync_advances_local_epoch_before_snapshot_send() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();
        let mut pending = RuntimePending::default();
        apply_comment_strategy_schema(&mut dispatcher);
        dispatcher.set_local_strategy_epoch(41);

        let mut fields = StrategyFields::new();
        fields.insert("Comment", FieldValue::String("edited".to_string()));
        let strategy = StrategySnapshot {
            strategy_id: 0x5157,
            strategy_ver: 3,
            last_date: 1234,
            checked: true,
            kind: 1,
            path: "Local".into(),
            fields,
        };

        assert!(handle_command(
            &mut client,
            &mut dispatcher,
            RuntimeCommand::StrategySnapshotBatch(vec![strategy.clone()]),
            &mut pending,
        ));
        assert_eq!(
            dispatcher.local_strategy_epoch(),
            42,
            "Delphi increments cfg.ServerStratEpoch before sending an edited local snapshot"
        );

        let (sliced, high, low) = client.take_send_queues_for_test();
        let item = sliced
            .into_iter()
            .chain(high)
            .chain(low)
            .find(|item| item.cmd == Command::Strat.to_byte())
            .expect("strategy snapshot command must be queued");
        let crate::commands::strat::StratCommand::Snapshot(snapshot) =
            crate::commands::strat::StratCommand::parse(&item.data)
                .expect("queued strategy snapshot must parse")
        else {
            panic!("expected TStratSnapshot");
        };
        assert_eq!(snapshot.server_epoch, 42);
        assert_eq!(snapshot.client_max_last_date, strategy.last_date);
        let batch = crate::commands::strategy_serializer::parse_strategy_batch(&snapshot.data)
            .expect("strategy snapshot payload must parse");
        assert_eq!(batch.strategies.len(), 1);
        assert_eq!(batch.strategies[0].strategy_id, strategy.strategy_id);
    }

    #[test]
    fn startup_defers_strategy_sync_until_schema_gate_is_ready() {
        let mut client = ready_client();
        let mut dispatcher = crate::events::EventDispatcher::new();
        let mut pending = RuntimePending::default();

        let mut fields = StrategyFields::new();
        fields.insert("Comment", FieldValue::String("early edit".to_string()));
        let strategy = StrategySnapshot {
            strategy_id: 0xE4E4,
            strategy_ver: 1,
            last_date: 77,
            checked: true,
            kind: 1,
            path: "Local".into(),
            fields,
        };

        let (tx, rx) = mpsc::channel();
        tx.send(RuntimeCommand::StrategySnapshotBatch(
            vec![strategy.clone()],
        ))
        .unwrap();

        let mut deferred = VecDeque::new();
        let (stop, changed) = drain_commands_during_startup(&rx, &mut deferred);
        assert!(!stop);
        assert!(!changed);
        assert_eq!(deferred.len(), 1);
        assert!(client.take_send_queues_for_test().0.is_empty());

        apply_comment_strategy_schema(&mut dispatcher);
        let (stop, changed) = drain_deferred_and_live_commands(
            &mut client,
            &mut dispatcher,
            &rx,
            &mut pending,
            &mut deferred,
        );
        assert!(!stop);
        assert!(changed);
        assert!(deferred.is_empty());

        let (sliced, high, low) = client.take_send_queues_for_test();
        let item = sliced
            .into_iter()
            .chain(high)
            .chain(low)
            .find(|item| item.cmd == Command::Strat.to_byte())
            .expect("deferred strategy sync must be sent after schema is available");
        let crate::commands::strat::StratCommand::Snapshot(snapshot) =
            crate::commands::strat::StratCommand::parse(&item.data)
                .expect("queued strategy snapshot must parse")
        else {
            panic!("expected TStratSnapshot");
        };
        let batch = crate::commands::strategy_serializer::parse_strategy_batch(&snapshot.data)
            .expect("deferred strategy snapshot payload must parse");
        assert_eq!(batch.strategies.len(), 1);
        assert_eq!(batch.strategies[0].strategy_id, strategy.strategy_id);
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
        let first = snapshot.read().clone().expect("first snapshot");
        assert_eq!(first.revision(), 1);

        publish_snapshot(&dispatcher, &snapshot);
        let second = snapshot.read().clone().expect("second snapshot");
        assert_eq!(second.revision(), 2);
        assert_eq!(second.orders().len(), first.orders().len());
    }

    #[test]
    fn published_order_snapshot_is_persistent_without_cloning_protocol_state() {
        let mut dispatcher = crate::events::EventDispatcher::new();
        let uid = 0x5151;
        seed_runtime_order(&mut dispatcher, uid, OrderWorkerStatus::BuySet, 1);
        let held = dispatcher.snapshot();

        seed_runtime_order(&mut dispatcher, uid, OrderWorkerStatus::SellSet, 2);
        let fresh = dispatcher.snapshot();

        assert_eq!(
            held.orders().get(uid).unwrap().status,
            OrderWorkerStatus::BuySet
        );
        assert_eq!(
            fresh.orders().get(uid).unwrap().status,
            OrderWorkerStatus::SellSet
        );
    }
}
