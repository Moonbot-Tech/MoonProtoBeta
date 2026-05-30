//! Runtime-owned pending request state and the per-tick pollers that drain it.
//!
//! Each `Pending*` struct holds an in-flight async Engine API request whose
//! response is consumed by the matching `poll_*` helper inside the runtime
//! loop. The loop owns one [`RuntimePending`] for the whole session.

use super::*;

#[derive(Default)]
pub(super) struct RuntimePending {
    pub(super) auto_candles_scope: Option<std::sync::Arc<crate::state::TradeStorageScope>>,
    pub(super) auto_candles_requested: bool,
    pub(super) auto_candles: Vec<PendingAutoCandles>,
    pub(super) auto_candles_apply: Vec<PendingAutoCandlesApply>,
    pub(super) coin_card_candles: Vec<PendingCoinCardCandles>,
    pub(super) account_refreshes: Vec<PendingAccountRefresh>,
    pub(super) transfer_assets: Vec<PendingTransferAssets>,
    pub(super) transfer_assets_batches: Vec<PendingTransferAssetsBatch>,
    pub(super) next_transfer_assets_batch_id: u64,
    pub(super) engine_actions: Vec<PendingEngineAction>,
}

pub(super) struct PendingAutoCandles {
    pub(super) uid: u64,
    pub(super) rx: mpsc::Receiver<crate::client::MergedCandles>,
}

pub(super) struct PendingAutoCandlesApply {
    pub(super) uid: u64,
    pub(super) summary: crate::state::CandlesSnapshotApplySummary,
    pub(super) rx: mpsc::Receiver<()>,
}

pub(super) struct PendingTransferAssets {
    pub(super) kind: crate::state::ExchangeKind,
    pub(super) batch_id: Option<u64>,
    pub(super) request_uid: Option<u64>,
    pub(super) deadline: Instant,
    pub(super) rx: mpsc::Receiver<crate::commands::engine_api::EngineResponse>,
}

pub(super) struct PendingTransferAssetsBatch {
    pub(super) id: u64,
    pub(super) remaining: usize,
    pub(super) updated: usize,
    pub(super) failed: usize,
}

pub(super) struct PendingCoinCardCandles {
    pub(super) ticket: super::super::CoinCardCandlesTicket,
    pub(super) deadline: Instant,
    pub(super) rx: mpsc::Receiver<crate::commands::engine_api::EngineResponse>,
}

pub(super) struct PendingAccountRefresh {
    pub(super) kind: PendingAccountRefreshKind,
    pub(super) request_uid: Option<u64>,
    pub(super) deadline: Instant,
    pub(super) rx: mpsc::Receiver<crate::commands::engine_api::EngineResponse>,
}

#[derive(Clone, Copy)]
pub(super) enum PendingAccountRefreshKind {
    HedgeMode,
    ApiExpiration,
}

fn remove_api_pending(api_pending: &ApiPending, request_uid: Option<u64>) {
    if let Some(uid) = request_uid {
        api_pending.remove(uid);
    }
}

pub(super) struct PendingEngineAction {
    pub(super) kind: crate::events::EngineActionKind,
    pub(super) ticket: super::super::EngineActionTicket,
    pub(super) deadline: Instant,
    pub(super) rx: mpsc::Receiver<crate::commands::engine_api::EngineResponse>,
}

pub(super) fn poll_auto_candles(
    pending: &mut RuntimePending,
    dispatcher: &mut crate::events::EventDispatcher,
) -> bool {
    let mut changed = false;
    let mut i = 0;
    while i < pending.auto_candles.len() {
        match pending.auto_candles[i].rx.try_recv() {
            Ok(merged) => {
                let request_uid = merged.uid;
                let fallback_uid = pending.auto_candles[i].uid;
                let summary = dispatcher.apply_candles_snapshot(&merged.markets);
                pending.auto_candles.swap_remove(i);
                if let Some(summary) = summary {
                    if let Some(rx) = dispatcher.market_history_barrier_async() {
                        pending.auto_candles_apply.push(PendingAutoCandlesApply {
                            uid: request_uid,
                            summary,
                            rx,
                        });
                    } else {
                        dispatcher.queue_candles_snapshot_event(
                            crate::state::CandlesSnapshotEvent::Failed {
                                request_uid: Some(request_uid),
                                error: "market history worker unavailable after snapshot apply"
                                    .to_string(),
                            },
                        );
                        changed = true;
                    }
                } else {
                    dispatcher.queue_candles_snapshot_event(
                        crate::state::CandlesSnapshotEvent::Failed {
                            request_uid: Some(if request_uid != 0 {
                                request_uid
                            } else {
                                fallback_uid
                            }),
                            error: "candles snapshot was not applied to retained history"
                                .to_string(),
                        },
                    );
                    changed = true;
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let uid = pending.auto_candles.swap_remove(i).uid;
                dispatcher.queue_candles_snapshot_event(
                    crate::state::CandlesSnapshotEvent::Failed {
                        request_uid: Some(uid),
                        error: "pending full candles receiver closed before response".to_string(),
                    },
                );
                changed = true;
            }
            Err(mpsc::TryRecvError::Empty) => {
                i += 1;
            }
        }
    }

    let mut i = 0;
    while i < pending.auto_candles_apply.len() {
        match pending.auto_candles_apply[i].rx.try_recv() {
            Ok(()) => {
                let applied = pending.auto_candles_apply.swap_remove(i);
                dispatcher.queue_candles_snapshot_event(
                    crate::state::CandlesSnapshotEvent::Ready {
                        request_uid: applied.uid,
                        summary: applied.summary,
                    },
                );
                changed = true;
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let applied = pending.auto_candles_apply.swap_remove(i);
                dispatcher.queue_candles_snapshot_event(
                    crate::state::CandlesSnapshotEvent::Failed {
                        request_uid: Some(applied.uid),
                        error: "market history worker barrier closed before ack".to_string(),
                    },
                );
                changed = true;
            }
            Err(mpsc::TryRecvError::Empty) => {
                i += 1;
            }
        }
    }
    changed
}

pub(super) fn poll_coin_card_candles(
    pending: &mut Vec<PendingCoinCardCandles>,
    dispatcher: &mut crate::events::EventDispatcher,
    api_pending: &ApiPending,
) -> bool {
    let mut changed = false;
    let mut i = 0;
    let now = Instant::now();
    while i < pending.len() {
        match pending[i].rx.try_recv() {
            Ok(resp) => {
                let ticket = pending.swap_remove(i).ticket;
                changed |=
                    dispatcher.apply_coin_card_candles_response(ticket.market, ticket.kind, resp);
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let ticket = pending.swap_remove(i).ticket;
                dispatcher.coin_card_candles_request_failed(
                    ticket.market,
                    ticket.kind,
                    ticket.request_uid,
                    "pending CoinCard candles receiver closed before response",
                );
            }
            Err(mpsc::TryRecvError::Empty) => {
                if pending[i].deadline <= now {
                    let ticket = pending.swap_remove(i).ticket;
                    remove_api_pending(api_pending, ticket.request_uid);
                    dispatcher.coin_card_candles_request_failed(
                        ticket.market,
                        ticket.kind,
                        ticket.request_uid,
                        "pending CoinCard candles request timed out",
                    );
                    changed = true;
                } else {
                    i += 1;
                }
            }
        }
    }
    changed
}

pub(super) fn poll_transfer_assets(
    pending: &mut RuntimePending,
    dispatcher: &mut crate::events::EventDispatcher,
    api_pending: &ApiPending,
) -> bool {
    let mut changed = false;
    let mut i = 0;
    let now = Instant::now();
    while i < pending.transfer_assets.len() {
        match pending.transfer_assets[i].rx.try_recv() {
            Ok(resp) => {
                let item = pending.transfer_assets.swap_remove(i);
                let success = dispatcher.apply_transfer_assets_response(item.kind, resp);
                changed |= success;
                finish_transfer_assets_batch_item(pending, dispatcher, item.batch_id, success);
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let item = pending.transfer_assets.swap_remove(i);
                dispatcher.transfer_assets_request_failed(
                    item.kind,
                    "pending transfer-assets receiver closed before response",
                );
                changed = true;
                finish_transfer_assets_batch_item(pending, dispatcher, item.batch_id, false);
            }
            Err(mpsc::TryRecvError::Empty) => {
                if pending.transfer_assets[i].deadline <= now {
                    let item = pending.transfer_assets.swap_remove(i);
                    remove_api_pending(api_pending, item.request_uid);
                    dispatcher.transfer_assets_request_failed(
                        item.kind,
                        "pending transfer-assets request timed out",
                    );
                    changed = true;
                    finish_transfer_assets_batch_item(pending, dispatcher, item.batch_id, false);
                } else {
                    i += 1;
                }
            }
        }
    }
    changed
}

pub(super) fn poll_account_refreshes(
    pending: &mut Vec<PendingAccountRefresh>,
    dispatcher: &mut crate::events::EventDispatcher,
    api_pending: &ApiPending,
) -> bool {
    let mut changed = false;
    let mut i = 0;
    let now = Instant::now();
    while i < pending.len() {
        match pending[i].rx.try_recv() {
            Ok(resp) => {
                let item = pending.swap_remove(i);
                changed |= match item.kind {
                    PendingAccountRefreshKind::HedgeMode => {
                        dispatcher.apply_hedge_mode_response(resp)
                    }
                    PendingAccountRefreshKind::ApiExpiration => {
                        dispatcher.apply_api_expiration_response(resp)
                    }
                };
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let item = pending.swap_remove(i);
                match item.kind {
                    PendingAccountRefreshKind::HedgeMode => dispatcher.hedge_mode_request_failed(
                        item.request_uid,
                        "pending hedge-mode receiver closed before response",
                    ),
                    PendingAccountRefreshKind::ApiExpiration => dispatcher
                        .api_expiration_request_failed(
                            item.request_uid,
                            "pending API-expiration receiver closed before response",
                        ),
                }
            }
            Err(mpsc::TryRecvError::Empty) => {
                if pending[i].deadline <= now {
                    let item = pending.swap_remove(i);
                    remove_api_pending(api_pending, item.request_uid);
                    match item.kind {
                        PendingAccountRefreshKind::HedgeMode => dispatcher
                            .hedge_mode_request_failed(
                                item.request_uid,
                                "pending hedge-mode request timed out",
                            ),
                        PendingAccountRefreshKind::ApiExpiration => dispatcher
                            .api_expiration_request_failed(
                                item.request_uid,
                                "pending API-expiration request timed out",
                            ),
                    }
                } else {
                    i += 1;
                }
            }
        }
    }
    changed
}

pub(super) fn finish_transfer_assets_batch_item(
    pending: &mut RuntimePending,
    dispatcher: &mut crate::events::EventDispatcher,
    batch_id: Option<u64>,
    success: bool,
) {
    let Some(batch_id) = batch_id else {
        return;
    };
    let Some(pos) = pending
        .transfer_assets_batches
        .iter()
        .position(|batch| batch.id == batch_id)
    else {
        return;
    };
    let batch = &mut pending.transfer_assets_batches[pos];
    batch.remaining = batch.remaining.saturating_sub(1);
    if success {
        batch.updated += 1;
    } else {
        batch.failed += 1;
    }
    if batch.remaining != 0 {
        return;
    }
    let batch = pending.transfer_assets_batches.swap_remove(pos);
    dispatcher.queue_events([crate::events::Event::TransferAssets(
        crate::state::TransferAssetsEvent::RefreshCompleted {
            request_id: batch.id,
            requested: batch.updated + batch.failed,
            updated: batch.updated,
            failed: batch.failed,
            revision: dispatcher.transfer_assets().revision(),
        },
    )]);
}

pub(super) fn poll_engine_actions(
    pending: &mut Vec<PendingEngineAction>,
    dispatcher: &mut crate::events::EventDispatcher,
    api_pending: &ApiPending,
) {
    let mut i = 0;
    let now = Instant::now();
    while i < pending.len() {
        match pending[i].rx.try_recv() {
            Ok(resp) => {
                let kind = pending[i].kind.clone();
                dispatcher.queue_engine_action_response(kind, resp);
                pending.swap_remove(i);
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let action = pending.swap_remove(i);
                dispatcher.queue_engine_action_disconnected(
                    action.kind,
                    action.ticket.request_uid,
                    action.ticket.method,
                    "pending Engine API action receiver closed before response",
                );
            }
            Err(mpsc::TryRecvError::Empty) => {
                if pending[i].deadline <= now {
                    let action = pending.swap_remove(i);
                    remove_api_pending(api_pending, action.ticket.request_uid);
                    dispatcher.queue_engine_action_disconnected(
                        action.kind,
                        action.ticket.request_uid,
                        action.ticket.method,
                        "pending Engine API action timed out",
                    );
                } else {
                    i += 1;
                }
            }
        }
    }
}
