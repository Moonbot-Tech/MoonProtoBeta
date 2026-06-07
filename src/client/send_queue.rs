use super::SlicedAck;
use crate::protocol::{control, slider::Slider};
/// Send priority matching Delphi `TMoonProtoSendPriority`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SendPriority {
    /// `MPS_Sliced`: large reliable payload sent through the slicing engine.
    Sliced,
    /// `MPS_High`: small direct payload with ACK/retry handling.
    High,
    /// `MPS_Low`: best-effort low-priority payload, one per send cycle.
    #[allow(dead_code)]
    Low,
}

/// Unique key for command deduplication.
///
/// This matches Delphi `TMoonUniqueKey`: commands with the same `(kind, uid)`
/// replace older pending commands in send queues.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UniqueKey {
    /// `TUniqueCommandKind` ordinal (`0` means no dedup).
    pub kind: u8,
    /// Command-specific dedup identity, usually a server order UID or fixed
    /// singleton slot.
    pub uid: u64,
}

/// `UK_None`: no queue deduplication.
pub(crate) const UK_NONE: u8 = 0;
/// `UK_OrderStatus`: low-level order-status request key.
#[allow(dead_code)]
pub(crate) const UK_ORDER_STATUS: u8 = 1;
/// `UK_OrderStatusShort`: low-level short order-status request key.
#[allow(dead_code)]
pub(crate) const UK_ORDER_STATUS_SHORT: u8 = 2;
/// `UK_OrderMove`: replace/cancel/stops/panic/VStop dedup by order task id.
pub(crate) const UK_ORDER_MOVE: u8 = 3;
/// `UK_StopMove`: legacy stop-move dedup ordinal from Delphi.
#[allow(dead_code)]
pub(crate) const UK_STOP_MOVE: u8 = 4;
/// `UK_StratSnapshot`: singleton strategy snapshot dedup key.
pub(crate) const UK_STRAT_SNAPSHOT: u8 = 5;
/// `UK_BaseUISettings`: singleton client-settings snapshot dedup key.
pub(crate) const UK_BASE_UI_SETTINGS: u8 = 6;
/// `UK_StratSellPriceUpdate`: per-strategy sell-price dedup key.
pub(crate) const UK_STRAT_SELL_PRICE_UPDATE: u8 = 7;
/// `UK_BalanceFull`: singleton full-balance snapshot dedup key.
#[allow(dead_code)]
pub(crate) const UK_BALANCE_FULL: u8 = 8;
/// `UK_TurnMMDetection`: MM-orders subscription command key.
pub(crate) const UK_TURN_MM_DETECTION: u8 = 9;
/// `UK_ImmuneClicks`: batch order-immunity dedup key.
pub(crate) const UK_IMMUNE_CLICKS: u8 = 10;
/// `UK_LevManageSettings`: singleton leverage-management settings key.
pub(crate) const UK_LEV_MANAGE_SETTINGS: u8 = 11;
/// `UK_ArbPrices`: arbitrage price command key.
#[allow(dead_code)]
pub(crate) const UK_ARB_PRICES: u8 = 12;
/// `UK_DexSwitch`: DEX switch command key.
pub(crate) const UK_DEX_SWITCH: u8 = 13;
/// `UK_SpotSwitch`: spot-mode switch command key.
pub(crate) const UK_SPOT_SWITCH: u8 = 14;
/// `UK_ChartTextSnapshot`: per-client chart text snapshot key.
#[allow(dead_code)]
pub(crate) const UK_CHART_TEXT_SNAPSHOT: u8 = 15;
/// `UK_ChartTextState`: singleton chart text state key.
pub(crate) const UK_CHART_TEXT_STATE: u8 = 16;

impl UniqueKey {
    /// No deduplication.
    pub(crate) fn none() -> Self {
        Self {
            kind: UK_NONE,
            uid: 0,
        }
    }
    /// Return whether this key disables deduplication.
    pub(crate) fn is_none(&self) -> bool {
        self.kind == UK_NONE
    }
    /// UKey for order move/cancel/stops/panic/vstop commands keyed by task id.
    pub(crate) fn order_move(task_id: u64) -> Self {
        Self {
            kind: UK_ORDER_MOVE,
            uid: task_id,
        }
    }
    /// UKey for `TSetImmuneCommand`, keyed by the wrapping sum of item UIDs.
    pub(crate) fn immune_clicks(items_uid_sum: u64) -> Self {
        Self {
            kind: UK_IMMUNE_CLICKS,
            uid: items_uid_sum,
        }
    }

    /// `UK_BaseUISettings` with Delphi `TClientSettingsCommand.SetUKey`
    /// semantics: every settings snapshot competes for the single UID=1 slot.
    pub(crate) fn base_ui_settings_slot() -> Self {
        Self {
            kind: UK_BASE_UI_SETTINGS,
            uid: 1,
        }
    }
    /// Delphi `TMMOrdersSubscribeCommand` UKey: `(UK_TurnMMDetection, UID)`.
    pub(crate) fn turn_mm_detection_for(uid: u64) -> Self {
        Self {
            kind: UK_TURN_MM_DETECTION,
            uid,
        }
    }
    /// `UK_LevManageSettings` with Delphi `TLevManageCommand.SetUKey`
    /// semantics: every leverage-management snapshot competes for UID=1.
    pub(crate) fn lev_manage_settings_slot() -> Self {
        Self {
            kind: UK_LEV_MANAGE_SETTINGS,
            uid: 1,
        }
    }
    /// Delphi `TSwitchDexCommand` UKey: `(UK_DexSwitch, UID)`.
    pub(crate) fn dex_switch_for(uid: u64) -> Self {
        Self {
            kind: UK_DEX_SWITCH,
            uid,
        }
    }
    /// Delphi `TSwitchSpotCommand` UKey: `(UK_SpotSwitch, UID)`.
    pub(crate) fn spot_switch_for(uid: u64) -> Self {
        Self {
            kind: UK_SPOT_SWITCH,
            uid,
        }
    }
    /// `UK_ChartTextState` with Delphi `TChartTextStateCommand.SetUKey`
    /// semantics: every state update competes for UID=1.
    pub(crate) fn chart_text_state_slot() -> Self {
        Self {
            kind: UK_CHART_TEXT_STATE,
            uid: 1,
        }
    }
    /// `UK_StratSellPriceUpdate` keyed by `strategy_id` so dedup is per
    /// strategy.
    pub(crate) fn strat_sell_price_update(strategy_id: u64) -> Self {
        Self {
            kind: UK_STRAT_SELL_PRICE_UPDATE,
            uid: strategy_id,
        }
    }
    /// `UK_StratSnapshot` singleton slot for full strategy snapshots.
    pub(crate) fn strat_snapshot() -> Self {
        Self {
            kind: UK_STRAT_SNAPSHOT,
            uid: 1,
        }
    }
}

/// Item in the send queue (matches TMoonProtoDataToSend)
#[derive(Clone)]
pub(crate) struct SendItem {
    pub data: Vec<u8>,   // serialized command stream
    pub cmd: u8,         // TMoonProtoCommand ordinal
    pub encrypted: bool, // FCrypted
    pub priority: SendPriority,
    pub retry_left: i32,   // RetryLeft
    pub max_retries: i32,  // MaxRetryCount
    pub msg_num: u64,      // for ACK tracking (assigned in Crypt)
    pub last_sent_at: i64, // ms timestamp of last send
    pub u_key: UniqueKey,  // dedup key (matches TMoonUniqueKey)
}

#[inline]
pub(crate) fn initial_retry_left(encrypted: bool, max_retries: i32) -> i32 {
    if encrypted {
        (max_retries - 1).max(0)
    } else {
        0
    }
}

/// Delphi `TMoonProtoBaseNet.DataToSend*` queues.
///
/// `SendCmdInt` appends directly into one of these grow-only lists under
/// `SendLock`; the writer tick later copies and clears them through
/// `GetCopySendList`. Keep the same machine effect: no local capacity cap, and
/// UKey dedup only for Sliced/High queues, removing the first older item with
/// the same key before appending the new item.
#[derive(Default)]
pub(crate) struct SendQueues {
    pub(crate) sliced: Vec<SendItem>,
    pub(crate) high: Vec<SendItem>,
    pub(crate) low: Vec<SendItem>,
}

impl SendQueues {
    pub(crate) fn push_send_cmd_int(&mut self, item: SendItem) {
        let queue = match item.priority {
            SendPriority::Sliced => &mut self.sliced,
            SendPriority::High => &mut self.high,
            SendPriority::Low => &mut self.low,
        };

        if !item.u_key.is_none()
            && matches!(item.priority, SendPriority::Sliced | SendPriority::High)
        {
            if let Some(pos) = queue.iter().position(|queued| queued.u_key == item.u_key) {
                queue.remove(pos);
            }
        }

        queue.push(item);
    }

    pub(crate) fn take_into(
        &mut self,
        sliced: &mut Vec<SendItem>,
        high: &mut Vec<SendItem>,
        low: &mut Vec<SendItem>,
    ) {
        sliced.append(&mut self.sliced);
        high.append(&mut self.high);
        low.append(&mut self.low);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.sliced.is_empty() && self.high.is_empty() && self.low.is_empty()
    }
}

/// Delphi `TMoonProtoBaseNet.SendLock` shared state.
///
/// The writer snapshots `DataToSend*`, `ACKs`, and `TmpSlider` under one lock,
/// then performs all heavy protocol work outside it. Receive-side code may only
/// append/copy small already-decoded values here.
#[derive(Default)]
pub(crate) struct SendLockState {
    pub(crate) send_queues: SendQueues,
    pub(crate) incoming_sliced_acks: Vec<SlicedAck>,
    pub(crate) tmp_slider: Slider,
}

impl SendLockState {
    pub(crate) fn push_send_cmd_int(&mut self, item: SendItem) {
        self.send_queues.push_send_cmd_int(item);
    }

    pub(crate) fn take_send_snapshot(
        &mut self,
        sliced: &mut Vec<SendItem>,
        high: &mut Vec<SendItem>,
        low: &mut Vec<SendItem>,
        acks: &mut Vec<SlicedAck>,
    ) -> Option<Slider> {
        self.send_queues.take_into(sliced, high, low);
        acks.append(&mut self.incoming_sliced_acks);
        self.copy_tmp_slider()
    }

    pub(crate) fn push_sliced_ack(&mut self, ack: SlicedAck) {
        self.incoming_sliced_acks.push(ack);
    }

    pub(crate) fn copy_tmp_slider(&mut self) -> Option<Slider> {
        let has_new_data = self.tmp_slider.has_new_data;
        let copied = has_new_data.then(|| self.tmp_slider.clone());
        self.tmp_slider.has_new_data = false;
        copied
    }

    pub(crate) fn apply_ping_ack_bitmap(&mut self, payload: &[u8]) {
        // DataReadInt(MPC_Ping): parse server's ACK bitmap into TmpSlider only.
        // Delphi drops PendingH later in writer CheckSeningData via
        // CopyRecvdData -> ApplyRegularHLAck.
        if payload.len() > control::PING_SIZE {
            let srv_ack_start = u64::from_le_bytes(payload[42..50].try_into().unwrap());
            let ack_data_len = payload.len() - control::PING_SIZE;
            let r_count = (ack_data_len / 8).min(64);
            let mut bits = [0u64; 64];
            for i in 0..r_count {
                let start = control::PING_SIZE + i * 8;
                bits[i] = u64::from_le_bytes(payload[start..start + 8].try_into().unwrap());
            }
            self.tmp_slider.bit_field = bits;
            self.tmp_slider.start_num = srv_ack_start;
            self.tmp_slider.has_new_data = true;
            self.tmp_slider.r_count = r_count as i32;
        }
    }

    pub(crate) fn reset_tmp_slider(&mut self) {
        self.tmp_slider = Slider::new();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.send_queues.is_empty()
    }
}
