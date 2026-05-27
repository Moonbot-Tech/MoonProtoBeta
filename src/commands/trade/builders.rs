//! Client -> server `MPC_Order` builders.

use super::*;

pub(super) fn write_base_command_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64) {
    out.push(cmd_id);
    out.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
}

pub(super) fn write_market_header(
    out: &mut Vec<u8>,
    cmd_id: u8,
    uid: u64,
    market_name: &str,
    currency: u8,
    platform: u8,
) {
    write_base_command_header(out, cmd_id, uid);
    out.push(currency);
    out.push(platform);
    write_string(out, market_name);
}

pub(super) fn write_trade_epoch_header(
    out: &mut Vec<u8>,
    cmd_id: u8,
    ctx: TradeCtx,
    market_name: &str,
    epoch: u16,
    status: OrderWorkerStatus,
) {
    write_market_header(
        out,
        cmd_id,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.extend_from_slice(&epoch.to_le_bytes());
    out.push(status.to_byte());
}

/// Route fields shared by client-originated trade command builders.
///
/// Regular applications should obtain this from [`crate::Client::trade_ctx`],
/// [`crate::Client::random_trade_ctx`], or from tracked order state via
/// [`crate::state::Order::trade_ctx`]. Low-level protocol tools can use
/// [`TradeCtx::with_route`] when they intentionally provide raw Delphi enum
/// ordinals themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeCtx {
    pub uid: u64,
    pub currency: u8,
    pub platform: u8,
}

impl TradeCtx {
    /// Build a context with explicit Delphi route ordinals.
    ///
    /// `currency` is `Ord(cfg.BaseCurrency)` and `platform` is
    /// `Ord(cfg.Header.Current)` on the server. Prefer the higher-level helpers
    /// on [`crate::Client`] unless you are writing a protocol tool.
    pub fn with_route(uid: u64, currency: u8, platform: u8) -> Self {
        Self {
            uid,
            currency,
            platform,
        }
    }
}

/// CmdId=6: build `TOrderReplaceCommand`.
///
/// Delphi `TOrderReplaceCommand.Create` always sends `Epoch=0` and
/// `Status=OS_None` for client-originated replace commands.
pub fn build_order_replace(
    ctx: TradeCtx,
    market_name: &str,
    order_type: OrderType,
    new_price: f64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_trade_epoch_header(&mut out, 6, ctx, market_name, 0, OrderWorkerStatus::None);
    out.push(order_type.to_byte());
    out.extend_from_slice(&new_price.to_le_bytes());
    out
}

/// CmdId=9: request all active orders.
pub fn build_all_statuses_request(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11);
    write_base_command_header(&mut out, 9, uid);
    out
}

/// CmdId=10: `TOrderCancelCommand`, cancel one order.
pub fn build_order_cancel(
    ctx: TradeCtx,
    market_name: &str,
    epoch: u16,
    status: OrderWorkerStatus,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_trade_epoch_header(&mut out, 10, ctx, market_name, epoch, status);
    out
}

/// CmdId=11: TJoinOrdersCommand.
pub fn build_join_orders(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        11,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out
}

/// CmdId=12: TSplitOrderCommand.
pub fn build_split_order(
    ctx: TradeCtx,
    market_name: &str,
    split_parts: i32,
    split_small: bool,
    split_small_sell: bool,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        12,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.extend_from_slice(&split_parts.to_le_bytes());
    out.push(split_small as u8);
    out.push(split_small_sell as u8);
    out
}

/// CmdId=13: TMoveAllSellsCommand.
pub fn build_move_all_sells(
    ctx: TradeCtx,
    market_name: &str,
    params: MoveAllSellsParams,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    write_market_header(
        &mut out,
        13,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(params.cmd_type.to_byte());
    out.push(params.move_kind.to_byte());
    out.extend_from_slice(&params.price.to_le_bytes());
    params.price_zone.write_to(&mut out);
    out.push(params.side.to_byte());
    out
}

/// CmdId=14: TDoClosePositionCommand.
pub fn build_do_close_position(ctx: TradeCtx, market_name: &str, market_sell: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        14,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(market_sell as u8);
    out
}

/// CmdId=15: TDoLimitClosePositionCommand (= JoinOrdersCommand format).
pub fn build_do_limit_close_position(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        15,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out
}

/// CmdId=16: TDoSplitPositionCommand (= JoinOrdersCommand format).
pub fn build_do_split_position(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        16,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out
}

/// CmdId=17: TDoSellOrderCommand.
pub fn build_do_sell_order(ctx: TradeCtx, market_name: &str, price: f64, size: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    write_market_header(
        &mut out,
        17,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.extend_from_slice(&price.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out
}

/// CmdId=18: `TOrderStatusRequest`, request one order status.
pub fn build_order_status_request(ctx: TradeCtx, market_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_trade_epoch_header(&mut out, 18, ctx, market_name, 0, OrderWorkerStatus::None);
    out
}

/// CmdId=20: TOrderStopsUpdate.
pub fn build_order_stops_update(
    ctx: TradeCtx,
    market_name: &str,
    epoch: u16,
    status: OrderWorkerStatus,
    stops: &StopSettings,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_trade_epoch_header(&mut out, 20, ctx, market_name, epoch, status);
    stops.write_to(&mut out);
    out
}

/// CmdId=21: TTurnPanicSellCommand.
///
/// Delphi `TTurnPanicSellCommand.Create` does not set inherited
/// `TTradeEpochCommand` fields on the client path, so object zero-init gives
/// `Epoch=0` and `Status=OS_None`.
pub fn build_turn_panic_sell(ctx: TradeCtx, market_name: &str, turn_on: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_trade_epoch_header(&mut out, 21, ctx, market_name, 0, OrderWorkerStatus::None);
    out.push(turn_on as u8);
    out
}

/// CmdId=22: TSetImmuneCommand.
pub fn build_set_immune(uid: u64, items: &[ImmuneItem]) -> Vec<u8> {
    let wire_count = items.len() as u8;
    let wire_items = &items[..wire_count as usize];
    let mut out = Vec::with_capacity(11 + 1 + wire_items.len() * 9);
    write_base_command_header(&mut out, 22, uid);
    out.push(wire_count);
    for it in wire_items {
        it.write_to(&mut out);
    }
    out
}

/// CmdId=27: TMoveAllBuysCommand.
pub fn build_move_all_buys(ctx: TradeCtx, market_name: &str, params: MoveAllBuysParams) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    write_market_header(
        &mut out,
        27,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(params.cmd_type.to_byte());
    out.push(params.move_kind.to_byte());
    out.extend_from_slice(&params.price.to_le_bytes());
    out.push(params.side.to_byte());
    out
}

/// CmdId=29: TVStopUpdate.
pub fn build_vstop_update(
    ctx: TradeCtx,
    market_name: &str,
    epoch: u16,
    params: VStopUpdateParams,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    write_trade_epoch_header(&mut out, 29, ctx, market_name, epoch, params.status);
    out.push(params.vstop_on as u8);
    out.push(params.vstop_fixed as u8);
    out.extend_from_slice(&params.vstop_level.to_le_bytes());
    out.extend_from_slice(&params.vstop_vol.to_le_bytes());
    out
}

/// CmdId=30: TDoMarketSplitPositionCommand (= JoinOrdersCommand format).
pub fn build_do_market_split_position(ctx: TradeCtx, market_name: &str, is_short: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        30,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out
}

/// CmdId=23: `TPenaltyCommand`, mark a market penalty/cooldown.
///
/// Delphi call sites: TaskWorkers.pas:8361, Unit1.pas:11859/23750.
pub fn build_penalty(ctx: TradeCtx, market_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        23,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out
}

/// CmdId=3: `TNewOrderCommand`, request a new order.
pub fn build_new_order(
    ctx: TradeCtx,
    market_name: &str,
    is_short: bool,
    price: f64,
    strat_id: u64,
    order_size: f64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    write_market_header(
        &mut out,
        3,
        ctx.uid,
        market_name,
        ctx.currency,
        ctx.platform,
    );
    out.push(is_short as u8);
    out.extend_from_slice(&price.to_le_bytes());
    out.extend_from_slice(&strat_id.to_le_bytes());
    out.extend_from_slice(&order_size.to_le_bytes());
    out
}
