//! Client -> server `MPC_Order` builders.

use super::*;
use crate::commands::market::{BaseCurrency, ExchangeCode};
use crate::commands::registry::write_string;

pub(crate) fn write_base_command_header(out: &mut Vec<u8>, cmd_id: u8, uid: u64) {
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

/// Route fields retained for the legacy market-shaped penalty/visual commands.
/// Canonical order state and actions address orders by wire OrderID and do
/// not carry currency/platform bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeCtx {
    pub uid: u64,
    pub currency: BaseCurrency,
    pub platform: ExchangeCode,
}

impl TradeCtx {
    pub fn with_route(uid: u64, currency: BaseCurrency, platform: ExchangeCode) -> Self {
        Self {
            uid,
            currency,
            platform,
        }
    }
}

pub(crate) fn build_move_all_sells(market_name: &str, params: MoveAllSellsParams) -> Vec<u8> {
    let payload = match params.cmd_type {
        MoveAllCmdType::MoveKind => OrderCommandPayload::MoveAllKind {
            market_name: market_name.to_owned(),
            leg: 0,
            move_kind: params.move_kind.to_byte(),
            side: params.side.to_byte(),
            price: params.price,
        },
        MoveAllCmdType::PriceZone => OrderCommandPayload::MoveAllZone {
            market_name: market_name.to_owned(),
            side: params.side.to_byte(),
            min_price: params.price_zone.min_p,
            max_price: params.price_zone.max_p,
        },
        MoveAllCmdType::Pers => OrderCommandPayload::MoveAllPercent {
            market_name: market_name.to_owned(),
            leg: 0,
            percent: params.price,
        },
        _ => OrderCommandPayload::MoveAllKind {
            market_name: market_name.to_owned(),
            leg: 0,
            move_kind: params.move_kind.to_byte(),
            side: params.side.to_byte(),
            price: params.price,
        },
    };
    build_order_command(next_order_action_id(), payload)
}

pub(crate) fn build_move_all_buys(market_name: &str, params: MoveAllBuysParams) -> Vec<u8> {
    let payload = match params.cmd_type {
        MoveAllBuysCmdType::Pers => OrderCommandPayload::MoveAllPercent {
            market_name: market_name.to_owned(),
            leg: 1,
            percent: params.price,
        },
        _ => OrderCommandPayload::MoveAllKind {
            market_name: market_name.to_owned(),
            leg: 1,
            move_kind: params.move_kind.to_byte(),
            side: params.side.to_byte(),
            price: params.price,
        },
    };
    build_order_command(next_order_action_id(), payload)
}

pub(crate) fn build_do_close_position(
    request_uid: u64,
    market_name: &str,
    market_sell: bool,
) -> Vec<u8> {
    build_close_position(request_uid, market_name, 0, market_sell)
}

pub(crate) fn build_do_limit_close_position(
    request_uid: u64,
    market_name: &str,
    is_short: bool,
) -> Vec<u8> {
    build_close_position(request_uid, market_name, 1, is_short)
}

pub(crate) fn build_do_split_position(
    request_uid: u64,
    market_name: &str,
    is_short: bool,
) -> Vec<u8> {
    build_close_position(request_uid, market_name, 2, is_short)
}

pub(crate) fn build_do_market_split_position(
    request_uid: u64,
    market_name: &str,
    is_short: bool,
) -> Vec<u8> {
    build_close_position(request_uid, market_name, 3, is_short)
}

fn build_close_position(request_uid: u64, market_name: &str, mode: u8, flag: bool) -> Vec<u8> {
    build_order_command(
        request_uid,
        OrderCommandPayload::ClosePosition {
            market_name: market_name.to_owned(),
            mode,
            flag,
        },
    )
}

pub(crate) fn build_do_sell_order(
    request_uid: u64,
    market_name: &str,
    price: f64,
    size: f64,
) -> Vec<u8> {
    build_order_command(
        request_uid,
        OrderCommandPayload::ManualSell {
            market_name: market_name.to_owned(),
            price,
            size,
        },
    )
}

/// CmdId=23 remains the market-shaped penalty side effect.
pub(crate) fn build_penalty(ctx: TradeCtx, market_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_market_header(
        &mut out,
        23,
        ctx.uid,
        market_name,
        ctx.currency.to_byte(),
        ctx.platform.to_byte(),
    );
    out
}
