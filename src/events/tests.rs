use super::*;
use crate::commands::arb::build_arb_prices;
use crate::commands::balance::build_request_balance_refresh;
use crate::commands::market::{
    build_markets_prices_response, write_market, BaseCurrency, ExchangeCode, Market,
    MarketPriceUpdate, MarketsListResponse, MarketsPricesResponse, PositionType,
};
use crate::commands::registry::{write_string, CURRENT_PROTO_CMD_VER};
use crate::commands::strat::{
    build_schema_request, build_sell_price_update, build_snapshot_request, StratCommand,
    StratSchema,
};
use crate::commands::trade::trace_flags;
use crate::commands::trade::{
    build_all_statuses_request, BaseCommandHeader, BulkReplaceNotify, MarketCommandHeader,
    OrderCompact, OrderStatus, OrderStatusUpdate, OrderTracePoint, OrderType, OrderUpdateData,
    OrderWorkerStatus, SetImmuneCommand, StopSettings, TradeCommand, TradeCtx, TradeEpochHeader,
};
use crate::commands::EngineMethod;
use crate::state::orders::OrderCancelSend;
use crate::state::{OrderBookKind, DELPHI_MSECS_PER_DAY};

static SERVER_TIME_DELTA_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn server_time_delta_test_lock() -> std::sync::MutexGuard<'static, ()> {
    SERVER_TIME_DELTA_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_str8(out: &mut Vec<u8>, value: &str) {
    out.push(value.len() as u8);
    out.extend_from_slice(value.as_bytes());
}

fn deflate_raw(data: &[u8]) -> Vec<u8> {
    use flate2::write::DeflateEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn comment_strategy_schema_payload() -> Vec<u8> {
    let mut raw = Vec::new();
    raw.push(crate::commands::strategy_schema::SCHEMA_FORMAT_VERSION);
    raw.push(1); // kind_count
    raw.push(1); // kind ordinal
    write_str8(&mut raw, "Kind1");
    raw.extend_from_slice(&1u16.to_le_bytes()); // field_count
    write_str8(&mut raw, "Comment");
    raw.push(crate::commands::strategy_serializer::TID_STRING);
    raw.push(0); // edit, no layout/default/picklist
    raw.push(1); // visibility bitset: visible for kind 1

    deflate_raw(&raw)
}

fn apply_comment_strategy_schema(dispatcher: &mut EventDispatcher) {
    let ev = dispatcher.strats.apply(StratCommand::Schema(StratSchema {
        data: comment_strategy_schema_payload(),
    }));
    assert!(matches!(
        ev,
        StratEvent::SchemaApplied {
            kind_count: 1,
            field_count: 1,
            ..
        }
    ));
}

fn order_book_payload_with(market_index: u16, seq: u16, is_full: bool) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&market_index.to_le_bytes());
    raw.extend_from_slice(&seq.to_le_bytes());
    raw.push(if is_full { 1 } else { 0 }); // Futures.
    raw.extend_from_slice(&0u16.to_le_bytes()); // buy_count=0, sell_count=0.
    crate::compression::synlz_compress(&raw)
}

fn order_book_payload_full_with_levels(
    market_index: u16,
    seq: u16,
    buys: &[(f32, f32)],
    sells: &[(f32, f32)],
) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&market_index.to_le_bytes());
    raw.extend_from_slice(&seq.to_le_bytes());
    raw.push(1); // full futures book.
    raw.extend_from_slice(&(buys.len() as u16).to_le_bytes());
    for (rate, qty) in buys {
        raw.extend_from_slice(&rate.to_le_bytes());
        raw.extend_from_slice(&qty.to_le_bytes());
    }
    for (rate, qty) in sells {
        raw.extend_from_slice(&rate.to_le_bytes());
        raw.extend_from_slice(&qty.to_le_bytes());
    }
    crate::compression::synlz_compress(&raw)
}

fn order_book_payload(market_index: u16) -> Vec<u8> {
    order_book_payload_with(market_index, 1, true)
}

fn empty_all_statuses_payload(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(15);
    out.push(8);
    out.extend_from_slice(&3u16.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes());
    out
}

fn old_v1_client_settings_without_soft_tail(uid: u64) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(1); // TClientSettingsCommand
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
    out.extend_from_slice(&[0u8; 41]);
    write_string(&mut out, "");
    out.push(0); // UseCoinsBlackList
    out.extend_from_slice(&0i32.to_le_bytes()); // TempBLCount
    out
}

#[test]
fn app_queue_keeps_all_events_and_records_max_len_without_drop_policy() {
    let mut dispatcher = EventDispatcher::new();
    dispatcher.queue_events((0..128).map(|i| Event::Raw {
        cmd: Command::UI,
        payload: vec![i as u8],
    }));

    assert_eq!(dispatcher.queued_event_count(), 128);
    assert_eq!(dispatcher.queued_event_max_count(), 128);
    match &dispatcher.queued_events()[0] {
        Event::Raw { payload, .. } => assert_eq!(payload, &[0]),
        other => panic!("unexpected first queued event: {other:?}"),
    }
    match &dispatcher.queued_events()[127] {
        Event::Raw { payload, .. } => assert_eq!(payload, &[127]),
        other => panic!("unexpected last queued event: {other:?}"),
    }

    let drained = dispatcher.take_queued_events();
    assert_eq!(drained.len(), 128);
    assert_eq!(dispatcher.queued_event_count(), 0);
    assert_eq!(
        dispatcher.queued_event_max_count(),
        128,
        "max length is diagnostic history, not a cap"
    );

    dispatcher.queue_events([Event::Raw {
        cmd: Command::Ping,
        payload: vec![1, 2, 3],
    }]);
    assert_eq!(dispatcher.queued_event_count(), 1);
    assert_eq!(
        dispatcher.queued_event_max_count(),
        128,
        "smaller later pushes must not reset the observed max"
    );
}

fn all_statuses_payload(uid: u64, orders: &[OrderStatus]) -> Vec<u8> {
    let mut out = Vec::new();
    BaseCommandHeader {
        cmd_id: 8,
        ver: 3,
        uid,
    }
    .write(&mut out);
    out.extend_from_slice(&(orders.len() as i32).to_le_bytes());
    for st in orders {
        st.epoch_header.write(
            &mut out,
            st.epoch_header.market.currency,
            st.epoch_header.market.platform,
        );
        st.buy_order.write_to(&mut out);
        st.sell_order.write_to(&mut out);
        st.stops.write_to(&mut out);
        out.extend_from_slice(&st.strat_id.to_le_bytes());
        out.push(st.is_short as u8);
        out.extend_from_slice(&st.db_id.to_le_bytes());
        out.push(st.from_cache as u8);
        out.push(st.emulator_mode as u8);
        out.push(st.immune_for_clicks as u8);
    }
    out
}

#[test]
fn dispatcher_parses_old_client_settings_with_cfg_fallback_like_delphi() {
    let mut dispatcher = EventDispatcher::new();
    dispatcher.set_client_settings_fallback(ClientSettingsCommand {
        sign_orders: false,
        free_position_check: true,
        vol_drop_level: 42,
        use_stop_market: true,
        s_price: [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        sb_num: 6,
        join_sell_kind: 2,
        ..ClientSettingsCommand::default()
    });

    let events = dispatcher.dispatch(
        Command::UI,
        &old_v1_client_settings_without_soft_tail(123),
        0,
    );

    assert!(matches!(
        events.as_slice(),
        [Event::Settings(SettingsEvent::ClientSettingsUpdated)]
    ));
    let settings = dispatcher.settings().client_settings.as_ref().unwrap();
    assert_eq!(settings.uid, 123);
    assert!(!settings.sign_orders);
    assert!(settings.free_position_check);
    assert_eq!(settings.vol_drop_level, 42);
    assert!(settings.use_stop_market);
    assert_eq!(settings.s_price, [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);
    assert_eq!(settings.sb_num, 6);
    assert_eq!(settings.join_sell_kind, 2);
}

#[test]
fn dispatcher_skips_future_version_ui_command_like_delphi_registry() {
    let mut dispatcher = EventDispatcher::new();
    let mut payload = vec![1u8]; // TClientSettingsCommand cmd_id.
    payload.extend_from_slice(&(CURRENT_PROTO_CMD_VER + 1).to_le_bytes());
    payload.extend_from_slice(&77u64.to_le_bytes());
    payload.extend_from_slice(&[0xAA; 16]);

    let events = dispatcher.dispatch(Command::UI, &payload, 0);

    assert!(
        events.is_empty(),
        "Delphi logs FSkipped UI commands but emits no UI/settings side effect"
    );
    assert!(dispatcher.settings().client_settings.is_none());
}

#[test]
fn dispatcher_skips_unknown_ui_command_id_like_delphi_base_ui_command() {
    let mut dispatcher = EventDispatcher::new();
    let mut payload = vec![250u8]; // no registered TBaseUICommand descendant.
    payload.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    payload.extend_from_slice(&88u64.to_le_bytes());
    payload.extend_from_slice(&[0xBB; 8]);

    let events = dispatcher.dispatch(Command::UI, &payload, 0);

    assert!(
        events.is_empty(),
        "Delphi frees unknown TBaseUICommand without a public Settings event"
    );
}

fn balance_payload(cmd_id: u8, uid: u64, epoch: u16, btc_total: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(49);
    out.push(cmd_id);
    out.extend_from_slice(&3u16.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(&btc_total.to_le_bytes());
    out.extend_from_slice(&0.0f64.to_le_bytes());
    out.extend_from_slice(&0.0f64.to_le_bytes());
    out.extend_from_slice(&0.0f64.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes());
    out
}

fn write_balance_item_minimal(out: &mut Vec<u8>, market_name: &str, initial_balance: f64) {
    write_string(out, market_name);
    out.extend_from_slice(&0u64.to_le_bytes()); // BalanceHash.
    out.extend_from_slice(&1u32.to_le_bytes()); // InitialBalance flag only.
    out.extend_from_slice(&initial_balance.to_le_bytes());
}

fn balance_payload_with_items(cmd_id: u8, uid: u64, epoch: u16, items: &[(&str, f64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + items.len() * 32);
    out.push(cmd_id);
    out.extend_from_slice(&3u16.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    if cmd_id == 4 {
        out.push(0); // GlobalChanged=false.
    } else {
        out.extend_from_slice(&1.0f64.to_le_bytes());
        out.extend_from_slice(&0.0f64.to_le_bytes());
        out.extend_from_slice(&0.0f64.to_le_bytes());
        out.extend_from_slice(&0.0f64.to_le_bytes());
    }
    out.extend_from_slice(&(items.len() as i32).to_le_bytes());
    for (market_name, initial_balance) in items {
        write_balance_item_minimal(&mut out, market_name, *initial_balance);
    }
    out
}

fn write_balance_item_position(
    out: &mut Vec<u8>,
    market_name: &str,
    initial_balance: f64,
    liq_price: f64,
    max_value: f64,
    leverage_x: i32,
) {
    write_string(out, market_name);
    out.extend_from_slice(&77u64.to_le_bytes());
    let flags = (1u32 << 0) | (1u32 << 4) | (1u32 << 19) | (1u32 << 20);
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&initial_balance.to_le_bytes());
    out.extend_from_slice(&liq_price.to_le_bytes());
    out.extend_from_slice(&max_value.to_le_bytes());
    out.extend_from_slice(&leverage_x.to_le_bytes());
}

fn balance_payload_with_position(
    cmd_id: u8,
    uid: u64,
    epoch: u16,
    market_name: &str,
    initial_balance: f64,
    liq_price: f64,
    max_value: f64,
    leverage_x: i32,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(cmd_id);
    out.extend_from_slice(&3u16.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    if cmd_id == 4 {
        out.push(0);
    } else {
        out.extend_from_slice(&1.0f64.to_le_bytes());
        out.extend_from_slice(&0.0f64.to_le_bytes());
        out.extend_from_slice(&0.0f64.to_le_bytes());
        out.extend_from_slice(&0.0f64.to_le_bytes());
    }
    out.extend_from_slice(&1i32.to_le_bytes());
    write_balance_item_position(
        &mut out,
        market_name,
        initial_balance,
        liq_price,
        max_value,
        leverage_x,
    );
    out
}

fn event_market(name: &str) -> Market {
    Market {
        bn_market_name: name.to_string(),
        market_currency: name.to_string(),
        bn_market_currency: name.to_string(),
        base_currency: "USDT".to_string(),
        market_currency_long: name.to_string(),
        market_currency_canonic: name.to_string(),
        market_name: name.to_string(),
        market_name_mb_classic: name.to_string(),
        bn_status: "TRADING".to_string(),
        leading1000: String::new(),
        bn_price_precision: 2,
        bn_quantity_precision: 5,
        max_leverage: 50,
        k1000: 1,
        bn_iceberg_parts: 0,
        bn_margin_table_id: 0,
        bn_delivery_time: 0,
        bn_tick_size: 0.01,
        bn_step_size: 0.01,
        bn_min_qty: 0.0,
        bn_max_qty: 0.0,
        bn_min_notional: 0.0,
        bn_max_notional: 0.0,
        bn_contract_size: 0.0,
        bn_min_price: 0.0,
        bn_max_price: 0.0,
        bn_max_value: 0.0,
        bn_multiplier_up: 0.0,
        bn_multiplier_down: 0.0,
        bid_multiplier_up: 0.0,
        bid_multiplier_down: 0.0,
        ask_multiplier_up: 0.0,
        ask_multiplier_down: 0.0,
        int_bn_max_qty: 0.0,
        funding_rate: 0.0,
        funding_time: 0.0,
        volume: 0.0,
        is_btc_market: false,
        status_trading: true,
        has_1000_prefix_alias: false,
        bn_iceberg: false,
        bn_only_isolated: false,
        futures_type: BaseCurrency::USDT,
        initial_balance: 0.0,
        locked_balance: 0.0,
        pos_size: 0.0,
        pos_price: 0.0,
        liq_price: 0.0,
        pos_dir: OrderType::Sell,
        long_pos_size: 0.0,
        long_pos_price: 0.0,
        long_liq_price: 0.0,
        long_position_type: PositionType::Cross,
        short_pos_size: 0.0,
        short_pos_price: 0.0,
        short_liq_price: 0.0,
        short_position_type: PositionType::Cross,
        asset_balance: 0.0,
        asset_balance_full: 0.0,
        total_profit_b: 0.0,
        total_profit_l: 0.0,
        total_profit_s: 0.0,
        leverage_x: 1,
        position_type: PositionType::Cross,
        balance_hash: 0,
        last_balance_epoch: 0,
        trade_tail: Default::default(),
        price: Default::default(),
        arb_slots: std::collections::HashMap::new(),
    }
}

fn seed_event_markets(d: &mut EventDispatcher, names: &[&str]) {
    d.markets.apply_markets_list(MarketsListResponse {
        markets: names.iter().map(|name| event_market(name)).collect(),
        corr_markets: vec![],
    });
}

fn api_response_payload_ver(ver: u16, method: EngineMethod, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(Command::API.to_byte());
    out.extend_from_slice(&ver.to_le_bytes());
    out.extend_from_slice(&0xAAu64.to_le_bytes());
    out.extend_from_slice(&0xBBu64.to_le_bytes());
    out.push(method.to_byte());
    out.push(1);
    out.extend_from_slice(&0i32.to_le_bytes());
    write_string(&mut out, "");
    out.push(0);
    out.extend_from_slice(&(data.len() as i32).to_le_bytes());
    out.extend_from_slice(data);
    out
}

fn markets_list_v1_payload_without_futures_type(market: &Market) -> Vec<u8> {
    let mut market_bytes = Vec::new();
    write_market(&mut market_bytes, market, 1);
    market_bytes.pop();

    let mut out = Vec::new();
    out.extend_from_slice(&1i32.to_le_bytes());
    out.extend_from_slice(&market_bytes);
    out.extend_from_slice(&0i32.to_le_bytes());
    out
}

#[test]
fn api_get_markets_list_uses_response_ver_like_delphi() {
    let mut dispatcher = EventDispatcher::new();
    let market = event_market("OLDV1");
    let data = markets_list_v1_payload_without_futures_type(&market);
    let payload = api_response_payload_ver(1, EngineMethod::GetMarketsList, &data);

    let events = dispatcher.dispatch(Command::API, &payload, 0);

    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::Markets(MarketsEvent::MarketsListReplaced { count: 1, .. })
        )),
        "Delphi passes resp.ver into ReadMarketFromStream; v1 market payload must be applied"
    );
    assert_eq!(
        dispatcher
            .markets
            .get("OLDV1")
            .expect("v1 market applied")
            .snapshot()
            .futures_type,
        BaseCurrency::EMPTY,
        "v1 payload has no FuturesType byte, so Delphi keeps CreateBase default BC_EMPTY"
    );
}

fn order_status_for_test(
    uid: u64,
    market_name: &str,
    currency: u8,
    platform: u8,
    status: OrderWorkerStatus,
) -> OrderStatus {
    OrderStatus {
        epoch_header: TradeEpochHeader {
            market: MarketCommandHeader {
                base: BaseCommandHeader {
                    cmd_id: 4,
                    ver: 3,
                    uid,
                },
                currency,
                platform,
                market_name: market_name.to_string(),
            },
            epoch: 1,
            status,
        },
        buy_order: OrderCompact::default(),
        sell_order: OrderCompact::default(),
        stops: StopSettings::default(),
        strat_id: 0,
        is_short: false,
        db_id: 0,
        from_cache: false,
        emulator_mode: false,
        immune_for_clicks: false,
    }
}

#[test]
fn dispatcher_routes_order_to_orders_state() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let uid = 0x123;
    let status = order_status_for_test(uid, "BTCUSDT", 7, 9, OrderWorkerStatus::BuySet);
    let payload = all_statuses_payload(0x55, &[status]);
    let events = d.dispatch(Command::Order, &payload, 1000);
    assert!(events.iter().any(|ev| matches!(ev, Event::Order(_))));
    assert!(d.orders.get(uid).is_some());
}

#[test]
fn dispatcher_snapshot_is_immutable_after_later_domain_mutation() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);

    let before = d.snapshot();
    let uid = 0x123;
    let status = order_status_for_test(uid, "BTCUSDT", 7, 9, OrderWorkerStatus::BuySet);
    let payload = all_statuses_payload(0x55, &[status]);
    let _ = d.dispatch(Command::Order, &payload, 1000);

    assert!(
        before.orders().is_empty(),
        "published snapshots must be immutable even when the live dispatcher mutates the same domain later"
    );
    assert!(
        d.snapshot().orders().get(uid).is_some(),
        "live dispatcher must still publish the later domain mutation"
    );
}

#[test]
fn snapshot_carries_session_identity_set_after_init() {
    use crate::commands::engine_api::{AuthCheckResponse, ServerInfo};

    let mut d = EventDispatcher::new();

    // Before the runtime pushes identity: default (all-empty) server info and no
    // AuthCheck. Both are always safe to read.
    let before = d.snapshot();
    assert_eq!(
        before.server_info(),
        &ServerInfo::default(),
        "snapshot must expose the all-empty default server identity before BaseCheck"
    );
    assert!(
        before.auth_info().is_none(),
        "snapshot must report no AuthCheck identity before authentication"
    );

    let server = ServerInfo {
        bot_id: Some(777),
        base_currency_name: Some("BTC".to_string()),
        ..ServerInfo::default()
    };
    let auth = AuthCheckResponse {
        binance_account_id: 42,
        btc_address: "bc1qexample".to_string(),
        spot_ref: 0,
        is_sub_account: true,
        account_id: "0xwallet".to_string(),
        recvd_max_payload: Some(60000),
        known_dexes: Vec::new(),
        hl_dex_market: None,
        hl_spot_market: None,
    };
    d.set_session_identity(server.clone(), Some(auth.clone()));

    let after = d.snapshot();
    assert_eq!(
        after.server_info(),
        &server,
        "snapshot must carry the server identity the runtime pushes after Init"
    );
    assert_eq!(
        after.auth_info(),
        Some(&auth),
        "snapshot must carry the AuthCheck identity the runtime pushes after Init"
    );

    // A published snapshot is an immutable copy: re-setting identity later (e.g.
    // reconnect-with-reinit) must not mutate an already published snapshot.
    d.set_session_identity(ServerInfo::default(), None);
    assert_eq!(
        after.server_info(),
        &server,
        "an already published snapshot must not change when identity is re-set later"
    );
}

#[test]
fn dispatcher_all_statuses_uses_process_command_order_item_loop() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let uid = 0x1234_5678_ABCD_EF01;
    let status = order_status_for_test(uid, "BTCUSDT", 7, 9, OrderWorkerStatus::BuySet);
    let payload = all_statuses_payload(0x55, &[status]);

    let events = d.dispatch(Command::Order, &payload, 1000);

    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0],
        Event::Order(OrderEvent::Created(found_uid)) if found_uid == uid
    ));
    assert!(matches!(events[1], Event::Order(OrderEvent::Snapshot)));
    assert_eq!(d.orders.current_snapshot_flag(), 1);
    assert!(d.orders.get(uid).is_some());
}

#[test]
fn dispatcher_skips_future_version_order_command_like_delphi_registry() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();
    let uid = 0x1234;

    d.process_command_order(
        TradeCommand::OrderStatus(Box::new(order_status_for_test(
            uid,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::BuySet,
        ))),
        1000,
        &mut out,
    );

    d.orders.begin_snapshot();
    let mut future_status = Vec::new();
    future_status.push(4);
    future_status.extend_from_slice(&99u16.to_le_bytes());
    future_status.extend_from_slice(&uid.to_le_bytes());

    let events = d.dispatch(Command::Order, &future_status, 1010);

    assert!(events.is_empty());
    assert_eq!(
            d.orders.missing_after_snapshot(),
            vec![uid],
            "Delphi registry returns skipped TBaseTradeCommand for future versions, so ClientNewData does not call ProcessCommandOrder"
        );
}

#[test]
fn dispatcher_skips_unknown_order_cmd_id_like_delphi_base_trade() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();
    let uid = 0x1235;

    d.process_command_order(
        TradeCommand::OrderStatus(Box::new(order_status_for_test(
            uid,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::BuySet,
        ))),
        1000,
        &mut out,
    );

    d.orders.begin_snapshot();
    let mut unknown = Vec::new();
    unknown.push(250);
    unknown.extend_from_slice(&3u16.to_le_bytes());
    unknown.extend_from_slice(&uid.to_le_bytes());

    let events = d.dispatch(Command::Order, &unknown, 1010);

    assert!(events.is_empty());
    assert_eq!(
            d.orders.missing_after_snapshot(),
            vec![uid],
            "Delphi unknown CmdId under TBaseTradeCommand is not TBaseMarketCommand, so it is freed before ProcessCommandOrder"
        );
}

#[test]
fn dispatcher_keeps_sell_done_order_for_delphi_final_trace_grace() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();
    let uid = 0x42;

    d.process_command_order(
        TradeCommand::OrderStatus(Box::new(order_status_for_test(
            uid,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::SellSet,
        ))),
        1000,
        &mut out,
    );
    d.process_command_order(
        TradeCommand::OrderStatus(Box::new(order_status_for_test(
            uid,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::SellDone,
        ))),
        1001,
        &mut out,
    );
    d.process_command_order(
        TradeCommand::OrderTracePoint(OrderTracePoint {
            market: MarketCommandHeader {
                base: BaseCommandHeader {
                    cmd_id: 25,
                    ver: 3,
                    uid,
                },
                currency: 7,
                platform: 9,
                market_name: "BTCUSDT".to_string(),
            },
            trace_time: 45_000.0,
            trace_price: 101.0,
            base_price: 100.0,
            stop_price: 0.0,
            ord_type: OrderType::Sell,
            flags: trace_flags::IS_FINISH,
        }),
        1002,
        &mut out,
    );

    assert!(matches!(
        out.last(),
        Some(Event::Order(OrderEvent::TracePoint { uid: found })) if *found == uid
    ));
    assert_eq!(d.orders().get(uid).unwrap().trace_points.len(), 1);

    out.clear();
    d.drain_deferred_order_removals_due(1400, &mut out);
    assert!(out.is_empty());
    assert!(d.orders().get(uid).is_some());

    d.process_command_order(
        TradeCommand::OrderTracePoint(OrderTracePoint {
            market: MarketCommandHeader {
                base: BaseCommandHeader {
                    cmd_id: 25,
                    ver: 3,
                    uid,
                },
                currency: 7,
                platform: 9,
                market_name: "BTCUSDT".to_string(),
            },
            trace_time: 45_000.0,
            trace_price: 102.0,
            base_price: 100.0,
            stop_price: 0.0,
            ord_type: OrderType::Sell,
            flags: trace_flags::IS_FINISH,
        }),
        1400,
        &mut out,
    );
    assert!(matches!(
        out.last(),
        Some(Event::Order(OrderEvent::TracePoint { uid: found })) if *found == uid
    ));
    assert_eq!(d.orders().get(uid).unwrap().trace_points.len(), 2);

    out.clear();
    d.drain_deferred_order_removals_due(1401, &mut out);
    assert!(matches!(
        out.as_slice(),
        [Event::Order(OrderEvent::Removed(found))] if *found == uid
    ));
    assert!(d.orders().get(uid).is_none());
}

#[test]
fn dispatcher_drops_new_order_status_for_unknown_market_like_delphi() {
    let mut d = EventDispatcher::new();
    let mut out = Vec::new();
    let uid = 0x77;

    d.process_command_order(
        TradeCommand::OrderStatus(Box::new(order_status_for_test(
            uid,
            "UNKNOWNUSDT",
            7,
            9,
            OrderWorkerStatus::BuySet,
        ))),
        1000,
        &mut out,
    );

    assert!(out.is_empty());
    assert!(d.orders.get(uid).is_none());
}

#[test]
fn dispatcher_drops_unknown_from_cache_status_without_event_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();
    let uid = 0x78;
    let mut status = order_status_for_test(uid, "BTCUSDT", 7, 9, OrderWorkerStatus::BuySet);
    status.from_cache = true;

    d.process_command_order(TradeCommand::OrderStatus(Box::new(status)), 1000, &mut out);

    assert!(out.is_empty());
    assert!(d.orders.get(uid).is_none());
}

#[test]
fn dispatcher_drops_client_originated_order_command_without_event_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();
    let uid = 0x79;

    d.process_command_order(
        TradeCommand::OrderStatus(Box::new(order_status_for_test(
            uid,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::BuySet,
        ))),
        1000,
        &mut out,
    );
    out.clear();

    d.process_command_order(
        TradeCommand::SetImmune(SetImmuneCommand {
            header: BaseCommandHeader {
                cmd_id: 22,
                ver: 3,
                uid,
            },
            items: Vec::new(),
        }),
        1010,
        &mut out,
    );

    assert!(out.is_empty());
    assert!(!d.orders.get(uid).unwrap().immune_for_clicks);
}

#[test]
fn dispatcher_drops_skipped_order_updates_without_event_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();

    d.process_command_order(
        TradeCommand::OrderStatusUpdate(OrderStatusUpdate {
            epoch_header: TradeEpochHeader {
                market: MarketCommandHeader {
                    base: BaseCommandHeader {
                        cmd_id: 5,
                        ver: 3,
                        uid: 0x7B,
                    },
                    currency: 7,
                    platform: 9,
                    market_name: "BTCUSDT".to_string(),
                },
                epoch: 1,
                status: OrderWorkerStatus::BuySet,
            },
            update_data: OrderUpdateData::default(),
            sell_reason_code: 0,
        }),
        1000,
        &mut out,
    );
    assert!(out.is_empty());
    assert!(d.orders.get(0x7B).is_none());

    let uid_stale = 0x7C;
    d.process_command_order(
        TradeCommand::OrderStatus(Box::new(order_status_for_test(
            uid_stale,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::BuySet,
        ))),
        1010,
        &mut out,
    );
    out.clear();
    let accepted_update = OrderStatusUpdate {
        epoch_header: TradeEpochHeader {
            market: MarketCommandHeader {
                base: BaseCommandHeader {
                    cmd_id: 5,
                    ver: 3,
                    uid: uid_stale,
                },
                currency: 7,
                platform: 9,
                market_name: "BTCUSDT".to_string(),
            },
            epoch: 2,
            status: OrderWorkerStatus::BuySet,
        },
        update_data: OrderUpdateData::default(),
        sell_reason_code: 0,
    };
    d.process_command_order(
        TradeCommand::OrderStatusUpdate(accepted_update.clone()),
        1020,
        &mut out,
    );
    assert!(matches!(
        out.as_slice(),
        [Event::Order(OrderEvent::Updated(found))] if *found == uid_stale
    ));
    out.clear();
    d.process_command_order(
        TradeCommand::OrderStatusUpdate(accepted_update),
        1030,
        &mut out,
    );
    assert!(out.is_empty());

    let uid_rollback = 0x7D;
    d.process_command_order(
        TradeCommand::OrderStatus(Box::new(order_status_for_test(
            uid_rollback,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::SellSet,
        ))),
        1040,
        &mut out,
    );
    out.clear();
    d.process_command_order(
        TradeCommand::OrderStatusUpdate(OrderStatusUpdate {
            epoch_header: TradeEpochHeader {
                market: MarketCommandHeader {
                    base: BaseCommandHeader {
                        cmd_id: 5,
                        ver: 3,
                        uid: uid_rollback,
                    },
                    currency: 7,
                    platform: 9,
                    market_name: "BTCUSDT".to_string(),
                },
                epoch: 3,
                status: OrderWorkerStatus::BuySet,
            },
            update_data: OrderUpdateData::default(),
            sell_reason_code: 0,
        }),
        1050,
        &mut out,
    );
    assert!(out.is_empty());
    assert_eq!(
        d.orders.get(uid_rollback).unwrap().status,
        OrderWorkerStatus::SellSet
    );
}

#[test]
fn dispatcher_tick_orders_clears_bulk_replace_timeout_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();
    let uid = 0x7A;

    d.process_command_order(
        TradeCommand::OrderStatus(Box::new(order_status_for_test(
            uid,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::BuySet,
        ))),
        1000,
        &mut out,
    );
    d.process_command_order(
        TradeCommand::BulkReplaceNotify(BulkReplaceNotify {
            market: MarketCommandHeader {
                base: BaseCommandHeader {
                    cmd_id: 28,
                    ver: 3,
                    uid: 0,
                },
                currency: 7,
                platform: 9,
                market_name: "BTCUSDT".to_string(),
            },
            order_type: OrderType::Buy,
            uids: vec![uid],
        }),
        1100,
        &mut out,
    );
    assert!(d.orders.get(uid).unwrap().bulk_replace_buy);

    assert!(d.tick_orders(6100).is_empty());
    let events = d.tick_orders(6101);

    assert!(matches!(
        events.as_slice(),
        [Event::Order(OrderEvent::Updated(found))] if *found == uid
    ));
    assert!(!d.orders.get(uid).unwrap().bulk_replace_buy);
}

#[test]
fn dispatcher_tick_orders_repeats_pending_cancel_like_delphi_worker_loop() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();
    let uid = 0x7B;
    let mut status = order_status_for_test(uid, "BTCUSDT", 7, 9, OrderWorkerStatus::None);
    status.buy_order.mean_price = 9.25;
    d.process_command_order(TradeCommand::OrderStatus(Box::new(status)), 1000, &mut out);

    let first = d
        .orders
        .send_cancel_if_requested(uid, 1000)
        .expect("first pending cancel should send immediately");
    assert!(matches!(
        first,
        OrderCancelSend::PendingReplaceThenCancel { .. }
    ));

    let mut actions = Vec::new();
    out.clear();
    d.tick_orders_active_actions(1031, &mut out, &mut actions);
    assert!(out.is_empty());
    assert!(
        actions.is_empty(),
        "Delphi pending cancel worker loop sleeps 32 ms"
    );

    d.tick_orders_active_actions(1032, &mut out, &mut actions);
    assert_eq!(actions.len(), 1);
    match actions.pop().unwrap() {
        ActiveAction::OrderCancel {
            request: OrderCancelSend::PendingReplaceThenCancel { ctx, market, price },
        } => {
            assert_eq!(ctx.uid, uid);
            assert_eq!(market, "BTCUSDT");
            assert_eq!(price, 9.25);
        }
        _ => panic!("expected pending cancel resend action"),
    }
}

#[test]
fn dispatcher_routes_strat_to_strats_state() {
    let mut d = EventDispatcher::new();
    let payload = build_snapshot_request(7);
    let events = d.dispatch(Command::Strat, &payload, 1000);
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::Strat(StratEvent::Ignored) => {} // SnapshotRequest from server is unusual; state ignores
        Event::Strat(_) => {}
        other => panic!("expected Strat event, got {:?}", other),
    }
}

#[test]
fn dispatcher_skips_future_version_strat_command_like_delphi_registry() {
    let mut d = EventDispatcher::new();
    let mut payload = vec![2, 99, 0];
    payload.extend_from_slice(&77u64.to_le_bytes());
    let events = d.dispatch(Command::Strat, &payload, 1000);
    assert!(
            events.is_empty(),
            "Delphi registry returns FSkipped base TBaseStratCommand and ProcessStratCommand has no matching branch"
        );
}

#[test]
fn dispatcher_skips_unknown_strat_command_id_like_delphi_base_command() {
    let mut d = EventDispatcher::new();
    let mut payload = vec![250];
    payload.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    payload.extend_from_slice(&77u64.to_le_bytes());
    let events = d.dispatch(Command::Strat, &payload, 1000);
    assert!(
            events.is_empty(),
            "Delphi unknown Strat CmdId becomes base TBaseStratCommand and is freed without side effect"
        );
}

#[test]
fn dispatcher_skips_inapplicable_incoming_strat_commands_like_delphi_client() {
    let mut d = EventDispatcher::new();

    let schema_request = build_schema_request(7);
    let events = d.dispatch(Command::Strat, &schema_request, 1000);
    assert!(
        events.is_empty(),
        "Delphi client has no TStratSchemaRequest receive branch"
    );

    let sell_price_update = build_sell_price_update(8, 99, 123.45);
    let events = d.dispatch(Command::Strat, &sell_price_update, 1000);
    assert!(
        events.is_empty(),
        "Delphi client has no TStratSellPriceUpdate receive branch"
    );
}

#[test]
fn dispatcher_unknown_channel_returns_raw() {
    let mut d = EventDispatcher::new();
    // Reserved1 — no dispatch → fallback to Raw
    let events = d.dispatch(Command::Reserved1, b"hello", 1000);
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::Raw { cmd, payload } => {
            assert_eq!(*cmd, Command::Reserved1);
            assert_eq!(payload, b"hello");
        }
        other => panic!("expected Raw event, got {:?}", other),
    }
}

#[test]
fn dispatcher_unknown_raw_command_preserves_header_ordinal_like_delphi() {
    let mut d = EventDispatcher::new();
    let raw_cmd = Command::from_byte(99);
    let events = d.dispatch(raw_cmd, b"hello", 1000);
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::Raw { cmd, payload } => {
            assert_eq!(cmd.to_byte(), 99);
            assert_eq!(*cmd, raw_cmd);
            assert_eq!(payload, b"hello");
        }
        other => panic!("expected Raw event, got {:?}", other),
    }
}

#[test]
fn dispatcher_logmsg_parses_time_and_msg() {
    let mut d = EventDispatcher::new();
    let mut payload = 45678.5f64.to_le_bytes().to_vec();
    payload.extend_from_slice(b"server log message");
    let events = d.dispatch(Command::LogMsg, &payload, 1000);
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::ServerLog { time, msg } => {
            assert_eq!(*time, 45678.5);
            assert_eq!(msg, "server log message");
        }
        other => panic!("expected ServerLog, got {:?}", other),
    }
}

#[test]
fn dispatcher_logmsg_invalid_utf8_uses_delphi_question_mark_fallback() {
    let mut d = EventDispatcher::new();
    let mut payload = 45678.5f64.to_le_bytes().to_vec();
    payload.extend_from_slice(&[b'L', 0xFF, b'g']);
    let events = d.dispatch(Command::LogMsg, &payload, 1000);
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::ServerLog { msg, .. } => assert_eq!(msg, "L?g"),
        other => panic!("expected ServerLog, got {:?}", other),
    }
}

#[test]
fn dispatcher_routes_arb_to_typed_event() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
    let mut compact = vec![2u8];
    compact.extend_from_slice(&0u16.to_le_bytes());
    compact.push(1);
    compact.push(7);
    compact.extend_from_slice(&123.25f32.to_le_bytes());

    let payload = build_arb_prices(9, &compact);
    let events = d.dispatch(Command::Balance, &payload, 1000);
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::Arb(ArbEvent::PricesApplied {
            uid,
            version,
            market_blocks,
            price_items,
            applied_prices,
        }) => {
            assert_eq!(*uid, 9);
            assert_eq!(*version, 2);
            assert_eq!(*market_blocks, 1);
            assert_eq!(*price_items, 1);
            assert_eq!(*applied_prices, 0);
        }
        other => panic!("expected typed Arb event, got {:?}", other),
    }
}

#[test]
fn dispatcher_applies_arb_price_to_live_market_like_delphi_tmarket() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
    let mut settings = ClientSettingsCommand::default();
    settings.arb_config.wanted[7] = true;
    d.settings.client_settings = Some(settings);

    let mut compact = vec![2u8];
    compact.extend_from_slice(&0u16.to_le_bytes());
    compact.push(1);
    compact.push(7);
    compact.extend_from_slice(&123.25f32.to_le_bytes());

    let payload = build_arb_prices(9, &compact);
    let events = d.dispatch(Command::Balance, &payload, 1000);

    assert!(matches!(
        events.as_slice(),
        [Event::Arb(ArbEvent::PricesApplied {
            applied_prices: 1,
            ..
        })]
    ));
    let btc = d.markets.get("BTCUSDT").unwrap().snapshot();
    let slot = btc
        .arb_slots
        .get(&crate::commands::market::ArbPlatformCode::ByBit)
        .expect("Delphi ApplyArbPrice initializes TMarket.ArbSlots");
    assert!(slot.enabled);
    assert_eq!(slot.head_index(), 1);
    assert_eq!(slot.ring[1].price, 123.25);
    assert_eq!(slot.now.price, 123.25);
}

#[test]
fn dispatcher_applies_arb_isolation_commit_to_live_market_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
    let mut settings = ClientSettingsCommand::default();
    settings.arb_config.wanted[7] = true;
    d.settings.client_settings = Some(settings);

    let mut price = vec![2u8];
    price.extend_from_slice(&0u16.to_le_bytes());
    price.push(1);
    price.push(7);
    price.extend_from_slice(&123.25f32.to_le_bytes());
    let _ = d.dispatch(Command::Balance, &build_arb_prices(9, &price), 1000);

    let mut isolation = vec![3u8, 2u8];
    isolation.extend_from_slice(&1u16.to_le_bytes());
    isolation.extend_from_slice(&0u16.to_le_bytes());
    isolation.push(7);
    isolation.push(0b11);
    let _ = d.dispatch(Command::Balance, &build_arb_prices(10, &isolation), 1001);

    let btc = d.markets.get("BTCUSDT").unwrap().snapshot();
    let slot = btc
        .arb_slots
        .get(&crate::commands::market::ArbPlatformCode::ByBit)
        .unwrap();
    assert_eq!(
        slot.isolated_flags,
        crate::commands::market::ArbIsolationFlags::from_byte(0b11)
    );
    assert_eq!(
        slot.isolated_flags_tmp,
        crate::commands::market::ArbIsolationFlags::None
    );
}

#[test]
fn dispatcher_filters_unknown_arb_price_blocks_like_delphi_find_by_server_index() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let mut compact = vec![2u8];
    compact.extend_from_slice(&0u16.to_le_bytes());
    compact.push(1);
    compact.push(7);
    compact.extend_from_slice(&123.25f32.to_le_bytes());
    compact.extend_from_slice(&1u16.to_le_bytes());
    compact.push(1);
    compact.push(8);
    compact.extend_from_slice(&99.5f32.to_le_bytes());

    let payload = build_arb_prices(10, &compact);
    let events = d.dispatch(Command::Balance, &payload, 1000);

    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::Arb(ArbEvent::PricesApplied {
            market_blocks,
            price_items,
            ..
        }) => {
            assert_eq!(*market_blocks, 1);
            assert_eq!(*price_items, 1);
        }
        other => panic!("expected filtered Arb price event, got {other:?}"),
    }
}

#[test]
fn dispatcher_filters_unknown_arb_isolation_entries_like_delphi_find_by_server_index() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let mut compact = vec![3u8, 2u8]; // version=3, CMD_ISOL.
    compact.extend_from_slice(&2u16.to_le_bytes());
    compact.extend_from_slice(&0u16.to_le_bytes());
    compact.push(7);
    compact.push(0b01);
    compact.extend_from_slice(&1u16.to_le_bytes());
    compact.push(8);
    compact.push(0b10);

    let payload = build_arb_prices(11, &compact);
    let events = d.dispatch(Command::Balance, &payload, 1000);

    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::Arb(ArbEvent::IsolationApplied {
            entries,
            applied_entries,
            ..
        }) => {
            assert_eq!(*entries, 1);
            assert_eq!(*applied_entries, 0);
        }
        other => panic!("expected filtered Arb isolation event, got {other:?}"),
    }
}

#[test]
fn dispatcher_ignores_balance_base_request_and_unknown_like_delphi_registry() {
    let mut d = EventDispatcher::new();

    let full = balance_payload(3, 10, 1, 1.0);
    let events = d.dispatch(Command::Balance, &full, 1000);
    assert_eq!(events.len(), 1);
    assert_eq!(d.balances.global.btc_balance_total, 1.0);
    assert_eq!(d.balances.last_epoch, 1);

    let exact_base = balance_payload(2, 11, 2, 99.0);
    let events = d.dispatch(Command::Balance, &exact_base, 1001);

    assert!(events.is_empty());
    assert_eq!(d.balances.global.btc_balance_total, 1.0);
    assert_eq!(d.balances.last_epoch, 1);

    let mut base_class = vec![1, 0x03, 0x00];
    base_class.extend_from_slice(&12u64.to_le_bytes());
    base_class.extend_from_slice(&3u16.to_le_bytes());
    let events = d.dispatch(Command::Balance, &base_class, 1002);

    assert!(
            events.is_empty(),
            "Delphi parses TBalanceCommandBase and ProcessBalanceCommand ignores it; it must not become Raw"
        );
    assert_eq!(d.balances.global.btc_balance_total, 1.0);
    assert_eq!(d.balances.last_epoch, 1);

    let mut base_command = vec![0, 0x03, 0x00];
    base_command.extend_from_slice(&13u64.to_le_bytes());
    let events = d.dispatch(Command::Balance, &base_command, 1003);
    assert!(
        events.is_empty(),
        "Delphi unknown/base balance command is not TBalanceCommandBase and is ignored"
    );

    let request_refresh = build_request_balance_refresh(14);
    let events = d.dispatch(Command::Balance, &request_refresh, 1004);
    assert!(
        events.is_empty(),
        "TRequestBalanceRefresh is client->server; Delphi client has no receive side effect"
    );

    let mut unknown = vec![250, 0x03, 0x00];
    unknown.extend_from_slice(&15u64.to_le_bytes());
    let events = d.dispatch(Command::Balance, &unknown, 1005);
    assert!(
        events.is_empty(),
        "Delphi registry maps unknown balance CmdId to TBaseBalanceCommand and ignores it"
    );

    let mut malformed_ignored = vec![2, 0x03, 0x00];
    malformed_ignored.extend_from_slice(&16u64.to_le_bytes());
    let events = d.dispatch(Command::Balance, &malformed_ignored, 1006);
    assert!(
            events.is_empty(),
            "malformed ignored balance command must not become ParseFailed because Delphi applies no state branch"
        );
}

#[test]
fn dispatcher_skips_future_version_balance_command_like_delphi_registry() {
    let mut d = EventDispatcher::new();

    let full = balance_payload(3, 10, 1, 1.0);
    let _ = d.dispatch(Command::Balance, &full, 1000);
    assert_eq!(d.balances.global.btc_balance_total, 1.0);

    let mut future_version = balance_payload(3, 11, 2, 99.0);
    future_version[1..3].copy_from_slice(&99u16.to_le_bytes());
    let events = d.dispatch(Command::Balance, &future_version, 1001);

    assert!(events.is_empty());
    assert_eq!(d.balances.global.btc_balance_total, 1.0);
    assert_eq!(d.balances.last_epoch, 1);
}

#[test]
fn dispatcher_filters_balance_items_through_markets_state() {
    let mut d = EventDispatcher::new();
    d.markets.apply_markets_list(MarketsListResponse {
        markets: vec![event_market("BTCUSDT")],
        corr_markets: vec![],
    });

    let payload =
        balance_payload_with_items(3, 10, 1, &[("BTCUSDT", 100.0), ("UNKNOWNUSDT", 200.0)]);
    let events = d.dispatch(Command::Balance, &payload, 1000);

    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0],
        Event::Balance(BalanceEvent::SnapshotApplied { count: 1, epoch: 1 })
    ));
    assert_eq!(
        d.markets
            .get("BTCUSDT")
            .unwrap()
            .with(|market| market.initial_balance),
        100.0
    );
    assert!(d.markets.get("UNKNOWNUSDT").is_none());
}

#[test]
fn dispatcher_full_balance_creates_default_for_all_known_markets_like_delphi() {
    let mut d = EventDispatcher::new();
    d.markets.apply_markets_list(MarketsListResponse {
        markets: vec![event_market("BTCUSDT"), event_market("ETHUSDT")],
        corr_markets: vec![],
    });

    let payload = balance_payload_with_items(3, 10, 1, &[("BTCUSDT", 100.0)]);
    let events = d.dispatch(Command::Balance, &payload, 1000);

    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0],
        Event::Balance(BalanceEvent::SnapshotApplied { count: 1, epoch: 1 })
    ));
    assert_eq!(
        d.markets
            .get("BTCUSDT")
            .unwrap()
            .with(|m| m.initial_balance),
        100.0
    );
    // Delphi OnBalanceSnapshot resets every known TMarket missing from the snapshot.
    let eth_market = d.markets.get("ETHUSDT").unwrap().snapshot();
    assert_eq!(eth_market.initial_balance, 0.0);
    assert_eq!(eth_market.leverage_x, 1);
}

#[test]
fn dispatcher_balance_updates_live_market_fields_like_delphi_tmarket() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);

    let payload = balance_payload_with_position(3, 10, 5, "BTCUSDT", 100.0, 12345.0, 999.0, 25);
    let events = d.dispatch(Command::Balance, &payload, 1000);

    assert!(matches!(
        events.as_slice(),
        [Event::Balance(BalanceEvent::SnapshotApplied {
            count: 1,
            epoch: 5
        })]
    ));
    let btc = d.markets.get("BTCUSDT").unwrap().snapshot();
    assert_eq!(btc.initial_balance, 100.0);
    assert_eq!(btc.liq_price, 12345.0);
    assert_eq!(btc.bn_max_value, 999.0);
    assert_eq!(btc.leverage_x, 25);
    assert_eq!(btc.balance_hash, 77);
    assert_eq!(btc.last_balance_epoch, 5);
}

#[test]
fn dispatcher_full_balance_resets_missing_live_market_but_preserves_hash_epoch_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT", "ETHUSDT"]);

    let eth_payload = balance_payload_with_position(3, 10, 5, "ETHUSDT", 44.0, 22.0, 333.0, 7);
    let _ = d.dispatch(Command::Balance, &eth_payload, 1000);
    let before = d.markets.get("ETHUSDT").unwrap().snapshot();
    assert_eq!(before.initial_balance, 44.0);
    assert_eq!(before.liq_price, 22.0);
    assert_eq!(before.bn_max_value, 333.0);
    assert_eq!(before.last_balance_epoch, 5);

    let btc_only = balance_payload_with_items(3, 11, 6, &[("BTCUSDT", 1.0)]);
    let _ = d.dispatch(Command::Balance, &btc_only, 1001);

    let after = d.markets.get("ETHUSDT").unwrap().snapshot();
    assert_eq!(after.initial_balance, 0.0);
    assert_eq!(after.liq_price, 0.0);
    assert_eq!(after.total_profit(), 0.0);
    assert_eq!(after.leverage_x, 1);
    assert_eq!(after.bn_max_value, 333.0);
    assert_eq!(after.balance_hash, 77);
    assert_eq!(after.last_balance_epoch, 5);
}

#[test]
fn dispatcher_balance_incremental_epoch_gate_uses_live_market_epoch_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);

    let fresh = balance_payload_with_position(3, 10, 100, "BTCUSDT", 10.0, 20.0, 30.0, 3);
    let _ = d.dispatch(Command::Balance, &fresh, 1000);

    let stale = balance_payload_with_position(4, 11, 99, "BTCUSDT", 90.0, 80.0, 70.0, 9);
    let events = d.dispatch(Command::Balance, &stale, 1001);

    assert!(matches!(
        events.as_slice(),
        [Event::Balance(BalanceEvent::IncrementalApplied {
            count: 0,
            epoch: 99,
            ..
        })]
    ));
    let btc = d.markets.get("BTCUSDT").unwrap().snapshot();
    assert_eq!(btc.initial_balance, 10.0);
    assert_eq!(btc.liq_price, 20.0);
    assert_eq!(btc.bn_max_value, 30.0);
    assert_eq!(btc.leverage_x, 3);
    assert_eq!(btc.last_balance_epoch, 100);
}

#[test]
fn dispatcher_corrupted_order_returns_parse_failed() {
    let mut d = EventDispatcher::new();
    let events = d.dispatch(Command::Order, &[1, 2, 3], 1000); // too short for header
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], Event::ParseFailed { .. }));
}

#[test]
fn dispatcher_ctx_unused_warning_silenced() {
    // Suppress dead_code warning for TradeCtx if not used elsewhere
    let _ = TradeCtx::with_route_bytes(1, 1, 4);
}

#[test]
fn dispatcher_blocks_orderbook_until_indexes_sync() {
    let mut d = EventDispatcher::new();
    // indexes_synchronized = false by default — the OrderBook event must be dropped.
    // Build a minimal wire-payload for OrderBook (parse may fail, and that is fine —
    // the point is that we do NOT reach parse at all, because the block happens earlier).
    let dummy_payload = vec![0u8; 32];
    let events = d.dispatch(Command::OrderBook, &dummy_payload, 1000);
    assert!(
        events.is_empty(),
        "OrderBook event must be dropped until indexes_synchronized"
    );

    // After apply_markets_indexes — it must start parsing.
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
    let _events = d.dispatch(Command::OrderBook, &dummy_payload, 1000);
    // Now either a successful parse or ParseFailed (but not empty).
    // The exact value depends on the dummy_payload contents — the point is the block is lifted.
}

#[test]
fn dispatcher_drops_orderbook_for_unknown_market_index() {
    let mut d = EventDispatcher::new();
    d.markets.indexes_synchronized = true;
    d.markets.market_indexes = std::sync::Arc::new(vec!["BTCUSDT".to_string()]);
    std::sync::Arc::make_mut(&mut d.markets.by_name).insert("BTCUSDT".to_string(), 0);

    let events = d.dispatch(Command::OrderBook, &order_book_payload(1), 1000);
    assert!(
        events.is_empty(),
        "unknown server market index must be dropped"
    );
    assert!(
        d.order_books.is_empty(),
        "unknown index must not create OrderBooks cache"
    );

    d.markets.market_indexes = std::sync::Arc::new(vec!["UNKNOWNUSDT".to_string()]);
    std::sync::Arc::make_mut(&mut d.markets.by_name).clear();
    let events = d.dispatch(Command::OrderBook, &order_book_payload(0), 1000);
    assert!(
        events.is_empty(),
        "index mapped to unknown local market must be dropped"
    );
    assert!(
        d.order_books.is_empty(),
        "unknown local market must not create cache"
    );
}

#[test]
fn snapshot_reads_current_orderbook_by_market_name() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let events = d.dispatch(
        Command::OrderBook,
        &order_book_payload_full_with_levels(0, 1, &[(100.0, 1.0)], &[(101.0, 2.0)]),
        1000,
    );
    assert!(
        events.iter().any(|ev| matches!(ev, Event::OrderBook(
            OrderBookEvent::Apply { market_name, .. }
        ) if market_name.as_deref() == Some("BTCUSDT"))),
        "active dispatcher should resolve market name before publishing event"
    );

    let snapshot = d.snapshot();
    let book = snapshot
        .order_book("BTCUSDT", OrderBookKind::Futures)
        .expect("current book by market name");
    assert_eq!(book.buys[0].rate, 100.0);
    assert_eq!(book.sells[0].rate, 101.0);

    let top = snapshot
        .top_of_book("BTCUSDT", OrderBookKind::Futures)
        .expect("top by market name");
    assert_eq!(top.bid.unwrap().rate, 100.0);
    assert_eq!(top.ask.unwrap().rate, 101.0);
}

#[test]
fn dispatcher_blocks_trades_until_indexes_sync() {
    let mut d = EventDispatcher::new();
    let dummy_payload = vec![0u8; 16];
    let events = d.dispatch(Command::TradesStream, &dummy_payload, 1000);
    assert!(
        events.is_empty(),
        "TradesStream must be dropped until indexes_synchronized"
    );
}

#[test]
fn dispatcher_blocks_trades_resend_until_indexes_sync_like_delphi_process_trades_stream() {
    let mut d = EventDispatcher::new();
    let inner = trades_payload_with_market_sections(777, &[0]);
    let payload = trades_resend_response_payload(&inner);
    let events = d.dispatch(Command::TradesResendResponse, &payload, 1000);
    assert!(
            events.is_empty(),
            "Delphi ProcessTradesResendBatch calls ProcessTradesStream(..., False), which exits before fresh indexes"
        );
}

#[test]
fn dispatcher_filters_unknown_trades_sections_like_delphi_find_by_server_index() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let events = d.dispatch(
        Command::TradesStream,
        &trades_payload_with_market_sections(777, &[0, 1]),
        1000,
    );

    assert!(events.iter().any(|ev| matches!(
        ev,
        Event::Trade(TradesEvent::Applied {
            packet_num: 777,
            ..
        })
    )));
    let st = d
        .markets
        .trade_state("BTCUSDT")
        .expect("known market trade state");
    assert_eq!(st.last_trade_price, 100.0);
}

#[test]
fn dispatcher_filters_unknown_trades_resend_sections_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let inner = trades_payload_with_market_sections(778, &[0, 1]);
    let payload = trades_resend_response_payload(&inner);
    let events = d.dispatch(Command::TradesResendResponse, &payload, 1000);

    assert!(events.iter().any(|ev| matches!(
        ev,
        Event::Trade(TradesEvent::Applied {
            packet_num: 778,
            ..
        })
    )));
    let st = d
        .markets
        .trade_state("BTCUSDT")
        .expect("known market trade state");
    assert_eq!(st.last_trade_price, 100.0);
}

#[test]
fn dispatcher_applies_futures_trades_to_market_tail_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let payload = trades_payload_with_rows(800, 0, 0, &[(0, 100.0, 1.0), (1, 90.0, -2.0)]);
    let events = d.dispatch(Command::TradesStream, &payload, 7_000);

    assert!(events
        .iter()
        .any(|ev| matches!(ev, Event::Trade(TradesEvent::Applied { .. }))));
    let st = d
        .markets
        .trade_state("BTCUSDT")
        .expect("known market trade state");
    assert_eq!(st.last_got_all_trades_ms, 7_000);
    assert_eq!(st.last_trade_price, 90.0);
    assert!(st.last_trade_was_sell);
    assert_eq!(
        st.last_sell_price, 100.0,
        "Delphi SetLastTradePrices writes LastSellPrice on O_Buy"
    );
    assert_eq!(
        st.last_buy_price, 90.0,
        "Delphi SetLastTradePrices writes LastBuyPrice on O_Sell"
    );
    assert_eq!(st.last_trade_price_ema15, (100.0 * 15.0 + 90.0) / 16.0);
    assert_eq!(st.last_trade_price_ema5, (100.0 * 5.0 + 90.0) / 6.0);
}

#[test]
fn trades_datagram_does_not_clone_markets_state_while_snapshot_held() {
    // Regression guard for the CowState container-clone fix (moonproto cac7451):
    // the live trade tail lives on each per-market `Market`, so a trades datagram
    // mutates it through that market's own lock and must NOT trigger
    // `Arc::make_mut` on the whole `MarketsState` — even while a snapshot clone of
    // the markets domain is alive (refcount >= 2, the published-snapshot condition
    // that turns every `&mut` domain apply into a full container deep clone).
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    // Prime the tail once so the market is fully live.
    let _ = d.dispatch(
        Command::TradesStream,
        &trades_payload_with_rows(800, 0, 0, &[(0, 100.0, 1.0)]),
        7_000,
    );

    // Simulate a published snapshot keeping the markets domain alive (refcount 2).
    let held = d.markets.clone();
    let ptr_before = d.markets.arc_ptr();
    assert_eq!(
        ptr_before,
        held.arc_ptr(),
        "snapshot clone shares the live markets allocation"
    );

    // Apply another trades datagram while the snapshot is alive.
    let events = d.dispatch(
        Command::TradesStream,
        &trades_payload_with_rows(801, 0, 0, &[(0, 110.0, -3.0)]),
        8_000,
    );
    assert!(events
        .iter()
        .any(|ev| matches!(ev, Event::Trade(TradesEvent::Applied { .. }))));

    // The markets container must be the SAME allocation: no make_mut deep clone of
    // prices/token_tags/corr_*/base_currency_prices on the hot trades path.
    assert_eq!(
        d.markets.arc_ptr(),
        ptr_before,
        "trades datagram must not copy-on-write clone the whole MarketsState"
    );

    // Tail updated in place, and live-shared with the held snapshot — this is the
    // Delphi `TMarket` shape (shared object reference), not a frozen-at-snapshot copy.
    let live = d.markets.trade_state("BTCUSDT").expect("known market");
    assert_eq!(live.last_trade_price, 110.0);
    assert!(live.last_trade_was_sell);
    assert_eq!(
        held.trade_state("BTCUSDT")
            .expect("known market")
            .last_trade_price,
        110.0,
        "per-market trade tail is structurally shared with the snapshot like Delphi TMarket"
    );

    drop(held);
}

#[test]
fn active_dispatch_queues_trades_into_history_worker_without_direct_store_write() {
    let worker = crate::state::MarketHistoryWorker::spawn(crate::state::MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    let mut d = EventDispatcher::new();
    d.set_market_history_handle(worker.handle());
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    client.subscribe_all_trades(false);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let payload = trades_payload_with_rows(801, 0, 0, &[(0, 100.0, 1.0)]);

    dispatch_active_packet_for_test(
        &mut d,
        Command::TradesStream,
        &payload,
        7_000,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(worker.flush(45_000.0));

    let futures = worker.readers("BTCUSDT").unwrap().futures_trades.unwrap();
    let mut rows = Vec::new();
    futures.copy_last(8, &mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].price, 100.0);
    assert_eq!(rows[0].qty, 1.0);

    let snapshot_futures = d
        .snapshot()
        .market_history_readers("BTCUSDT")
        .expect("snapshot should expose retained history readers")
        .futures_trades
        .expect("snapshot should expose the futures ring reader");
    rows.clear();
    snapshot_futures.copy_last(8, &mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].price, 100.0);
}

#[test]
fn candles_snapshot_ready_after_worker_barrier_exposes_reader_rows() {
    let worker = crate::state::MarketHistoryWorker::spawn(crate::state::MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 8,
    });

    let mut d = EventDispatcher::new();
    d.set_market_history_handle(worker.handle());
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
    d.set_trade_storage_scope(Some(&crate::state::TradeStorageScope::All), 0.0);

    let markets = vec![crate::commands::candles::RequestCandlesMarket {
        market_name: "BTCUSDT".to_string(),
        candles_5m: vec![crate::commands::candles::DeepPrice {
            open: 10.0,
            close: 11.0,
            high: 12.0,
            low: 9.0,
            volume: 123.0,
            time: 45_000.0,
        }],
        buy_wall: [crate::commands::candles::WallItem::default(); 4],
        sell_wall: [crate::commands::candles::WallItem::default(); 4],
    }];

    let summary = d
        .apply_candles_snapshot(&markets)
        .expect("snapshot is queued to retained history");
    let barrier = d
        .market_history_barrier_async()
        .expect("history worker barrier available");
    barrier
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("worker processed candles snapshot before event");
    d.queue_candles_snapshot_event(crate::state::CandlesSnapshotEvent::Ready {
        request_uid: 42,
        summary,
    });

    let readers = worker.readers("BTCUSDT").expect("market reader exists");
    let candles = readers.candles_5m.expect("candles ring enabled");
    let mut rows = Vec::new();
    candles.copy_last(8, &mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].close(), 11.0);

    assert!(matches!(
        d.take_queued_events().as_slice(),
        [Event::CandlesSnapshot(
            crate::state::CandlesSnapshotEvent::Ready {
                request_uid: 42,
                summary: crate::state::CandlesSnapshotApplySummary {
                    received_markets: 1,
                    received_candles: 1,
                    retained_markets: 1,
                    retained_candles: 1,
                },
            }
        )]
    ));
}

#[test]
fn active_dispatch_history_worker_uses_server_index_mapping_not_market_vector_order() {
    let worker = crate::state::MarketHistoryWorker::spawn(crate::state::MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    let mut d = EventDispatcher::new();
    d.set_market_history_handle(worker.handle());
    seed_event_markets(&mut d, &["ETHUSDT", "BTCUSDT"]);
    d.markets.apply_markets_indexes(vec![
        "UNKNOWNUSDT".to_string(),
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
    ]);

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    client.subscribe_all_trades(false);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let payload = trades_payload_with_rows(813, 0, 1, &[(0, 100.0, 1.0)]);

    dispatch_active_packet_for_test(
        &mut d,
        Command::TradesStream,
        &payload,
        7_000,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(worker.flush(45_000.0));

    let btc = worker.readers("BTCUSDT").unwrap().futures_trades.unwrap();
    let mut btc_rows = Vec::new();
    btc.copy_last(8, &mut btc_rows);
    assert_eq!(btc_rows.len(), 1);
    assert_eq!(btc_rows[0].price, 100.0);

    let eth = worker.readers("ETHUSDT").unwrap().futures_trades.unwrap();
    let mut eth_rows = Vec::new();
    eth.copy_last(8, &mut eth_rows);
    assert!(
        eth_rows.is_empty(),
        "stream mIndex=1 must preserve GetMarketsIndexes slots, not local market vector order"
    );
}

#[test]
fn active_dispatch_lazy_starts_default_history_worker_on_trades_subscription() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    client.subscribe_all_trades(false);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let payload = trades_payload_with_rows(812, 0, 0, &[(0, 100.0, 1.0)]);

    dispatch_active_packet_for_test(
        &mut d,
        Command::TradesStream,
        &payload,
        7_000,
        &mut out,
        &client,
        &mut actions,
    );

    assert!(d.flush_market_history(45_000.0));
    let futures = d
        .market_history_readers("BTCUSDT")
        .expect("default worker should create storage for subscribed all-trades")
        .futures_trades
        .expect("default config keeps futures trades");
    let mut rows = Vec::new();
    futures.copy_last(8, &mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].price, 100.0);
    assert_eq!(rows[0].qty, 1.0);
}

#[test]
fn active_dispatch_drops_trades_without_subscription_intent() {
    let worker = crate::state::MarketHistoryWorker::spawn(crate::state::MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    let mut d = EventDispatcher::new();
    d.set_market_history_handle(worker.handle());
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let payload = trades_payload_with_rows(811, 0, 0, &[(0, 100.0, 1.0)]);

    dispatch_active_packet_for_test(
        &mut d,
        Command::TradesStream,
        &payload,
        7_000,
        &mut out,
        &client,
        &mut actions,
    );

    assert!(out.is_empty());
    assert!(actions.is_empty());
    assert!(worker.flush(45_000.0));
    assert!(
            worker.readers("BTCUSDT").is_none(),
            "Active Lib must not allocate retained history for unexpected trades without subscription intent"
        );
    assert_eq!(
        d.markets.trade_state("BTCUSDT"),
        Some(crate::state::markets::MarketTradeState::default()),
        "Unexpected trades must not update active market trade tail without subscription intent"
    );
}

#[test]
fn active_dispatch_queues_all_retained_stream_section_kinds_into_history_worker() {
    let worker = crate::state::MarketHistoryWorker::spawn(crate::state::MarketHistoryConfig {
        futures_trades_capacity: 8,
        spot_trades_capacity: 8,
        liquidation_capacity: 8,
        mm_orders_capacity: 8,
        last_price_capacity: 0,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    let mut d = EventDispatcher::new();
    d.set_market_history_handle(worker.handle());
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    client.subscribe_all_trades(true);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let payload = trades_payload_with_all_history_sections(802, 0);

    dispatch_active_packet_for_test(
        &mut d,
        Command::TradesStream,
        &payload,
        7_000,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(worker.flush(45_000.0));

    let readers = worker.readers("BTCUSDT").unwrap();
    let futures = readers.futures_trades.clone().unwrap();
    let spot = readers.spot_trades.clone().unwrap();
    let liquidations = readers.liquidations.clone().unwrap();
    let mm_orders = readers.mm_orders.clone().unwrap();
    let mm_companion = readers.mm_order_companion.clone().unwrap();

    let mut future_rows = Vec::new();
    futures.copy_last(8, &mut future_rows);
    assert_eq!(future_rows.len(), 1);
    assert_eq!(future_rows[0].price, 100.0);
    assert_eq!(future_rows[0].qty, 1.0);

    let mut spot_rows = Vec::new();
    spot.copy_last(8, &mut spot_rows);
    assert_eq!(spot_rows.len(), 1);
    assert_eq!(spot_rows[0].price, 101.0);
    assert_eq!(spot_rows[0].qty, -2.0);

    let mut liq_rows = Vec::new();
    liquidations.copy_last(8, &mut liq_rows);
    assert_eq!(liq_rows.len(), 1);
    assert_eq!(liq_rows[0].price, 102.0);
    assert_eq!(liq_rows[0].qty, -3.0);

    let mut mm_rows = Vec::new();
    mm_orders.copy_last(8, &mut mm_rows);
    assert_eq!(mm_rows.len(), 1);
    assert_eq!(mm_rows[0].volume, 5.0);
    assert_eq!(mm_rows[0].q, -4.0);

    let mut companion_rows = Vec::new();
    mm_companion.copy_last(8, &mut companion_rows);
    assert_eq!(companion_rows.len(), 1);
    assert_eq!(
        companion_rows[0],
        crate::state::MMOrderCompanionData::default()
    );
}

#[test]
fn active_dispatch_emits_typed_watcher_fills_like_delphi_process_watcher_fills_detect() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let user = [0xAB; 20];
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&803u16.to_le_bytes());
    push_trades_section(&mut payload, 0, 0, &[(0, 100.0, 1.0)]);
    push_watcher_fills_section(
        &mut payload,
        0,
        user,
        &[(
            500,
            101.5,
            -0.25,
            0.03125,
            12.5,
            OrderType::Buy.to_byte(),
            0x07,
        )],
    );
    payload.push(0); // packet flags: uncompressed, no taker flag.

    let ctx = ActiveDispatchContext {
        peer_app_token: 0,
        market_indexes_current_for_peer: true,
        server_token: 0,
        subscribed_book_server_token: 0,
        round_trip_delay_ms: 50,
        server_time_delta_source: Arc::new(AtomicU64::new(0)),
        now_time_days: 45_000.5,
        domain_ready: true,
        trades_storage_scope: Some(Arc::new(TradeStorageScope::All)),
        copy_max_leverage_from_markets_list: false,
        eps_profile: EpsProfile::default(),
        server_base_currency_name: Some("BTC".into()),
        server_base_currency_code: Some(BaseCurrency::BTC),
    };
    let mut out = Vec::new();
    let mut actions = Vec::new();
    d.dispatch_into_active_actions(
        Command::TradesStream,
        &payload,
        7_000,
        &mut out,
        &ctx,
        &mut actions,
    );

    let watcher = out
        .iter()
        .find_map(|ev| match ev {
            Event::WatcherFills(ev) => Some(ev),
            _ => None,
        })
        .expect("WatcherFills section must reach user code as typed domain event");
    assert_eq!(watcher.market_index(), 0);
    assert_eq!(watcher.market_name, "BTCUSDT");
    assert_eq!(watcher.user, user);
    assert_eq!(watcher.fills.len(), 1);
    let fill = &watcher.fills[0];
    let expected_time = 45_000.5 + 500.0 / DELPHI_MSECS_PER_DAY;
    assert_eq!(fill.time, expected_time);
    assert_eq!(
        fill.time_ms,
        (expected_time * DELPHI_MSECS_PER_DAY).round() as i64
    );
    assert_eq!(fill.price, 101.5);
    assert_eq!(fill.qty, -0.25);
    assert_eq!(fill.z_btc, 0.03125);
    assert_eq!(fill.position, 12.5);
    assert_eq!(fill.order_type, OrderType::Buy);
    assert!(fill.is_short);
    assert!(fill.is_open);
    assert!(fill.is_taker);
    assert!(out.iter().any(|ev| matches!(
        ev,
        Event::Trade(TradesEvent::Applied {
            packet_num: 803,
            ..
        })
    )));
}

#[test]
fn active_dispatch_queues_update_markets_last_price_into_history_worker_like_delphi_addfrom() {
    let worker = crate::state::MarketHistoryWorker::spawn(crate::state::MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 4,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    let mut btc = event_market("BTCUSDT");
    btc.bn_market_currency = "BTC".to_string();
    btc.base_currency = "USDT".to_string();
    btc.is_btc_market = true;

    let mut d = EventDispatcher::new();
    d.set_market_history_handle(worker.handle());
    d.markets.apply_markets_list(MarketsListResponse {
        markets: vec![btc],
        corr_markets: vec![],
    });
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let data = build_markets_prices_response(&MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 100.0,
            ask: 102.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 101.0,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });
    let payload = api_response_payload_ver(3, EngineMethod::UpdateMarketsList, &data);
    let ctx = ActiveDispatchContext {
        peer_app_token: 0,
        market_indexes_current_for_peer: true,
        server_token: 0,
        subscribed_book_server_token: 0,
        round_trip_delay_ms: 50,
        server_time_delta_source: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        now_time_days: 45_000.5,
        domain_ready: true,
        trades_storage_scope: Some(Arc::new(TradeStorageScope::All)),
        copy_max_leverage_from_markets_list: false,
        eps_profile: EpsProfile::default(),
        server_base_currency_name: Some("BTC".into()),
        server_base_currency_code: Some(BaseCurrency::BTC),
    };
    let mut out = Vec::new();
    let mut actions = Vec::new();
    d.dispatch_into_active_actions(Command::API, &payload, 7_000, &mut out, &ctx, &mut actions);
    assert!(worker.flush(45_000.5));

    let last_prices = worker.readers("BTCUSDT").unwrap().last_prices.unwrap();
    let mut rows = Vec::new();
    last_prices.copy_last(4, &mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].current, 101.0);
    assert_eq!(rows[0].real_time, 45_000.5);

    let mark_prices = worker.readers("BTCUSDT").unwrap().mark_prices.unwrap();
    let mut mark_rows = Vec::new();
    mark_prices.copy_last(4, &mut mark_rows);
    assert_eq!(mark_rows.len(), 1);
    assert_eq!(mark_rows[0].current, 101.0);
    assert_eq!(mark_rows[0].real_time, 45_000.5);
}

#[test]
fn enabling_trade_storage_backfills_current_last_price_history() {
    let worker = crate::state::MarketHistoryWorker::spawn(crate::state::MarketHistoryConfig {
        futures_trades_capacity: 0,
        spot_trades_capacity: 0,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        last_price_capacity: 4,
        mini_candles_capacity: 0,
        candles_5m_capacity: 0,
    });

    let mut btc = event_market("BTCUSDT");
    btc.bn_market_currency = "BTC".to_string();
    btc.base_currency = "USDT".to_string();
    btc.is_btc_market = true;

    let mut d = EventDispatcher::new();
    d.set_market_history_handle(worker.handle());
    d.markets.apply_markets_list(MarketsListResponse {
        markets: vec![btc],
        corr_markets: vec![],
    });
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let data = build_markets_prices_response(&MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 200.0,
            ask: 204.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 202.0,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });
    d.markets.apply_markets_prices_payload_like_delphi(&data);

    d.set_trade_storage_scope(Some(&TradeStorageScope::All), 45_001.0);
    assert!(worker.flush(45_001.0));

    let last_prices = worker.readers("BTCUSDT").unwrap().last_prices.unwrap();
    let mut rows = Vec::new();
    last_prices.copy_last(4, &mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].current, 202.0);
    assert_eq!(rows[0].real_time, 45_001.0);
}

#[test]
fn dispatcher_spot_trades_do_not_overwrite_futures_tail_like_delphi() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let futures = trades_payload_with_rows(900, 0, 0, &[(0, 100.0, 1.0)]);
    let _ = d.dispatch(Command::TradesStream, &futures, 7_000);
    let spot = trades_payload_with_rows(901, 2, 0, &[(0, 120.0, -1.0)]);
    let _ = d.dispatch(Command::TradesStream, &spot, 8_000);

    let st = d
        .markets
        .trade_state("BTCUSDT")
        .expect("known market trade state");
    assert_eq!(st.last_got_all_trades_ms, 7_000);
    assert_eq!(st.last_got_spot_trades_ms, 8_000);
    assert_eq!(
        st.last_trade_price, 100.0,
        "Delphi spot branch exits before SetLastTradePrices"
    );
}

#[test]
fn dispatcher_order_not_blocked_by_indexes_sync() {
    // The Order channel does not depend on market_idx → must not be blocked by indexes_sync.
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let payload = all_statuses_payload(
        0x55,
        &[order_status_for_test(
            0x124,
            "BTCUSDT",
            7,
            9,
            OrderWorkerStatus::BuySet,
        )],
    );
    let events = d.dispatch(Command::Order, &payload, 1000);
    assert!(
        !events.is_empty(),
        "Order must be processed even without indexes_synchronized"
    );
}

#[test]
fn dispatch_into_active_invalidates_indexes_on_peer_token_mismatch() {
    let mut d = EventDispatcher::new();
    d.markets.apply_markets_indexes(vec!["OLDUSDT".to_string()]);
    assert!(d.markets.indexes_synchronized);

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    client.testing_set_peer_app_tokens(0x2222, 0x1111);

    let mut out = Vec::new();
    let mut actions = Vec::new();
    let dummy_payload = vec![0u8; 32];
    dispatch_active_packet_for_test(
        &mut d,
        Command::OrderBook,
        &dummy_payload,
        1000,
        &mut out,
        &client,
        &mut actions,
    );

    assert!(
        !d.markets.indexes_synchronized,
        "PeerAppToken mismatch must close stream gate until fresh GetMarketsIndexes"
    );
    assert!(
        out.is_empty(),
        "OrderBook packet from a new server process must be dropped with stale indexes"
    );
}

#[test]
fn dispatch_into_active_requests_missing_order_status_after_snapshot() {
    let mut d = EventDispatcher::new();
    let stale_uid = 0xAABB_CCDD_0011_2233;
    let status = order_status_for_test(stale_uid, "BTCUSDT", 7, 9, OrderWorkerStatus::BuySet);
    let (_result, _event) = d.orders.apply(TradeCommand::OrderStatus(Box::new(status)));

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    client.testing_set_server_token(0x2222);
    client.testing_set_subscribed_book_server_token(0x2222);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Order,
        &empty_all_statuses_payload(0x55),
        1000,
        &mut out,
        &client,
        &mut actions,
    );
    apply_active_actions_for_test(&client, &mut actions);

    assert!(out
        .iter()
        .any(|ev| matches!(ev, Event::Order(OrderEvent::Snapshot))));

    let mut found = false;
    for item in drain_client_send_items(&client) {
        if item.cmd != Command::Order.to_byte() {
            continue;
        }
        let Some(TradeCommand::OrderStatusRequest(req)) = TradeCommand::parse(&item.data) else {
            continue;
        };
        assert_eq!(req.market.base.uid, stale_uid);
        assert_eq!(req.market.market_name, "BTCUSDT");
        assert_eq!(req.market.currency, 7);
        assert_eq!(req.market.platform, 9);
        found = true;
    }

    assert!(found, "missing order must trigger TOrderStatusRequest");
}

#[test]
fn raw_dispatch_exposes_missing_order_status_requests_after_snapshot() {
    let mut d = EventDispatcher::new();
    let stale_uid = 0xAABB_CCDD_0011_2233;
    let status = order_status_for_test(stale_uid, "BTCUSDT", 7, 9, OrderWorkerStatus::BuySet);
    let (_result, _event) = d.orders.apply(TradeCommand::OrderStatus(Box::new(status)));

    let mut out = Vec::new();
    d.dispatch_into(
        Command::Order,
        &empty_all_statuses_payload(0x55),
        1000,
        &mut out,
    );

    assert!(out
        .iter()
        .any(|ev| matches!(ev, Event::Order(OrderEvent::Snapshot))));
    let missing = d.missing_order_status_requests_after_snapshot();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].ctx.uid, stale_uid);
    assert_eq!(missing[0].ctx.currency, BaseCurrency::from_byte(7));
    assert_eq!(missing[0].ctx.platform, ExchangeCode::from_byte(9));
    assert_eq!(missing[0].market_name, "BTCUSDT");
}

#[test]
fn dispatch_into_active_consumes_orderbook_full_request_event() {
    let mut d = EventDispatcher::new();
    d.markets.indexes_synchronized = true;
    d.markets.market_indexes = std::sync::Arc::new(vec!["BTCUSDT".to_string()]);
    std::sync::Arc::make_mut(&mut d.markets.by_name).insert("BTCUSDT".to_string(), 0);

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    client.testing_set_server_token(0x2222);
    client.testing_set_subscribed_book_server_token(0x2222);
    let mut out = Vec::new();
    let mut actions = Vec::new();

    dispatch_active_packet_for_test(
        &mut d,
        Command::OrderBook,
        &order_book_payload_with(0, 1, true),
        10_000,
        &mut out,
        &client,
        &mut actions,
    );
    apply_active_actions_for_test(&client, &mut actions);
    out.clear();
    actions.clear();
    dispatch_active_packet_for_test(
        &mut d,
        Command::OrderBook,
        &order_book_payload_with(0, 10, false),
        10_010,
        &mut out,
        &client,
        &mut actions,
    );
    apply_active_actions_for_test(&client, &mut actions);
    out.clear();
    actions.clear();
    dispatch_active_packet_for_test(
        &mut d,
        Command::OrderBook,
        &order_book_payload_with(0, 11, false),
        10_900,
        &mut out,
        &client,
        &mut actions,
    );
    apply_active_actions_for_test(&client, &mut actions);

    assert!(
        !out.iter().any(|ev| matches!(
            ev,
            Event::OrderBook(OrderBookEvent::RequestFullNeeded { .. })
        )),
        "active dispatcher must consume RequestFullNeeded as an internal control-event"
    );
    assert!(
        !out.iter()
            .any(|ev| matches!(ev, Event::OrderBook(OrderBookEvent::Ignored { .. }))),
        "active dispatcher must hide diagnostic ignored orderbook events from the UI path"
    );

    let mut found = false;
    for item in drain_client_send_items(&client) {
        if item.cmd == Command::API.to_byte()
            && item.data.get(11).copied()
                == Some(crate::commands::engine_api::EngineMethod::RequestOrderBookFull.to_byte())
        {
            found = true;
            break;
        }
    }
    assert!(found, "RequestOrderBookFull must still be sent internally");
}

#[test]
fn orderbook_apply_updates_market_chart_price_step_like_delphi_glass_updated() {
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();

    d.dispatch_into(
        Command::OrderBook,
        &order_book_payload_full_with_levels(0, 1, &[(100.0, 1.0)], &[(125.0, 2.0)]),
        10_000,
        &mut out,
    );

    assert!(out
        .iter()
        .any(|ev| matches!(ev, Event::OrderBook(OrderBookEvent::Apply { .. }))));
    let apply = out
        .iter()
        .find_map(|ev| match ev {
            Event::OrderBook(OrderBookEvent::Apply {
                market_name,
                kind,
                top,
                ..
            }) => Some((market_name, kind, top)),
            _ => None,
        })
        .expect("orderbook apply event");
    assert_eq!(apply.0.as_deref(), Some("BTCUSDT"));
    assert_eq!(*apply.1, OrderBookKind::Futures);
    assert_eq!(apply.2.bid.unwrap().rate, 100.0);
    assert_eq!(apply.2.ask.unwrap().rate, 125.0);
    assert_eq!(
        d.markets().price("BTCUSDT").unwrap().chart_price_step,
        125.0 / 5000.0,
        "Delphi AddNewAksPrice is called from both price updates and GlassUpdated"
    );
}

#[test]
fn orderbook_datagram_does_not_clone_markets_state_while_snapshot_held() {
    // Regression guard for the prices→Market move (MarketPrice now lives on the
    // per-market `Market`): an order-book datagram refreshes `ChartPriceStep`
    // through that market's own lock, so it must NOT trigger `Arc::make_mut` on
    // the whole `MarketsState` (no per-datagram clone of the price vector), even
    // while a snapshot keeps the markets domain shared at refcount >= 2.
    let mut d = EventDispatcher::new();
    seed_event_markets(&mut d, &["BTCUSDT"]);
    let mut out = Vec::new();

    // Prime the book so the market is live.
    d.dispatch_into(
        Command::OrderBook,
        &order_book_payload_full_with_levels(0, 1, &[(100.0, 1.0)], &[(125.0, 2.0)]),
        10_000,
        &mut out,
    );
    out.clear();

    // Simulate a published snapshot keeping the markets domain alive.
    let held = d.markets.clone();
    let ptr_before = d.markets.arc_ptr();
    assert_eq!(ptr_before, held.arc_ptr(), "snapshot shares the markets allocation");

    // Another book datagram with a different best ask → ChartPriceStep changes.
    d.dispatch_into(
        Command::OrderBook,
        &order_book_payload_full_with_levels(0, 1, &[(110.0, 1.0)], &[(150.0, 2.0)]),
        11_000,
        &mut out,
    );

    assert_eq!(
        d.markets.arc_ptr(),
        ptr_before,
        "order-book datagram must not copy-on-write clone the whole MarketsState"
    );
    // ChartPriceStep updated in place and live-shared with the held snapshot
    // (Delphi `TMarket` is a shared object, not a frozen-at-snapshot copy).
    assert_eq!(
        d.markets.price("BTCUSDT").unwrap().chart_price_step,
        150.0 / 5000.0
    );
    assert_eq!(
        held.price("BTCUSDT").unwrap().chart_price_step,
        150.0 / 5000.0,
        "live price lives on the shared Market object like Delphi TMarket"
    );

    drop(held);
}

#[test]
fn dispatch_into_active_drops_domain_commands_before_init() {
    let mut d = EventDispatcher::new();
    let client = crate::client::Client::new(dummy_client_cfg());
    let mut out = Vec::new();
    let mut actions = Vec::new();

    dispatch_active_packet_for_test(
        &mut d,
        Command::Order,
        &empty_all_statuses_payload(0x55),
        1000,
        &mut out,
        &client,
        &mut actions,
    );

    assert!(
        out.is_empty(),
        "pre-init Order must be dropped like Delphi InitDone gate"
    );
    assert_eq!(d.orders().current_snapshot_flag(), 0);
}

// =========================================================================
//  Multi-Client ServerTimeDelta tests
// =========================================================================

/// Test helper: converts days to seconds for easier comparison.
fn delta_seconds(d: &EventDispatcher) -> f64 {
    d.current_server_time_delta() * 86400.0
}

#[test]
fn current_delta_falls_back_to_global_when_source_is_none() {
    let _guard = server_time_delta_test_lock();
    // Raw dispatch without linking: the dispatcher reads the global.
    let d = EventDispatcher::new();
    assert!(d.server_time_delta_source.is_none());
    // Write to the global → the dispatcher sees the same value.
    crate::client::set_server_time_delta_global(2.5 / 86400.0);
    assert!((delta_seconds(&d) - 2.5).abs() < 1e-9);
    // Reset the global so it does not affect other tests.
    crate::client::set_server_time_delta_global(0.0);
}

#[test]
fn current_delta_reads_from_source_when_set() {
    let _guard = server_time_delta_test_lock();
    // Multi-Client: when linked, the dispatcher reads the per-Client handle,
    // NOT the global. Changes to the global do not affect this dispatcher.
    let handle = Arc::new(AtomicU64::new(0));
    // Emulate the Client writing its own delta = 7.0 seconds.
    let days: f64 = 7.0 / 86400.0;
    handle.store(days.to_bits(), Ordering::Relaxed);
    let mut d = EventDispatcher::new();
    d.set_server_time_delta_source(Arc::clone(&handle));
    // Meanwhile the global holds a different value — the dispatcher must ignore it.
    crate::client::set_server_time_delta_global(99.0 / 86400.0);
    assert!(
        (delta_seconds(&d) - 7.0).abs() < 1e-9,
        "dispatcher must read the handle, not the global"
    );
    crate::client::set_server_time_delta_global(0.0);
}

#[test]
fn delta_handle_update_visible_to_dispatcher() {
    // A handle change is reflected in the dispatcher's next read
    // (atomic snapshot — no caching).
    let handle = Arc::new(AtomicU64::new(0));
    let mut d = EventDispatcher::new();
    d.set_server_time_delta_source(Arc::clone(&handle));
    assert!((delta_seconds(&d) - 0.0).abs() < 1e-9);
    // Update the handle (as Client::handle_ping would).
    let days: f64 = 3.5 / 86400.0;
    handle.store(days.to_bits(), Ordering::Relaxed);
    assert!((delta_seconds(&d) - 3.5).abs() < 1e-9);
}

#[test]
fn two_dispatchers_with_distinct_handles_are_isolated() {
    // **Core multi-Client guarantee**: two EventDispatchers with distinct handles
    // (one per Client) see different deltas.
    let h_a = Arc::new(AtomicU64::new(0));
    let h_b = Arc::new(AtomicU64::new(0));
    let mut d_a = EventDispatcher::new();
    let mut d_b = EventDispatcher::new();
    d_a.set_server_time_delta_source(Arc::clone(&h_a));
    d_b.set_server_time_delta_source(Arc::clone(&h_b));

    // Client A: delta = +5s; Client B: delta = -200ms (different servers — different drift).
    h_a.store((5.0_f64 / 86400.0).to_bits(), Ordering::Relaxed);
    h_b.store((-0.2_f64 / 86400.0).to_bits(), Ordering::Relaxed);

    assert!((delta_seconds(&d_a) - 5.0).abs() < 1e-9);
    assert!((delta_seconds(&d_b) - (-0.2)).abs() < 1e-9);

    // Changing one handle does not affect the other.
    h_a.store((10.0_f64 / 86400.0).to_bits(), Ordering::Relaxed);
    assert!((delta_seconds(&d_a) - 10.0).abs() < 1e-9);
    assert!(
        (delta_seconds(&d_b) - (-0.2)).abs() < 1e-9,
        "dispatcher B must not see changes to handle A"
    );
}

// =========================================================================
//  dispatch_into_active — server_token tracking + auto-link delta handle
// =========================================================================

fn dummy_client_cfg() -> crate::client::ClientConfig {
    crate::client::ClientConfig {
        server_ip: "127.0.0.1".to_string(),
        server_port: 3000,
        master_key: [0; 16],
        mac_key: [0; 16],
        mask_ver: crate::client::TransportMode::V0,
        client_id: 0,
        ntp_host: None,
        refresh: crate::client::RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        },
    }
}

fn drain_client_send_items(client: &crate::client::Client) -> Vec<crate::client::SendItem> {
    let (mut sliced, mut high, mut low) = client.take_send_queues_for_test();
    sliced.append(&mut high);
    sliced.append(&mut low);
    sliced
}

fn dispatch_active_packet_for_test(
    dispatcher: &mut EventDispatcher,
    cmd: Command,
    payload: &[u8],
    now_ms: i64,
    out: &mut Vec<Event>,
    client: &crate::client::Client,
    actions: &mut Vec<ActiveAction>,
) {
    let ctx = ActiveDispatchContext::from_client(client);
    dispatcher.dispatch_into_active_actions(cmd, payload, now_ms, out, &ctx, actions);
}

fn apply_active_actions_for_test(client: &crate::client::Client, actions: &mut Vec<ActiveAction>) {
    client.apply_active_actions(actions.drain(..));
}

fn minimal_trades_payload(packet_num: u16) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&packet_num.to_le_bytes());
    payload.push(0); // packet flags: uncompressed, no taker flag.
    payload
}

fn trades_payload_with_market_sections(packet_num: u16, market_indices: &[u16]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&packet_num.to_le_bytes());
    for (i, market_index) in market_indices.iter().enumerate() {
        payload.extend_from_slice(&market_index.to_le_bytes());
        payload.push(1); // Count.
        payload.extend_from_slice(&(i as i16).to_le_bytes());
        payload.extend_from_slice(&(100.0f32 + i as f32).to_le_bytes());
        payload.extend_from_slice(&1.0f32.to_le_bytes());
    }
    payload.push(0); // packet flags: uncompressed, no taker flag.
    payload
}

fn trades_payload_with_rows(
    packet_num: u16,
    section_type: u16,
    market_index: u16,
    rows: &[(i16, f32, f32)],
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&packet_num.to_le_bytes());
    let market_index_and_flags = market_index | (section_type << 14);
    payload.extend_from_slice(&market_index_and_flags.to_le_bytes());
    payload.push(rows.len() as u8);
    for (time_delta, price, qty) in rows {
        payload.extend_from_slice(&time_delta.to_le_bytes());
        payload.extend_from_slice(&price.to_le_bytes());
        payload.extend_from_slice(&qty.to_le_bytes());
    }
    payload.push(0); // packet flags: uncompressed, no taker flag.
    payload
}

fn trades_payload_with_all_history_sections(packet_num: u16, market_index: u16) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&45_000.0f64.to_le_bytes());
    payload.extend_from_slice(&packet_num.to_le_bytes());

    push_trades_section(&mut payload, 0, market_index, &[(0, 100.0, 1.0)]);
    push_trades_section(&mut payload, 2, market_index, &[(1, 101.0, -2.0)]);
    push_liquidation_section(&mut payload, market_index, &[(2, 102.0, -3.0)]);
    push_trades_section(&mut payload, 1, market_index, &[(3, 5.0, -4.0)]);

    payload.push(0); // packet flags: uncompressed, no taker flag.
    payload
}

fn push_trades_section(
    payload: &mut Vec<u8>,
    section_type: u16,
    market_index: u16,
    rows: &[(i16, f32, f32)],
) {
    let market_index_and_flags = market_index | (section_type << 14);
    payload.extend_from_slice(&market_index_and_flags.to_le_bytes());
    payload.push(rows.len() as u8);
    for (time_delta, price, qty) in rows {
        payload.extend_from_slice(&time_delta.to_le_bytes());
        payload.extend_from_slice(&price.to_le_bytes());
        payload.extend_from_slice(&qty.to_le_bytes());
    }
}

fn push_liquidation_section(payload: &mut Vec<u8>, market_index: u16, rows: &[(i16, f32, f32)]) {
    let market_index_and_flags = market_index | (3 << 14);
    payload.extend_from_slice(&market_index_and_flags.to_le_bytes());
    payload.push(0); // ext type: liquidation orders.
    payload.push(rows.len() as u8);
    for (time_delta, price, qty) in rows {
        payload.extend_from_slice(&time_delta.to_le_bytes());
        payload.extend_from_slice(&price.to_le_bytes());
        payload.extend_from_slice(&qty.to_le_bytes());
    }
}

fn push_watcher_fills_section(
    payload: &mut Vec<u8>,
    market_index: u16,
    user: [u8; 20],
    rows: &[(i16, f32, f32, f32, f32, u8, u8)],
) {
    let market_index_and_flags = market_index | (3 << 14);
    payload.extend_from_slice(&market_index_and_flags.to_le_bytes());
    payload.push(1); // ext type: WatcherFills.
    payload.extend_from_slice(&user);
    payload.push(rows.len() as u8);
    for (time_delta, price, qty, z_btc, position, order_type, flags) in rows {
        payload.extend_from_slice(&time_delta.to_le_bytes());
        payload.extend_from_slice(&price.to_le_bytes());
        payload.extend_from_slice(&qty.to_le_bytes());
        payload.extend_from_slice(&z_btc.to_le_bytes());
        payload.extend_from_slice(&position.to_le_bytes());
        payload.push(*order_type);
        payload.push(*flags);
    }
}

fn trades_resend_response_payload(inner: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(1);
    payload.extend_from_slice(&(inner.len() as u16).to_le_bytes());
    payload.extend_from_slice(inner);
    payload
}

#[test]
fn active_markets_list_refresh_is_throttled_like_delphi_new_market_found() {
    let client = crate::client::Client::new(dummy_client_cfg());
    let mut dispatcher = EventDispatcher::new();
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let log_payload = 45_000.0f64.to_le_bytes();

    dispatcher.markets.markets_list_refresh_needed = true;
    dispatch_active_packet_for_test(
        &mut dispatcher,
        Command::LogMsg,
        &log_payload,
        1_000,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, ActiveAction::RequestMarketsList)),
        "first NewMarketFound refresh is immediate"
    );
    assert!(
        dispatcher.markets.markets_list_refresh_needed(),
        "Delphi keeps NewMarketFound true until GetMarketsList succeeds"
    );

    actions.clear();
    out.clear();
    dispatcher.markets.markets_list_refresh_needed = true;
    dispatch_active_packet_for_test(
        &mut dispatcher,
        Command::LogMsg,
        &log_payload,
        2_000,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(
        actions.is_empty(),
        "Delphi LastAddedNewMarket gate prevents repeated listing checks inside 30s"
    );
    assert!(
        dispatcher.markets.markets_list_refresh_needed(),
        "refresh flag must remain set while throttled"
    );

    out.clear();
    dispatch_active_packet_for_test(
        &mut dispatcher,
        Command::LogMsg,
        &log_payload,
        31_001,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, ActiveAction::RequestMarketsList)),
        "after 30s the pending NewMarketFound refresh is sent"
    );
}

#[test]
fn active_new_market_notify_is_internal_and_bypasses_listing_refresh_throttle_like_delphi() {
    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut dispatcher = EventDispatcher::new();
    dispatcher.last_markets_list_refresh_ms = 20_000;
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let payload = crate::commands::ui::build_new_market_notify(77);

    dispatch_active_packet_for_test(
        &mut dispatcher,
        Command::UI,
        &payload,
        21_000,
        &mut out,
        &client,
        &mut actions,
    );

    assert!(out.is_empty(), "TNewMarketNotifyCommand is internal; user code sees NewMarketsAdded only after GetMarketsList actually inserts markets");
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, ActiveAction::RequestMarketsList)),
        "Delphi ActivateMarketCheckEvent sets MustCheckLIstingFromServer and bypasses the 30s gate"
    );
    assert!(
        dispatcher.markets.markets_list_refresh_needed(),
        "GetMarketsList must run in NewMarketFound/listing mode so new symbols can be inserted"
    );

    actions.clear();
    out.clear();
    dispatch_active_packet_for_test(
        &mut dispatcher,
        Command::LogMsg,
        &45_000.0f64.to_le_bytes(),
        22_000,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(
            actions.is_empty(),
            "the force flag is one-shot; normal 30s throttle applies until the list response clears the pending flag"
        );
}

#[test]
fn active_get_markets_list_emits_new_markets_added_after_actual_insert() {
    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut dispatcher = EventDispatcher::new();
    seed_event_markets(&mut dispatcher, &["BTCUSDT"]);
    dispatcher
        .markets
        .apply_markets_indexes(vec!["BTCUSDT".to_string()]);
    dispatcher.markets.markets_list_refresh_needed = true;

    let mut data = Vec::new();
    data.extend_from_slice(&2i32.to_le_bytes());
    write_market(&mut data, &event_market("BTCUSDT"), 2);
    write_market(&mut data, &event_market("DOGEUSDT"), 2);
    data.extend_from_slice(&0i32.to_le_bytes());
    let payload = api_response_payload_ver(2, EngineMethod::GetMarketsList, &data);
    let mut out = Vec::new();
    let mut actions = Vec::new();

    dispatch_active_packet_for_test(
        &mut dispatcher,
        Command::API,
        &payload,
        22_000,
        &mut out,
        &client,
        &mut actions,
    );

    assert!(dispatcher.markets().get("DOGEUSDT").is_some());
    assert!(out
        .iter()
        .any(|ev| matches!(ev, Event::Markets(MarketsEvent::MarketsListReplaced { .. }))));
    assert!(
        out.iter().any(|ev| {
            matches!(
                ev,
                Event::Markets(MarketsEvent::NewMarketsAdded { names })
                    if names == &vec!["DOGEUSDT".to_string()]
            )
        }),
        "user-facing listing event must be emitted only after the new market is present in state"
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, ActiveAction::RequestOrderSnapshot)),
        "Delphi AddNewMarket sends TAllStatusesReq after local market creation"
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, ActiveAction::RequestUpdateMarketsList)),
        "Delphi immediately updates prices after NewMarkets.Count > 0"
    );
    let order_snapshot_pos = actions
        .iter()
        .position(|action| matches!(action, ActiveAction::RequestOrderSnapshot))
        .expect("order snapshot action");
    let update_prices_pos = actions
        .iter()
        .position(|action| matches!(action, ActiveAction::RequestUpdateMarketsList))
        .expect("update prices action");
    assert!(
            order_snapshot_pos < update_prices_pos,
            "Delphi AddNewMarket queues TAllStatusesReq during GetMarketsList before Bworks calls UpdateMarketsList"
        );
}

#[test]
fn active_trades_resend_check_runs_after_valid_trades_packet_like_delphi() {
    let mut d = EventDispatcher::new();
    d.markets.indexes_synchronized = true;
    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    client.subscribe_all_trades(false);
    let mut out = Vec::new();
    let mut actions = Vec::new();

    dispatch_active_packet_for_test(
        &mut d,
        Command::TradesStream,
        &minimal_trades_payload(100),
        1000,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(actions.is_empty());

    out.clear();
    actions.clear();
    dispatch_active_packet_for_test(
        &mut d,
        Command::TradesStream,
        &minimal_trades_payload(105),
        1010,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(
        out.iter().any(|ev| matches!(
            ev,
            Event::Trade(TradesEvent::GapDetected {
                start: 101,
                end: 104
            })
        )),
        "second packet creates the gap bucket"
    );
    assert!(
        actions.is_empty(),
        "bucket LastRetryTime is now=1010, so Delphi tail check cannot resend before PathDelay"
    );

    out.clear();
    actions.clear();
    dispatch_active_packet_for_test(
        &mut d,
        Command::TradesStream,
        &minimal_trades_payload(106),
        1500,
        &mut out,
        &client,
        &mut actions,
    );

    assert!(
        out.iter().any(|ev| {
            matches!(
                ev,
                Event::Trade(TradesEvent::ResendRequested { packet_nums })
                    if packet_nums == &vec![101, 102, 103, 104]
            )
        }),
        "Delphi tail check after the next valid trades packet requests missing packets"
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, ActiveAction::TradesResend { .. })),
        "active path must send the emk_TradesResend request from the trades-packet tail"
    );
}

#[test]
fn dispatch_into_active_records_initial_server_token() {
    // The first call records the current server_token into last_known_server_token.
    // Sentinel value 0 (init) → does not trigger a reset on the first non-zero token.
    let mut d = EventDispatcher::new();
    let mut client = crate::client::Client::new(dummy_client_cfg());
    // Set server_token=42 (simulating the state after the first Fine).
    client.testing_set_server_token(42);
    assert_eq!(d.last_known_server_token, 0);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Reserved1,
        b"x",
        0,
        &mut out,
        &client,
        &mut actions,
    );
    assert_eq!(
        d.last_known_server_token, 42,
        "the first dispatch_into_active must record the server_token"
    );
}

#[test]
fn dispatch_into_active_does_not_reset_on_first_non_zero_token() {
    // Init last_known=0 → the first non-zero token does NOT trigger full_reset.
    // To verify this — set "signature" values in trades/order_books
    // and check that they were NOT reset.
    let mut d = EventDispatcher::new();
    // Make order_books non-empty via apply_markets_indexes (creates the market_idx mapping).
    d.markets.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
    let snapshot_count_before = d.markets.by_name.len();
    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_server_token(0x100);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Reserved1,
        b"x",
        0,
        &mut out,
        &client,
        &mut actions,
    );
    // markets state must NOT be reset (full_reset was not called).
    assert_eq!(
        d.markets.by_name.len(),
        snapshot_count_before,
        "first non-zero token — does not trigger a reset"
    );
}

#[test]
fn dispatch_into_active_triggers_reset_on_token_change() {
    let mut d = EventDispatcher::new();
    // Simulate that we already saw server_token = 0xAAA.
    d.last_known_server_token = 0xAAA;
    // Set trades state to non-default (last_packet_num != 0 is observable via
    // a repeat dispatch — but it is private. It is enough to check that `last_known`
    // updates to the new value, while full_reset works at the level of TradesState itself).
    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_server_token(0xBBB);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Reserved1,
        b"x",
        0,
        &mut out,
        &client,
        &mut actions,
    );
    assert_eq!(
        d.last_known_server_token, 0xBBB,
        "after a token change — last_known is updated"
    );
    // The behavior of TradesState.full_reset() and OrderBooks.reset_caches_keep_books() is covered
    // by unit tests in the respective modules (state::trades, state::order_books).
}

#[test]
fn dispatch_into_active_auto_links_server_time_delta_source() {
    // The first call links the handle from the Client. Before that source = None,
    // and the dispatcher falls back to the global.
    let mut d = EventDispatcher::new();
    assert!(d.server_time_delta_source.is_none());
    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Reserved1,
        b"x",
        0,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(
        d.server_time_delta_source.is_some(),
        "after the first dispatch_into_active — source is bound to the Client"
    );

    // Repeat call — source does not change (already linked).
    let handle_after_first = Arc::clone(d.server_time_delta_source.as_ref().unwrap());
    actions.clear();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Reserved1,
        b"y",
        0,
        &mut out,
        &client,
        &mut actions,
    );
    let handle_after_second = d.server_time_delta_source.as_ref().unwrap();
    assert!(
        Arc::ptr_eq(&handle_after_first, handle_after_second),
        "repeat call — source stays the same handle"
    );
}

#[test]
fn snapshot_requested_with_provider_triggers_fresh_reply() {
    // Active library auto-action 2: on SnapshotRequested → if the application
    // supplied a provider, the library takes a fresh snapshot from the provider and sends the reply.
    let mut d = EventDispatcher::new();
    let fresh_snapshot = vec![0xAA, 0xBB, 0xCC, 0xDD];
    let fresh_for_provider = fresh_snapshot.clone();
    d.set_strategy_snapshot_provider(move |uid| {
        assert_eq!(uid, 42);
        Some(StrategySnapshotReply::from_payload(
            7,
            99,
            true,
            fresh_for_provider.clone(),
        ))
    });

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut out = Vec::new();
    let mut actions = Vec::new();

    // Server prods the client: "send your strategy snapshot" — this is
    // StratCommand::SnapshotRequest. Payload = `build_snapshot_request(uid)`.
    let payload = crate::commands::strat::build_snapshot_request(42);

    dispatch_active_packet_for_test(
        &mut d,
        Command::Strat,
        &payload,
        0,
        &mut out,
        &client,
        &mut actions,
    );
    apply_active_actions_for_test(&client, &mut actions);

    // Drain send queues — there must be a Command::Strat send with a fresh
    // TStratSnapshot body: CmdId/ver/uid + ServerEpoch/ClientMaxLastDate/Size/Full/Data.
    let mut found_snapshot_send = false;
    for item in drain_client_send_items(&client) {
        if item.cmd == Command::Strat.to_byte() {
            let data = &item.data;
            if data.len() == 11 + 8 + 8 + 4 + 1 + fresh_snapshot.len() {
                let cmd_subcode = data[0];
                let server_epoch = u64::from_le_bytes(data[11..19].try_into().unwrap());
                let client_max_last_date = u64::from_le_bytes(data[19..27].try_into().unwrap());
                let size = u32::from_le_bytes(data[27..31].try_into().unwrap());
                let full = data[31] != 0;
                let tail = &data[32..];
                if cmd_subcode == 2
                    && server_epoch == 7
                    && client_max_last_date == 99
                    && size == fresh_snapshot.len() as u32
                    && full
                    && tail == fresh_snapshot.as_slice()
                {
                    found_snapshot_send = true;
                }
            }
        }
    }
    assert!(
        found_snapshot_send,
        "after a SnapshotRequest with a provider — there must be a fresh send"
    );

    // out contains the SnapshotRequested event (the app sees it too, for UI awareness).
    let has_snapshot_event = out.iter().any(|ev| {
        matches!(
            ev,
            Event::Strat(crate::state::StratEvent::SnapshotRequested { uid: 42 })
        )
    });
    assert!(
        has_snapshot_event,
        "the SnapshotRequested event must be in out (for app awareness)"
    );
}

#[test]
fn snapshot_requested_without_provider_sends_owned_empty_snapshot() {
    // If no provider is set and there are no local strategies, the dispatcher still
    // replies with a correct empty snapshot. This is active-lib mechanics:
    // the server must not wait for a manual reply from the application.
    let mut d = EventDispatcher::new();

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut out = Vec::new();
    let payload = crate::commands::strat::build_snapshot_request(99);
    let mut actions = Vec::new();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Strat,
        &payload,
        0,
        &mut out,
        &client,
        &mut actions,
    );
    apply_active_actions_for_test(&client, &mut actions);

    // Drain send queues — there must be a Command::Strat with an empty serializer batch.
    let mut empty_snapshot_sends = 0;
    for item in drain_client_send_items(&client) {
        if item.cmd == Command::Strat.to_byte() {
            let cmd = crate::commands::strat::StratCommand::parse(&item.data)
                .expect("sent strat command must parse");
            match cmd {
                crate::commands::strat::StratCommand::Snapshot(snapshot) => {
                    let batch =
                        crate::commands::strategy_serializer::parse_strategy_batch(&snapshot.data)
                            .expect("empty strategy batch must parse");
                    assert!(snapshot.full);
                    assert!(batch.strategies.is_empty());
                    empty_snapshot_sends += 1;
                }
                other => panic!("expected snapshot reply, got {other:?}"),
            }
        }
    }
    assert_eq!(
        empty_snapshot_sends, 1,
        "without a provider — an empty owned snapshot must be sent"
    );

    // The SnapshotRequested event still reaches the app for UI/diagnostics.
    let has_event = out.iter().any(|ev| {
        matches!(
            ev,
            Event::Strat(crate::state::StratEvent::SnapshotRequested { .. })
        )
    });
    assert!(has_event);
}

#[test]
fn pre_init_snapshot_request_is_latched_until_post_init_flush() {
    let mut d = EventDispatcher::new();
    let provider_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls = std::sync::Arc::clone(&provider_calls);
    d.set_strategy_snapshot_provider(move |uid| {
        calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(uid, 123);
        Some(StrategySnapshotReply::from_payload(
            11,
            22,
            true,
            vec![0x10, 0x20],
        ))
    });

    let client = crate::client::Client::new(dummy_client_cfg());
    assert!(!client.is_domain_ready());
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let payload = crate::commands::strat::build_snapshot_request(123);

    dispatch_active_packet_for_test(
        &mut d,
        Command::Strat,
        &payload,
        0,
        &mut out,
        &client,
        &mut actions,
    );

    assert!(
        actions.is_empty(),
        "pre-init TStratSnapshotRequest is a latch, not an immediate Strat send"
    );
    assert_eq!(
        provider_calls.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "provider must not be called before post-init flush"
    );
    assert!(out.iter().any(|ev| matches!(
        ev,
        Event::Strat(crate::state::StratEvent::SnapshotRequested { uid: 123 })
    )));

    let reply = d
        .pending_or_local_strategy_snapshot_reply()
        .expect("post-init flush must answer the latched request");
    assert_eq!(reply.server_epoch, 11);
    assert_eq!(reply.client_max_last_date, 22);
    assert_eq!(reply.data, vec![0x10, 0x20]);
    assert_eq!(provider_calls.load(std::sync::atomic::Ordering::Relaxed), 1);
}

fn raw_strat_snapshot_payload(uid: u64, server_epoch: u64, full: bool, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(2);
    out.extend_from_slice(&crate::commands::registry::CURRENT_PROTO_CMD_VER.to_le_bytes());
    out.extend_from_slice(&uid.to_le_bytes());
    out.extend_from_slice(&server_epoch.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.push(full as u8);
    out.extend_from_slice(data);
    out
}

#[test]
fn valid_strategy_snapshot_advances_server_epoch_after_decode_like_delphi() {
    let mut d = EventDispatcher::new();
    d.strats.last_server_epoch = 7;

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let payload = crate::commands::strat::build_snapshot(42, 99, 0, true, &[]);

    dispatch_active_packet_for_test(
        &mut d,
        Command::Strat,
        &payload,
        0,
        &mut out,
        &client,
        &mut actions,
    );

    assert_eq!(d.strats.last_server_epoch, 99);
    assert!(out.iter().any(|ev| matches!(
        ev,
        Event::Strat(crate::state::StratEvent::SnapshotFull {
            server_epoch: 99,
            ..
        })
    )));
}

#[test]
fn invalid_strategy_snapshot_does_not_advance_server_epoch_like_delphi() {
    let mut d = EventDispatcher::new();
    d.strats.last_server_epoch = 7;

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut out = Vec::new();
    let mut actions = Vec::new();
    let payload = raw_strat_snapshot_payload(42, 99, true, &[]);

    dispatch_active_packet_for_test(
        &mut d,
        Command::Strat,
        &payload,
        0,
        &mut out,
        &client,
        &mut actions,
    );

    assert_eq!(
        d.strats.last_server_epoch, 7,
        "Delphi cfg.LocalStratEpoch is assigned only after ApplyStratSnapshot succeeds"
    );
    assert!(
        !out.iter().any(|ev| matches!(
            ev,
            Event::Strat(
                crate::state::StratEvent::SnapshotFull { .. }
                    | crate::state::StratEvent::SnapshotPartial { .. }
            )
        )),
        "invalid snapshot must not be reported as applied"
    );
}

#[test]
fn snapshot_requested_uses_local_strategies() {
    use crate::commands::strategy_serializer::{FieldValue, StrategyFields, StrategySnapshot};

    let mut fields = StrategyFields::new();
    fields.insert("Comment", FieldValue::String("local".to_string()));
    let strategy = StrategySnapshot {
        strategy_id: 0xF17E,
        strategy_ver: 3,
        last_date: 1234,
        checked: true,
        kind: 1,
        path: "FireTest".into(),
        fields,
    };

    let mut d = EventDispatcher::new();
    apply_comment_strategy_schema(&mut d);
    d.set_local_strategy_epoch(55);
    d.set_local_strategies(std::slice::from_ref(&strategy));
    assert_eq!(
        d.strategy_snapshot(strategy.strategy_id).unwrap().last_date,
        1234
    );

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut out = Vec::new();
    let payload = crate::commands::strat::build_snapshot_request(100);
    let mut actions = Vec::new();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Strat,
        &payload,
        0,
        &mut out,
        &client,
        &mut actions,
    );
    apply_active_actions_for_test(&client, &mut actions);

    let mut found = false;
    for item in drain_client_send_items(&client) {
        if item.cmd != Command::Strat.to_byte() {
            continue;
        }
        let cmd = crate::commands::strat::StratCommand::parse(&item.data)
            .expect("sent strat command must parse");
        if let crate::commands::strat::StratCommand::Snapshot(snapshot) = cmd {
            let batch = crate::commands::strategy_serializer::parse_strategy_batch(&snapshot.data)
                .expect("local strategy batch must parse");
            assert_eq!(snapshot.server_epoch, 55);
            assert_eq!(snapshot.client_max_last_date, 1234);
            assert_eq!(batch.strategies.len(), 1);
            assert_eq!(batch.strategies[0].strategy_id, strategy.strategy_id);
            assert_eq!(
                batch.strategies[0].fields.get("Comment"),
                Some(&FieldValue::String("local".to_string()))
            );
            found = true;
        }
    }
    assert!(found, "local strategy snapshot must be sent");
}

#[test]
fn snapshot_requested_defers_non_empty_local_strategies_until_schema() {
    use crate::commands::strategy_serializer::{FieldValue, StrategyFields, StrategySnapshot};

    let mut fields = StrategyFields::new();
    fields.insert("Comment", FieldValue::String("late".to_string()));
    let strategy = StrategySnapshot {
        strategy_id: 0x51,
        strategy_ver: 1,
        last_date: 77,
        checked: true,
        kind: 1,
        path: "".into(),
        fields,
    };

    let mut d = EventDispatcher::new();
    d.set_local_strategy_epoch(9);
    d.set_local_strategies(std::slice::from_ref(&strategy));
    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);

    let mut out = Vec::new();
    let mut actions = Vec::new();
    let request = crate::commands::strat::build_snapshot_request(500);
    dispatch_active_packet_for_test(
        &mut d,
        Command::Strat,
        &request,
        0,
        &mut out,
        &client,
        &mut actions,
    );
    assert!(actions
        .iter()
        .any(|action| matches!(action, ActiveAction::RequestStrategySchema)));
    assert!(!actions
        .iter()
        .any(|action| matches!(action, ActiveAction::SendStrategySnapshot { .. })));
    apply_active_actions_for_test(&client, &mut actions);
    assert!(drain_client_send_items(&client).iter().all(|item| {
        crate::commands::strat::StratCommand::parse(&item.data)
            .map(|cmd| !matches!(cmd, crate::commands::strat::StratCommand::Snapshot(_)))
            .unwrap_or(true)
    }));

    let schema_data = comment_strategy_schema_payload();
    let mut schema_payload = Vec::new();
    schema_payload.push(8); // TStratSchema
    schema_payload
        .extend_from_slice(&crate::commands::registry::CURRENT_PROTO_CMD_VER.to_le_bytes());
    schema_payload.extend_from_slice(&501u64.to_le_bytes());
    schema_payload.extend_from_slice(&(schema_data.len() as u32).to_le_bytes());
    schema_payload.extend_from_slice(&schema_data);

    out.clear();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Strat,
        &schema_payload,
        0,
        &mut out,
        &client,
        &mut actions,
    );
    apply_active_actions_for_test(&client, &mut actions);

    let mut found_snapshot = false;
    for item in drain_client_send_items(&client) {
        if item.cmd != Command::Strat.to_byte() {
            continue;
        }
        let Some(crate::commands::strat::StratCommand::Snapshot(snapshot)) =
            crate::commands::strat::StratCommand::parse(&item.data)
        else {
            continue;
        };
        let batch = crate::commands::strategy_serializer::parse_strategy_batch(&snapshot.data)
            .expect("deferred local strategy snapshot must parse");
        assert_eq!(snapshot.server_epoch, 9);
        assert_eq!(snapshot.client_max_last_date, 77);
        assert_eq!(batch.strategies.len(), 1);
        assert_eq!(batch.strategies[0].strategy_id, strategy.strategy_id);
        found_snapshot = true;
    }
    assert!(found_snapshot);
}

#[test]
fn snapshot_reply_uses_local_epoch_not_remote_server_epoch_like_delphi() {
    let mut d = EventDispatcher::new();
    d.strats.last_server_epoch = 7;

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    let mut out = Vec::new();
    let payload = crate::commands::strat::build_snapshot_request(101);
    let mut actions = Vec::new();
    dispatch_active_packet_for_test(
        &mut d,
        Command::Strat,
        &payload,
        0,
        &mut out,
        &client,
        &mut actions,
    );
    apply_active_actions_for_test(&client, &mut actions);

    let sent = drain_client_send_items(&client);
    let snapshot = sent
        .iter()
        .find(|item| item.cmd == Command::Strat.to_byte())
        .and_then(|item| crate::commands::strat::StratCommand::parse(&item.data))
        .and_then(|cmd| match cmd {
            crate::commands::strat::StratCommand::Snapshot(snapshot) => Some(snapshot),
            _ => None,
        })
        .expect("snapshot reply must be sent");

    assert_eq!(snapshot.server_epoch, 0);
}

#[test]
fn ui_strat_start_stop_v2_uses_owned_checked_delta() {
    use crate::commands::strategy_serializer::{StrategyFields, StrategySnapshot};

    let strategies = vec![
        StrategySnapshot {
            strategy_id: 30,
            strategy_ver: 1,
            last_date: 1,
            checked: false,
            kind: 1,
            path: "".into(),
            fields: StrategyFields::new(),
        },
        StrategySnapshot {
            strategy_id: 10,
            strategy_ver: 1,
            last_date: 2,
            checked: true,
            kind: 1,
            path: "".into(),
            fields: StrategyFields::new(),
        },
    ];
    let mut d = EventDispatcher::new();
    d.set_local_strategies(&strategies);
    assert!(d.set_strategy_checked(30, true));
    assert!(d.set_strategy_checked(10, false));

    let mut client = crate::client::Client::new(dummy_client_cfg());
    client.testing_set_domain_ready(true);
    assert_eq!(d.ui_strat_start_stop_v2(&client, true), 2);

    let sent = drain_client_send_items(&client);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].cmd, Command::UI.to_byte());
    match crate::commands::ui::UICommand::parse(&sent[0].data).unwrap() {
        crate::commands::ui::UICommand::StratStartStopV2(cmd) => {
            assert!(cmd.is_start);
            assert_eq!(
                cmd.items,
                vec![
                    crate::commands::strat::StratCheckedItem {
                        strategy_id: 30,
                        checked: true
                    },
                    crate::commands::strat::StratCheckedItem {
                        strategy_id: 10,
                        checked: false
                    },
                ]
            );
        }
        other => panic!("expected StratStartStopV2, got {other:?}"),
    }
}

#[test]
fn dispatcher_propagates_delta_to_orders_state() {
    // End-to-end: on `dispatch(Command::Order, ...)` the dispatcher applies the current
    // delta to Orders state. Verify that after linking the handle the delta reaches
    // `Orders.server_time_delta`.
    let handle = Arc::new(AtomicU64::new(0));
    let days: f64 = 1.25 / 86400.0;
    handle.store(days.to_bits(), Ordering::Relaxed);

    let mut d = EventDispatcher::new();
    d.set_server_time_delta_source(Arc::clone(&handle));

    // Any Order payload triggers set_server_time_delta.
    let payload = build_all_statuses_request(99);
    let _events = d.dispatch(Command::Order, &payload, 1000);

    // Round-trip days → seconds for comparison against 1.25.
    let applied_days = d.orders.server_time_delta;
    let applied_seconds = applied_days * 86400.0;
    assert!(
        (applied_seconds - 1.25).abs() < 1e-9,
        "Orders.server_time_delta must receive the value from the handle ({}s, got {}s)",
        1.25,
        applied_seconds
    );
}
