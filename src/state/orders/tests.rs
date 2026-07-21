use super::*;
use crate::commands::registry::CURRENT_PROTO_CMD_VER;

const SERVER_TOKEN: u64 = 11;
const APP_TOKEN: u64 = 22;

fn header(cmd_id: u8, uid: u64) -> BaseCommandHeader {
    BaseCommandHeader {
        cmd_id,
        ver: CURRENT_PROTO_CMD_VER,
        uid,
    }
}

fn desc(market: &str) -> OrderDescription {
    OrderDescription::for_test(market, false, false)
}

fn state(status: OrderWorkerStatus, tag: u8) -> CanonicalOrderState {
    let mut state = CanonicalOrderState::default();
    state.0[0] = status.to_byte();
    state.0[9] = tag;
    state.0[10] = tag & 7;
    state.0[11..19].copy_from_slice(&(100.0 + f64::from(tag)).to_le_bytes());
    state.0[19..27].copy_from_slice(&(5.0 + f64::from(tag)).to_le_bytes());
    state.0[333..341].copy_from_slice(&(200.0 + f64::from(tag)).to_le_bytes());
    state
}

fn put_i64(state: &mut CanonicalOrderState, offset: usize, value: i64) {
    state.0[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(state: &mut CanonicalOrderState, offset: usize, value: u64) {
    state.0[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn put_f64(state: &mut CanonicalOrderState, offset: usize, value: f64) {
    state.0[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn put_f32(state: &mut CanonicalOrderState, offset: usize, value: f32) {
    state.0[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn fill_leg(
    state: &mut CanonicalOrderState,
    exec: usize,
    placement: usize,
    slow: usize,
    seed: f64,
) {
    put_f64(state, exec, seed + 1.0);
    put_f64(state, exec + 8, seed + 2.0);
    put_f64(state, exec + 16, seed + 3.0);
    put_f64(state, exec + 24, seed + 4.0);
    state.0[exec + 32] = 5;

    put_i64(state, placement, seed as i64 + 10);
    put_f64(state, placement + 8, seed + 11.0);
    put_i64(state, placement + 16, 1_700_000_000_000 + seed as i64);
    put_f64(state, placement + 24, seed + 12.0);
    put_f64(state, placement + 32, seed + 13.0);
    put_i64(state, placement + 40, 1_700_000_100_000 + seed as i64);
    put_i64(state, placement + 48, 1_700_000_200_000 + seed as i64);
    state.0[placement + 56] = 14;
    state.0[placement + 57] = OrderType::BuyLimit.to_byte();
    state.0[placement + 58] = OrderSubType::ReduceOnly.to_byte();
    state.0[placement + 59] = 15;
    state.0[placement + 60] = 1;
    state.0[placement + 61] = 2;
    state.0[placement + 62] = 0;

    put_f64(state, slow, seed + 16.0);
    put_f64(state, slow + 8, seed + 17.0);
    put_f32(state, slow + 16, seed as f32 + 18.0);
}

fn image(uid: u64, rev: u64, desc: OrderDescription, state: CanonicalOrderState) -> TradeCommand {
    TradeCommand::OrderImage(OrderImage {
        header: header(41, uid),
        state_rev: rev,
        desc,
        section_mask: ORDER_SECTION_ALL_MASK,
        state,
    })
}

fn patch(uid: u64, rev: u64, hash: u32, mask: u16, state: CanonicalOrderState) -> TradeCommand {
    TradeCommand::OrderPatch(OrderPatch {
        header: header(42, uid),
        state_rev: rev,
        state_hash: hash,
        section_mask: mask,
        state,
    })
}

fn apply(
    orders: &mut OrderState,
    command: TradeCommand,
    app_token: u64,
    market_exists: bool,
) -> (Vec<OrderEvent>, Vec<OrderRepair>) {
    let mut events = Vec::new();
    let mut repairs = Vec::new();
    orders.apply_protocol(
        command,
        1_000,
        SERVER_TOKEN,
        app_token,
        0.0,
        &|_| market_exists,
        &mut events,
        &mut repairs,
    );
    (events, repairs)
}

fn market_header(cmd_id: u8, uid: u64, market_name: &str) -> MarketCommandHeader {
    MarketCommandHeader {
        base: header(cmd_id, uid),
        currency: 0,
        platform: 0,
        market_name: market_name.to_owned(),
    }
}

#[test]
fn exact_image_creates_projection_without_repair() {
    let mut orders = OrderState::new();
    let (events, repairs) = apply(
        &mut orders,
        image(7, 3, desc("BTCUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        true,
    );
    assert!(repairs.is_empty());
    assert!(matches!(events.as_slice(), [OrderEvent::Created(order)] if order.uid == 7));
    assert_eq!(orders.get(7).unwrap().status, OrderWorkerStatus::BuySet);
    assert!(orders.mirrors[&7].is_exact());
}

#[test]
fn full_image_materializes_every_canonical_section() {
    let mut canonical = CanonicalOrderState::default();
    canonical.0[0] = OrderWorkerStatus::None.to_byte();
    put_u64(&mut canonical, 1, 0x1122_3344_5566_7788);
    canonical.0[9] = 14;
    canonical.0[10] = OFL_IMMUNE | OFL_PANIC_ON | OFL_PANIC_AUTO;
    put_f64(&mut canonical, 11, 101.25);
    put_f64(&mut canonical, 19, 2.5);
    canonical.0[27] = 1;
    put_f64(&mut canonical, 28, 111.5);
    canonical.0[36] = 1;
    fill_leg(&mut canonical, 37, 70, 133, 100.0);
    fill_leg(&mut canonical, 153, 186, 249, 200.0);

    let stops = StopSettings::disabled()
        .with_stop_loss_fixed(95.0, 0.25)
        .with_trailing_percent(1.5, 0.1)
        .with_take_profit_price(120.0);
    let mut stop_bytes = Vec::new();
    stops.write_to(&mut stop_bytes);
    canonical.0[269..315].copy_from_slice(&stop_bytes);
    canonical.0[315] = 1;
    canonical.0[316] = 1;
    put_f64(&mut canonical, 317, 97.5);
    put_f64(&mut canonical, 325, 0.75);
    put_f64(&mut canonical, 333, 118.0);
    canonical.0[341] = 1;

    let mut orders = OrderState::new();
    let (events, repairs) = apply(
        &mut orders,
        image(
            701,
            1,
            OrderDescription::for_test("BTCUSDT", true, true),
            canonical,
        ),
        APP_TOKEN,
        true,
    );
    assert!(repairs.is_empty());
    assert!(matches!(events.as_slice(), [OrderEvent::Created(order)] if order.uid == 701));

    let order = orders.get(701).unwrap();
    assert_eq!(order.status, OrderWorkerStatus::None);
    assert_eq!(order.strat_id, 0x1122_3344_5566_7788);
    assert_eq!(order.sell_reason, SellReason::TakeProfit);
    assert!(order.immune_for_clicks);
    assert!(order.panic_sell);
    assert!(order.panic_sell_auto);
    assert_eq!(order.pending_buy_cond_price, Some(101.25));
    assert_eq!(order.buy_size, 2.5);
    assert!(order.bulk_replace_buy);
    assert!(order.bulk_replace_sell);
    assert!(order.emulator_mode);
    assert!(order.is_short);
    assert!(!order.job_is_done);

    let buy = order.buy_order;
    assert_eq!(buy.quantity_remaining, 101.0);
    assert_eq!(buy.actual_q, 102.0);
    assert_eq!(buy.total_btc, 103.0);
    assert_eq!(buy.mean_price, 104.0);
    assert_eq!(buy.partial_done, 5);
    assert_eq!(buy.int_id, 110);
    assert_eq!(buy.actual_price, 111.0);
    assert_eq!(buy.open_time().unix_millis(), 1_700_000_000_100);
    assert_eq!(buy.quantity, 112.0);
    assert_eq!(buy.quantity_base, 113.0);
    assert_eq!(buy.close_time().unix_millis(), 1_700_000_100_100);
    assert_eq!(buy.create_time().unix_millis(), 1_700_000_200_100);
    assert_eq!(buy.stop_flag, 14);
    assert_eq!(buy.order_type, OrderType::BuyLimit);
    assert_eq!(buy.sub_type, OrderSubType::ReduceOnly);
    assert_eq!(buy.leverage, 15);
    assert!(buy.is_opened());
    assert!(buy.is_closed());
    assert!(!buy.canceled());
    assert!(buy.is_short());
    assert_eq!(buy.spent_btc, 116.0);
    assert_eq!(buy.tmp_btc, 117.0);
    assert_eq!(buy.panic_sell_down, 118.0);

    let sell = order.sell_order;
    assert_eq!(sell.quantity_remaining, 201.0);
    assert_eq!(sell.actual_q, 202.0);
    assert_eq!(sell.total_btc, 203.0);
    assert_eq!(sell.mean_price, 204.0);
    assert_eq!(sell.int_id, 210);
    assert_eq!(sell.actual_price, 211.0);
    assert_eq!(sell.open_time().unix_millis(), 1_700_000_000_200);
    assert_eq!(sell.spent_btc, 216.0);
    assert_eq!(sell.tmp_btc, 217.0);
    assert_eq!(sell.panic_sell_down, 218.0);
    assert!(sell.is_short());

    assert_eq!(order.buy_price, buy.actual_price);
    assert_eq!(order.sell_price, sell.actual_price);
    assert_eq!(order.stops, stops);
    assert!(order.vstop_on);
    assert!(order.vstop_fixed);
    assert_eq!(order.vstop_level, 97.5);
    assert_eq!(order.vstop_vol, 0.75);
    assert_eq!(order.planned_sell_price, 118.0);
    assert!(order.use_market_stop);
}

#[test]
fn short_description_materializes_direction_in_both_order_legs() {
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(
            70,
            1,
            OrderDescription::for_test("BTCUSDT", false, true),
            state(OrderWorkerStatus::BuySet, 1),
        ),
        APP_TOKEN,
        true,
    );
    let order = orders.get(70).unwrap();
    assert!(order.is_short);
    assert!(order.buy_order.is_short());
    assert!(order.sell_order.is_short());
}

#[test]
fn terminal_order_ignores_late_trace_and_corridor_packets() {
    let uid = 71;
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(
            uid,
            1,
            desc("BTCUSDT"),
            state(OrderWorkerStatus::SellDone, 1),
        ),
        APP_TOKEN,
        true,
    );

    let trace = TradeCommand::OrderTracePoint(OrderTracePoint {
        market: market_header(25, uid, "BTCUSDT"),
        trace_time: 45_000.0,
        trace_price: 101.0,
        base_price: 100.0,
        stop_price: 99.0,
        ord_type: OrderType::Sell,
        flags: trace_flags::IS_INITIAL,
    });
    let corridor = TradeCommand::CorridorUpdate(CorridorUpdate {
        market: market_header(26, uid, "BTCUSDT"),
        price_down: 95.0,
        price_up: 105.0,
    });

    let (trace_events, _) = apply(&mut orders, trace, APP_TOKEN, true);
    let (corridor_events, _) = apply(&mut orders, corridor, APP_TOKEN, true);
    let order = orders.get(uid).unwrap();

    assert!(trace_events.is_empty());
    assert!(corridor_events.is_empty());
    assert!(order.buy_trace_line.is_none());
    assert!(order.sell_trace_line.is_none());
    assert!(!order.is_moon_shot);
    assert_eq!(order.corridor_price_down, 0.0);
    assert_eq!(order.corridor_price_up, 0.0);
}

#[test]
fn sparse_image_zeroes_sections_not_carried_on_wire() {
    let original = OrderImage {
        header: header(41, 8),
        state_rev: 4,
        desc: desc("ETHUSDT"),
        section_mask: 1 << OSEC_PHASE,
        state: state(OrderWorkerStatus::SellSet, 9),
    };
    let mut bytes = Vec::new();
    original.write(&mut bytes);
    let parsed = TradeCommand::parse(&bytes).unwrap();
    let mut orders = OrderState::new();
    let (_, repairs) = apply(&mut orders, parsed, APP_TOKEN, true);
    assert!(repairs.is_empty());
    assert_eq!(orders.mirrors[&8].state.section(OSEC_FLAGS), [0, 0]);
    assert!(orders.mirrors[&8].is_exact());
}

#[test]
fn unknown_patch_requests_full_image_and_does_not_create() {
    let mut orders = OrderState::new();
    let (_, repairs) = apply(
        &mut orders,
        patch(
            9,
            1,
            123,
            1 << OSEC_FLAGS,
            state(OrderWorkerStatus::None, 2),
        ),
        APP_TOKEN,
        true,
    );
    assert!(orders.mirrors.is_empty());
    assert_eq!(
        repairs,
        [OrderRepair {
            order_id: 9,
            exact_rev: 0
        }]
    );
}

#[test]
fn torn_patch_requests_repair_then_promotes_when_all_sections_arrive() {
    let uid = 10;
    let description = desc("SOLUSDT");
    let initial = state(OrderWorkerStatus::BuySet, 1);
    let mut target = initial.clone();
    target.copy_section_from(&state(OrderWorkerStatus::BuySet, 6), OSEC_FLAGS);
    target.copy_section_from(&state(OrderWorkerStatus::SellSet, 6), OSEC_PHASE);
    let target_hash = state_hash(2, &description, &target);
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(uid, 1, description.clone(), initial),
        APP_TOKEN,
        true,
    );

    let (_, repairs) = apply(
        &mut orders,
        patch(uid, 2, target_hash, 1 << OSEC_FLAGS, target.clone()),
        APP_TOKEN,
        true,
    );
    assert_eq!(
        repairs,
        [OrderRepair {
            order_id: uid,
            exact_rev: 0
        }]
    );
    assert!(!orders.mirrors[&uid].is_exact());

    let remaining = ORDER_SECTION_ALL_MASK & !(1 << OSEC_FLAGS);
    let (_, repairs) = apply(
        &mut orders,
        patch(uid, 2, target_hash, remaining, target),
        APP_TOKEN,
        true,
    );
    assert!(repairs.is_empty());
    assert!(orders.mirrors[&uid].is_exact());
    assert_eq!(orders.get(uid).unwrap().status, OrderWorkerStatus::SellSet);
}

#[test]
fn stale_patch_cannot_roll_back_newer_sections() {
    let uid = 11;
    let description = desc("BTCUSDT");
    let newer = state(OrderWorkerStatus::SellSet, 5);
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(uid, 5, description, newer),
        APP_TOKEN,
        true,
    );
    let (_, repairs) = apply(
        &mut orders,
        patch(
            uid,
            4,
            0,
            ORDER_SECTION_ALL_MASK,
            state(OrderWorkerStatus::BuySet, 1),
        ),
        APP_TOKEN,
        true,
    );
    assert!(repairs.is_empty());
    assert_eq!(orders.get(uid).unwrap().status, OrderWorkerStatus::SellSet);
    assert_eq!(orders.mirrors[&uid].replica_rev(), 5);
}

#[test]
fn image_description_is_immutable() {
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(12, 1, desc("BTCUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        true,
    );
    let (events, repairs) = apply(
        &mut orders,
        image(12, 2, desc("ETHUSDT"), state(OrderWorkerStatus::SellSet, 2)),
        APP_TOKEN,
        true,
    );
    assert!(events.is_empty() && repairs.is_empty());
    assert_eq!(orders.get(12).unwrap().market_name, "BTCUSDT");
    assert_eq!(orders.get(12).unwrap().status, OrderWorkerStatus::BuySet);
}

#[test]
fn unknown_gone_creates_tombstone_and_blocks_late_image() {
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        TradeCommand::OrderNotFound(header(46, 13)),
        APP_TOKEN,
        true,
    );
    apply(
        &mut orders,
        image(13, 1, desc("BTCUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        true,
    );
    assert!(orders.get(13).is_none());
    assert!(orders.is_tombstoned(13));
}

#[test]
fn gone_ignores_terminal_state_even_before_exact_promotion() {
    let mut exact = OrderState::new();
    apply(
        &mut exact,
        image(
            14,
            1,
            desc("BTCUSDT"),
            state(OrderWorkerStatus::SellDone, 1),
        ),
        APP_TOKEN,
        true,
    );
    apply(
        &mut exact,
        TradeCommand::OrderNotFound(header(46, 14)),
        APP_TOKEN,
        true,
    );
    assert!(exact.mirrors.contains_key(&14));

    let mut torn = OrderState::new();
    let description = desc("BTCUSDT");
    apply(
        &mut torn,
        image(
            15,
            1,
            description.clone(),
            state(OrderWorkerStatus::BuySet, 1),
        ),
        APP_TOKEN,
        true,
    );
    let terminal = state(OrderWorkerStatus::SellDone, 2);
    apply(
        &mut torn,
        patch(
            15,
            2,
            state_hash(2, &description, &terminal),
            1 << OSEC_PHASE,
            terminal,
        ),
        APP_TOKEN,
        true,
    );
    assert!(!torn.mirrors[&15].is_exact());
    apply(
        &mut torn,
        TradeCommand::OrderNotFound(header(46, 15)),
        APP_TOKEN,
        true,
    );
    assert!(torn.mirrors.contains_key(&15));
    assert!(!torn.is_tombstoned(15));
}

#[test]
fn peer_app_token_changes_world_but_server_token_does_not() {
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(16, 1, desc("BTCUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        true,
    );
    apply(
        &mut orders,
        image(
            161,
            1,
            desc("XRPUSDT"),
            state(OrderWorkerStatus::SellDone, 1),
        ),
        APP_TOKEN,
        true,
    );
    let mut events = Vec::new();
    let mut repairs = Vec::new();
    orders.apply_protocol(
        image(17, 1, desc("ETHUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        2_000,
        SERVER_TOKEN + 1,
        APP_TOKEN,
        0.0,
        &|_| true,
        &mut events,
        &mut repairs,
    );
    assert!(orders.get(16).is_some() && orders.get(17).is_some());

    let (events, _) = apply(
        &mut orders,
        image(18, 1, desc("SOLUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN + 1,
        true,
    );
    assert!(events
        .iter()
        .any(|event| matches!(event, OrderEvent::Removed(order) if order.uid == 16)));
    assert!(events
        .iter()
        .any(|event| matches!(event, OrderEvent::Removed(order) if order.uid == 161)));
    assert!(orders.get(16).is_none() && orders.get(161).is_none() && orders.get(18).is_some());
}

#[test]
fn mirror_parks_until_market_exists_then_attaches_without_network_repair() {
    let mut orders = OrderState::new();
    let (_, repairs) = apply(
        &mut orders,
        image(19, 1, desc("NEWCOIN"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        false,
    );
    assert!(repairs.is_empty());
    assert!(orders.get(19).is_none());
    assert!(orders.mirrors.contains_key(&19));

    let mut events = Vec::new();
    orders.rescan_parked(2_000, &|market| market == "NEWCOIN", &mut events);
    assert!(matches!(events.as_slice(), [OrderEvent::Created(order)] if order.uid == 19));
    assert!(orders.get(19).is_some());
}

#[test]
fn catalog_reconciles_exact_rows_and_repairs_missing_or_different_rows() {
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(20, 3, desc("BTCUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        true,
    );
    apply(
        &mut orders,
        image(21, 4, desc("ETHUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        true,
    );
    let catalog = OrdersCatalog {
        header: header(44, 0),
        from_uid: 20,
        range_end_uid: 22,
        records: vec![
            OrderCatalogRecord {
                order_id: 20,
                state_rev: 3,
            },
            OrderCatalogRecord {
                order_id: 21,
                state_rev: 5,
            },
            OrderCatalogRecord {
                order_id: 22,
                state_rev: 1,
            },
        ],
    };
    let (_, repairs) = apply(
        &mut orders,
        TradeCommand::OrdersCatalog(catalog),
        APP_TOKEN,
        true,
    );
    assert!(repairs.contains(&OrderRepair {
        order_id: 21,
        exact_rev: 4
    }));
    assert!(repairs.contains(&OrderRepair {
        order_id: 22,
        exact_rev: 0
    }));
    assert!(!repairs.iter().any(|repair| repair.order_id == 20));
}

#[test]
fn snapshot_uses_the_same_cold_membership_reconcile_as_catalog() {
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(30, 3, desc("BTCUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        true,
    );
    apply(
        &mut orders,
        image(31, 4, desc("ETHUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        true,
    );

    let mut snapshot_bytes = Vec::new();
    header(43, 0).write(&mut snapshot_bytes);
    snapshot_bytes.extend_from_slice(&30u64.to_le_bytes());
    snapshot_bytes.extend_from_slice(&32u64.to_le_bytes());
    snapshot_bytes.extend_from_slice(&32u64.to_le_bytes());
    snapshot_bytes.push(1); // StateRev ULEB
    desc("SOLUSDT").wire_bytes(&mut snapshot_bytes);
    snapshot_bytes.extend_from_slice(&ORDER_SECTION_ALL_MASK.to_le_bytes());
    snapshot_bytes.extend_from_slice(&state(OrderWorkerStatus::BuySet, 2).0);
    let snapshot = TradeCommand::parse(&snapshot_bytes).expect("valid snapshot wire");
    let (events, repairs) = apply(&mut orders, snapshot, APP_TOKEN, true);

    assert!(orders.get(32).is_some());
    assert!(repairs.contains(&OrderRepair {
        order_id: 30,
        exact_rev: 3,
    }));
    assert!(repairs.contains(&OrderRepair {
        order_id: 31,
        exact_rev: 4,
    }));
    assert!(events
        .iter()
        .any(|event| matches!(event, OrderEvent::Snapshot)));
}

#[test]
fn sell_almost_done_is_not_terminal() {
    assert!(!state(OrderWorkerStatus::SellAlmostDone, 1).is_terminal());
    for status in [
        OrderWorkerStatus::BuyFail,
        OrderWorkerStatus::BuyCancel,
        OrderWorkerStatus::SellFail,
        OrderWorkerStatus::SellCancel,
        OrderWorkerStatus::SellDone,
    ] {
        assert!(state(status, 1).is_terminal(), "{status:?}");
    }
}

#[test]
fn buy_replace_uses_canonical_target_size() {
    let uid = 23;
    let mut order_state = state(OrderWorkerStatus::BuySet, 1);
    order_state.0[19..27].copy_from_slice(&12.5f64.to_le_bytes());
    order_state.0[94..102].copy_from_slice(&3.0f64.to_le_bytes());
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(uid, 1, desc("BTCUSDT"), order_state),
        APP_TOKEN,
        true,
    );
    let payload = orders.send_replace_if_requested(uid, 123.0, 5_000).unwrap();
    assert!(matches!(payload, OrderCommandPayload::TargetBuy { size, .. } if size == 12.5));
}

#[test]
fn order_maintenance_uses_deadlines_instead_of_per_tick_map_scans() {
    let mut orders = OrderState::new();
    let uid = 0xA11;
    apply(
        &mut orders,
        image(uid, 1, desc("BTCUSDT"), state(OrderWorkerStatus::BuySet, 1)),
        APP_TOKEN,
        true,
    );

    assert!(orders
        .send_replace_if_requested(uid, 123.0, 1_000)
        .is_some());
    assert!(!orders.has_due_tick_work(6_000));
    assert!(orders.has_due_tick_work(6_001));
    assert_eq!(orders.tick_bulk_replace_timeouts(6_001).len(), 1);
    assert!(!orders.has_due_tick_work(6_002));

    let mut empty = OrderState::new();
    assert!(!empty.has_due_tick_work(29_999));
    assert!(empty.has_due_tick_work(30_000));
    assert!(empty.tick_order_trace_line_shrink(30_000).is_empty());
    assert!(!empty.has_due_tick_work(30_001));
}

#[test]
fn market_panic_off_updates_every_sell_and_emits_auto_stop_setters() {
    let stops = StopSettings::disabled()
        .with_stop_loss_fixed(95.0, 0.25)
        .with_trailing_percent(1.5, 0.1)
        .with_take_profit_price(120.0);
    let mut auto = state(OrderWorkerStatus::SellSet, 0);
    auto.0[10] = OFL_PANIC_ON | OFL_PANIC_AUTO;
    let mut stop_bytes = Vec::new();
    stops.write_to(&mut stop_bytes);
    auto.0[269..315].copy_from_slice(&stop_bytes);
    auto.0[315] = 1;
    auto.0[316] = 1;
    put_f64(&mut auto, 317, 97.5);
    put_f64(&mut auto, 325, 0.75);

    let manual = state(OrderWorkerStatus::SellSet, 0);
    let mut other_market = state(OrderWorkerStatus::SellSet, 0);
    other_market.0[10] = OFL_PANIC_ON;

    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(801, 1, desc("BTC"), auto),
        APP_TOKEN,
        true,
    );
    apply(
        &mut orders,
        image(802, 1, desc("BTC"), manual),
        APP_TOKEN,
        true,
    );
    apply(
        &mut orders,
        image(803, 1, desc("ETH"), other_market),
        APP_TOKEN,
        true,
    );

    let commands = orders.switch_panic_sell_by_market("BTC", true);
    assert!(commands[..2]
        .iter()
        .all(|command| matches!(command, OrderCommandPayload::Panic { .. })));
    assert!(commands[2..].iter().all(|command| matches!(
        command,
        OrderCommandPayload::Stops { .. } | OrderCommandPayload::VStop { .. }
    )));
    let mut panic_off = Vec::new();
    let mut saw_stops = false;
    let mut saw_vstop = false;
    for command in commands {
        match command {
            OrderCommandPayload::Panic {
                order_id,
                enabled: false,
            } => panic_off.push(order_id),
            OrderCommandPayload::Stops { order_id, stops } => {
                assert_eq!(order_id, 801);
                assert!(!bool::from(stops.stop_loss_on));
                assert!(!bool::from(stops.trailing_on));
                assert!(bool::from(stops.use_take_profit));
                assert_eq!(stops.sl_level, 95.0);
                assert_eq!(stops.trailing_level, 1.5);
                assert_eq!(stops.take_profit, 120.0);
                saw_stops = true;
            }
            OrderCommandPayload::VStop {
                order_id,
                enabled,
                fixed,
                level,
                volume,
            } => {
                assert_eq!(order_id, 801);
                assert!(!enabled);
                assert!(fixed);
                assert_eq!(level, 97.5);
                assert_eq!(volume, 0.75);
                saw_vstop = true;
            }
            other => panic!("unexpected market-panic command: {other:?}"),
        }
    }
    panic_off.sort_unstable();
    assert_eq!(panic_off, [801, 802]);
    assert!(saw_stops);
    assert!(saw_vstop);

    let auto = orders.get(801).unwrap();
    assert!(!auto.panic_sell);
    assert!(!auto.panic_sell_auto);
    assert!(!bool::from(auto.stops.stop_loss_on));
    assert!(!bool::from(auto.stops.trailing_on));
    assert!(!auto.vstop_on);
    assert_eq!(auto.vstop_level, 97.5);
    assert_eq!(auto.vstop_vol, 0.75);
    assert!(!orders.get(802).unwrap().panic_sell);
    assert!(orders.get(803).unwrap().panic_sell);
}

#[test]
fn panic_sell_all_lights_only_retained_sell_orders() {
    let mut orders = OrderState::new();
    apply(
        &mut orders,
        image(811, 1, desc("BTC"), state(OrderWorkerStatus::SellSet, 0)),
        APP_TOKEN,
        true,
    );
    apply(
        &mut orders,
        image(812, 1, desc("BTC"), state(OrderWorkerStatus::BuySet, 0)),
        APP_TOKEN,
        true,
    );
    apply(
        &mut orders,
        image(813, 1, desc("BTC"), state(OrderWorkerStatus::SellDone, 0)),
        APP_TOKEN,
        true,
    );

    assert!(orders.mark_panic_sell_all());
    assert!(orders.get(811).unwrap().panic_sell);
    assert!(!orders.get(812).unwrap().panic_sell);
    assert!(!orders.get(813).unwrap().panic_sell);
    assert!(!orders.mark_panic_sell_all());
}
