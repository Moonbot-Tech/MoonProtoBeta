use super::*;
use crate::commands::candles::DeepPrice;
use crate::commands::market::{
    write_market, ArbIsolationFlags, ArbPlatformCode, BaseCurrency, CorrMarketPriceUpdate,
    MarketArbNowEntry, MarketPriceUpdate, MarketsListResponse, MarketsPricesResponse, PositionType,
    MAX_MARKETS_LIST_ROWS,
};
use crate::commands::trade::OrderType;
use crate::commands::ui::LevManage;
use crate::MoonTime;

fn mk_market(name: &str, idx: u16) -> Market {
    Market {
        bn_market_name: name.to_string(),
        market_currency: name.to_string(),
        bn_market_currency: name.to_string(),
        base_currency: "USDT".to_string(),
        market_currency_long: name.to_string(),
        market_currency_canonic: name.to_string(),
        market_name: format!("{}USDT", name),
        market_name_mb_classic: format!("{}USDT", name),
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
        funding_rate: 0.0001 * idx as f64,
        funding_time: 45000.0 + idx as f64,
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
        delta_state: Default::default(),
        market_blacklisted_cfg: false,
        max_control_lev: 0,
        arb_slots: std::collections::HashMap::new(),
    }
}

fn mk_pair_market(name: &str, bn_currency: &str, base_currency: &str, idx: u16) -> Market {
    let mut market = mk_market(name, idx);
    market.market_currency = bn_currency.to_string();
    market.bn_market_currency = bn_currency.to_string();
    market.base_currency = base_currency.to_string();
    market
}

fn lev_manage_config(text: &str) -> LevManage {
    LevManage {
        uid: 0,
        cmd_ver: 1,
        auto_max_order: true,
        auto_lev_up: true,
        auto_isolated: false,
        auto_cross: false,
        auto_fix_lev: false,
        fix_lev: 20,
        tlg_report: false,
        lev_control: text.to_string(),
    }
}

fn push_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn push_price_update(
    out: &mut Vec<u8>,
    m_index: u16,
    bid: f64,
    ask: f64,
    mark_price: f64,
    mark_price_found: bool,
) {
    out.extend_from_slice(&m_index.to_le_bytes());
    out.extend_from_slice(&bid.to_le_bytes());
    out.extend_from_slice(&ask.to_le_bytes());
    out.extend_from_slice(&mark_price.to_le_bytes());
    out.push(mark_price_found as u8);
}

fn deep_price(mean: f32, time: MoonTime) -> DeepPrice {
    DeepPrice {
        open: mean,
        close: mean,
        high: mean,
        low: mean,
        volume: 1.0,
        time: time.to_delphi_days(),
    }
}

fn assert_near(actual: f64, expected: f64, eps: f64) {
    assert!(
        (actual - expected).abs() <= eps,
        "actual={actual}, expected={expected}, eps={eps}"
    );
}

#[test]
fn apply_markets_list_initial_populates_state() {
    let mut st = MarketsState::new();
    let resp = MarketsListResponse {
        markets: vec![mk_market("BTC", 0), mk_market("ETH", 1)],
        corr_markets: vec![],
    };
    let ev = st.apply_markets_list(resp);
    assert!(matches!(
        ev,
        MarketsEvent::MarketsListReplaced {
            count: 2,
            corr_count: 0
        }
    ));
    assert_eq!(st.market_count(), 2);
    assert_eq!(st.get("BTC").unwrap().snapshot().bn_market_name, "BTC");
    assert_eq!(st.get("ETH").unwrap().snapshot().bn_market_name, "ETH");
    assert!(st.get("DOGE").is_none());
    assert_eq!(st.market_name_by_index(1), Some("ETH"));
    assert_eq!(st.market_index_by_name("ETH"), Some(1));
}

#[test]
fn apply_lev_manage_sets_market_max_pos_like_markets_table() {
    let mut st = MarketsState::new();
    let mut btc = mk_market("BTCUSDT", 0);
    btc.market_currency = "BTC".to_string();
    btc.is_btc_market = true;
    let mut eth = mk_market("ETHUSDT", 1);
    eth.market_currency = "ETH".to_string();
    eth.is_btc_market = true;
    let mut sol = mk_market("SOLUSDT", 2);
    sol.market_currency = "SOL".to_string();
    sol.is_btc_market = true;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![btc, eth, sol],
        corr_markets: vec![],
    });

    let lev = lev_manage_config("100 def 250 BT* 3k ETH");
    st.apply_lev_manage_to_markets(&lev);

    assert_eq!(st.get("BTCUSDT").unwrap().max_pos_limit(), 250);
    assert_eq!(st.get("ETHUSDT").unwrap().max_pos_limit(), 3_000);
    assert_eq!(
        st.get("SOLUSDT").unwrap().max_pos_limit(),
        0,
        "Delphi keeps per-market MaxPos zero when only cfg.AutoLevControlOther/def applies"
    );
    assert_eq!(lev.default_max_pos_limit(), 100);
}

#[test]
fn apply_lev_manage_clears_absent_markets_on_new_config() {
    let mut st = MarketsState::new();
    let mut btc = mk_market("BTCUSDT", 0);
    btc.market_currency = "BTC".to_string();
    btc.is_btc_market = true;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![btc],
        corr_markets: vec![],
    });

    st.apply_lev_manage_to_markets(&lev_manage_config("250 BTC"));
    assert_eq!(st.get("BTCUSDT").unwrap().max_pos_limit(), 250);
    st.apply_lev_manage_to_markets(&lev_manage_config("100 def"));
    assert_eq!(st.get("BTCUSDT").unwrap().max_pos_limit(), 0);
}

#[test]
fn market_search_returns_stable_handles_by_user_input_meaning() {
    let mut st = MarketsState::new();
    let mut btc = mk_market("BTCUSDT", 0);
    btc.market_currency = "BTC".to_string();
    btc.market_currency_canonic = "BTC".to_string();
    btc.market_currency_long = "Bitcoin".to_string();
    btc.market_name = "USDT-BTC".to_string();

    let mut eth = mk_market("ETHUSDT", 1);
    eth.market_currency = "ETH".to_string();
    eth.market_currency_canonic = "ETH".to_string();
    eth.market_currency_long = "Ethereum".to_string();
    eth.market_name = "USDT-ETH".to_string();

    let mut sol = mk_market("SOLUSDT", 2);
    sol.market_currency = "SOL".to_string();
    sol.market_currency_canonic = "SOL".to_string();
    sol.market_currency_long = "Solana".to_string();
    sol.market_name = "USDT-SOL".to_string();

    st.apply_markets_list(MarketsListResponse {
        markets: vec![btc, eth, sol],
        corr_markets: vec![],
    });

    assert_eq!(st.find("BTCUSDT").unwrap().name(), "BTCUSDT");
    assert_eq!(st.find("btc").unwrap().name(), "BTCUSDT");
    assert_eq!(st.find("so").unwrap().name(), "SOLUSDT");
    assert_eq!(st.find("ther").unwrap().name(), "ETHUSDT");
    assert!(st.find("").is_none());

    let names: Vec<_> = st
        .search("t", 2)
        .into_iter()
        .map(|m| m.name().to_string())
        .collect();
    assert_eq!(names, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    assert!(st.get("BTCUSDT").unwrap().ptr_eq(&st.find("BTC").unwrap()));
}

#[test]
fn market_handle_balance_position_reads_live_market_fields() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });

    let handle = st.get("BTCUSDT").unwrap();
    handle.with_mut(|market| {
        market.pos_size = 2.5;
        market.pos_price = 65000.0;
        market.liq_price = 42000.0;
        market.leverage_x = 10;
        market.total_profit_b = 1.0;
        market.total_profit_l = 2.0;
        market.total_profit_s = 3.0;
    });

    let pos = handle.balance_position();
    assert_eq!(pos.pos_size, 2.5);
    assert_eq!(pos.pos_price, 65000.0);
    assert_eq!(pos.liq_price, 42000.0);
    assert_eq!(pos.leverage_x, 10);
    assert_eq!(pos.total_profit(), 6.0);
}

#[test]
fn market_handle_reads_arb_slot_without_raw_map_access() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });

    let handle = st.get("BTCUSDT").unwrap();
    handle.with_mut(|market| {
        let slot = market.arb_slots.entry(ArbPlatformCode::ByBit).or_default();
        slot.isolated_flags = ArbIsolationFlags::from_byte(3);
        slot.now = MarketArbNowEntry {
            price: 42.5,
            time: 45_000.25,
        };
    });

    let slot = handle.arb_slot(ArbPlatformCode::ByBit).unwrap();
    assert_eq!(slot.isolated_flags, ArbIsolationFlags::from_byte(3));
    assert_eq!(slot.now.price, 42.5);
    assert_eq!(
        handle.arb_now(ArbPlatformCode::ByBit).unwrap().time,
        45_000.25
    );
    assert!(handle.arb_slot(ArbPlatformCode::Gate).is_none());
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList
fn apply_markets_list_payload_keeps_read_market_on_late_corr_parse_error() {
    let mut st = MarketsState::new();
    let market = mk_market("BTCUSDT", 0);
    let mut data = Vec::new();
    data.extend_from_slice(&1i32.to_le_bytes());
    write_market(&mut data, &market, 2);
    data.extend_from_slice(&1i32.to_le_bytes());
    data.extend_from_slice(&7u16.to_le_bytes()); // broken CorrMarket name

    let ev = st.apply_markets_list_payload_with_local_shift(&data, 2, 0.0);

    assert!(ev.is_none());
    assert!(
        st.get("BTCUSDT").is_some(),
        "Delphi applies each market before reading CorrMarkets"
    );
    assert_eq!(
        st.market_name_by_index(0),
        Some("BTCUSDT"),
        "Delphi rebuilds SrvMarkets after the market loop and before CorrMarkets"
    );
}

#[test]
fn apply_markets_list_payload_rejects_absurd_market_count_before_loop() {
    let mut st = MarketsState::new();
    let mut data = Vec::new();
    data.extend_from_slice(&((MAX_MARKETS_LIST_ROWS as i32) + 1).to_le_bytes());

    let ev = st.apply_markets_list_payload_with_local_shift(&data, 2, 0.0);

    assert!(ev.is_none());
    assert_eq!(st.market_count(), 0);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList
fn apply_markets_list_payload_batches_new_market_cow() {
    let mut st = MarketsState::new();
    let mut data = Vec::new();
    data.extend_from_slice(&3i32.to_le_bytes());
    write_market(&mut data, &mk_market("BTCUSDT", 0), 2);
    write_market(&mut data, &mk_market("ETHUSDT", 1), 2);
    write_market(&mut data, &mk_market("DOGEUSDT", 2), 2);
    data.extend_from_slice(&0i32.to_le_bytes());

    let ev = st.apply_markets_list_payload_with_local_shift(&data, 2, 0.0);

    assert!(matches!(
        ev,
        Some(MarketsEvent::MarketsListReplaced {
            count: 3,
            corr_count: 0
        })
    ));
    assert_eq!(
        st.markets_version(),
        1,
        "initial GetMarketsList must build the handle list once, not COW per row"
    );
    assert!(st.get("DOGEUSDT").is_some());
}

#[test]
fn markets_list_payload_timing_records_only_coarse_production_phases() {
    let mut st = MarketsState::new();
    let mut data = Vec::new();
    data.extend_from_slice(&2i32.to_le_bytes());
    write_market(&mut data, &mk_market("BTCUSDT", 0), 2);
    write_market(&mut data, &mk_market("ETHUSDT", 1), 2);
    data.extend_from_slice(&0i32.to_le_bytes());

    let ev = st.apply_markets_list_payload_with_local_shift(&data, 2, 0.0);

    assert!(ev.is_some());
    let timing = st.last_markets_list_apply_timing().unwrap();
    assert_eq!(timing.market_count, 2);
    assert_eq!(timing.corr_count, 0);
    assert_eq!(timing.payload_len, data.len());
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList
fn apply_markets_list_preserves_absent_existing_markets() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0), mk_market("DOGEUSDT", 1)],
        corr_markets: vec![],
    });
    st.apply_token_tags(vec![
        MarketTokenTags {
            market_name: "BTCUSDT".to_string(),
            tags: TokenTags::MONITORING,
        },
        MarketTokenTags {
            market_name: "DOGEUSDT".to_string(),
            tags: TokenTags::GAMING,
        },
    ]);

    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });

    assert!(
        st.get("DOGEUSDT").is_some(),
        "Delphi GetMarketsList updates/adds but does not delete old Markets entries"
    );
    assert!(st.tags("BTCUSDT").contains(TokenTags::MONITORING));
    assert!(
        st.tags("DOGEUSDT").contains(TokenTags::GAMING),
        "absent old markets keep their token tags because the market is still present"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList (NewMarketFound)
fn apply_markets_list_does_not_add_new_market_without_new_market_found() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0), mk_market("DOGEUSDT", 1)],
        corr_markets: vec![],
    });

    assert!(st.get("BTCUSDT").is_some());
    assert!(
        st.get("DOGEUSDT").is_none(),
        "Delphi frees unknown TMarket when not FirstCreateMarkets and not NewMarketFound"
    );
    assert!(
        st.market_name_by_index(1).is_none(),
        "Delphi does not rebuild SrvMarkets for a plain repeated GetMarketsList"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList (NewMarketFound)
fn apply_markets_list_adds_new_market_and_clears_new_market_found() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
    st.markets_list_refresh_needed = true;

    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0), mk_market("DOGEUSDT", 1)],
        corr_markets: vec![],
    });

    assert!(st.get("DOGEUSDT").is_some());
    assert_eq!(st.take_new_markets_added(), vec!["DOGEUSDT".to_string()]);
    assert!(
        !st.markets_list_refresh_needed(),
        "Delphi clears NewMarketFound only after successful GetMarketsList apply"
    );
    assert_eq!(
        st.market_name_by_index(1),
        Some("DOGEUSDT"),
        "Delphi rebuilds SrvMarkets from GetMarketsList IndexMap when NewMarketFound"
    );

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 1,
            bid: 0.1,
            ask: 0.2,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 0.15,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });
    assert_eq!(st.price("DOGEUSDT").unwrap().bid, 0.1);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList
fn apply_markets_list_merges_existing_market_and_preserves_live_price() {
    let mut st = MarketsState::new();
    let mut old = mk_market("BTCUSDT", 1);
    old.bn_max_value = 123.0;
    old.funding_rate = 0.0007;
    old.funding_time = 45000.0;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![old],
        corr_markets: vec![],
    });
    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 50000.0,
            ask: 50001.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 50000.5,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });

    let mut incoming = mk_market("BTCUSDT", 2);
    incoming.bn_tick_size = 0.25;
    incoming.bn_max_value = 0.0;
    incoming.funding_rate = 0.0999;
    incoming.funding_time = 46000.0;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![incoming],
        corr_markets: vec![],
    });

    let market = st.get("BTCUSDT").unwrap().snapshot();
    assert_eq!(market.bn_tick_size, 0.25);
    assert_eq!(
        market.bn_max_value, 123.0,
        "Delphi CopyFromMarket ignores non-positive bnMaxValue"
    );
    assert_eq!(
        market.funding_rate, 0.0007,
        "Delphi GetMarketsList CopyFromMarket does not overwrite FundingRate"
    );
    assert_eq!(market.funding_time, 46000.0);

    let price = st.price("BTCUSDT").unwrap();
    assert_eq!(price.bid, 50000.0);
    assert_eq!(price.ask, 50001.0);
    assert_eq!(price.funding_rate, 0.0007);
    assert_eq!(price.funding_time, 46000.0);
    assert!(price.mark_price_found);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList (TMarket reuse on re-list)
fn market_handle_survives_listing_cow_and_sees_in_place_updates() {
    let mut st = MarketsState::new();
    let mut old = mk_market("BTCUSDT", 1);
    old.bn_tick_size = 0.01;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![old],
        corr_markets: vec![],
    });
    let btc = st.get("BTCUSDT").expect("initial handle");

    st.markets_list_refresh_needed = true;
    let mut incoming_btc = mk_market("BTCUSDT", 1);
    incoming_btc.bn_tick_size = 0.25;
    let eth = mk_market("ETHUSDT", 2);
    st.apply_markets_list(MarketsListResponse {
        markets: vec![incoming_btc, eth],
        corr_markets: vec![],
    });

    let fresh_btc = st.get("BTCUSDT").expect("fresh lookup");
    assert!(
        btc.ptr_eq(&fresh_btc),
        "Delphi TMarkets COW replaces containers, not existing TMarket objects"
    );
    assert_eq!(btc.snapshot().bn_tick_size, 0.25);
    assert!(st.get("ETHUSDT").is_some());
}

#[test]
fn apply_markets_list_keeps_existing_max_leverage_without_delphi_engine_flag() {
    let mut st = MarketsState::new();
    let mut old = mk_market("BTCUSDT", 1);
    old.max_leverage = 25;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![old],
        corr_markets: vec![],
    });

    let mut incoming = mk_market("BTCUSDT", 2);
    incoming.max_leverage = 125;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![incoming],
        corr_markets: vec![],
    });

    assert_eq!(
        st.get("BTCUSDT").unwrap().snapshot().max_leverage,
        25,
        "Delphi CopyFromMarket copies MaxLeverage only when ES_MaxLevInGetMarkets is set"
    );
}

#[test]
fn apply_markets_list_copies_existing_max_leverage_with_delphi_engine_flag() {
    let mut st = MarketsState::new();
    st.set_copy_max_leverage_from_markets_list(true);
    let mut old = mk_market("BTCUSDT", 1);
    old.max_leverage = 25;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![old],
        corr_markets: vec![],
    });

    let mut incoming = mk_market("BTCUSDT", 2);
    incoming.max_leverage = 125;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![incoming],
        corr_markets: vec![],
    });

    assert_eq!(st.get("BTCUSDT").unwrap().snapshot().max_leverage, 125);
}

#[test]
fn apply_prices_updates_by_index() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTC", 0), mk_market("ETH", 1)],
        corr_markets: vec![],
    });

    let prices = MarketsPricesResponse {
        send_funding: false,
        prices: vec![
            MarketPriceUpdate {
                m_index: 0,
                bid: 50000.0,
                ask: 50001.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 50000.5,
                mark_price_found: true,
            },
            MarketPriceUpdate {
                m_index: 1,
                bid: 3000.0,
                ask: 3000.5,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 3000.25,
                mark_price_found: true,
            },
        ],
        send_corr_markets: false,
        corr_prices: vec![],
    };
    let ev = st.apply_markets_prices(prices);
    assert!(matches!(
        ev,
        MarketsEvent::PricesUpdated {
            count: 2,
            included_funding: false,
            ..
        }
    ));
    assert_eq!(st.price("BTC").unwrap().bid, 50000.0);
    assert_eq!(st.price("BTC").unwrap().ask, 50001.0);
    assert_eq!(st.price("ETH").unwrap().mark_price, 3000.25);
}

#[test]
// parity: MoonBot MarketsU.pas:CheckHourlyValues/AddFrom/SetDelta500 + Bworks.pas:Exchange1hDelta
fn candle_baselines_and_price_updates_publish_signed_delphi_deltas() {
    let mut st = MarketsState::new();
    let mut btc = mk_pair_market("BTCUSDT", "BTC", "USDT", 0);
    btc.is_btc_market = true;
    let mut eth = mk_pair_market("ETHUSDT", "ETH", "USDT", 1);
    eth.is_btc_market = true;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![btc, eth],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

    let now = MoonTime::from_unix_millis(3 * crate::time::MILLIS_PER_HOUR);
    let candles = vec![
        deep_price(
            100.0,
            MoonTime::from_unix_millis(now.unix_millis() - 10 * crate::time::MILLIS_PER_MINUTE),
        ),
        deep_price(
            100.0,
            MoonTime::from_unix_millis(now.unix_millis() - 5 * crate::time::MILLIS_PER_MINUTE),
        ),
        deep_price(100.0, now),
    ];
    let eth_candles = vec![
        deep_price(
            50.0,
            MoonTime::from_unix_millis(now.unix_millis() - 10 * crate::time::MILLIS_PER_MINUTE),
        ),
        deep_price(
            50.0,
            MoonTime::from_unix_millis(now.unix_millis() - 5 * crate::time::MILLIS_PER_MINUTE),
        ),
        deep_price(50.0, now),
    ];
    st.apply_candles_delta_baselines(
        [
            ("BTCUSDT", candles.as_slice()),
            ("ETHUSDT", eth_candles.as_slice()),
        ],
        now,
        30_000,
    );

    let expected_btc_delta = (110.0 - 100.0) / 100.0 * 100.0;
    let expected_eth_delta = (70.0 - 50.0) / 50.0 * 100.0;
    st.apply_markets_prices_at(
        MarketsPricesResponse {
            send_funding: false,
            prices: vec![
                MarketPriceUpdate {
                    m_index: 0,
                    bid: 109.0,
                    ask: 111.0,
                    funding_rate: 0.0,
                    funding_time: 0.0,
                    mark_price: 110.0,
                    mark_price_found: true,
                },
                MarketPriceUpdate {
                    m_index: 1,
                    bid: 69.0,
                    ask: 71.0,
                    funding_rate: 0.0,
                    funding_time: 0.0,
                    mark_price: 70.0,
                    mark_price_found: true,
                },
            ],
            send_corr_markets: false,
            corr_prices: vec![],
        },
        31_000,
    );

    let delta = st.delta_state("BTCUSDT").unwrap();
    assert_near(delta.coin_1h_avg, 100.0, 0.000001);
    assert_near(delta.coin_1h_delta, expected_btc_delta, 0.000001);
    assert_near(delta.coin_1h_delta_ema, expected_btc_delta, 0.000001);
    assert_near(
        st.delta_state("ETHUSDT").unwrap().coin_1h_delta,
        expected_eth_delta,
        0.000001,
    );

    let global = st.global_deltas();
    assert_near(global.btc_1h_avg, 100.0, 0.000001);
    assert_near(global.btc_1h_delta, expected_btc_delta, 0.000001);
    assert_near(
        global.exchange_1h_delta,
        expected_btc_delta + expected_eth_delta,
        0.000001,
    );
    assert_eq!(global.exchange_market_count, 2);
}

#[test]
// parity: MoonBot MarketsU.pas:TMarket.IsBlackListed + Bworks.pas:Exchange1hDelta
fn exchange_signed_delta_can_exclude_cfg_blacklisted_markets() {
    let mut st = MarketsState::new();
    let mut btc = mk_pair_market("BTCUSDT", "BTC", "USDT", 0);
    btc.is_btc_market = true;
    btc.delta_state.coin_1h_delta = 10.0;
    btc.delta_state.coin_24h_delta_ema = 100.0;
    let mut eth = mk_pair_market("ETHUSDT", "ETH", "USDT", 1);
    eth.is_btc_market = true;
    eth.delta_state.coin_1h_delta = 20.0;
    eth.delta_state.coin_24h_delta_ema = 200.0;
    let mut sol = mk_pair_market("SOLUSDT", "SOL", "USDT", 2);
    sol.is_btc_market = true;
    sol.delta_state.coin_1h_delta = 30.0;
    sol.delta_state.coin_24h_delta_ema = 300.0;

    st.apply_markets_list(MarketsListResponse {
        markets: vec![btc, eth, sol],
        corr_markets: vec![],
    });

    let global = st.global_deltas();
    assert_near(global.exchange_1h_delta, 60.0, 0.000001);
    assert_near(global.exchange_24h_delta, 600.0, 0.000001);
    assert_eq!(global.exchange_market_count, 3);

    assert!(st.set_coin_blacklist_text(" ETH "));
    let global = st.global_deltas();
    assert_near(global.exchange_1h_delta, 60.0, 0.000001);

    assert!(st.set_exclude_blacklisted_markets_from_exchange_delta(true));
    let global = st.global_deltas();
    assert_near(global.exchange_1h_delta, 40.0, 0.000001);
    assert_near(global.exchange_24h_delta, 400.0, 0.000001);
    assert_eq!(global.exchange_market_count, 2);
    assert!(st.exclude_blacklisted_markets_from_exchange_delta());
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList
fn apply_prices_resets_mark_price_found_before_each_batch() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTC", 0), mk_market("ETH", 1)],
        corr_markets: vec![],
    });

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![
            MarketPriceUpdate {
                m_index: 0,
                bid: 10.0,
                ask: 11.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 10.5,
                mark_price_found: true,
            },
            MarketPriceUpdate {
                m_index: 1,
                bid: 20.0,
                ask: 21.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 20.5,
                mark_price_found: true,
            },
        ],
        send_corr_markets: false,
        corr_prices: vec![],
    });
    assert!(st.price("BTC").unwrap().mark_price_found);
    assert!(st.price("ETH").unwrap().mark_price_found);

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 1,
            bid: 22.0,
            ask: 23.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 22.5,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });

    assert!(
        !st.price("BTC").unwrap().mark_price_found,
        "Delphi clears CurrentMarkPriceFound before reading each UpdateMarketsList batch"
    );
    assert!(st.price("ETH").unwrap().mark_price_found);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList
fn direct_price_payload_clears_mark_found_on_empty_scalar_payload() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTC", 0), mk_market("ETH", 1)],
        corr_markets: vec![],
    });
    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![
            MarketPriceUpdate {
                m_index: 0,
                bid: 10.0,
                ask: 11.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 10.5,
                mark_price_found: true,
            },
            MarketPriceUpdate {
                m_index: 1,
                bid: 20.0,
                ask: 21.0,
                funding_rate: 0.0,
                funding_time: 0.0,
                mark_price: 20.5,
                mark_price_found: true,
            },
        ],
        send_corr_markets: false,
        corr_prices: vec![],
    });
    assert!(st.price("BTC").unwrap().mark_price_found);
    assert!(st.price("ETH").unwrap().mark_price_found);

    let event = st.apply_markets_prices_payload(&[]).unwrap();
    assert!(matches!(
        event,
        MarketsEvent::PricesUpdated {
            count: 0,
            included_funding: false,
            included_corr: false,
        }
    ));

    assert!(
        !st.price("BTC").unwrap().mark_price_found,
        "Delphi clears CurrentMarkPriceFound before reading scalar UpdateMarketsList header"
    );
    assert!(!st.price("ETH").unwrap().mark_price_found);
}

#[test]
fn direct_price_payload_rejects_impossible_huge_price_count_without_history_growth() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let mut data = Vec::new();
    data.push(0); // HasFunding=false
    data.extend_from_slice(&i32::MAX.to_le_bytes());
    let mut rows = Vec::new();

    let ev = st.apply_markets_prices_payload_with_local_shift(&data, 0.0, Some(&mut rows));

    assert!(ev.is_none());
    assert!(
        rows.is_empty(),
        "malformed UpdateMarketsList must not grow LastPrice history rows"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList
fn apply_prices_updates_last_price_and_min_lot() {
    let mut market = mk_market("BTCUSDT", 0);
    market.bn_step_size = 0.01;
    market.bn_min_qty = 0.02;
    market.bn_min_notional = 5.0;

    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![market],
        corr_markets: vec![],
    });

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 100.0,
            ask: 110.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 105.0,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });

    let price = st.price("BTCUSDT").unwrap();
    assert_eq!(price.last_bid, 100.0);
    assert_eq!(price.last_ask, 110.0);
    assert_eq!(price.p_last, 105.0);
    assert_eq!(
        price.min_lot_size, 5.0,
        "Delphi uses Max(Max(step,minQty) * pLast, bnMinNotional)"
    );
    assert_eq!(
        price.chart_price_step,
        110.0 / 5000.0,
        "Delphi AddNewAksPrice sets ChartPriceStep from Ask"
    );

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 120.0,
            ask: 0.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 0.0,
            mark_price_found: false,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });
    assert_eq!(
        st.price("BTCUSDT").unwrap().chart_price_step,
        110.0 / 5000.0,
        "Delphi AddNewAksPrice exits when Ask is zero and keeps previous ChartPriceStep"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList
fn apply_prices_updates_market_funding_fields() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: true,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 100.0,
            ask: 110.0,
            funding_rate: 0.0125,
            funding_time: 46000.25,
            mark_price: 105.0,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });

    let market = st.get("BTCUSDT").unwrap().snapshot();
    assert_eq!(market.funding_rate, 0.0125);
    assert_eq!(market.funding_time, 46000.25);

    let price = st.price("BTCUSDT").unwrap();
    assert_eq!(price.funding_rate, 0.0125);
    assert_eq!(price.funding_time, 46000.25);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList
fn apply_prices_payload_keeps_read_updates_on_late_corr_parse_error() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    let mut data = Vec::new();
    data.push(0); // HasFunding=false
    data.extend_from_slice(&1i32.to_le_bytes());
    push_price_update(&mut data, 0, 10.0, 11.0, 10.5, true);
    data.push(1); // HasCorrMarkets=true
    data.extend_from_slice(&1i32.to_le_bytes());
    data.extend_from_slice(&10u16.to_le_bytes()); // broken corr market string

    let ev = st.apply_markets_prices_payload_with_local_shift(&data, 0.0, None);

    assert!(ev.is_none());
    let price = st.price("BTCUSDT").unwrap();
    assert_eq!(price.bid, 10.0);
    assert_eq!(price.ask, 11.0);
    assert!(price.mark_price_found);
}

#[test]
fn apply_prices_uses_server_index_mapping() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0), mk_market("ETHUSDT", 1)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()]);

    let prices = MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 3000.0,
            ask: 3001.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 3000.5,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    };
    st.apply_markets_prices(prices);

    assert_eq!(st.price("ETHUSDT").unwrap().bid, 3000.0);
    assert_eq!(st.price("BTCUSDT").unwrap().bid, 0.0);
    assert_eq!(st.price_by_index(0).unwrap().bid, 3000.0);
}

#[test]
fn apply_prices_skips_stale_server_index_mapping() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0), mk_market("ETHUSDT", 1)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()]);
    st.mark_indexes_stale();

    let prices = MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 3000.0,
            ask: 3001.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 3000.5,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    };
    st.apply_markets_prices(prices);

    assert_eq!(st.price("ETHUSDT").unwrap().bid, 0.0);
    assert!(st.price_by_index(0).is_none());
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList
fn apply_prices_marks_refresh_needed_for_unknown_indexed_market() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["DOGEUSDT".to_string()]);

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 0.1,
            ask: 0.2,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 0.15,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });

    assert!(
        st.markets_list_refresh_needed(),
        "Delphi sets NewMarketFound when SrvMarkets.FindByServerIndex returns nil"
    );
    assert!(
        st.price("BTCUSDT").unwrap().bid == 0.0,
        "unknown market row must not be applied to a wrong local market"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList
fn apply_prices_marks_refresh_needed_for_out_of_range_index() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["BTCUSDT".to_string()]);

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 2,
            bid: 0.1,
            ask: 0.2,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 0.15,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });

    assert!(
        st.markets_list_refresh_needed(),
        "Delphi SrvMarkets.FindByServerIndex(out-of-range) returns nil and sets NewMarketFound"
    );
    assert_eq!(st.price("BTCUSDT").unwrap().bid, 0.0);
}

#[test]
fn apply_markets_list_clears_refresh_needed_after_listing_refresh() {
    let mut st = MarketsState::new();
    st.markets_list_refresh_needed = true;
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("DOGEUSDT", 0)],
        corr_markets: vec![],
    });

    assert!(!st.markets_list_refresh_needed());
    assert!(st.get("DOGEUSDT").is_some());
}

#[test]
fn apply_prices_with_funding_updates_funding() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTC", 0)],
        corr_markets: vec![],
    });
    // Initial funding from Market.funding_rate
    let initial_funding = st.price("BTC").unwrap().funding_rate;
    assert_eq!(initial_funding, 0.0);

    let prices = MarketsPricesResponse {
        send_funding: true,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 50000.0,
            ask: 50001.0,
            funding_rate: 0.0005,
            funding_time: 45123.5,
            mark_price: 50000.5,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    };
    st.apply_markets_prices(prices);
    assert_eq!(st.price("BTC").unwrap().funding_rate, 0.0005);
    assert_eq!(st.price("BTC").unwrap().funding_time, 45123.5);
}

#[test]
fn apply_prices_without_funding_keeps_existing() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTC", 5)], // funding_rate = 0.0005 from constructor
        corr_markets: vec![],
    });
    let pre = st.price("BTC").unwrap().funding_rate;
    assert_eq!(pre, 0.0005); // from Market.funding_rate

    let prices = MarketsPricesResponse {
        send_funding: false, // funding not sent
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 50000.0,
            ask: 50001.0,
            funding_rate: 99.0,
            funding_time: 99.0, // these values must be ignored
            mark_price: 50000.5,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    };
    st.apply_markets_prices(prices);
    // funding_rate must be preserved (send_funding=false)
    assert_eq!(st.price("BTC").unwrap().funding_rate, 0.0005);
}

#[test]
fn apply_prices_out_of_range_skipped() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTC", 0)],
        corr_markets: vec![],
    });
    let prices = MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 999, // out of range
            bid: 1.0,
            ask: 1.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 0.0,
            mark_price_found: false,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    };
    // Must not panic
    let _ = st.apply_markets_prices(prices);
    assert_eq!(st.price("BTC").unwrap().bid, 0.0); // not updated
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList
fn apply_markets_list_skips_corr_market_with_empty_base_currency() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![],
        corr_markets: vec![CorrMarket {
            bn_market_name: "DOGEBTC".to_string(),
            bn_market_currency: "DOGE".to_string(),
            bn_tick_size: 0.0,
            base_currency_name: String::new(),
        }],
    });

    assert_eq!(
        st.corr_count(),
        0,
        "Delphi calls AddOrSetCorrMarket only when BaseCur is not empty"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList
fn apply_markets_list_preserves_existing_corr_market_currency() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![],
        corr_markets: vec![CorrMarket {
            bn_market_name: "DOGEBTC".to_string(),
            bn_market_currency: "DOGE".to_string(),
            bn_tick_size: 0.00000001,
            base_currency_name: "BTC".to_string(),
        }],
    });

    st.apply_markets_list(MarketsListResponse {
        markets: vec![],
        corr_markets: vec![CorrMarket {
            bn_market_name: "DOGEBTC".to_string(),
            bn_market_currency: "WRONG".to_string(),
            bn_tick_size: 0.00000002,
            base_currency_name: "USDT".to_string(),
        }],
    });

    let cm = st.corr_markets.get("DOGEBTC").unwrap();
    assert_eq!(
        cm.bn_market_currency, "DOGE",
        "Delphi AddOrSetCorrMarket writes bnMarketCurrency only when TCorrMarket is created"
    );
    assert_eq!(cm.bn_tick_size, 0.00000002);
    assert_eq!(cm.base_currency_name, "USDT");
}

#[test]
// parity: MoonBot MarketsU.pas:TMarkets.CheckCorrMarkets
fn check_corr_markets_sets_ref_btc_market() {
    let mut st = MarketsState::new();
    st.set_server_base_currency(Some("USDT"), Some(BaseCurrency::USDT));
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_pair_market("DOGEUSDT", "DOGE", "USDT", 0)],
        corr_markets: vec![CorrMarket {
            bn_market_name: "DOGEBTC".to_string(),
            bn_market_currency: "DOGE".to_string(),
            bn_tick_size: 0.00000001,
            base_currency_name: "BTC".to_string(),
        }],
    });

    assert_eq!(
        st.ref_btc_corr_market("DOGEUSDT")
            .map(|cm| cm.bn_market_name.as_str()),
        Some("DOGEBTC")
    );
}

#[test]
// parity: MoonBot MarketsU.pas:TMarkets.CheckCorrMarkets
fn check_corr_markets_skips_btc_base() {
    let mut st = MarketsState::new();
    st.set_server_base_currency(Some("BTC"), Some(BaseCurrency::BTC));
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_pair_market("DOGEUSDT", "DOGE", "USDT", 0)],
        corr_markets: vec![CorrMarket {
            bn_market_name: "DOGEBTC".to_string(),
            bn_market_currency: "DOGE".to_string(),
            bn_tick_size: 0.00000001,
            base_currency_name: "BTC".to_string(),
        }],
    });

    assert!(
        st.ref_btc_corr_market("DOGEUSDT").is_none(),
        "Delphi CheckCorrMarkets does nothing when cfg.BaseCurrency = BC_BTC"
    );
}

#[test]
// parity: MoonBot MarketsU.pas:TMarkets.UpdateCurrencyPrices
fn update_currency_prices_uses_usdt_market() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_pair_market("BTCUSDT", "BTC", "USDT", 0)],
        corr_markets: vec![CorrMarket {
            bn_market_name: "DOGEBTC".to_string(),
            bn_market_currency: "DOGE".to_string(),
            bn_tick_size: 0.00000001,
            base_currency_name: "BTC".to_string(),
        }],
    });

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 49_999.0,
            ask: 50_000.0,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 0.0,
            mark_price_found: false,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    });

    let btc = st.base_currency_price("BTC").unwrap();
    assert_eq!(btc.usdt_market.as_deref(), Some("BTCUSDT"));
    assert_eq!(btc.last_price, 50_000.0);
}

#[test]
// parity: MoonBot MarketsU.pas:TMarkets.UpdateCurrencyPrices
fn update_currency_prices_uses_usdt_corr_market() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![],
        corr_markets: vec![
            CorrMarket {
                bn_market_name: "DOGEBTC".to_string(),
                bn_market_currency: "DOGE".to_string(),
                bn_tick_size: 0.00000001,
                base_currency_name: "BTC".to_string(),
            },
            CorrMarket {
                bn_market_name: "BTCUSDT".to_string(),
                bn_market_currency: "BTC".to_string(),
                bn_tick_size: 0.01,
                base_currency_name: "USDT".to_string(),
            },
        ],
    });

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![],
        send_corr_markets: true,
        corr_prices: vec![CorrMarketPriceUpdate {
            bn_market_name: "BTCUSDT".to_string(),
            last_price: 50_000.0,
        }],
    });

    let btc = st.base_currency_price("BTC").unwrap();
    assert_eq!(btc.usdt_corr_market.as_deref(), Some("BTCUSDT"));
    assert_eq!(btc.last_price, 50_000.0);
    assert_eq!(st.base_currency_price("USDT").unwrap().last_price, 1.0);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList (corr-market prices)
fn apply_corr_prices_merges() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![],
        corr_markets: vec![CorrMarket {
            bn_market_name: "DOGEBTC".to_string(),
            bn_market_currency: "DOGE".to_string(),
            bn_tick_size: 0.0,
            base_currency_name: "BTC".to_string(),
        }],
    });
    std::sync::Arc::make_mut(&mut st.corr_prices).insert("ETHBTC".to_string(), 0.07);
    assert_eq!(st.corr_count(), 1);

    let prices = MarketsPricesResponse {
        send_funding: false,
        prices: vec![],
        send_corr_markets: true,
        corr_prices: vec![CorrMarketPriceUpdate {
            bn_market_name: "DOGEBTC".to_string(),
            last_price: 0.00000123,
        }],
    };
    st.apply_markets_prices(prices);
    assert_eq!(st.corr_prices.get("DOGEBTC").copied(), Some(0.00000123));
    assert_eq!(
        st.corr_prices.get("ETHBTC").copied(),
        Some(0.07),
        "Delphi updates sent corr prices but does not clear absent ones"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:UpdateMarketsList (corr-market prices)
fn apply_corr_prices_ignores_unknown_corr_market() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![],
        corr_markets: vec![CorrMarket {
            bn_market_name: "DOGEBTC".to_string(),
            bn_market_currency: "DOGE".to_string(),
            bn_tick_size: 0.0,
            base_currency_name: "BTC".to_string(),
        }],
    });

    st.apply_markets_prices(MarketsPricesResponse {
        send_funding: false,
        prices: vec![],
        send_corr_markets: true,
        corr_prices: vec![
            CorrMarketPriceUpdate {
                bn_market_name: "DOGEBTC".to_string(),
                last_price: 0.00000123,
            },
            CorrMarketPriceUpdate {
                bn_market_name: "UNKNOWNBTC".to_string(),
                last_price: 0.5,
            },
        ],
    });

    assert_eq!(st.corr_prices.get("DOGEBTC").copied(), Some(0.00000123));
    assert_eq!(
        st.corr_prices.get("UNKNOWNBTC"),
        None,
        "Delphi UpdateMarketsList applies CorrMarket price only when GetCorrMarket(MName) is found"
    );
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:CheckBinanceTags
fn apply_token_tags_clears_missing_markets() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![
            mk_market("BTCUSDT", 0),
            mk_market("DOGEUSDT", 1),
            mk_market("ETHUSDT", 2),
        ],
        corr_markets: vec![],
    });

    let ev = st.apply_token_tags(vec![
        MarketTokenTags {
            market_name: "BTCUSDT".to_string(),
            tags: TokenTags::MONITORING,
        },
        MarketTokenTags {
            market_name: "DOGEUSDT".to_string(),
            tags: TokenTags::GAMING | TokenTags::NEW,
        },
    ]);
    assert!(matches!(ev, MarketsEvent::TokenTagsUpdated { count: 2 }));
    assert!(st.tags("BTCUSDT").contains(TokenTags::MONITORING));
    assert!(st.tags("DOGEUSDT").contains(TokenTags::GAMING));
    assert!(st.tags("NOPE").is_empty());

    let ev = st.apply_token_tags(vec![
        MarketTokenTags {
            market_name: "ETHUSDT".to_string(),
            tags: TokenTags::ALPHA,
        },
        MarketTokenTags {
            market_name: "UNKNOWN".to_string(),
            tags: TokenTags::FAN,
        },
    ]);
    assert!(matches!(ev, MarketsEvent::TokenTagsUpdated { count: 1 }));
    assert!(
        st.tags("BTCUSDT").is_empty(),
        "Delphi clears TokenTags for markets not seen in the latest response"
    );
    assert!(st.tags("ETHUSDT").contains(TokenTags::ALPHA));
    assert!(st.tags("UNKNOWN").is_empty());
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:CheckBinanceTags
fn apply_token_tags_payload_keeps_read_tags_on_late_parse_error() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0), mk_market("ETHUSDT", 1)],
        corr_markets: vec![],
    });
    st.apply_token_tags(vec![
        MarketTokenTags {
            market_name: "BTCUSDT".to_string(),
            tags: TokenTags::MONITORING,
        },
        MarketTokenTags {
            market_name: "ETHUSDT".to_string(),
            tags: TokenTags::GAMING,
        },
    ]);

    let mut data = Vec::new();
    data.extend_from_slice(&2i32.to_le_bytes());
    push_str(&mut data, "BTCUSDT");
    data.extend_from_slice(&(TokenTags::ALPHA.bits() as i32).to_le_bytes());
    data.extend_from_slice(&10u16.to_le_bytes()); // broken second market string

    let ev = st.apply_token_tags_payload(&data);

    assert!(ev.is_none());
    assert!(st.tags("BTCUSDT").contains(TokenTags::ALPHA));
    assert!(
        st.tags("ETHUSDT").contains(TokenTags::GAMING),
        "Delphi clears unseen tags only after the read loop completes"
    );
}

#[test]
fn apply_token_tags_payload_rejects_absurd_count_before_loop() {
    let mut st = MarketsState::new();
    let mut data = Vec::new();
    data.extend_from_slice(&((MAX_MARKETS_LIST_ROWS as i32) + 1).to_le_bytes());

    assert!(st.apply_token_tags_payload(&data).is_none());
}

#[test]
fn apply_markets_indexes() {
    let mut st = MarketsState::new();
    let names = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    let ev = st.apply_markets_indexes(names.clone());
    assert!(matches!(ev, MarketsEvent::IndexesUpdated { count: 2 }));
    assert_eq!(st.market_index_names(), names.as_slice());
}

#[test]
fn apply_markets_indexes_sets_synchronized_flag() {
    // Active library: indexes_synchronized = false by default (init state).
    // EventDispatcher blocks TradesStream/OrderBook until this point.
    let mut st = MarketsState::new();
    assert!(!st.indexes_synchronized, "default: not synchronized");
    st.apply_markets_indexes(vec!["A".to_string()]);
    assert!(st.indexes_synchronized, "after apply: synchronized");
}

#[test]
fn market_index_helpers_use_server_mapping() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0), mk_market("ETHUSDT", 1)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()]);

    assert_eq!(st.market_name_by_index(0), Some("ETHUSDT"));
    assert_eq!(
        st.market_by_index(1).unwrap().snapshot().bn_market_name,
        "BTCUSDT"
    );
    assert_eq!(st.market_index_by_name("BTCUSDT"), Some(1));
    assert_eq!(st.market_index_by_name("NOPE"), None);
}

#[test]
fn market_index_helpers_hide_stale_mapping() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0)],
        corr_markets: vec![],
    });
    st.apply_markets_indexes(vec!["BTCUSDT".to_string()]);
    st.mark_indexes_stale();

    assert_eq!(st.market_name_by_index(0), None);
    assert!(st.market_by_index(0).is_none());
    assert_eq!(st.market_index_by_name("BTCUSDT"), None);
}

#[test]
// parity: MoonBot MoonProtoEngine.pas:GetMarketsList (rebuilds Markets lookups)
fn get_markets_list_rebuilds_stale_server_indexes() {
    let mut st = MarketsState::new();
    st.apply_markets_list(MarketsListResponse {
        markets: vec![mk_market("BTCUSDT", 0), mk_market("ETHUSDT", 1)],
        corr_markets: vec![],
    });
    st.mark_indexes_stale();

    st.apply_markets_list(MarketsListResponse {
        markets: vec![
            mk_market("ETHUSDT", 0),
            mk_market("BTCUSDT", 1),
            mk_market("NEWUSDT", 2),
        ],
        corr_markets: vec![],
    });

    assert!(
        st.indexes_synchronized,
        "Delphi GetMarketsList rebuilds SrvMarkets when PeerAppToken changed"
    );
    assert_eq!(st.market_name_by_index(0), Some("ETHUSDT"));
    assert_eq!(
        st.market_by_index(1).unwrap().snapshot().bn_market_name,
        "BTCUSDT"
    );
    assert!(
        st.get("NEWUSDT").is_none(),
        "token-change rebuild does not by itself enable unknown market insertion"
    );
    assert!(
        st.market_by_index(2).is_none(),
        "SrvMarkets slot can point to a name that has no local TMarket yet"
    );
}
