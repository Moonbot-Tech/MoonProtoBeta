use super::*;

fn sample_market(name: &str, with_v2: bool) -> Market {
    Market {
        bn_market_name: name.to_string(),
        market_currency: "BTC".to_string(),
        bn_market_currency: "BTC".to_string(),
        base_currency: "USDT".to_string(),
        market_currency_long: "Bitcoin".to_string(),
        market_currency_canonic: "BTC".to_string(),
        market_name: format!("{}USDT", name),
        market_name_mb_classic: format!("{}_USDT", name),
        bn_status: "TRADING".to_string(),
        leading1000: String::new(),
        bn_price_precision: 2,
        bn_quantity_precision: 5,
        max_leverage: 125,
        k1000: 1,
        bn_iceberg_parts: 0,
        bn_margin_table_id: 0,
        bn_delivery_time: 0,
        bn_tick_size: 0.01,
        bn_step_size: 0.00001,
        bn_min_qty: 0.00001,
        bn_max_qty: 9000.0,
        bn_min_notional: 5.0,
        bn_max_notional: 0.0,
        bn_contract_size: 1.0,
        bn_min_price: 0.01,
        bn_max_price: 1000000.0,
        bn_max_value: 0.0,
        bn_multiplier_up: 1.05,
        bn_multiplier_down: 0.95,
        bid_multiplier_up: 0.0,
        bid_multiplier_down: 0.0,
        ask_multiplier_up: 0.0,
        ask_multiplier_down: 0.0,
        int_bn_max_qty: 0.0,
        funding_rate: 0.0001,
        funding_time: 45123.5,
        volume: 1234567.0,
        is_btc_market: true,
        status_trading: true,
        has_1000_prefix_alias: false,
        bn_iceberg: false,
        bn_only_isolated: false,
        futures_type: if with_v2 {
            BaseCurrency::USDT
        } else {
            BaseCurrency::EMPTY
        },
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

#[test]
fn market_roundtrip_v1() {
    let m = sample_market("BTC", false);
    let mut buf = Vec::new();
    write_market_with_local_shift(&mut buf, &m, 1, 0.0);
    let mut r = EngineStreamReader::new(&buf);
    let m2 = read_market_with_local_shift(&mut r, 1, 0.0).unwrap();
    assert_eq!(m, m2);
    assert_eq!(
        r.remaining(),
        1,
        "Delphi writer always writes FuturesType, but v1 reader leaves it unread"
    );
}

#[test]
fn market_v1_defaults_futures_type_to_empty_like_delphi_create_base() {
    let mut m = sample_market("BTC", true);
    m.futures_type = BaseCurrency::UNKNOWN;
    let mut buf = Vec::new();
    write_market_with_local_shift(&mut buf, &m, 1, 0.0);

    let mut r = EngineStreamReader::new(&buf);
    let m2 = read_market_with_local_shift(&mut r, 1, 0.0).unwrap();

    assert_eq!(m2.futures_type, BaseCurrency::EMPTY);
    assert_eq!(r.remaining(), 1);
}

#[test]
fn market_listed_type_matches_delphi_get_markets_list_post_pass() {
    let mut spot = sample_market("BTC", false);
    spot.futures_type = BaseCurrency::EMPTY;
    assert_eq!(spot.listed_type(), ListedType::SPOT);

    let mut both = sample_market("ETH", true);
    both.futures_type = BaseCurrency::USDT;
    assert_eq!(both.listed_type(), ListedType::BOTH);

    let mut unknown_non_empty = sample_market("NEW", true);
    unknown_non_empty.futures_type = BaseCurrency::UNKNOWN;
    assert_eq!(unknown_non_empty.listed_type(), ListedType::BOTH);
}

#[test]
fn market_roundtrip_v2_with_futures_type() {
    let m = sample_market("ETH", true);
    let mut buf = Vec::new();
    write_market_with_local_shift(&mut buf, &m, 2, 0.0);
    let mut r = EngineStreamReader::new(&buf);
    let m2 = read_market_with_local_shift(&mut r, 2, 0.0).unwrap();
    assert_eq!(m2.futures_type, BaseCurrency::USDT);
    assert_eq!(m, m2);
}

#[test]
fn market_mb_classic_backfilled_when_empty() {
    // If `market_name_mb_classic = ""` in the payload, after reading it must become = market_name.
    let mut m = sample_market("LTC", true);
    m.market_name_mb_classic = String::new();
    m.market_name = "LTCUSDT".to_string();
    let mut buf = Vec::new();
    write_market_with_local_shift(&mut buf, &m, 2, 0.0);
    let mut r = EngineStreamReader::new(&buf);
    let m2 = read_market_with_local_shift(&mut r, 2, 0.0).unwrap();
    assert_eq!(m2.market_name_mb_classic, "LTCUSDT");
}

#[test]
fn market_reader_applies_delphi_local_funding_shift() {
    let m = sample_market("BTC", true);
    let mut buf = Vec::new();
    write_market_with_local_shift(&mut buf, &m, 2, 0.0);
    let mut r = EngineStreamReader::new(&buf);

    let m2 = read_market_with_local_shift(&mut r, 2, 180.0).unwrap();

    assert_eq!(m2.funding_time, m.funding_time + 180.0 / 1440.0);
}

#[test]
fn market_writer_removes_delphi_local_funding_shift() {
    let m = sample_market("BTC", true);
    let mut buf = Vec::new();
    write_market_with_local_shift(&mut buf, &m, 2, 180.0);

    let mut wire_reader = EngineStreamReader::new(&buf);
    let wire_m = read_market_with_local_shift(&mut wire_reader, 2, 0.0).unwrap();
    assert_eq!(wire_m.funding_time, m.funding_time - 180.0 / 1440.0);

    let mut client_reader = EngineStreamReader::new(&buf);
    let client_m = read_market_with_local_shift(&mut client_reader, 2, 180.0).unwrap();
    assert_eq!(client_m.funding_time, m.funding_time);
}

#[test]
fn market_write_str_writes_only_declared_wrapped_len_like_delphi() {
    let s = "M".repeat(65_537);
    let mut buf = Vec::new();
    write_str(&mut buf, &s);

    assert_eq!(&buf[..2], &1u16.to_le_bytes());
    assert_eq!(buf.len(), 2 + 1);

    let mut r = EngineStreamReader::new(&buf);
    assert_eq!(r.read_str().unwrap(), "M");
}

#[test]
fn market_count_reader_does_not_precheck_remaining_like_delphi() {
    let bytes = 2i32.to_le_bytes();
    let mut r = EngineStreamReader::new(&bytes);

    let count = r.read_count().unwrap();

    assert_eq!(count, 2);
    assert_eq!(r.bounded_count_capacity(count, 27), 0);
}

#[test]
fn engine_stream_scalars_zero_tail_like_delphi_read_helpers() {
    let mut r = EngineStreamReader::new(&[0x34, 0x12, 0x78]);

    assert_eq!(r.read_word(), Some(0x1234));
    assert_eq!(r.read_int(), Some(0x78));
    assert_eq!(r.read_bool(), Some(false));
    assert_eq!(r.position(), 3);
}

#[test]
fn market_reader_zero_tails_short_fixed_tail_after_valid_strings_like_delphi() {
    let mut buf = Vec::new();
    for s in [
        "BNBTC", "BTC", "BTC", "USDT", "Bitcoin", "BTC", "BTCUSDT", "", "TRADING", "",
    ] {
        write_str(&mut buf, s);
    }

    let mut r = EngineStreamReader::new(&buf);
    let market = read_market_with_local_shift(&mut r, 1, 0.0).unwrap();

    assert_eq!(market.market_name, "BTCUSDT");
    assert_eq!(market.market_name_mb_classic, "BTCUSDT");
    assert_eq!(market.bn_price_precision, 0);
    assert_eq!(market.bn_delivery_time, 0);
    assert_eq!(market.bn_tick_size, 0.0);
    assert!(!market.status_trading);
    assert_eq!(market.futures_type, BaseCurrency::EMPTY);
    assert_eq!(r.position(), buf.len());
}

#[test]
fn market_prices_row_zero_tails_short_fixed_payload_like_delphi() {
    let mut buf = Vec::new();
    buf.push(1); // send_funding
    buf.extend_from_slice(&1i32.to_le_bytes()); // one market price row

    let parsed = parse_markets_prices_response_with_local_shift(&buf, 0.0).unwrap();

    assert!(parsed.send_funding);
    assert_eq!(parsed.prices.len(), 1);
    assert_eq!(parsed.prices[0].m_index, 0);
    assert_eq!(parsed.prices[0].bid, 0.0);
    assert_eq!(parsed.prices[0].ask, 0.0);
    assert_eq!(parsed.prices[0].funding_rate, 0.0);
    assert_eq!(parsed.prices[0].funding_time, 0.0);
    assert_eq!(parsed.prices[0].mark_price, 0.0);
    assert!(!parsed.prices[0].mark_price_found);
    assert!(!parsed.send_corr_markets);
}

#[test]
fn token_tags_string_stays_readbuffer_fail_fast() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1i32.to_le_bytes());
    buf.extend_from_slice(&4u16.to_le_bytes());
    buf.extend_from_slice(b"BT");

    assert!(parse_token_tags_response(&buf).is_none());
}

#[test]
fn corr_market_roundtrip() {
    let c = CorrMarket {
        bn_market_name: "BTCUSDT".to_string(),
        bn_market_currency: "BTC".to_string(),
        bn_tick_size: 0.5,
        base_currency_name: "USDT".to_string(),
    };
    let mut buf = Vec::new();
    write_corr_market(&mut buf, &c);
    let mut r = EngineStreamReader::new(&buf);
    let c2 = read_corr_market(&mut r).unwrap();
    assert_eq!(c, c2);
}

#[test]
fn markets_list_response_roundtrip() {
    let resp = MarketsListResponse {
        markets: vec![sample_market("BTC", true), sample_market("ETH", true)],
        corr_markets: vec![CorrMarket {
            bn_market_name: "DOGEBTC".to_string(),
            bn_market_currency: "DOGE".to_string(),
            bn_tick_size: 0.00000001,
            base_currency_name: "BTC".to_string(),
        }],
    };
    let buf = build_markets_list_response_with_local_shift(&resp, 2, 0.0);
    let parsed = parse_markets_list_response(&buf, 2).unwrap();
    assert_eq!(parsed.markets.len(), 2);
    assert_eq!(parsed.markets[0].bn_market_name, "BTC");
    assert_eq!(parsed.markets[1].bn_market_name, "ETH");
    assert_eq!(parsed.corr_markets.len(), 1);
    assert_eq!(parsed.corr_markets[0].bn_market_name, "DOGEBTC");
}

#[test]
fn markets_prices_response_with_funding() {
    let resp = MarketsPricesResponse {
        send_funding: true,
        prices: vec![
            MarketPriceUpdate {
                m_index: 0,
                bid: 50000.0,
                ask: 50001.0,
                funding_rate: 0.0001,
                funding_time: 45123.5,
                mark_price: 50000.5,
                mark_price_found: true,
            },
            MarketPriceUpdate {
                m_index: 1,
                bid: 3000.0,
                ask: 3000.5,
                funding_rate: -0.0002,
                funding_time: 45123.5,
                mark_price: 3000.25,
                mark_price_found: false,
            },
        ],
        send_corr_markets: true,
        corr_prices: vec![CorrMarketPriceUpdate {
            bn_market_name: "DOGEBTC".to_string(),
            last_price: 0.0000001,
        }],
    };
    let buf = build_markets_prices_response_with_local_shift(&resp, 0.0);
    let parsed = parse_markets_prices_response_with_local_shift(&buf, 0.0).unwrap();
    assert!(parsed.send_funding);
    assert_eq!(parsed.prices.len(), 2);
    assert_eq!(parsed.prices[0].bid, 50000.0);
    assert_eq!(parsed.prices[1].funding_rate, -0.0002);
    assert!(parsed.send_corr_markets);
    assert_eq!(parsed.corr_prices.len(), 1);
    assert_eq!(parsed.corr_prices[0].last_price, 0.0000001);
}

#[test]
fn markets_prices_response_no_funding_no_corr() {
    let resp = MarketsPricesResponse {
        send_funding: false,
        prices: vec![MarketPriceUpdate {
            m_index: 42,
            bid: 100.0,
            ask: 100.5,
            funding_rate: 0.0,
            funding_time: 0.0,
            mark_price: 100.25,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    };
    let buf = build_markets_prices_response_with_local_shift(&resp, 0.0);
    let parsed = parse_markets_prices_response_with_local_shift(&buf, 0.0).unwrap();
    assert!(!parsed.send_funding);
    assert_eq!(parsed.prices.len(), 1);
    assert_eq!(parsed.prices[0].m_index, 42);
    // funding_rate must be 0 when send_funding=false
    assert_eq!(parsed.prices[0].funding_rate, 0.0);
    assert!(!parsed.send_corr_markets);
}

#[test]
fn market_prices_parser_applies_delphi_local_funding_shift() {
    let resp = MarketsPricesResponse {
        send_funding: true,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 1.0,
            ask: 2.0,
            funding_rate: 0.1,
            funding_time: 45123.0,
            mark_price: 1.5,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    };
    let buf = build_markets_prices_response_with_local_shift(&resp, 0.0);

    let parsed = parse_markets_prices_response_with_local_shift(&buf, 180.0).unwrap();

    assert_eq!(parsed.prices[0].funding_time, 45123.0 + 180.0 / 1440.0);
}

#[test]
fn market_prices_writer_removes_delphi_local_funding_shift() {
    let resp = MarketsPricesResponse {
        send_funding: true,
        prices: vec![MarketPriceUpdate {
            m_index: 0,
            bid: 1.0,
            ask: 2.0,
            funding_rate: 0.1,
            funding_time: 45123.0,
            mark_price: 1.5,
            mark_price_found: true,
        }],
        send_corr_markets: false,
        corr_prices: vec![],
    };
    let buf = build_markets_prices_response_with_local_shift(&resp, 180.0);

    let wire = parse_markets_prices_response_with_local_shift(&buf, 0.0).unwrap();
    assert_eq!(wire.prices[0].funding_time, 45123.0 - 180.0 / 1440.0);

    let client = parse_markets_prices_response_with_local_shift(&buf, 180.0).unwrap();
    assert_eq!(client.prices[0].funding_time, 45123.0);
}

#[test]
fn markets_indexes_response_roundtrip() {
    let names = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "DOGEUSDT".to_string(),
    ];
    let buf = build_markets_indexes_response(&names);
    let parsed = parse_markets_indexes_response(&buf).unwrap();
    assert_eq!(parsed, names);
}

#[test]
fn token_tags_response_roundtrip() {
    let items = vec![
        MarketTokenTags {
            market_name: "BTCUSDT".to_string(),
            tags: TokenTags::MONITORING | TokenTags::ALPHA,
        },
        MarketTokenTags {
            market_name: "DOGEUSDT".to_string(),
            tags: TokenTags::GAMING | TokenTags::NEW,
        },
    ];
    let buf = build_token_tags_response(&items);
    let parsed = parse_token_tags_response(&buf).unwrap();
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].market_name, "BTCUSDT");
    assert!(parsed[0].tags.contains(TokenTags::MONITORING));
    assert!(parsed[0].tags.contains(TokenTags::ALPHA));
    assert!(parsed[1].tags.contains(TokenTags::GAMING));
    assert!(parsed[1].tags.contains(TokenTags::NEW));
}

#[test]
fn base_currency_preserves_raw_delphi_ordinal_byte() {
    assert_eq!(BaseCurrency::from_byte(0), BaseCurrency::BTC);
    assert_eq!(BaseCurrency::from_byte(1), BaseCurrency::USDT);
    assert_eq!(BaseCurrency::from_byte(8), BaseCurrency::USDC);
    assert_eq!(BaseCurrency::from_byte(25), BaseCurrency::EMPTY);
    assert_eq!(BaseCurrency::from_byte(26), BaseCurrency::UNKNOWN);
    assert_eq!(BaseCurrency::from_byte(99).to_byte(), 99);
    assert_ne!(
        BaseCurrency::from_byte(99),
        BaseCurrency::UNKNOWN,
        "Delphi stores the raw enum ordinal; Rust must not collapse unknown wire bytes"
    );
}
