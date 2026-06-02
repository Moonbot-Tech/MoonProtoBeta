use super::*;

fn header_bytes(cmd_id: u8, uid: u64) -> Vec<u8> {
    let mut v = vec![cmd_id];
    v.extend_from_slice(&CURRENT_PROTO_CMD_VER.to_le_bytes());
    v.extend_from_slice(&uid.to_le_bytes());
    v
}

#[test]
fn parse_settings_request() {
    let payload = header_bytes(CMD_SETTINGS_REQUEST, 99);
    match UICommand::parse(&payload).unwrap() {
        UICommand::SettingsRequest { uid } => assert_eq!(uid, 99),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn strat_start_stop_roundtrip() {
    let raw = build_strat_start_stop(7, true);
    match UICommand::parse(&raw).unwrap() {
        UICommand::StratStartStop(s) => {
            assert_eq!(s.uid, 7);
            assert!(s.is_start);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn strat_start_stop_v2_roundtrip() {
    let items = vec![
        StratCheckedItem {
            strategy_id: 10,
            checked: true,
        },
        StratCheckedItem {
            strategy_id: 20,
            checked: false,
        },
        StratCheckedItem {
            strategy_id: 30,
            checked: true,
        },
    ];
    let raw = build_strat_start_stop_v2(42, false, &items);
    match UICommand::parse(&raw).unwrap() {
        UICommand::StratStartStopV2(s) => {
            assert_eq!(s.uid, 42);
            assert!(!s.is_start);
            assert_eq!(s.items, items);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn mm_orders_subscribe_roundtrip() {
    let raw = build_mm_orders_subscribe(1, true);
    match UICommand::parse(&raw).unwrap() {
        UICommand::MMOrdersSubscribe(m) => assert!(m.subscribe),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn update_version_roundtrip() {
    let raw = build_update_version(2, "MoonBot-7.99", true);
    match UICommand::parse(&raw).unwrap() {
        UICommand::UpdateVersion(u) => {
            assert_eq!(u.version_name, "MoonBot-7.99");
            assert!(u.is_release);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn emu_trade_point_uses_private_wire_struct() {
    assert_eq!(std::mem::size_of::<WireEmuTradePoint>(), 6);
    assert_eq!(EMU_TRADE_POINT_SIZE, 6);

    let point = EmuTradePoint {
        time_delta_ms: 65535,
        price: -0.0,
    };
    let mut bytes = Vec::new();
    point.write_to(&mut bytes);

    let mut expected = Vec::new();
    expected.extend_from_slice(&65535u16.to_le_bytes());
    expected.extend_from_slice(&(-0.0f32).to_le_bytes());
    assert_eq!(bytes, expected);

    let parsed = EmuTradePoint::from_bytes(&bytes).expect("valid TEmuTradePoint");
    assert_eq!(parsed.time_delta_ms, 65535);
    assert_eq!(parsed.price.to_bits(), (-0.0f32).to_bits());
}

#[test]
fn emu_trade_point_public_constructors_encode_side() {
    let buy = EmuTradePoint::buy(10, -100.5);
    assert_eq!(buy.time_delta_ms, 10);
    assert_eq!(buy.abs_price(), 100.5);
    assert!(!buy.is_sell());

    let sell = EmuTradePoint::sell(20, 101.25);
    assert_eq!(sell.time_delta_ms, 20);
    assert_eq!(sell.abs_price(), 101.25);
    assert!(sell.is_sell());
}

#[test]
fn emu_trades_roundtrip() {
    let points = vec![
        EmuTradePoint {
            time_delta_ms: 0,
            price: 100.5,
        },
        EmuTradePoint {
            time_delta_ms: 1500,
            price: -101.2,
        }, // sell
        EmuTradePoint {
            time_delta_ms: 3000,
            price: 99.8,
        },
    ];
    let raw = build_emu_trades(3, 42, 45123.5, &points);
    match UICommand::parse(&raw).unwrap() {
        UICommand::EmuTrades(e) => {
            assert_eq!(e.m_index, 42);
            assert_eq!(e.base_time, 45123.5);
            assert_eq!(e.points, points);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn emu_trades_builder_never_wraps_word_count() {
    let points = vec![EmuTradePoint::buy(0, 1.0); usize::from(u16::MAX) + 1];
    let raw = build_emu_trades(3, 42, 45123.5, &points);
    let count_pos = 11 + 2 + 8;
    let count = u16::from_le_bytes([raw[count_pos], raw[count_pos + 1]]);
    assert_eq!(count, u16::MAX);
    assert_eq!(raw.len(), count_pos + 2 + usize::from(u16::MAX) * 6);
}

#[test]
fn lev_manage_roundtrip() {
    let cmd = LevManage {
        uid: 5,
        cmd_ver: 77,
        auto_max_order: true,
        auto_lev_up: false,
        auto_isolated: true,
        auto_cross: false,
        auto_fix_lev: true,
        fix_lev: 25,
        tlg_report: true,
        lev_control: "BTC,ETH".to_string(),
    };
    let raw = build_lev_manage(5, &cmd);
    match UICommand::parse(&raw).unwrap() {
        UICommand::LevManage(l) => {
            assert_eq!(l.uid, 5);
            assert_eq!(l.cmd_ver, 1);
            assert!(l.auto_max_order);
            assert!(!l.auto_lev_up);
            assert!(l.auto_isolated);
            assert_eq!(l.fix_lev, 25);
            assert_eq!(l.lev_control, "BTC,ETH");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn trigger_manage_roundtrip() {
    let markets = vec![1u16, 2, 3, 4, 5];
    let keys = vec![10u16, 20, 30];
    let raw = build_trigger_manage(11, 1, false, &markets, &keys);
    match UICommand::parse(&raw).unwrap() {
        UICommand::TriggerManage(t) => {
            assert_eq!(t.action, 1);
            assert!(!t.all_markets);
            assert_eq!(t.markets, markets);
            assert_eq!(t.keys, keys);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
// parity: MoonBot MoonProtoUIStruct.pas (TStratStartStopCommandV2/TEmuTradesCommand/TTriggerManageCommand CreateFromStream)
fn ui_word_count_parsers_keep_declared_count_with_zero_tail() {
    let mut raw = header_bytes(CMD_STRAT_START_STOP_V2, 42);
    raw.push(1);
    raw.extend_from_slice(&2u16.to_le_bytes());
    raw.extend_from_slice(&10u64.to_le_bytes());
    raw.push(1);
    match UICommand::parse(&raw).unwrap() {
        UICommand::StratStartStopV2(s) => {
            assert_eq!(
                s.items,
                vec![
                    StratCheckedItem {
                        strategy_id: 10,
                        checked: true,
                    },
                    StratCheckedItem {
                        strategy_id: 0,
                        checked: false,
                    },
                ]
            );
        }
        _ => panic!("wrong variant"),
    }

    let mut raw = header_bytes(CMD_EMU_TRADES, 43);
    raw.extend_from_slice(&7u16.to_le_bytes());
    raw.extend_from_slice(&45123.5f64.to_le_bytes());
    raw.extend_from_slice(&2u16.to_le_bytes());
    raw.extend_from_slice(&123u16.to_le_bytes());
    raw.extend_from_slice(&(-77.5f32).to_le_bytes());
    match UICommand::parse(&raw).unwrap() {
        UICommand::EmuTrades(e) => {
            assert_eq!(
                e.points,
                vec![
                    EmuTradePoint {
                        time_delta_ms: 123,
                        price: -77.5,
                    },
                    EmuTradePoint {
                        time_delta_ms: 0,
                        price: 0.0,
                    },
                ]
            );
        }
        _ => panic!("wrong variant"),
    }

    let mut raw = header_bytes(CMD_TRIGGER_MANAGE, 44);
    raw.push(1);
    raw.push(0);
    raw.extend_from_slice(&2u16.to_le_bytes());
    raw.extend_from_slice(&123u16.to_le_bytes());
    match UICommand::parse(&raw).unwrap() {
        UICommand::TriggerManage(t) => {
            assert_eq!(t.markets, vec![123, 0]);
            assert_eq!(
                t.keys,
                vec![0, 0],
                "Delphi reuses the previous local Count when the second Count read gets EOF"
            );
        }
        _ => panic!("wrong variant"),
    }

    let mut raw = header_bytes(CMD_TRIGGER_MANAGE, 45);
    raw.push(1);
    raw.push(0);
    raw.extend_from_slice(&1u16.to_le_bytes());
    raw.extend_from_slice(&123u16.to_le_bytes());
    raw.extend_from_slice(&2u16.to_le_bytes());
    raw.extend_from_slice(&9u16.to_le_bytes());
    match UICommand::parse(&raw).unwrap() {
        UICommand::TriggerManage(t) => {
            assert_eq!(t.markets, vec![123]);
            assert_eq!(t.keys, vec![9, 0]);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
// parity: MoonBot MoonProtoUIStruct.pas (fixed-scalar UI command CreateFromStream)
fn ui_fixed_scalar_commands_use_zero_tail() {
    match UICommand::parse(&header_bytes(CMD_STRAT_START_STOP, 1)).unwrap() {
        UICommand::StratStartStop(s) => assert!(!s.is_start),
        _ => panic!("wrong variant"),
    }

    match UICommand::parse(&header_bytes(CMD_MM_ORDERS_SUBSCRIBE, 2)).unwrap() {
        UICommand::MMOrdersSubscribe(s) => assert!(!s.subscribe),
        _ => panic!("wrong variant"),
    }

    let mut raw = header_bytes(CMD_UPDATE_VERSION, 3);
    raw.extend_from_slice(&0u16.to_le_bytes());
    match UICommand::parse(&raw).unwrap() {
        UICommand::UpdateVersion(s) => {
            assert_eq!(s.version_name, "");
            assert!(!s.is_release);
        }
        _ => panic!("wrong variant"),
    }

    let mut raw = header_bytes(CMD_EMU_TRADES, 4);
    raw.push(0x34);
    match UICommand::parse(&raw).unwrap() {
        UICommand::EmuTrades(e) => {
            assert_eq!(e.m_index, 0x34);
            assert_eq!(e.base_time.to_bits(), 0);
            assert!(e.points.is_empty());
        }
        _ => panic!("wrong variant"),
    }

    match UICommand::parse(&header_bytes(CMD_RESET_PROFIT, 5)).unwrap() {
        UICommand::ResetProfit(r) => assert_eq!(r.reset_kind, 0),
        _ => panic!("wrong variant"),
    }

    let mut raw = header_bytes(CMD_ARB_ACTIVATE_NOTIFY, 6);
    raw.push(1);
    match UICommand::parse(&raw).unwrap() {
        UICommand::ArbActivateNotify(a) => assert_eq!(a.arb_valid.to_bits(), 1),
        _ => panic!("wrong variant"),
    }

    match UICommand::parse(&header_bytes(CMD_SWITCH_DEX, 7)).unwrap() {
        UICommand::SwitchDex(s) => assert_eq!(s.dex_name, ""),
        _ => panic!("wrong variant"),
    }

    let mut raw = header_bytes(CMD_SWITCH_DEX, 8);
    raw.extend_from_slice(&[3, b'A']);
    match UICommand::parse(&raw).unwrap() {
        UICommand::SwitchDex(s) => assert_eq!(s.dex_name.as_bytes(), b"A\0\0"),
        _ => panic!("wrong variant"),
    }

    match UICommand::parse(&header_bytes(CMD_SWITCH_SPOT, 9)).unwrap() {
        UICommand::SwitchSpot(s) => assert_eq!(s.spot_index, SpotMarketKind::Crypto),
        _ => panic!("wrong variant"),
    }
}

#[test]
// parity: MoonBot MoonProtoUIStruct.pas:TStratStartStopCommandV2.CreateFromStream
fn word_count_builders_write_only_declared_wrapped_count() {
    let items: Vec<_> = (0..65_537u64)
        .map(|i| StratCheckedItem {
            strategy_id: i + 100,
            checked: i % 2 == 0,
        })
        .collect();
    let raw = build_strat_start_stop_v2(42, true, &items);
    assert_eq!(raw.len(), 11 + 1 + 2 + 9);
    match UICommand::parse(&raw).unwrap() {
        UICommand::StratStartStopV2(s) => {
            assert!(s.is_start);
            assert_eq!(s.items, vec![items[0]]);
        }
        _ => panic!("wrong variant"),
    }

    let markets: Vec<_> = (0..65_537usize).map(|i| i as u16).collect();
    let keys: Vec<_> = (0..65_537usize)
        .map(|i| i.wrapping_add(900) as u16)
        .collect();
    let raw = build_trigger_manage(11, 1, false, &markets, &keys);
    assert_eq!(raw.len(), 11 + 1 + 1 + 2 + 2 + 2 + 2);
    match UICommand::parse(&raw).unwrap() {
        UICommand::TriggerManage(t) => {
            assert_eq!(t.markets, vec![markets[0]]);
            assert_eq!(t.keys, vec![keys[0]]);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn reset_profit_roundtrip() {
    let raw = build_reset_profit(8, 1);
    match UICommand::parse(&raw).unwrap() {
        UICommand::ResetProfit(r) => assert_eq!(r.reset_kind, 1),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn arb_activate_notify_roundtrip() {
    let raw = build_arb_activate_notify(9, 45678.25);
    match UICommand::parse(&raw).unwrap() {
        UICommand::ArbActivateNotify(a) => assert_eq!(a.arb_valid, 45678.25),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn manage_command_kinds_map_to_delphi_ordinals() {
    // Delphi TTriggerManageCommand.Action: 0 = Clear, 1 = Set.
    assert_eq!(TriggerAction::Clear.to_byte(), 0);
    assert_eq!(TriggerAction::Set.to_byte(), 1);
    // Delphi TResetProfitCommand.ResetKind: 0 = CurProfit, 1 = AllProfit.
    assert_eq!(ResetProfitKind::CurrentProfit.to_byte(), 0);
    assert_eq!(ResetProfitKind::AllProfit.to_byte(), 1);

    // The typed kind must produce exactly the same wire bytes as the raw ordinal.
    assert_eq!(
        build_reset_profit(8, ResetProfitKind::AllProfit.to_byte()),
        build_reset_profit(8, 1)
    );
    assert_eq!(
        build_trigger_manage(11, TriggerAction::Set.to_byte(), false, &[1u16], &[2u16]),
        build_trigger_manage(11, 1, false, &[1u16], &[2u16])
    );
}

#[test]
fn switch_dex_truncates_to_15() {
    let raw = build_switch_dex(13, "VeryLongDexName_OverflowExtra");
    match UICommand::parse(&raw).unwrap() {
        UICommand::SwitchDex(s) => {
            assert_eq!(s.uid, 13);
            assert_eq!(s.dex_name, "VeryLongDexName"); // 15 chars
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn switch_dex_short_name() {
    let raw = build_switch_dex(14, "Uni");
    match UICommand::parse(&raw).unwrap() {
        UICommand::SwitchDex(s) => assert_eq!(s.dex_name, "Uni"),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn switch_dex_invalid_utf8_uses_delphi_question_mark_fallback() {
    let mut raw = Vec::new();
    write_header(&mut raw, CMD_SWITCH_DEX, 16);
    raw.push(4);
    raw.extend_from_slice(&[b'D', 0xFF, b'X', 0x80]);
    raw.extend_from_slice(&[0; 11]);

    match UICommand::parse(&raw).unwrap() {
        UICommand::SwitchDex(s) => assert_eq!(s.dex_name, "D?X?"),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn switch_spot_roundtrip() {
    let raw = build_switch_spot(15, 1);
    match UICommand::parse(&raw).unwrap() {
        UICommand::SwitchSpot(s) => assert_eq!(s.spot_index, SpotMarketKind::Predict),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn new_market_notify_empty() {
    let raw = build_new_market_notify(20);
    match UICommand::parse(&raw).unwrap() {
        UICommand::NewMarketNotify(n) => assert_eq!(n.uid, 20),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn client_settings_roundtrip_full() {
    let mut wanted = [false; 256];
    wanted[0] = true;
    wanted[1] = true;
    wanted[100] = true;
    wanted[255] = true;

    let cmd = ClientSettingsCommand {
        uid: 1,
        x_sell: 50,
        x_sell_scalp: 10,
        x_tmode: true,
        fixed_sell_mode: false,
        fixed_sell_price: 0.05,
        price_drop_level: 1.5,
        trailing_drop: 0.5,
        g_take_profit: 100.0,
        use_g_take_profit: true,
        unused_spread: 0,
        panic_if_price_drop: true,
        emu_mode: false,
        buy_iceberg: true,
        sell_iceberg: false,
        sign_orders: true,
        coins_black_list_text: "BTC,ETH".to_string(),
        use_coins_black_list: true,
        temp_bl_symbols: vec!["DOGE".to_string(), "SHIB".to_string()],
        temp_bl_times: vec![0.001, 0.002],
        use_manual_strategy: true,
        manual_strategy_id: 9999,
        free_position_check: true,
        vol_drop_level: 50,
        use_stop_market: true,
        as_cfg: vec![0xAAu8; AS_CFG_SIZE],
        as_cfg2: vec![0xBBu8; AS_CFG2_SIZE],
        s_price: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        sb_num: 7,
        join_sell_kind: 2,
        arb_config: ArbConfigCompact {
            wanted,
            show_absolute: true,
            show_numbers: false,
            show_lines: true,
            show_percent: false,
            show_right: true,
        },
    };
    let raw = build_client_settings(&cmd);
    match UICommand::parse(&raw).unwrap() {
        UICommand::ClientSettings(p) => {
            assert_eq!(p.uid, 1);
            assert_eq!(p.x_sell, 50);
            assert_eq!(p.fixed_sell_price, 0.05);
            assert!(p.buy_iceberg);
            assert!(!p.sell_iceberg);
            assert!(p.sign_orders);
            assert_eq!(p.coins_black_list_text, "BTC,ETH");
            assert_eq!(
                p.temp_bl_symbols,
                vec!["DOGE".to_string(), "SHIB".to_string()]
            );
            assert_eq!(p.temp_bl_times, vec![0.001, 0.002]);
            assert_eq!(p.manual_strategy_id, 9999);
            assert_eq!(p.s_price, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
            assert_eq!(p.sb_num, 7);
            assert_eq!(p.join_sell_kind, 2);
            assert_eq!(p.as_cfg.len(), AS_CFG_SIZE);
            assert_eq!(p.as_cfg2.len(), AS_CFG2_SIZE);
            assert!(p.arb_config.wanted[0]);
            assert!(p.arb_config.wanted[1]);
            assert!(p.arb_config.wanted[100]);
            assert!(p.arb_config.wanted[255]);
            assert!(!p.arb_config.wanted[2]);
            assert!(p.arb_config.show_absolute);
            assert!(!p.arb_config.show_numbers);
            assert!(p.arb_config.show_lines);
            assert!(!p.arb_config.show_percent);
            assert!(p.arb_config.show_right);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn client_settings_ui_helpers_match_delphi_meaning() {
    let mut settings = ClientSettingsCommand {
        x_sell: 50,
        x_sell_scalp: 10,
        x_tmode: true,
        fixed_sell_mode: false,
        fixed_sell_price: 12.0,
        s_price: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        sb_num: 9,
        join_sell_kind: 2,
        temp_bl_symbols: vec!["DOGE".to_string(), "SHIB".to_string()],
        temp_bl_times: vec![0.5, 0.25],
        ..ClientSettingsCommand::default()
    };

    assert_eq!(settings.effective_take_profit_percent(), 500.0);
    settings.x_sell = 0;
    assert_eq!(settings.effective_take_profit_percent(), 0.2);
    settings.fixed_sell_mode = true;
    assert_eq!(settings.effective_take_profit_percent(), 60.0);

    assert_eq!(settings.selected_fixed_sell_slot(), 6);
    assert_eq!(settings.selected_fixed_sell_price(), 6.0);
    assert_eq!(settings.selected_fixed_sell_percent(), 60.0);
    assert_eq!(settings.fixed_sell_preset_percent(2), Some(20.0));
    assert_eq!(settings.fixed_sell_preset_percent(0), None);
    assert_eq!(
        settings.fixed_sell_presets(),
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    );

    assert_eq!(settings.join_sell_mode(), JoinSellKind::FixedProfit);
    settings.set_join_sell_mode(JoinSellKind::FixedPrice);
    assert_eq!(settings.join_sell_kind, 1);
    assert_eq!(JoinSellKind::from_byte(7).to_byte(), 7);
    assert_eq!(JoinSellKind::FixedProfit.label(), "Fixed Profit");

    let entries: Vec<_> = settings.temp_blacklist_entries().collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].symbol, "DOGE");
    assert_eq!(entries[0].remaining_hours(), 12.0);
    assert_eq!(entries[1].symbol, "SHIB");
    assert_eq!(entries[1].remaining_hours(), 6.0);
}

#[test]
fn client_settings_autostart_helpers_match_delphi_layout() {
    let mut settings = ClientSettingsCommand::default();
    let cfg = AutoStartConfig {
        auto_start: true,
        auto_detect_on: true,
        strategies_on: false,
        work_time: true,
        auto_stop_if_loss: true,
        remember_state: false,
        sell_if_loss: true,
        dont_wait_sells: true,
        auto_stop_loss: 12.5,
        panic_btc: true,
        panic_market: false,
        auto_stop_if_loss_hours: true,
        auto_update: true,
        restart_after_err: true,
        restart_after_ping: false,
        ignore_emulator: true,
        stop_trades: 17,
        restart_err_time: 25,
        panic_btc_delta: -1.25,
        panic_market_delta: 2.5,
        auto_stop_on_errors: true,
        auto_stop_on_ping: true,
        sell_all_on_errors: false,
        sell_all_on_ping: true,
        errors_level: 9,
        ping_level: 11,
        restart_ping_time: 60,
        auto_stop_hours_val: -3.5,
        stop_hours: 4,
        stop_hours_trades: 5,
        panic_btc_delta_up: 6.75,
        work_time_from: 0.25,
        work_time_to: 0.75,
    };

    settings.set_auto_start_config(cfg.clone());
    assert_eq!(settings.as_cfg.len(), AS_CFG_SIZE);
    assert_eq!(settings.auto_start_config(), cfg);

    assert_eq!(&settings.as_cfg[0..8], &[1, 1, 0, 1, 1, 0, 1, 1]);
    assert_eq!(&settings.as_cfg[8..16], &12.5f64.to_le_bytes());
    assert_eq!(settings.as_cfg[16], 1); // PanicBTC
    assert_eq!(settings.as_cfg[23], 0); // Delphi alignment pad before integers
    assert_eq!(&settings.as_cfg[24..28], &17i32.to_le_bytes());
    assert_eq!(&settings.as_cfg[32..40], &(-1.25f64).to_le_bytes());
    assert_eq!(&settings.as_cfg[96..104], &0.75f64.to_le_bytes());

    settings.update_auto_start_config(|c| {
        c.auto_start = false;
        c.stop_hours = 8;
    });
    let edited = settings.auto_start_config();
    assert!(!edited.auto_start);
    assert_eq!(edited.stop_hours, 8);
    assert_eq!(edited.auto_stop_loss, 12.5);
}

#[test]
fn client_settings_autostart2_helpers_preserve_reserved_tail() {
    let mut settings = ClientSettingsCommand {
        as_cfg2: vec![0xCC; AS_CFG2_SIZE],
        ..ClientSettingsCommand::default()
    };

    settings.set_auto_start_config2(AutoStartConfig2 {
        restart_on_market: true,
        btc_higher_then: 1.25,
        btc_lower_then: -2.5,
        market_higher_then: 3.75,
        show_old_listing: true,
        reset_session: true,
        max_session_cap: 1000,
        rs_hours: 12,
    });

    assert_eq!(settings.as_cfg2.len(), AS_CFG2_SIZE);
    assert_eq!(settings.as_cfg2[0], 1);
    assert_eq!(&settings.as_cfg2[8..16], &1.25f64.to_le_bytes());
    assert_eq!(&settings.as_cfg2[16..24], &(-2.5f64).to_le_bytes());
    assert_eq!(&settings.as_cfg2[24..32], &3.75f64.to_le_bytes());
    assert_eq!(settings.as_cfg2[32], 1);
    assert_eq!(settings.as_cfg2[41], 1);
    assert_eq!(&settings.as_cfg2[76..80], &1000i32.to_le_bytes());
    assert_eq!(&settings.as_cfg2[80..84], &12i32.to_le_bytes());

    assert_eq!(&settings.as_cfg2[33..41], &[0xCC; 8]);
    assert_eq!(&settings.as_cfg2[42..44], &[0xCC; 2]);
    assert_eq!(&settings.as_cfg2[44..76], &[0xCC; 32]);
    assert_eq!(&settings.as_cfg2[84..88], &[0xCC; 4]);
    assert_eq!(&settings.as_cfg2[88..168], &[0xCC; 80]);

    let decoded = settings.auto_start_config2();
    assert!(decoded.restart_on_market);
    assert_eq!(decoded.btc_higher_then, 1.25);
    assert_eq!(decoded.btc_lower_then, -2.5);
    assert_eq!(decoded.market_higher_then, 3.75);
    assert!(decoded.show_old_listing);
    assert!(decoded.reset_session);
    assert_eq!(decoded.max_session_cap, 1000);
    assert_eq!(decoded.rs_hours, 12);
}

#[test]
fn client_settings_soft_tail_uses_delphi_cfg_fallback() {
    let mut raw = Vec::new();
    raw.push(CMD_CLIENT_SETTINGS);
    raw.extend_from_slice(&1u16.to_le_bytes());
    raw.extend_from_slice(&7u64.to_le_bytes());
    raw.extend_from_slice(&[0u8; 41]);
    write_string(&mut raw, "");
    raw.push(0);
    raw.extend_from_slice(&0i32.to_le_bytes());

    let fallback = ClientSettingsCommand {
        sign_orders: false,
        free_position_check: true,
        vol_drop_level: 77,
        use_stop_market: true,
        as_cfg: vec![0xAA, 0xAB],
        as_cfg2: vec![0xBA, 0xBB],
        s_price: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        sb_num: 9,
        join_sell_kind: 2,
        ..ClientSettingsCommand::default()
    };

    match UICommand::parse_with_client_settings_fallback(&raw, Some(&fallback)).unwrap() {
        UICommand::ClientSettings(p) => {
            assert_eq!(p.uid, 7);
            assert!(!p.sign_orders, "ver<2 keeps Delphi cfg SignOrders");
            assert!(!p.use_manual_strategy);
            assert_eq!(p.manual_strategy_id, 0);
            assert!(p.free_position_check);
            assert_eq!(p.vol_drop_level, 77);
            assert!(p.use_stop_market);
            assert_eq!(p.as_cfg, vec![0xAA, 0xAB]);
            assert_eq!(p.as_cfg2, vec![0xBA, 0xBB]);
            assert_eq!(p.s_price, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
            assert_eq!(p.sb_num, 9);
            assert_eq!(p.join_sell_kind, 2);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn client_settings_short_ascfg_overlays_delphi_cfg_fallback() {
    let mut raw = Vec::new();
    raw.push(CMD_CLIENT_SETTINGS);
    raw.extend_from_slice(&3u16.to_le_bytes());
    raw.extend_from_slice(&7u64.to_le_bytes());
    raw.extend_from_slice(&[0u8; 41]);
    raw.extend_from_slice(&[0u8; 3]); // BuyIceberg, SellIceberg, SignOrders
    write_string(&mut raw, "");
    raw.push(0);
    raw.extend_from_slice(&0i32.to_le_bytes());
    raw.push(0); // UseManualStrategy
    raw.extend_from_slice(&0u64.to_le_bytes());
    raw.push(0); // FreePositionCheck
    raw.extend_from_slice(&0i32.to_le_bytes());
    raw.push(0); // UseStopMarket
    raw.extend_from_slice(&2u16.to_le_bytes());
    raw.extend_from_slice(&[0x11, 0x22]);

    let fallback_as_cfg: Vec<u8> = (0..AS_CFG_SIZE).map(|i| i as u8).collect();
    let fallback_as_cfg2: Vec<u8> = (0..AS_CFG2_SIZE).map(|i| 255u8 - i as u8).collect();
    let fallback = ClientSettingsCommand {
        as_cfg: fallback_as_cfg.clone(),
        as_cfg2: fallback_as_cfg2.clone(),
        ..ClientSettingsCommand::default()
    };

    match UICommand::parse_with_client_settings_fallback(&raw, Some(&fallback)).unwrap() {
        UICommand::ClientSettings(p) => {
            assert_eq!(p.as_cfg.len(), AS_CFG_SIZE);
            assert_eq!(&p.as_cfg[..2], &[0x11, 0x22]);
            assert_eq!(&p.as_cfg[2..], &fallback_as_cfg[2..]);
            assert_eq!(p.as_cfg2, fallback_as_cfg2);
        }
        _ => panic!("wrong variant"),
    }
}

fn client_settings_v1_prefix_with_temp_bl_count(count: i32) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.push(CMD_CLIENT_SETTINGS);
    raw.extend_from_slice(&1u16.to_le_bytes());
    raw.extend_from_slice(&7u64.to_le_bytes());
    raw.extend_from_slice(&[0u8; 41]);
    write_string(&mut raw, "");
    raw.push(0);
    raw.extend_from_slice(&count.to_le_bytes());
    raw
}

#[test]
// parity: MoonBot MoonProtoUIStruct.pas:TClientSettingsCommand.CreateFromStream
fn client_settings_accepts_tail_after_blacklist_string() {
    let mut raw = Vec::new();
    raw.push(CMD_CLIENT_SETTINGS);
    raw.extend_from_slice(&1u16.to_le_bytes());
    raw.extend_from_slice(&7u64.to_le_bytes());
    raw.extend_from_slice(&[0u8; 41]);
    write_string(&mut raw, "");

    let fallback = ClientSettingsCommand {
        free_position_check: true,
        vol_drop_level: 77,
        use_stop_market: true,
        ..ClientSettingsCommand::default()
    };

    match UICommand::parse_with_client_settings_fallback(&raw, Some(&fallback)).unwrap() {
        UICommand::ClientSettings(p) => {
            assert!(!p.use_coins_black_list);
            assert!(p.temp_bl_symbols.is_empty());
            assert!(p.free_position_check);
            assert_eq!(p.vol_drop_level, 77);
            assert!(p.use_stop_market);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
// parity: MoonBot MoonProtoUIStruct.pas:TClientSettingsCommand.CreateFromStream
fn client_settings_temp_bl_time_zero_tails_after_valid_string() {
    let mut raw = client_settings_v1_prefix_with_temp_bl_count(1);
    write_string(&mut raw, "");

    match UICommand::parse(&raw).unwrap() {
        UICommand::ClientSettings(p) => {
            assert_eq!(p.temp_bl_symbols, vec!["".to_string()]);
            assert_eq!(p.temp_bl_times, vec![0.0]);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
// parity: MoonBot MoonProtoUIStruct.pas:TClientSettingsCommand.CreateFromStream
fn client_settings_soft_tail_preserves_existing_i32_high_bytes() {
    let mut raw = client_settings_v1_prefix_with_temp_bl_count(0);
    raw.push(0); // UseManualStrategy
    raw.extend_from_slice(&0u64.to_le_bytes());
    raw.push(1); // FreePositionCheck
    raw.push(0xAA); // first byte of VolDropLevel only

    let fallback = ClientSettingsCommand {
        vol_drop_level: 0x1122_3344,
        ..ClientSettingsCommand::default()
    };

    match UICommand::parse_with_client_settings_fallback(&raw, Some(&fallback)).unwrap() {
        UICommand::ClientSettings(p) => {
            assert!(p.free_position_check);
            assert_eq!(p.vol_drop_level, 0x1122_33AA);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn client_settings_rejects_impossible_temp_bl_count_without_silent_truncate() {
    let mut raw = client_settings_v1_prefix_with_temp_bl_count(2);
    write_string(&mut raw, "A");
    raw.extend_from_slice(&1.0f64.to_le_bytes());

    assert!(
            UICommand::parse(&raw).is_none(),
            "Delphi reads exactly TempBLCount items; Rust must not truncate count and parse tail at a wrong offset"
        );
}

#[test]
fn client_settings_rejects_negative_temp_bl_count_like_corrupt_stream() {
    let raw = client_settings_v1_prefix_with_temp_bl_count(-1);

    assert!(UICommand::parse(&raw).is_none());
}

#[test]
// parity: MoonBot MoonProtoBaseStruct.pas:TCommandRegistry.FromStream
fn version_gate_returns_skipped() {
    let mut payload = vec![CMD_CLIENT_SETTINGS, 99, 0];
    payload.extend_from_slice(&77u64.to_le_bytes());
    match UICommand::parse(&payload).unwrap() {
        UICommand::Skipped { cmd_id, uid, ver } => {
            assert_eq!(cmd_id, CMD_CLIENT_SETTINGS);
            assert_eq!(uid, 77);
            assert_eq!(ver, 99);
        }
        _ => panic!("wrong variant"),
    }
}
