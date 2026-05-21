# MAPPING.md — moonproto: доказательство полноты порта

> Пока есть ❌ — этап НЕ ЗАВЕРШЁН.

## UDPRead (MoonProtoUDPClient.pas:454-665) → client.rs poll_recv + handle_command

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 1 | 482-484 | DNS/service packet → ignore | transport_unpack returns None | ✅ |
| 2 | 487-492 | MaskVer=1 STUN unwrap | transport_unpack → ext_unwrap | ✅ |
| 3 | 494-498 | Size < header → drop | transport_unpack checks len | ✅ |
| 4 | 500 | OuterLightCrypt decrypt | transport_unpack → outer_light_crypt | ✅ |
| 5 | 502-517 | MAC verify | transport_unpack → calculate_mac32 | ✅ |
| 6 | 519-526 | Version check (!=3 → drop) | transport_unpack → ver != TRANSPORT_VER | ✅ |
| 7 | 528 | Inc TotalRecvBytes | client.rs: self.total_recv += n | ✅ |
| 8 | 531 | LastOnline := GetTimeMS | client.rs: self.last_online = now_ms | ✅ |
| 9 | 543 | DoCleanUp (clear old Receiving) | ✅ client.rs: slicer.clear_old() every 5s |
| 10 | 545-546 | Handshake cmds → FWaitingHello := false | client.rs handle_handshake sets waiting_hello=false | ✅ |
| 11 | 548-553 | MPC_WrongHello → MPS_Connected | client.rs:294 | ✅ |
| 12 | 555-565 | MPC_WantNewHello → Reset + NeedConnect + `LastSentHello := 0` как немедленный retry sentinel | client.rs: full_reset() + flags + `NEVER_SENT_MS`; `Sending` cleanup отличается, см. DEVIATION #27 | DEVIATION |
| 13 | 567-576 | MPC_NeedHelloAgain (700ms throttle) + `LastSentHello := 0` как немедленный retry sentinel | client.rs: last_need_hello_again + 700ms check + `NEVER_SENT_MS` | ✅ |
| 14 | 578-581 | WhoAreYou/Fine → HandleHandShake | client.rs handle_handshake | ✅ |
| 15 | 583-591 | MPC_SizeTest → SendSizeAck | client.rs handle_size_test | ✅ |
| 15a | 347-351 | DoSendPacket: `Package Size Too Big` не рвёт соединение | client.rs send_raw_packet ignores datagram-too-large errors | ✅ |
| 16 | 594-617 | MPC_ProbeMTU → ProbeMTUAck (DontFragment!), `ReceivedSize := TestSize` без upper clamp | client.rs: handle_probe_mtu echoes `test_size` verbatim for valid record size | ✅ |
| 17 | 620-625 | MPC_Sliced → OnNewSliced | client.rs handle Sliced | ✅ |
| 18 | 627-629 | MPC_SlicedACK → OnNewSlicedACK | client.rs: match arm (no-op, client doesn't send Sliced yet) | ✅ |
| 19 | 632-661 | MPC_Ping → update RTT/PMTU/OverHeat/RS + rate control | client.rs: handle_ping reads fields; `actual_pmtu = pmtu_raw` без clamp | ✅ |
| 20 | 663 | DataRead(cmd, data, client) | client.rs → on_data callback | ✅ |

## DataRead (MoonProtoCommon.pas:541-577) → client.rs handle_command

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 21 | 547 | MPC_Grouped → распаковка sub-команд циклом | client.rs: data_read if Grouped | ✅ |
| 22 | 554-564 | Grouped: read cmd(1)+sz(2)+data(sz), loop | client.rs: while loop in data_read | ✅ |
| 23 | 566-574 | Non-grouped: strip header, DataReadInt | client.rs: implicit (payload already stripped) | ✅ |

## DataReadInt (MoonProtoCommon.pas:488-538) → client.rs handle_command

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 24 | 497-500 | MPC_Crypted → DeCrypt | client.rs handle_crypted → crypted::decrypt_command | ✅ |
| 25 | 502-509 | IsCompressed → MPDecompress | client.rs: data_read_int checks COMPRESSED_FLAG → mp_decompress | ✅ |
| 26 | 513-528 | Ping → read TmpSlider (ACK bitmap from server) | client.rs: handle_ping reads ack_start + words inline | ✅ |
| 27 | 530-537 | OnNewData callback | client.rs → on_data | ✅ |

## Execute main loop (MoonProtoUDPClient.pas:669-828) → client.rs run()

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 28 | 682-721 | Bind socket (port rotation 1024-65000, 200 attempts) | client.rs bind_socket | ✅ |
| 29 | 703-706 | Socket buffers 8MB | client.rs: set_socket_buffers (setsockopt) | ✅ |
| 30 | 748-768 | NeedConnect: Hello or HelloAgain (interval = Max(1000,RTT)*2) | client.rs check_hello_send | ✅ |
| 31 | 771 | HelloAgainThrottle = Min(1500, Max(200, RTT+50)) | client.rs: check_offline_reconnect exact formula | ✅ |
| 32 | 772-785 | Offline detection → HelloAgain | client.rs: check_offline_reconnect | ✅ |
| 33 | 789-796 | HelloAgain timeout 7s → socket recreate | client.rs check_reconnect_timeout | ✅ |
| 34 | 799-804 | Dead zone (5s) → force reconnect | client.rs check_dead_zone | ✅ |
| 35 | 806-822 | ForceDisconnect: LogOff, close socket, Reset | client.rs: do_force_disconnect + full_reset; `Sending`/pending API cleanup отличается, см. DEVIATION #27 | DEVIATION |
| 36 | 826 | Sleep(DefaultNetThreadSleepTime = 5ms) | client.rs: sleep(5ms) | ✅ |

## SendPing (MoonProtoUDPClient.pas:213-232) → client.rs handle_ping (response)

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 37 | 221 | APing.Time := Now (adjusted) | client.rs: response[0..8] = delphi_now() | ✅ |
| 38 | 222 | APing.TotalSentBytes := AttemptedBytes | client.rs: response[25..33] = total_sent | ✅ |
| 39 | 223 | APing.TotalRecvBytes := TotalRecvBytes | client.rs: response[33..41] = total_recv | ✅ |
| 40 | 228-229 | BuildAckHalf → append ACK words | client.rs: slider.build_ack_half + extend | ✅ |

## HandleHandShake (MoonProtoUDPClient.pas:387-451) → client.rs handle_handshake

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 41 | 399 | Decode WhoAreYou with MasterKey, AAD=ClientID | client.rs: decrypt with `client_id.to_le_bytes()` AAD | ✅ |
| 42 | 416-419 | Save ServerToken, PeerAppToken | client.rs: self.server_token = ... | ✅ |
| 43 | 421-422 | Update Hello: MixTS, AppToken | client.rs: im.mix_ts, im.app_token | ✅ |
| 44 | 427 | GenerateSubKeys(MasterKey, ServerToken) | client.rs: generate_sub_keys | ✅ |
| 45 | 430-431 | FClient.Encode (session key, AAD=ClientID) | client.rs: encrypt with `client_id.to_le_bytes()` AAD | ✅ |
| 46 | 433-436 | SendCommand ImFriend x2 (Sleep 32ms) | client.rs: send x2, sleep 32ms | ✅ |
| 47 | 439-449 | MPC_Fine → AuthDone | client.rs: authorized=true, AuthDone | ✅ |

## SendHelloAgain (MoonProtoUDPClient.pas:193-211) → client.rs send_hello_again

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 48 | 199-200 | Hello.Init + MixTS = AtomicInc(ClientToken) | client.rs: Hello::new + client_token | ✅ |
| 49 | 204 | PeerMix = MixValues(Hello.Rnd, MixTS, ServerToken) | client.rs: mix_values(&hello.rnd, ...) | ✅ |
| 50 | 207 | FClient.Encode (session key) | client.rs: encrypt(&encode_key, ...) | ✅ |
| 51 | 208 | SendCommand MPC_HelloAgain | client.rs: send_packet(HelloAgain, ...) | ✅ |

## Compression (MoonProtoDataStruct.pas:283-358) → ???

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 52 | 283-316 | MPCompress (SynLZ / Deflate / RLE+SynLZ) | compression.rs + client.rs `maybe_compress` for outgoing payload >64B | ✅ |
| 53 | 319-358 | MPDecompress | compression.rs: synlz_decompress (algo 1) | ✅ |
| 53a | mORMot SynLZdecompress1pas local `offset: TOffsets` scratch | decompress offsets are per-call scratch, not persistent across packets | compression.rs resets thread-local offsets before each `synlz_decompress_inner` | ✅ |

## NTP time sync (IndyUDPHelper.pas + MoonProtoIntStruct.pas) → ntp.rs

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| NTP1 | IndyUDPHelper.pas:459-522 | `TMySNTP.GetBestNTP`: `BestDelay`, `ForceSync`, `ReceiveTimeout`, accept при `d < 50` / force / лучшем delay | ntp.rs: `NtpState` + `get_best_ntp_with_state` | ✅ |
| NTP2 | IndyUDPHelper.pas:489-496 | После `SyncedOnce` offset больше 1 минуты не принимается первые 2 раза, `TryCount` расширяется до 6 | ntp.rs: `large_offset_retry_count < 2`, `effective_try_count = min(6, +1)` | ✅ |
| NTP3 | IndyUDPHelper.pas:489-503 | Нет верхнего absolute cap на NTP offset; принятый sample записывается как `TimeOffset` | ntp.rs: удалён Rust-only `|offset| > 1 day` reject | ✅ |
| NTP4 | MoonProtoIntStruct.pas:1246-1302 | `TMoonProtoTymeSyncer.Execute`: initial sync, 5×100ms sleep, попытки при `GetTimeTryCount < 4`, reset после 1000 циклов | ntp.rs: `spawn_sync_thread` хранит `NtpState` и повторяет цикл | ✅ |

## UpdateChannelRDown (MoonProtoIntStruct.pas:1003-1055) → ???

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 54 | 1003-1055 | RS EMA update from TotalRecvBytes | N/A: server computes RS, client reads from Ping.RSQ | ✅ |

## ApplyRegularHLAck (MoonProtoIntStruct.pas:844-876) → ???

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 55 | 844-876 | Apply server's ACK slider to PendingH | client.rs: handle_ping applies server ACK to pending_h before H retry/send phase | ✅ |

## SendCmdInt / UKey dedup (MoonProtoCommon.pas:765-792, 896-939) → client.rs run loop

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 56 | MoonProtoCommon.pas:780-787,900-906,931-939 + MoonProtoIntStruct.pas:1152-1168 | `UKey != UK_None`: новая Sliced/High команда вытесняет старую с тем же ключом в очереди; при отправке чистит `Sending`/`PendingH` по ключу | client.rs: `dedup_send_items_by_u_key`, затем `sending.retain` / `pending_h.retain` перед `create_sliced_and_send` / `send_h_item` | ✅ |

## DoSendMPData / DoSendTmpList (MoonProtoCommon.pas:795-867, 933-939) → client.rs send_h_item / batch_send_direct

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 57 | 795-833 | Для H/L item сначала делает `Client.Crypt(data)` если нужно, затем считает `sz := d.ms.Size + GetHeaderSize + 3`; при overflow PMTU flush'ит batch или отправляет одиночный пакет | client.rs: `send_h_item` / `batch_send_direct` считают batch-size по encrypted/plain wire payload после `encrypt_with_cipher` | ✅ |

## CreateSlicedObject (MoonProtoIntStruct.pas:1058-1125) → client.rs create_sliced_and_send

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 58 | 1070-1075 | `PTMU := ActualPMTU - HeaderSize - SliceHeader`; если `data.ms.Size >= MaxSlicedDataSize(ActualPMTU)` → drop | client.rs: create_sliced_and_send computes payload PMTU with low-PMTU guard and uses `>= max_sliced_data_size` | ✅ |

---

## Итого Stage 1: 58 пунктов

- Все пункты закрыты или имеют подтверждённое описание поведения.
- AAD в handshake актуализирован: текущий Delphi `MakeCorrectAAD = true`, Rust
  передаёт `client_id` как AAD.
- DontFragment закрыт cross-platform реализацией, см. `DEVIATION #19`.
- MPCompress подключён через `client.rs::maybe_compress` для исходящих
  payload >64B (auto-compression, byte-exact с Delphi).

═══════════════════════════════════════════════════════════════════════════════

# STAGE 2 — Канальные команды

## Channel: Order (MoonProtoTradeStruct.pas → commands/trade.rs + state/orders.rs)

`TBaseTradeCommand` иерархия. 30 sub-commands (CmdId 1..30). Базовый header: `CmdId(1) + ver(2) + UID(8) = 11 bytes`. Packed records: `OrderCompact=117б`, `StopSettings=46б`, `OrderUpdateData=66б`.

| # | Delphi pas:line | Sub-command | Rust trade.rs:line | ✅ |
|---|---|---|---|---|
| O1 | MoonProtoTradeStruct.pas:78 | TBaseMarketCommand (CmdId=1) — currency:byte + platform:byte + market_name | TradeCommand::BaseMarket | ✅ |
| O2 | :104 | TTradeEpochCommand (CmdId=2) — +epoch:word + status:byte | TradeCommand::TradeEpoch | ✅ |
| O3 | :128 | TNewOrderCommand (CmdId=3) — +is_short:bool + price:double + strat_id:u64 + order_size:double | TradeCommand::NewOrder | ✅ |
| O4 | :151 | TOrderStatus (CmdId=4) — epoch_header + Buy/Sell:OrderCompact + Stops + strat_id + is_short + db_id + from_cache + (v2) emu + (v3) immune | TradeCommand::OrderStatus + OrderCompact(117б)/StopSettings(46б) | ✅ |
| O5 | :235 | TOrderStatusUpdate (CmdId=5) — epoch_header + UpdateData(66б) + (soft) sell_reason_code | TradeCommand::OrderStatusUpdate | ✅ |
| O6 | :291; :532-540 | TOrderReplaceCommand (CmdId=6) — epoch_header + order_type + new_price; C→S constructor forces `Epoch=0`, `Status=OS_None` | TradeCommand::OrderReplace + `build_order_replace` | ✅ |
| O7 | :308 | TOrderReplaceResponse (CmdId=7) — epoch_header + order_type + price + UpdateData + qty_base | TradeCommand::OrderReplaceResponse | ✅ |
| O8 | :350 | TAllStatuses (CmdId=8) — base_header + count:i32 + orders[]:OrderStatus | TradeCommand::AllStatuses | ✅ |
| O9 | :385 | TAllStatusesReq (CmdId=9) — base_header only | TradeCommand::AllStatusesReq | ✅ |
| O10 | :395 | TOrderCancelCommand (CmdId=10) — epoch_header | TradeCommand::OrderCancel | ✅ |
| O11 | :411 | TJoinOrdersCommand (CmdId=11) — market_header + is_short | TradeCommand::JoinOrders | ✅ |
| O12 | :428 | TSplitOrderCommand (CmdId=12) — market_header + split_parts + split_small + split_small_sell | TradeCommand::SplitOrder | ✅ |
| O13 | :451 | TMoveAllSellsCommand (CmdId=13) — market_header + cmd_type + move_kind + price + price_zone(min,max) + side | TradeCommand::MoveAllSells + PriceZone | ✅ |
| O14 | :493 | TDoClosePositionCommand (CmdId=14, MaxRetries=1) — market_header + market_sell | TradeCommand::DoClosePosition | ✅ |
| O15 | :513 | TDoLimitClosePositionCommand (CmdId=15, MaxRetries=1) — JoinOrders формат | TradeCommand::DoLimitClosePosition | ✅ |
| O16 | :533 | TDoSplitPositionCommand (CmdId=16, MaxRetries=1) — JoinOrders формат | TradeCommand::DoSplitPosition | ✅ |
| O17 | :553 | TDoSellOrderCommand (CmdId=17, MaxRetries=1) — market_header + price + size | TradeCommand::DoSellOrder | ✅ |
| O18 | :578 | TOrderStatusRequest (CmdId=18) — epoch_header | TradeCommand::OrderStatusRequest | ✅ |
| O19 | :594 | TOrderNotFound (CmdId=19) — epoch_header | TradeCommand::OrderNotFound | ✅ |
| O20 | :614 | TOrderStopsUpdate (CmdId=20) — epoch_header + StopSettings | TradeCommand::OrderStopsUpdate | ✅ |
| O21 | :634; :756-760 | TTurnPanicSellCommand (CmdId=21) — epoch_header + turn_on; C→S constructor leaves inherited epoch/status zero-initialized | TradeCommand::TurnPanicSell + `build_turn_panic_sell` | ✅ |
| O22 | :210-223,778-812 | TSetImmuneCommand (CmdId=22) — base_header + count:byte + items[uid:u64+value:bool]*count; UKey.UID=sum(items.uid) | TradeCommand::SetImmune + ImmuneItem; builder writes byte count without clamp | ✅ |
| O23 | :689 | TPenaltyCommand (CmdId=23) — market_header | TradeCommand::Penalty | ✅ |
| O24 | :702 | TTradeVisualCommand (CmdId=24) — market_header | TradeCommand::TradeVisual | ✅ |
| O25 | :716 | TOrderTracePoint (CmdId=25) — market_header + trace_time + trace_price/base_price/stop_price:single + ord_type + flags | TradeCommand::OrderTracePoint | ✅ |
| O26 | :754 | TCorridorUpdate (CmdId=26, Priority=Low) — market_header + price_down:single + price_up:single | TradeCommand::CorridorUpdate | ✅ |
| O27 | :775 | TMoveAllBuysCommand (CmdId=27) — market_header + cmd_type + move_kind + price + side (БЕЗ price_zone) | TradeCommand::MoveAllBuys | ✅ |
| O28 | :809 | TBulkReplaceNotify (CmdId=28) — market_header + order_type + count:word + uids[u64]*count | TradeCommand::BulkReplaceNotify | ✅ |
| O29 | :838 | TVStopUpdate (CmdId=29) — epoch_header + vstop_on/fixed:bool + vstop_level/vol:double | TradeCommand::VStopUpdate | ✅ |
| O30 | :869 | TDoMarketSplitPositionCommand (CmdId=30, MaxRetries=1) — JoinOrders формат | TradeCommand::DoMarketSplitPosition | ✅ |

### state/orders.rs apply logic
| # | Delphi src | Что | Rust orders.rs | ✅ |
|---|---|---|---|---|
| O-A1 | TaskWorkers.pas:1450-1666 ProcessCommandOrder | OrderStatus → create/update worker by UID | state/orders.rs:apply OrderStatus | ✅ |
| O-A2 | MoonProtoFunc.pas:188 EpochIsOK | `LastEpoch = NewEpoch` reject; `Word(LastEpoch - NewEpoch) <= 100` reject; otherwise accept | state/epoch.rs:epoch_is_ok | ✅ |
| O-A3 | TaskWorkers.pas:546 StatusPhase | Status → phase для rollback protection | state/orders.rs:status_phase | ✅ |
| O-A4 | MoonProtoClient.pas:553-555 | Filter not.FromCache AND m≠nil | DEVIATION #6 — Rust сохраняет все | DEVIATION |
| O-A5 | TaskWorkers.pas:1475-1666 setters | ApplyTo/Stops/VStop с побочными эффектами | DEVIATION #5 — Rust observer | DEVIATION |
| O-A6 | TaskWorkers.pas:7836-8167 TOrderNotFound | Сначала flag, удаление позже | DEVIATION #7 — Rust удаляет сразу + флаг | DEVIATION |
| O-A7 | MoonProtoClient.pas:570-590 CleanupMissingWorkers | После AllStatuses запросить статусы worker'ов, не пришедших в snapshot | state/orders.rs:missing_after_snapshot + events.rs:dispatch_into_active auto `request_order_status` | ✅ |
| O-A8 | server_time_delta correction | Ping.InitialTime - Now → applied к TDateTime fields | state/orders.rs:apply через server_time_delta | ✅ |
| O-A9 | TBulkReplaceNotify | Set flag на upcoming order replaces | state/orders.rs:apply BulkReplaceNotify | ✅ |
| O-A10 | TaskWorkers.pas:7836-8155,7400-7445 | Terminal order statuses remove worker/cache; `OS_SelLAlmostDone` is terminal like sell-done paths | commands/trade.rs:`OrderWorkerStatus::is_terminal`, state/orders.rs removal on terminal status | ✅ |

---

## Channel: OrderBook (MoonProtoOrderBook.pas → commands/order_book.rs + state/order_books.rs)

| # | Delphi | Что | Rust | ✅ |
|---|---|---|---|---|
| OB1 | MoonProtoOrderBook.pas:PackUpdate/WriteGlass | SynLZ compress + format | parse_order_book_packet (SynLZ decompress) | ✅ |
| OB2 | wire: market_idx(2) + seq(2) + flags(1) | header | parse_order_book_packet header | ✅ |
| OB3 | WriteGlass / MoonProto_ReadAndApplyFull/Diff | buy_count:Word + buys[]:(Single,Single) + sells[остаток] | OrderLevel { rate:f32, quantity:f32 } × n | ✅ |
| OB4 | flags bit 0 = Full vs Diff | type detection | OrderBookUpdate.is_full | ✅ |
| OB5 | TOrderBookCache.FindInsertPos/Add | sorted insert by `CompareSeq`, duplicate seq is inserted too | state/order_books.rs:binary_search_insert/add | ✅ |
| OB6 | BOOK_EXPIRED_TIMEOUT = 800ms | stale diff drop | state/order_books.rs:BOOK_EXPIRED_TIMEOUT | ✅ |
| OB7 | BOOK_FULL_REQUEST_THROTTLE = 5000ms | повторный full не чаще | state/order_books.rs:BOOK_FULL_REQUEST_THROTTLE | ✅ |
| OB8 | MoonProtoEngine.pas:ProcessOrderBookPacket normal mode | `(seq = ExpectedSeq) or (MoonProtoBookSeq = 0)` проверяется до stale-drop | state/order_books.rs: `cmp == 0 || last_applied_seq == 0` before stale branch | ✅ |
| OB9 | compare_seq wrapping math | u16 sequence comparison | state/order_books.rs:compare_seq | ✅ |
| OB10 | MoonProtoEngine.pas:ProcessOrderBookPacket corrupted mode | Apply diff as-is, then if count>=64 DropOldest, then cache Add, then throttled RequestFull | state/order_books.rs:corrupted branch + drop_oldest/add | ✅ |
| OB11 | MoonProtoEngine.pas:ProcessOrderBookPacket normal gap | Add gap packet; if cache expired OR count > 64 then Corrupted=true + TryRequestFull; cache is not cleared | state/order_books.rs:gap branch | ✅ |
| OB12 | MoonProtoOrderBook.pas:MoonProto_TryApplyCached | Drop stale cached packets, apply exact ExpectedSeq chain, stop at next gap | state/order_books.rs:drain_cache | ✅ |
| OB13 | MoonProtoEngine.pas:ProcessOrderBookPacket | `SrvMarkets.FindByServerIndex(marketIndex) = nil` → drop packet before cache/apply | events.rs:OrderBook dispatch checks `MarketsState::has_server_market_index` | ✅ |

---

## Channel: TradesStream (MoonProtoTradesStream.pas → commands/trades_stream.rs + state/trades.rs)

| # | Delphi | Что | Rust | ✅ |
|---|---|---|---|---|
| TS1 | TRADES_FLAG_COMPRESSED=0x01, TRADES_FLAG_HAS_TAKER=0x02 | flags trailing byte | trades_stream.rs:parse_trades_packet | ✅ |
| TS2 | PacketNum: Word | header | TradesPacket.packet_num: u16 | ✅ |
| TS3 | section: market_idx+kind flags(2) + count(1) + trades[] | section layout | TradeSection | ✅ |
| TS4 | kinds 0,2=Trades; 1=MMOrders; 3=Extended (Liq/Watcher) | sub-types | TradeSection.kind | ✅ |
| TS5 | zigzag-packed delta encoding | trades compression | parse_trades_packet delta decode | ✅ |
| TS6 | SynLZ over whole packet if compressed flag | outer compression | parse_trades_packet → SynLZ | ✅ |
| TS7 | GapBucket logic, 50 buckets max | gap detection | state/trades.rs:GapBucket | ✅ |
| TS8 | PathDelay = min(1800, max(300, RTT*(1.2 + retry*0.7))) | retry timing | state/trades.rs:tick | ✅ |
| TS9 | TRADES_PAUSE_TIMEOUT_MS = 30000 | reset on long silence | state/trades.rs:TRADES_PAUSE_TIMEOUT_MS | ✅ |
| TS10 | emk_TradesResend batches (200/batch) | batched resend request | state/trades.rs:tick batches | ✅ |
| TS11 | MPC_TradesResendResponse batch parser | multi-packet response | parse_trades_resend_response | ✅ |
| TS12 | on_packet_resend (не двигает last_packet_num) | historical apply | state/trades.rs:on_packet_resend | ✅ |
| TS13 | MoonProtoEngine.pas:1649-1658 overflow gap (`gap > MAX_RECVD_SIZE` / buckets full) | reset buckets, текущий пакет всё равно применяется, следующий пакет заново стартует tracking | state/trades.rs:overflow branch | ✅ |
| TS14 | MoonProtoEngine.pas:1625-1721 duplicate/resend tracking branches | duplicate/out-of-bucket resend не двигают tracking, но payload всё равно применяется секциями ниже | state/trades.rs:on_packet duplicate + on_packet_resend out-of-order emit diagnostic + Apply | ✅ |
| TS15 | MoonProtoEngine.pas:ProcessTradesStream TrackPackets=true branch | LastTradesPacketTime обновляется для duplicate и in-bucket out-of-order пакетов тоже | state/trades.rs:on_packet early-return branches refresh last_packet_time_ms | ✅ |

---

## Channel: Balance (MoonProtoBalanceStruct.pas → commands/balance.rs + state/balances.rs)

| # | Delphi | Что | Rust | ✅ |
|---|---|---|---|---|
| B1 | TBalanceCommand header: CmdId(1) + ver(2) + UID(8) | base | balance.rs:parse_balance_update | ✅ |
| B2 | cmd_id_sub: 2=legacy, 3=full, 4=incremental | mode dispatch | BalanceUpdate.cmd_id | ✅ |
| B3 | epoch:u16 + global_changed:bool + (if global) global_data | global block | BalanceUpdate.global + epoch | ✅ |
| B4 | count:integer + items[]: market_name:utf8 + balance_hash:uint64 + flags:cardinal + masked_fields | per-market | BalanceItem.parse | ✅ |
| B5 | 22 fields with bitmask flags | optional fields | BalanceItem 22 поля | ✅ |
| B6 | cmd_id=3 full: missing markets → reset to default | snapshot semantics | state/balances.rs:apply cmd_id=3 | ✅ |
| B7 | cmd_id=2 legacy: missing not reset | merge semantics | state/balances.rs:apply cmd_id=2 | ✅ |
| B8 | cmd_id=4 incremental: merge + global_changed gate | partial update | state/balances.rs:apply cmd_id=4 | ✅ |
| B9 | Incremental `EpochIsOK(m.LastBalanceEpoch, cmd.Epoch)` per market; full snapshot has no global epoch gate | out-of-order reject | state/balances.rs:last_epoch_by_market | ✅ |
| B10 | `ApplyBalanceItem`: `bnMaxValue` updates only when `item.bnMaxValue > _eps` | preserve previous max position value | state/balances.rs:preserve_max_value | ✅ |
| B11 | TRequestBalanceRefresh CmdId=5 → server forces next full balance snapshot | refresh request + wait for broadcast | balance.rs:build_request_balance_refresh + client.rs:request_balance_snapshot | ✅ |

---

## Channel: Strat (MoonProtoStratStruct.pas → commands/strat.rs + state/strats.rs)

| # | Delphi pas:line | Sub-command | Rust strat.rs | ✅ |
|---|---|---|---|---|
| S1 | :40 | TStratSnapshotRequest (CmdId=1) — empty body, server→client only; server drops client→server request | StratCommand::SnapshotRequest | ✅ |
| S2 | :60 | TStratSnapshot (CmdId=2, Sliced, UK_StratSnapshot) — ServerEpoch + ClientMaxLastDate + Size + Full + Data[Size], bidirectional | StratCommand::Snapshot + StratSnapshot + `build_snapshot` / `build_snapshot_from_strategies` | ✅ |
| S3 | :246 | TStratDelete (CmdId=3) — strategy_id + (soft) folder_path | StratCommand::Delete | ✅ |
| S4 | :276 | TStratSellPriceUpdate (CmdId=4, UK_StratSellPriceUpdate) — strategy_id + sell_price | StratCommand::SellPriceUpdate | ✅ |
| S5 | :298 | TStratCheckedSync (CmdId=5, Sliced) — count:Word + items + (soft) is_delta | StratCommand::CheckedSync + StratCheckedItem | ✅ |
| S6 | :357 | TStratCheckedEcho (CmdId=6) — count + items | StratCommand::CheckedEcho | ✅ |
| S7 | ver gate ver > 3 → Unknown | forward-compat | StratCommand::Unknown | ✅ |

### state/strats.rs
| # | Delphi | Что | Rust | ✅ |
|---|---|---|---|---|
| S-A1 | MoonProtoClient.pas:689-800 ProcessStratCommand | sync state apply | state/strats.rs:apply | ✅ |
| S-A2 | CheckedSync delta=false → reset others; delta=true → merge | full/delta semantics | state/strats.rs:apply CheckedSync | ✅ |
| S-A3 | TStratSnapshot.Data RTTI decode | bin → fields | state/strats.rs:apply_snapshot_decoded → strategy_serializer | ✅ |
| S-A4 | StrategySerializer.pas:635-705 LoadStrategyFromCompact | existing strategy skips stale snapshot when local LastDate and Ver are both >= incoming | state/strats.rs:upsert_from_snapshot rollback guard | ✅ |
| S-A5 | MoonProtoClient.pas:757-769 ProcessStratCommand CheckedSync | CheckedSync updates only existing strategies; unknown StrategyID is ignored | state/strats.rs:apply CheckedSync ignores missing id | ✅ |
| S-A6 | MoonProtoClient.pas:ProcessStratCommand ветка TStratSnapshotRequest | ответ всегда fresh `TStratSnapshot.CreateFromStrats(Strats)`, без кеша последнего server snapshot | events.rs:dispatch_into_active вызывает app-provided `strategy_snapshot_provider`; без provider только эмитит SnapshotRequested для ручного fresh-ответа | ✅ |

---

## Channel: StrategySerializer (StrategySerializer.pas → commands/strategy_serializer.rs)

| # | Delphi pas:line | Что | Rust strategy_serializer.rs | ✅ |
|---|---|---|---|---|
| SS1 | :66-77 | TID constants (Bool=1..Single=10, ZERO_FLAG=$80) | TID_BOOL=1..TID_SINGLE=10, TID_ZERO_FLAG=0x80 | ✅ |
| SS2 | :164-181 ReadDictionary | NameDict: Count:Word + (NameLen:Byte + UTF-8 bytes) × Count | read_dict | ✅ |
| SS3 | :213-232 ReadPathDictionary | Тот же формат | read_dict (reused) | ✅ |
| SS4 | :601-611 SaveStrategyToCompact header | ID(u64) + Ver(i32) + LastDate(u64) + Checked(u8) + Kind(u8) + PathID(u16) + FieldCount(u16) = 26 байт | read_strategy header | ✅ |
| SS5 | :676-691 field loop | FieldIdx(u16) + TypeID(u8) + value | read_strategy field loop | ✅ |
| SS6 | :424-428 WriteField ZERO branch | If IsZero → write (TypeID OR ZERO_FLAG) only, no value | write_field if v.is_zero() | ✅ |
| SS7 | :518-526 ReadField ZERO branch | If TypeID AND ZERO_FLAG → no value, set zero | FieldValue::zero(real_type) | ✅ |
| SS8 | :363, 508-509 SkipFieldByTypeID/ReadField | Unknown TypeID → skip 8 байт | try_read_field_value default arm | ✅ |
| SS9 | :337-355 IsZeroValue | Bool=!v; ints=AsInt=0; floats=Abs<1e-10; String='' | FieldValue::is_zero | ✅ |
| SS10 | :703 path resolve | PathID < Length(LoadedPaths) ? else '' | paths.get(path_id).cloned().unwrap_or_default() | ✅ |
| SS11 | :746 TDecompressionStream(-15) | DEFLATE raw, no zlib header | flate2::read::DeflateDecoder | ✅ |
| SS12 | :839 TCompressionStream(zcDefault, -15) | DEFLATE raw compress | flate2::write::DeflateEncoder | ✅ |
| SS13 | :160-161 MBClassic backfill | If empty → = MarketName | (применимо к Market.pas, не Serializer) | N/A |
| SS14 | :615 RTTI propmask iteration | RTTI order | DEVIATION #15 (writer alphabetical) | DEVIATION |
| SS15 | :513-516 ReadField type mismatch skip | Schema check | Rust читает по wire TypeID (no schema) | DEVIATION (by design) |
| SS16 | :746-750 LoadStrategiesFromStream | Decompressed.CopyFrom(TDecompressionStream, 0) без верхнего лимита распакованного snapshot | parse_strategy_batch читает DeflateDecoder до EOF без `.take()` cap | ✅ |
| SS17 | :760-783 LoadStrategiesFromStream | Count:Word стратегий; новые стратегии добавляются через Strats.Add без cap | StratsState добавляет все новые strategy_id без `MAX_STRATEGIES` cap | ✅ |

---

## Channel: Arb (MoonProtoBalanceStruct.pas:199-205, 607-633 → commands/arb.rs)

| # | Delphi | Что | Rust | ✅ |
|---|---|---|---|---|
| A1 | TArbPricesCommand CmdId=6 в MPC_Balance | sub-command | arb.rs:ARB_PRICES_CMD_ID=6 | ✅ |
| A2 | header + len:i32 + payload[len] | wire | parse_arb_prices | ✅ |
| A3 | ParseArbPayloadCompact structured decoder | bytes → typed prices/isolation | arb.rs:parse_arb_payload_compact + Event::Arb typed payload | ✅ |

---

## Channel: UI (MoonProtoUIStruct.pas → commands/ui.rs + state/settings.rs)

| # | Delphi pas:line | CmdId | Sub-command | Rust | ✅ |
|---|---|---|---|---|---|
| U1 | :20 | 1 | TClientSettingsCommand (Sliced, UK_BaseUISettings) — big snapshot | UICommand::ClientSettings | ✅ |
| U2 | :78 | 2 | TSettingsRequest — empty; штатный Delphi-клиент отправляет его при init и получает `TClientSettingsCommand` snapshot | UICommand::SettingsRequest + `Client::ui_settings_request` / `Client::request_client_settings` | ✅ |
| U3 | :82 | 3 | TStratStartStopCommand — IsStart:bool | UICommand::StratStartStop | ✅ |
| U4 | :93 | 4 | TStratStartStopCommandV2 — IsStart + Items[StratCheckedItem] | UICommand::StratStartStopV2 | ✅ |
| U5 | :105 | 5 | TMMOrdersSubscribeCommand (UK_TurnMMDetection) — Subscribe:bool | UICommand::MMOrdersSubscribe | ✅ |
| U5a | Unit1.pas/Strategies.pas + MoonProtoEngine.pas SubscribeAllTrades | MMOrders-флаг обновляется прямой UI-командой и bool-параметром `emk_SubscribeAllTrades`; после нового ServerToken должен быть восстановлен последний флаг | client.rs: `SubscriptionRegistry.mm_orders_sub`, распознавание исходящего `TMMOrdersSubscribeCommand`, `replay_subscriptions` | ✅ |
| U6 | :120 | 6 | TUpdateVersionCommand — VersionName:utf8 + IsRelease:bool | UICommand::UpdateVersion | ✅ |
| U7 | :131 | 7 | TEmuTradesCommand (Sliced) — mIndex + BaseTime + Points[6б each] | UICommand::EmuTrades + EmuTradePoint | ✅ |
| U8 | :144 | 8 | TNewMarketNotifyCommand — empty (Priority=High) | UICommand::NewMarketNotify | ✅ |
| U9 | :148 | 9 | TLevManageCommand (UK_LevManageSettings, Sliced) — v:byte + 5×bool + FixLev:i32 + TlgReport + LevControl:utf8 | UICommand::LevManage | ✅ |
| U10 | :166 | 10 | TTriggerManageCommand (Sliced) — Action + AllMarkets + Markets[u16] + Keys[u16] | UICommand::TriggerManage | ✅ |
| U11 | :179 | 11 | TResetProfitCommand — ResetKind:byte | UICommand::ResetProfit | ✅ |
| U12 | :189 | 12 | TArbActivateNotify — ArbValid:f64 | UICommand::ArbActivateNotify | ✅ |
| U13 | :199 | 13 | TSwitchDexCommand (High, UK_DexSwitch) — DexName:ShortString[15]=16 bytes | UICommand::SwitchDex | ✅ |
| U14 | :209 | 14 | TSwitchSpotCommand (High, UK_SpotSwitch) — SpotIndex:byte | UICommand::SwitchSpot | ✅ |
| U15 | :288-393 TClientSettingsCommand.CreateFromStream | Big parser with soft-reads, ASCfg blobs, ArbConfig compact | ui.rs:parse_client_settings | ✅ |
| U16 | :41 AS_CFG_SIZE=104 | TAutoStartConfig packed | const AS_CFG_SIZE | ✅ |
| U17 | :384 AS_CFG2_SIZE=168 | TAutoStartConfig2 packed | const AS_CFG2_SIZE | ✅ |
| U18 | ArbTypes.pas:25 ARB_CONFIG_VER=1 | ArbConfig version | const ARB_CONFIG_VER | ✅ |
| U19 | ArbConfig compact: wantedSet(32 байта set of byte) + flags(byte) + colorCount + skip | bitmask format | ArbConfigCompact + bit ops | ✅ |
| U20 | InitArbConfigDefaults (ArbTypes.pas:87) ShowLines=true, ShowPercent=true | defaults | ArbConfigCompact::default | ✅ |
| U21 | :326-336 soft-read UseManualStrategy/etc | использовать cfg.* как fallback | DEVIATION #14 — Rust = false/0/[] | DEVIATION |

---

## Channel: Market (MoonProtoSerialization.pas + MoonProtoEngineServer.pas → commands/market.rs + state/markets.rs)

| # | Delphi pas:line | Что | Rust market.rs | ✅ |
|---|---|---|---|---|
| M1 | Serialization.pas:42-98 WriteMarketToStream | 42 поля: 10 strings + 6 i32 + 1 i64 + 20 f64 + 5 bool + 1 byte (v2) | read_market/write_market | ✅ |
| M2 | :100-163 ReadMarketFromStream | reader with v2 gate | read_market with ver≥2 check | ✅ |
| M3 | :160-161 MBClassic backfill | If empty → MarketName | read_market backfill | ✅ |
| M4 | :169-178 WriteCorrMarketToStream | bn_market_name + bn_market_currency + bn_tick_size + base_currency_name | CorrMarket struct + write_corr_market | ✅ |
| M5 | :195-209 WriteMarketPricesToStream | mIndex + Bid + Ask + opt(funding) + MarkPrice + MarkPriceFound | MarketPriceUpdate | ✅ |
| M6 | :243-260 WriteTokenTagsToStream | MarketName + i32 (set of byte → 4 bytes via Move) | MarketTokenTags + TokenTags(u32) | ✅ |
| M7 | EngineServer.pas:60-82 WriteMarketsToStream | count:i32 + markets + corr_count + corr_markets | MarketsListResponse format | ✅ |
| M8 | :84-111 WriteMarketsPricesToStream | send_funding + count + prices + send_corr + corr_prices | MarketsPricesResponse format | ✅ |
| M9 | :278-284 emk_GetMarketsIndexes | count + names | parse_markets_indexes_response | ✅ |
| M10 | :324-333 emk_CheckBinanceTags | count + (MarketName + tags) | parse_token_tags_response | ✅ |
| M11 | Vars.pas:40 TBaseCurrency 27 values | enum BTC=0..Unknown=26 | BaseCurrency enum | ✅ |
| M12 | Vars.pas:64 TTokenTag 12 values | enum tag_none=0..tag_TradFi=11 | TokenTags bit 0..11 | ✅ |
| M13 | TEngineResponse.WriteStr/Int/Double/etc | primitives | EngineStreamReader | ✅ |
| M14 | Serialization.pas:97 WriteByte FuturesType (no ver gate) | always write | write_market always writes (DEVIATION-free) | ✅ |

### state/markets.rs
| # | Что | Rust | ✅ |
|---|---|---|---|
| M-A1 | apply GetMarketsList → полная замена + by_name + init prices из Market.funding_rate/funding_time | apply_markets_list | ✅ |
| M-A2 | apply UpdateMarketsList → `mIndex` resolves through `SrvMarkets.FindByServerIndex`, bounds/stale mapping miss is skipped, send_funding gate | apply_markets_prices via current `market_indexes` mapping | ✅ |
| M-A3 | apply GetMarketsIndexes → полная замена market_indexes | apply_markets_indexes | ✅ |
| M-A4 | apply CheckBinanceTags → обновить только перечисленные и известные рынки; отсутствующие в response tags не очищаются; при полном удалении рынка его tags исчезают вместе с market object | apply_token_tags + `apply_markets_list` tag prune | ✅ |
| M-A5 | BMarketsDetailsWorker.Execute вызывает `Engine.UpdateMarketsList` в цикле примерно каждые 2с при `FullProxy` (`CreateEngine` → `TMoonProtoEngine`) | `RefreshConfig::default().update_markets_every = Some(2s)` + `tick_periodic_refresh` | ✅ |
| M-A6 | BHeavyApiWorker.Execute вызывает `Engine.CheckBinanceTags` при старте, далее примерно каждые 60с, и после смены часа делает до 4 быстрых вызовов через 200-мс цикл (`TagsBurst < 4`) | `RefreshConfig::default().check_tags_every = Some(60s)` + `tick_periodic_refresh_at`: стартовый/60с tick и hourly burst 4× с шагом 200мс | ✅ |
| M-A7 | `TMoonProtoEngine.GetMarketsIndexes` строит server `mIndex` → `TMarket` через `SrvMarkets.Rebuild(IndexMap)`, `ProcessTradesStream` затем резолвит `MarketIdx` через `SrvMarkets.FindByServerIndex` | `MarketsState::market_name_by_index` / `market_by_index` / `market_index_by_name` поверх `market_indexes`, с `indexes_synchronized` gate для stale mapping | ✅ |

---

## Engine API (MoonProtoEngineStruct.pas → commands/engine_api.rs + engine_request.rs)

| # | Delphi | Что | Rust | ✅ |
|---|---|---|---|---|
| E1 | TEngineMethodKind enum (31 values) | method ids | EngineMethod enum (31 values) | ✅ |
| E2 | TEngineRequest CmdId=002 | request wire | engine_request.rs:ENGINE_REQUEST_CMD_ID=2 | ✅ |
| E3 | TEngineResponse CmdId=001 | response wire | engine_api.rs:parse_engine_response | ✅ |
| E4 | TEngineStreamCommand.Write*/Read* (Double/Int/Word/Byte/Int64/Bool/Str) | primitives | params::write_* / EngineStreamReader | ✅ |
| E5 | DEFLATE on Data if IsCompressed | response decompression | parse_engine_response Deflate | ✅ |
| E6 | UnencryptedMethods set | exclude from encryption | server-side, client принимает оба | N/A |
| E7 | MoonProtoEngineServer.pas:315-319 emk_GetBalance response `WriteDouble(q)` | typed payload parser | engine_api.rs:parse_get_balance_response | ✅ |
| E8 | MoonProtoEngineServer.pas:341-344 emk_QueryHedgeMode response `WriteBool(hedgeMode)` | typed payload parser | engine_api.rs:parse_query_hedge_mode_response | ✅ |
| E9 | MoonProtoServer.pas:1070-1128 emk_SubscribeOrderBook/emk_UnsubscribeOrderBook uses only `MarketNames`; MoonProtoOrderBook.pas:287-293 marks both `TOrderBookKind` books | high-level orderbook subscribe registry is per `market_name`; `OrderBookKind` remains event/full-request state only | client.rs:subscribe_orderbook/unsubscribe_orderbook + SubscriptionRegistry | ✅ |
| E10 | MoonProtoEngineServer.pas:ProcessRequest `emk_GetMarketsBalanceFull` вызывает `Engine.GetMarketsBalanceFull`, но `WriteBalancesToStream` оставлен TODO и payload не пишется | raw wrapper kept; docs/comments state successful response data is empty | client.rs:api_get_markets_balance_full + docs | ✅ |
| E11 | `TEngineMethodKind` содержит `emk_GetOrder`/`emk_GetOpenOrders`/`emk_GetActiveOrders`, но `MoonProtoEngineServer.pas:ProcessRequest` не имеет этих веток и возвращает `Unknown method` | raw wrappers kept only for enum/wire completeness; docs warn current reference server returns error 400 | client.rs:api_get_order/api_get_open_orders/api_get_active_orders + docs | ✅ |

### High-level wrappers in client.rs (Stage 3)
| # | Что | Rust | ✅ |
|---|---|---|---|
| E-W1 | api_pending registry: uid → Receiver | api_pending.rs:ApiPending | ✅ |
| E-W2 | send_api_request_async(raw) → Receiver | client.rs:send_api_request_async | ✅ |
| E-W3 | dispatch on Command::API → pending dispatch or fallback to on_data | client.rs:data_read_int | ✅ |
| E-W4 | 46+ high-level wrappers (29 Engine API + 17 Trade + Candles) | client.rs `impl Client` | ✅ |
| E-W5 | Trade wrappers с UKey dedup (UK_OrderMove, UK_ImmuneClicks) | client.rs:send_trade_keyed + UniqueKey constants | ✅ |
| E-W6 | LifecycleEvent: Connecting/Connected{fresh}/Disconnected/Reconnecting/ServerRestart/BindFailed | client.rs:check_lifecycle_transition + bind failure path | ✅ |
| E-W7 | bind_socket failure → BindFailed event + retry | client.rs:bind_socket failure path | ✅ |
| E-W8 | TEngineRequest effective `MPS_Sliced` + `MaxRetries=6` | client.rs:send_api_request sends `SendPriority::Sliced`, `max_retries=6` | ✅ |
| E-W9 | Однопоточный SendAndWait-style Engine API flow без ручного `Receiver` wait/parsing | client.rs:`request_engine_response` + typed `request_*` helpers | ✅ |
| E-W10 | Active-library setup helper поверх Delphi connection + init flow | client.rs:`connect_and_init` waits for ready client, then delegates to `run_init_sequence` | ✅ |
| E-W11 | Rust active API: one-shot ожидания не теряют typed events, пришедшие пока helper крутит loop | events.rs:`EventDispatcher::queued_events`/`take_queued_events`; client.rs:`run_with_dispatcher_queued` используется `run_until_response` и one-shot helpers | ✅ |

---

## Итого Stage 2: 130+ пунктов

- ✅ = 124+
- DEVIATION = строки перечислены в `DEVIATION.md` (статус см. в реестре)
- TODO = 0
- N/A = 1 (UnencryptedMethods — server-side)

═══════════════════════════════════════════════════════════════════════════════

═══════════════════════════════════════════════════════════════════════════════

# STAGE 3 — High-level API (Rust extensions over Delphi)

## events.rs (EventDispatcher) → высокоуровневая абстракция, Delphi-эквивалент = ProcessCommand* функции в MoonProtoClient.pas

| # | Delphi эквивалент | Rust events.rs | ✅ |
|---|---|---|---|
| EV1 | MoonProtoClient.pas:553-590 ProcessCommandOrder | dispatch(Command::Order) → TradeCommand::parse → orders.apply → Event::Order | ✅ |
| EV2 | MoonProtoClient.pas:OrderBook handler | dispatch(Command::OrderBook) → parse_order_book_packet → order_books.on_packet → Vec<Event::OrderBook> | ✅ |
| EV3 | MoonProtoClient.pas:394-407 ProcessTradesStreamCommand | dispatch(Command::TradesStream) → parse_trades_packet → trades.on_packet → Event::Trades | ✅ |
| EV4 | MoonProtoClient.pas:396-402 ProcessTradesResendBatch | dispatch(Command::TradesResendResponse) → parse_trades_resend_response → on_packet_resend for each | ✅ |
| EV5 | MoonProtoClient.pas:374-378 Balance/Arb split | dispatch(Command::Balance) → sub_cmd_id=2/3/4 → balances.apply; sub_cmd_id=6 → Arb passthrough | ✅ |
| EV6 | MoonProtoClient.pas:689-800 ProcessStratCommand | dispatch(Command::Strat) → StratCommand::parse → strats.apply | ✅ |
| EV7 | MoonProtoClient.pas:UI handler | dispatch(Command::UI) → UICommand::parse → settings.apply | ✅ |
| EV8 | MoonProtoClient.pas:802-876 ProcessApiCommand | dispatch(Command::API) → parse_engine_response → Event::EngineResponse | ✅ |
| EV9 | LogMsg / Service / прочие | dispatch(_) → Event::Raw { cmd, payload } | ✅ |
| EV10 | ParseFailed handling | для каналов с обязательным парсингом — Event::ParseFailed | ✅ |
| EV11 | MoonProtoEngine.pas:809-816 + 1577-1580 | PeerAppToken mismatch → GetMarketsIndexes, до успеха не обрабатывать market_idx streams | dispatch_into_active закрывает `indexes_synchronized`; GetMarketsIndexes response открывает через MarketsState | ✅ |

## commands/candles.rs → MarketsU.pas + MoonProtoEngineServer.pas + MoonProtoClient.pas

| # | Delphi | Rust candles.rs | ✅ |
|---|---|---|---|
| CD1 | MarketsU.pas:701-705 TDeepPrice packed (5×single + double) | DeepPrice struct (5×f32 + f64) = 28 bytes packed | ✅ |
| CD2 | EngineBase.pas:60 TMarketDeepHistoryKind (6 values) | DeepHistoryKind enum hk_1m..hk_1d (6 values, including Hour4) | ✅ |
| CD3 | EngineServer.pas:382-395 emk_GetCoinCardCandles request | get_coin_card_candles(market, ticks) → WriteByte(ticks as u8) | ✅ |
| CD4 | EngineServer.pas:391-392 response: WriteInt(N) + N×TDeepPrice | parse_coin_card_candles_response: i32 count + N × DeepPrice::read_from | ✅ |
| CD5 | MoonProtoClient.pas:824-826 ChunkIndex:Word + ChunkTotal:Word | CandlesAggregator::on_chunk reads u16+u16 header | ✅ |
| CD6 | MoonProtoClient.pas:832-840 resize on first chunk | CandlesAggregator resize_with(total, None) | ✅ |
| CD7 | MoonProtoClient.pas:843-848 chunk[index] := payload, dedup | CandlesAggregator chunks[index] = Some(...) if is_none() | ✅ |
| CD8 | MoonProtoClient.pas:856-872 merge всех chunks | CandlesAggregator drain merge при received == total | ✅ |

## api_pending.rs (ApiPending registry) → Delphi-эквивалент = TPendingRequest list

| # | Delphi эквивалент | Rust api_pending.rs | ✅ |
|---|---|---|---|
| AP1 | MoonProtoClient.pas:878-892 PendingRequests + FastLock | ApiPending { Mutex<HashMap<u64, Sender<EngineResponse>>> } | ✅ |
| AP2 | MoonProtoClient.pas:880-885 search by RequestUID | dispatch(resp) → map.remove(resp.request_uid) | ✅ |
| AP3 | Delphi: SendAndWait blocks until response | Rust: register(uid) → Receiver; same-thread wait через `Client::run_until_response` | ✅ |
| AP4 | `SendAndWait` owns pending lifetime: wait until caller timeout, then remove `TPendingRequest`; no independent fixed-age cleanup in client loop | `request_engine_response` removes pending on caller timeout; raw receiver slots live until response/reconnect/re-register; main loop no longer drops API pending by fixed 12s age | ✅ |
| AP5 | active library extension over Delphi pending list | pending response still reaches `EventDispatcher` in dispatcher mode so markets/indexes/tags state updates while `Receiver` gets the same response | ✅ |

## key_import.rs → MoonProtoFunc.pas DecodeBuffer + base64 import

| # | Delphi | Rust key_import.rs | ✅ |
|---|---|---|---|
| KI1 | Base64 decode | base64 crate decode | ✅ |
| KI2 | MoonProtoFunc.pas DecodeBuffer (XOR с фиксированным паттерном) | decode_buffer (byte-exact) | ✅ |
| KI3 | Layout: server_addr + master_key(16) + mac_key(16) + ... | `import_key` parse | ✅ |

## Связанные DEVIATION'ы

- #1-9: архитектурные отклонения (mpsc vs FastLock, observer-model, sync apply, etc.) — ПОДТВЕРЖДЕНО
- #10: AES-GCM IV mask + RDTSC — ИСПРАВЛЕНО
- #11: H/L auto-compression — ИСПРАВЛЕНО
- #12: NTP background thread — ИСПРАВЛЕНО
- #13: moonext ABI sync — ПОДТВЕРЖДЕНО (CLAUDE.md spec упрощённая, реальная ABI extended)
- #14: UI soft-read defaults — ПОДТВЕРЖДЕНО
- #15: strategy_serializer writer field order — ПОДТВЕРЖДЕНО
- #16: from_utf8_lossy vs UTF8 fallback '?' — ПОДТВЕРЖДЕНО
- #17: cpu_timestamp non-x86 fallback — ПОДТВЕРЖДЕНО
- #18: LifecycleEvent abstraction (Stage 3 high-level API) — ПОДТВЕРЖДЕНО
- #19: cross-platform DontFragment — ИСПРАВЛЕНО
- #20: socket buffers 8MB — ИСПРАВЛЕНО
- #21: reader shutdown handle — ИСПРАВЛЕНО
- #22: active session manager boundary — ПОДТВЕРЖДЕНО
- #23: per-Client ServerTimeDelta — ИСПРАВЛЕНО
- #24: first OrderBook Diff with local seq 0 + corrupted-mode diffs — ИСПРАВЛЕНО
- #25: PMTU clamp in `handle_ping` / `handle_probe_mtu` — ИСПРАВЛЕНО
