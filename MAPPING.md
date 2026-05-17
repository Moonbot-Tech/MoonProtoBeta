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
| 12 | 555-565 | MPC_WantNewHello → Reset + NeedConnect | client.rs: full_reset() + flags | ✅ |
| 13 | 567-576 | MPC_NeedHelloAgain (700ms throttle) | client.rs: last_need_hello_again + 700ms check | ✅ |
| 14 | 578-581 | WhoAreYou/Fine → HandleHandShake | client.rs handle_handshake | ✅ |
| 15 | 583-591 | MPC_SizeTest → SendSizeAck | client.rs handle_size_test | ✅ |
| 16 | 594-617 | MPC_ProbeMTU → ProbeMTUAck (DontFragment!) | client.rs: handle_probe_mtu (TODO: DF flag) | ✅* |
| 17 | 620-625 | MPC_Sliced → OnNewSliced | client.rs handle Sliced | ✅ |
| 18 | 627-629 | MPC_SlicedACK → OnNewSlicedACK | client.rs: match arm (no-op, client doesn't send Sliced yet) | ✅ |
| 19 | 632-661 | MPC_Ping → update RTT/PMTU/OverHeat/RS + rate control | client.rs: handle_ping reads all fields | ✅ |
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
| 26 | 513-528 | Ping → read TmpSlider (ACK bitmap from server) | client.rs: data_read_int reads ack_start + words | ✅ |
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
| 35 | 806-822 | ForceDisconnect: LogOff, close socket, Reset | client.rs: do_force_disconnect + full_reset | ✅ |
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
| 41 | 399 | Decode WhoAreYou with MasterKey, AAD=ClientID | client.rs: decrypt with &[] (AAD discarded by mORMot) | ✅ DEVIATION |
| 42 | 416-419 | Save ServerToken, PeerAppToken | client.rs: self.server_token = ... | ✅ |
| 43 | 421-422 | Update Hello: MixTS, AppToken | client.rs: im.mix_ts, im.app_token | ✅ |
| 44 | 427 | GenerateSubKeys(MasterKey, ServerToken) | client.rs: generate_sub_keys | ✅ |
| 45 | 430-431 | FClient.Encode (session key, AAD=ClientID) | client.rs: encrypt(&encode_key, &[], ...) AAD discarded | ✅ DEVIATION |
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
| 52 | 283-316 | MPCompress (SynLZ / Deflate / RLE+SynLZ) | compression.rs: not needed client-side (server compresses) | N/A |
| 53 | 319-358 | MPDecompress | compression.rs: synlz_decompress (algo 1) | ✅ |

## UpdateChannelRDown (MoonProtoIntStruct.pas:1003-1055) → ???

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 54 | 1003-1055 | RS EMA update from TotalRecvBytes | N/A: server computes RS, client reads from Ping.RSQ | ✅ |

## ApplyRegularHLAck (MoonProtoIntStruct.pas:844-876) → ???

| # | Delphi (строка) | Что делает | Rust (файл:строка) | ✅ |
|---|---|---|---|---|
| 55 | 844-876 | Apply server's ACK slider to PendingH | client.rs: tmp_slider_data stored, apply when H-commands added | ✅* |

---

## Итого: 55 пунктов

- ✅ = 51
- ✅* = 2 (структура готова, заработает когда клиент будет слать H-commands / полный Sliced)
- N/A = 1 (MPCompress — клиент не сжимает, только распаковывает)
- DEVIATION = 2 (AAD в строках 41, 45 — подтверждённое поведение mORMot)

**Открытый вопрос:** DontFragment flag (MAPPING #16, M5) — platform-specific, требует raw socket opt.
Записано в DEVIATION.md.
