//! Delphi timing and wire constants used by the client runtime.

// === Constants matching Delphi exactly ===
pub(super) const DEFAULT_SLEEP_MS: u64 = 5; // MoonProtoFunc.pas:19
pub(super) const DELPHI_IMFRIEND_RESEND_PAUSE_MS: u64 = 32; // MoonProtoUDPClient.pas:434
pub(super) const SETTINGS_HELPER_RETRY_PAUSE_MS: u64 = 5_000;
pub(super) const DELPHI_BASE_CHECK_UPDATE_AUTH_WAITS: usize = 34; // MoonProtoEngine.pas:574
pub(super) const DELPHI_BASE_CHECK_UPDATE_AUTH_WAIT_MS: u64 = 300; // MoonProtoEngine.pas:575
pub(super) const DELPHI_BASE_CHECK_UPDATE_RETRIES: usize = 10; // MoonProtoEngine.pas:586
pub(super) const DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS: u64 = 2_000; // MoonProtoEngine.pas:589
pub(super) const DELPHI_INIT_AUTH_RETRY_PAUSE_MS: u64 = 200; // Unit1.pas:5064-5068
                                                             // Rust active-lib can start domain Init immediately after `MPC_Fine`, before the
                                                             // server has had one Ping roundtrip to publish a non-zero `RoundTripDelay`.
                                                             // With RTT=0 the Delphi Sliced formula becomes a 10ms retry clock and can burn
                                                             // the full retry budget before any real ACK can return. Until Ping provides the
                                                             // real RTT, use the same 200ms floor that Delphi already applies to H retries.
pub(super) const UNKNOWN_RTT_SLICED_FLOOR_MS: i64 = 200;
pub(super) const RECONNECT_WAITING_MS: i64 = 7000; // MoonProtoUDPClient.pas:88
pub(super) const RECONNECT_THROTTLE_MS: i64 = 15000; // MoonProtoUDPClient.pas:89
pub(super) const OFFLINE_BASE_MS: i64 = 2300; // MoonProtoUDPClient.pas:772
pub(super) const DEAD_ZONE_MS: i64 = 5000; // MoonProtoUDPClient.pas:799
pub(super) const NEED_HELLO_AGAIN_THROTTLE_MS: i64 = 700; // MoonProtoUDPClient.pas:568
pub(super) const COMPRESSED_FLAG: u8 = 0x80; // MoonProtoDataStruct.pas:27
pub(super) const MIN_SIZE_TO_COMPRESS: usize = 64; // MoonProtoDataStruct.pas:31

// MPSliderLenBits div 2 - 1
pub(super) const INITIAL_CRYPTED_MSG_COUNTER: u64 = 64 * 64 / 2 - 1;
// During primary/rebind Fine wait the server may already send early encrypted
// app facts. Keep only the first receive half-window; accepting far-ahead
// pre-auth MsgNum values would let one packet move MPSlider past fresh startup
// packets after AuthDone.
pub(super) const PRE_AUTH_CRYPTED_MAX_MSG_NUM: u64 = 64 * 64 - 1;
pub(super) const NEVER_SENT_MS: i64 = i64::MIN / 2; // Delphi LastSentHello=0 analogue.
pub(super) const NEVER_TIME_MS: i64 = i64::MIN / 2;
pub(super) const NO_PENDING_ENGINE_REQUEST_UID: u64 = u64::MAX;
pub(super) const BIND_FAILED_FIRST_EVENT_MS: i64 = 15_000;
pub(super) const BIND_FAILED_REPEAT_EVENT_MS: i64 = 50_000;
pub(super) const TRADES_RECONNECT_THROTTLE_MS: i64 = 5_000; // MoonProtoEngine.NeedReconnectAllTrades
pub(super) const TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS: i64 = 100; // BWorks.pas Sleep(100)
pub(super) const ORDERBOOK_RECONNECT_THROTTLE_MS: i64 = 5_000; // MoonProtoEngine.NeedResubscribeOrderBooks
pub(super) const CANDLE_RECONNECT_THROTTLE_MS: i64 = 5_000; // MoonProtoEngine.CheckCandleTopics
