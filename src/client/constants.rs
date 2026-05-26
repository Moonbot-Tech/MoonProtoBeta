//! Delphi timing and wire constants used by the client runtime.

// === Constants matching Delphi exactly ===
pub(super) const DEFAULT_SLEEP_MS: u64 = 5; // MoonProtoFunc.pas:19
pub(super) const DELPHI_SEND_AND_WAIT_POLL_MS: u64 = 10; // MoonProtoEngine.pas:531
pub(super) const SETTINGS_HELPER_RETRY_PAUSE_MS: u64 = 5_000;
pub(super) const DELPHI_BASE_CHECK_UPDATE_AUTH_WAITS: usize = 34; // MoonProtoEngine.pas:574
pub(super) const DELPHI_BASE_CHECK_UPDATE_AUTH_WAIT_MS: u64 = 300; // MoonProtoEngine.pas:575
pub(super) const DELPHI_BASE_CHECK_UPDATE_RETRIES: usize = 10; // MoonProtoEngine.pas:586
pub(super) const DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS: u64 = 2_000; // MoonProtoEngine.pas:589
pub(super) const DELPHI_INIT_AUTH_RETRY_PAUSE_MS: u64 = 200; // Unit1.pas:5064-5068
pub(super) const RECONNECT_WAITING_MS: i64 = 7000; // MoonProtoUDPClient.pas:88
pub(super) const RECONNECT_THROTTLE_MS: i64 = 15000; // MoonProtoUDPClient.pas:89
pub(super) const OFFLINE_BASE_MS: i64 = 2300; // MoonProtoUDPClient.pas:772
pub(super) const DEAD_ZONE_MS: i64 = 5000; // MoonProtoUDPClient.pas:799
pub(super) const NEED_HELLO_AGAIN_THROTTLE_MS: i64 = 700; // MoonProtoUDPClient.pas:568
pub(super) const COMPRESSED_FLAG: u8 = 0x80; // MoonProtoDataStruct.pas:27
pub(super) const MIN_SIZE_TO_COMPRESS: usize = 64; // MoonProtoDataStruct.pas:31
pub(super) const NEVER_SENT_MS: i64 = i64::MIN / 2; // Delphi LastSentHello=0 analogue.
pub(super) const NEVER_TIME_MS: i64 = i64::MIN / 2;
pub(super) const NO_PENDING_ENGINE_REQUEST_UID: u64 = u64::MAX;
pub(super) const BIND_FAILED_FIRST_EVENT_MS: i64 = 15_000;
pub(super) const BIND_FAILED_REPEAT_EVENT_MS: i64 = 50_000;
pub(super) const TRADES_RECONNECT_THROTTLE_MS: i64 = 5_000; // MoonProtoEngine.NeedReconnectAllTrades
pub(super) const TRADES_RECONNECT_RESUBSCRIBE_DELAY_MS: i64 = 100; // BWorks.pas Sleep(100)
pub(super) const ORDERBOOK_RECONNECT_THROTTLE_MS: i64 = 5_000; // MoonProtoEngine.NeedResubscribeOrderBooks
