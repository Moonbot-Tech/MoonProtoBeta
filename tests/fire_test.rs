//! FireTest: live health test for the active MoonProto library.
//!
//! This test is intentionally ignored by default. It talks to a real MoonBot
//! server, enables client-side `err_emu=10%` before connecting, verifies the
//! full chunked candles snapshot under loss, then raises client-side
//! `err_emu=50%` for simple operations and reconnect health. Heavy candles are
//! intentionally excluded from the 50% gate; settings/strategy mutation,
//! cross-client broadcast, and the final forced reconnect run after resetting
//! the stochastic loss emulator.
//!
//! Config file is outside this crate repository:
//! `../moonproto.firetest.conf` relative to `moonproto/`.
//!
//! Minimal config:
//! ```text
//! server = 127.0.0.1:3000
//! key = <exported MoonBot key>
//! # mask_ver = 0
//! allow_mutation = true
//! market = BTCUSDT
//! strategy_field = Comment
//! # strategy_id = 123456789
//! # candles_timeout_secs = 30
//! # high_loss_timeout_secs = 60
//! ```
//!
//! Profiles:
//! - `MOONPROTO_FIRETEST_PROFILE=quick` — public `MoonClient` path, <=30s
//!   target health gate:
//!   connect/AuthDone/InitDone, BaseCheck/AuthCheck, markets/indexes/update,
//!   retained LastPrice/trades, derived trade/LastPrice snapshot, non-blocking
//!   CoinCard 4h candle request, trades + orderbook streams, ParseFailed=0,
//!   CPU summary.
//! - `MOONPROTO_FIRETEST_PROFILE=full` or unset — the complete destructive
//!   public `MoonClient` health/stress scenario below. Requires
//!   `allow_mutation=true`.
//!
//! FireTest checks live full-parse health for all real server packets. Crafted
//! malformed parser semantics (Delphi `Read` zero-tail vs `ReadBuffer`
//! fail-fast) belong in deterministic unit/parser tests next to each parser.
//! At the end of each profile it prints an ActiveLib UI-state report for
//! BTCUSDT/ETHUSDT: LastPrice/MarkPrice retained lines, volumes, deltas,
//! funding, balances/assets, and order events already observed during the run.
//! The full profile also switches the server to real/non-emulator mode for one
//! SOLUSDT limit-long cancel test: place 1000 USD 5% below market, wait for the
//! real server order UID, then cancel through the tracked ActiveLib order path.
//! Binance balance updates are intentionally not tied to that exact order:
//! FireTest only requires the full run to receive and apply live balance events.
//! Strategy snapshots are also dumped as raw `TStratSnapshot.Data` files under
//! `target/firetest_strategy_raw/` by default, so Delphi/Rust serializer and CPU
//! checks can run against the exact same live payload bytes.
//!
//! This is a diagnostic/protocol health test, not application example code. The
//! full profile uses the same public `MoonClient` path as regular applications,
//! while still inspecting protocol metrics, err_emu counters, raw strategy
//! payloads, reconnect phases, and destructive order/settings scenarios. Both
//! profiles print Sliced recovery math for the configured `err_emu`, so a
//! missing startup request/response is compared against the protocol retry
//! budget instead of being dismissed as random loss.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use moonproto::client::{set_err_emu, ErrEmuDiagnostics, ErrEmuSlicedDatagramDiagnostics};
use moonproto::commands::{
    parse_request_candles_data_response, parse_strategy_batch, CandlesAggregator,
    ClientSettingsCommand, DeepHistoryKind, EngineMethod, EngineResponse, FieldValue, OrderCompact,
    OrderWorkerStatus, RequestCandlesMarket, StrategyDynamicPicklist, StrategyFieldLayout,
    StrategyFieldUiKind, StrategyFields, StrategyKind, StrategySchema, StrategySnapshot,
};
use moonproto::events::Event;
use moonproto::state::{
    ApplyResult, BalanceEvent, LastPricePoint, MarkPricePoint, MarketPrice, Order, OrderBookEvent,
    OrderBookKind, OrderEvent, SettingsEvent, StratEvent, TradeHistoryRow, TradesEvent,
};
use moonproto::Command;
use moonproto::{
    parse_key_info, ClientConfig, ConnectConfig, EventDispatcherSnapshot, ExchangeKind,
    ImportedKeys, InitConfig, InitialStrategies, LifecycleEvent, MoonClient,
    ProtocolMetricsSnapshot, TradesStreamMode, TransportMode,
};

const DEFAULT_FIRETEST_ERR_EMU_PERCENT: u8 = 10;
const FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT: u8 = 50;
const FIRETEST_RECONNECT_MATH_ATTEMPTS: i32 = 10;
const FIRETEST_STRATEGY_ID: u64 = 0xF17E_5737_0000_0001;
const DEFAULT_WAIT_SECS: u64 = 5;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 45;
const DEFAULT_CANDLES_TIMEOUT_SECS: u64 = 90;
const DEFAULT_HIGH_LOSS_TIMEOUT_SECS: u64 = 60;
const DEFAULT_DISCONNECT_TIMEOUT_SECS: u64 = 45;
const DEFAULT_RECONNECT_TIMEOUT_SECS: u64 = 30;
const FIRETEST_SLICED_MAX_RETRIES: i32 = 6;
const PUMP_SLICE: Duration = Duration::from_millis(50);
const PRICE_NEIGHBORHOOD_PCT: f64 = 0.05;
const FIRETEST_ORDER_SIZE_USD: f64 = 1000.0;
const FIRETEST_REAL_BALANCE_ORDER_MARKET: &str = "SOLUSDT";
const FIRETEST_REAL_BALANCE_ORDER_DISCOUNT: f64 = 0.05;
const FIRETEST_REAL_BALANCE_ORDER_TIMEOUT: Duration = Duration::from_secs(5);
const EPS: f64 = 1e-9;
const QUICK_CONNECT_TIMEOUT_SECS: u64 = 18;
const QUICK_STREAM_TIMEOUT_SECS: u64 = 8;
const QUICK_TOTAL_TARGET_SECS: u64 = 30;
const FIRETEST_SLOW_STARTUP_DIAG_SECS: f64 = 8.0;
const ACTIVE_LIB_REPORT_MARKETS: [&str; 2] = ["BTCUSDT", "ETHUSDT"];
const FIRETEST_COIN_CARD_KIND: DeepHistoryKind = DeepHistoryKind::Hour4;
const FIRETEST_MIN_COIN_CARD_CANDLES: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FireProfile {
    Quick,
    Full,
}

#[derive(Clone)]
struct ParseFailureRecord {
    event_no: u64,
    cmd: Command,
    len: usize,
    hash: u64,
    dump: Option<PathBuf>,
}

impl FireProfile {
    fn from_env() -> Self {
        match std::env::var("MOONPROTO_FIRETEST_PROFILE") {
            Ok(value) if value.eq_ignore_ascii_case("quick") => Self::Quick,
            Ok(value) if value.eq_ignore_ascii_case("full") => Self::Full,
            Ok(value) => {
                panic!("bad MOONPROTO_FIRETEST_PROFILE={value:?}; expected `quick` or `full`")
            }
            Err(_) => Self::Full,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Full => "full",
        }
    }
}

fn firetest_err_emu_percent() -> u8 {
    match std::env::var("MOONPROTO_FIRETEST_ERR_EMU") {
        Ok(value) => value
            .parse::<u8>()
            .unwrap_or_else(|err| panic!("bad MOONPROTO_FIRETEST_ERR_EMU={value:?}: {err}")),
        Err(_) => DEFAULT_FIRETEST_ERR_EMU_PERCENT,
    }
}

#[derive(Clone)]
struct FireConfig {
    path: PathBuf,
    host: String,
    port: u16,
    mask_ver: TransportMode,
    key_b64: String,
    allow_mutation: bool,
    market: String,
    strategy_id: Option<u64>,
    strategy_field: String,
    wait: Duration,
    connect_timeout: Duration,
    candles_timeout: Duration,
    high_loss_timeout: Duration,
    disconnect_timeout: Duration,
    reconnect_timeout: Duration,
}

impl FireConfig {
    fn load_required() -> Self {
        let path = std::env::var_os("MOONPROTO_FIRETEST_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                manifest
                    .parent()
                    .expect("moonproto must have a parent directory")
                    .join("moonproto.firetest.conf")
            });
        let text = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!(
                "FIRETEST_CONFIG_MISSING: cannot read {}: {err}. \
                 During development this is a red health check. Create the file \
                 outside the moonproto repo or set MOONPROTO_FIRETEST_CONFIG.",
                path.display()
            )
        });

        let mut values = HashMap::<String, String>::new();
        for raw in text.lines() {
            let line = raw.trim_start_matches('\u{feff}').trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                panic!("bad FireTest config line: {raw}");
            };
            values.insert(
                key.trim().to_ascii_lowercase(),
                strip_quotes(value.trim()).to_string(),
            );
        }

        let key_b64 = values
            .get("key")
            .or_else(|| values.get("moonproto_key"))
            .unwrap_or_else(|| panic!("FireTest config missing `key`"))
            .to_string();
        let key_info = parse_key_info(&key_b64)
            .unwrap_or_else(|| panic!("invalid MoonProto key in FireTest config"));
        let (host, port) = match values.get("server").filter(|s| !s.trim().is_empty()) {
            Some(server) => parse_server(server),
            None => {
                let network = key_info.network.unwrap_or_else(|| {
                    panic!("FireTest config missing `server`, and this MoonBot key does not carry endpoint metadata")
                });
                let address = network.address.unwrap_or_else(|| {
                    panic!("FireTest config missing `server`, and this MoonBot key has no active IP address")
                });
                (address.to_string(), network.port)
            }
        };
        let mask_ver = values
            .get("mask_ver")
            .or_else(|| values.get("transport_mode"))
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                s.parse::<u8>()
                    .unwrap_or_else(|_| panic!("bad mask_ver: {s}"))
            })
            .map(TransportMode::from_byte)
            .or_else(|| key_info.network.map(|network| network.mask_ver))
            .unwrap_or(TransportMode::V0);
        let allow_mutation = parse_bool(
            values
                .get("allow_mutation")
                .map(String::as_str)
                .unwrap_or("false"),
        );
        let market = values
            .get("market")
            .cloned()
            .unwrap_or_else(|| "BTCUSDT".to_string());
        let strategy_id = values
            .get("strategy_id")
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                s.parse::<u64>()
                    .unwrap_or_else(|_| panic!("bad strategy_id: {s}"))
            });
        let strategy_field = values
            .get("strategy_field")
            .cloned()
            .unwrap_or_else(|| "Comment".to_string());
        let wait = Duration::from_secs(parse_u64(
            values.get("wait_secs").map(String::as_str),
            DEFAULT_WAIT_SECS,
        ));
        let connect_timeout = Duration::from_secs(parse_u64(
            values.get("connect_timeout_secs").map(String::as_str),
            DEFAULT_CONNECT_TIMEOUT_SECS,
        ));
        let candles_timeout = Duration::from_secs(parse_u64(
            values.get("candles_timeout_secs").map(String::as_str),
            DEFAULT_CANDLES_TIMEOUT_SECS,
        ));
        let high_loss_timeout = Duration::from_secs(parse_u64(
            values.get("high_loss_timeout_secs").map(String::as_str),
            DEFAULT_HIGH_LOSS_TIMEOUT_SECS,
        ));
        let disconnect_timeout = Duration::from_secs(parse_u64(
            values.get("disconnect_timeout_secs").map(String::as_str),
            DEFAULT_DISCONNECT_TIMEOUT_SECS,
        ));
        let reconnect_timeout = Duration::from_secs(parse_u64(
            values.get("reconnect_timeout_secs").map(String::as_str),
            DEFAULT_RECONNECT_TIMEOUT_SECS,
        ));

        Self {
            path,
            host,
            port,
            mask_ver,
            key_b64,
            allow_mutation,
            market,
            strategy_id,
            strategy_field,
            wait,
            connect_timeout,
            candles_timeout,
            high_loss_timeout,
            disconnect_timeout,
            reconnect_timeout,
        }
    }
}

fn strip_quotes(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(value)
}

fn parse_server(server: &str) -> (String, u16) {
    let Some((host, port)) = server.rsplit_once(':') else {
        panic!("bad server value `{server}`, expected host:port");
    };
    let port = port
        .parse::<u16>()
        .unwrap_or_else(|_| panic!("bad server port in `{server}`"));
    (host.to_string(), port)
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
}

fn parse_u64(value: Option<&str>, default: u64) -> u64 {
    value
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.parse::<u64>().unwrap_or_else(|_| panic!("bad u64: {v}")))
        .unwrap_or(default)
}

#[derive(Clone, Debug, Default)]
struct CandlesSnapshotSummary {
    uid: u64,
    zipped_bytes: usize,
    markets: usize,
    candles: usize,
    market_preview: String,
}

impl CandlesSnapshotSummary {
    fn is_healthy(&self) -> bool {
        self.markets > 0 && self.candles > 0
    }

    fn summary(&self) -> String {
        format!(
            "uid={} zipped={} markets={} candles={} preview=[{}]",
            self.uid, self.zipped_bytes, self.markets, self.candles, self.market_preview
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct MarketProbePrice {
    bid: f64,
    ask: f64,
    mark_price: f64,
    mark_price_found: bool,
}

impl From<&MarketPrice> for MarketProbePrice {
    fn from(value: &MarketPrice) -> Self {
        Self {
            bid: value.bid,
            ask: value.ask,
            mark_price: value.mark_price,
            mark_price_found: value.mark_price_found,
        }
    }
}

#[derive(Default)]
struct SessionStats {
    label: String,
    market: String,
    market_index: Option<u16>,
    connected_now: bool,
    server_events: u64,
    connected_fresh: u64,
    connected_again: u64,
    reconnecting: u64,
    disconnected: u64,
    engine_responses: u64,
    engine_method_counts: HashMap<u8, u64>,
    raw_events: u64,
    server_logs: u64,
    settings_events: u64,
    strategy_events: u64,
    strategy_snapshot_events: u64,
    strategy_schema_events: u64,
    strategy_schema_fields: usize,
    strategy_schema_kinds: usize,
    market_events: u64,
    trades_apply: u64,
    target_trade_packets: u64,
    orderbook_apply: u64,
    target_orderbook_full: u64,
    target_orderbook_update: u64,
    last_trade_price: Option<f64>,
    last_book_bid: Option<f64>,
    last_book_ask: Option<f64>,
    last_book_kind: Option<u8>,
    last_market_price: Option<MarketProbePrice>,
    market_invariant_error: Option<String>,
    order_events: u64,
    order_event_kinds: HashMap<&'static str, u64>,
    order_uid_by_request: HashMap<u64, u64>,
    order_status_by_uid: HashMap<u64, OrderWorkerStatus>,
    order_market_by_uid: HashMap<u64, String>,
    order_sell_reason_by_uid: HashMap<u64, String>,
    order_ignored_by_uid: HashMap<u64, ApplyResult>,
    balance_events: u64,
    balance_snapshot_events: u64,
    balance_incremental_events: u64,
    transfer_asset_events: u64,
    transfer_asset_updated_mask: u8,
    transfer_asset_failures: u64,
    coin_card_events: u64,
    coin_card_updates: u64,
    coin_card_failures: u64,
    coin_card_last_count: usize,
    parse_failed: u64,
    parse_failures: Vec<ParseFailureRecord>,
    candles_requested: bool,
    candles_chunks: u64,
    candles_ignored: u64,
    candles_payload_bytes: usize,
    candles_seen_chunks: Vec<bool>,
    candles_last_progress: (usize, usize),
    candles_complete: Option<CandlesSnapshotSummary>,
    candles_aggregator: CandlesAggregator,
    last_settings: Option<ClientSettingsCommand>,
    strategies_by_id: HashMap<u64, StrategySnapshot>,
}

impl Clone for SessionStats {
    fn clone(&self) -> Self {
        Self {
            label: self.label.clone(),
            market: self.market.clone(),
            market_index: self.market_index,
            connected_now: self.connected_now,
            server_events: self.server_events,
            connected_fresh: self.connected_fresh,
            connected_again: self.connected_again,
            reconnecting: self.reconnecting,
            disconnected: self.disconnected,
            engine_responses: self.engine_responses,
            engine_method_counts: self.engine_method_counts.clone(),
            raw_events: self.raw_events,
            server_logs: self.server_logs,
            settings_events: self.settings_events,
            strategy_events: self.strategy_events,
            strategy_snapshot_events: self.strategy_snapshot_events,
            strategy_schema_events: self.strategy_schema_events,
            strategy_schema_fields: self.strategy_schema_fields,
            strategy_schema_kinds: self.strategy_schema_kinds,
            market_events: self.market_events,
            trades_apply: self.trades_apply,
            target_trade_packets: self.target_trade_packets,
            orderbook_apply: self.orderbook_apply,
            target_orderbook_full: self.target_orderbook_full,
            target_orderbook_update: self.target_orderbook_update,
            last_trade_price: self.last_trade_price,
            last_book_bid: self.last_book_bid,
            last_book_ask: self.last_book_ask,
            last_book_kind: self.last_book_kind,
            last_market_price: self.last_market_price,
            market_invariant_error: self.market_invariant_error.clone(),
            order_events: self.order_events,
            order_event_kinds: self.order_event_kinds.clone(),
            order_uid_by_request: self.order_uid_by_request.clone(),
            order_status_by_uid: self.order_status_by_uid.clone(),
            order_market_by_uid: self.order_market_by_uid.clone(),
            order_sell_reason_by_uid: self.order_sell_reason_by_uid.clone(),
            order_ignored_by_uid: self.order_ignored_by_uid.clone(),
            balance_events: self.balance_events,
            balance_snapshot_events: self.balance_snapshot_events,
            balance_incremental_events: self.balance_incremental_events,
            transfer_asset_events: self.transfer_asset_events,
            transfer_asset_updated_mask: self.transfer_asset_updated_mask,
            transfer_asset_failures: self.transfer_asset_failures,
            coin_card_events: self.coin_card_events,
            coin_card_updates: self.coin_card_updates,
            coin_card_failures: self.coin_card_failures,
            coin_card_last_count: self.coin_card_last_count,
            parse_failed: self.parse_failed,
            parse_failures: self.parse_failures.clone(),
            candles_requested: self.candles_requested,
            candles_chunks: self.candles_chunks,
            candles_ignored: self.candles_ignored,
            candles_payload_bytes: self.candles_payload_bytes,
            candles_seen_chunks: self.candles_seen_chunks.clone(),
            candles_last_progress: self.candles_last_progress,
            candles_complete: self.candles_complete.clone(),
            candles_aggregator: CandlesAggregator::new(),
            last_settings: self.last_settings.clone(),
            strategies_by_id: self.strategies_by_id.clone(),
        }
    }
}

impl SessionStats {
    fn engine_method_count(&self, method: EngineMethod) -> u64 {
        self.engine_method_counts
            .get(&(method.to_byte()))
            .copied()
            .unwrap_or(0)
    }

    fn summary(&self) -> String {
        let parse_failed_detail = if self.parse_failures.is_empty() {
            String::new()
        } else {
            let recent = self.parse_failures.iter().rev().take(4).collect::<Vec<_>>();
            let mut parts = Vec::with_capacity(recent.len());
            for pf in recent.iter().rev() {
                parts.push(format!(
                    "#{}:{:?}:len{}:{:016X}",
                    pf.event_no, pf.cmd, pf.len, pf.hash
                ));
            }
            format!(" [{}]", parts.join(","))
        };
        let candles = self
            .candles_complete
            .as_ref()
            .map(CandlesSnapshotSummary::summary)
            .unwrap_or_else(|| {
                let missing = missing_chunk_indexes(&self.candles_seen_chunks);
                format!(
                    "incomplete chunks={} ignored={} payload_bytes={} progress={}/{} missing=[{}]",
                    self.candles_chunks,
                    self.candles_ignored,
                    self.candles_payload_bytes,
                    self.candles_last_progress.0,
                    self.candles_last_progress.1,
                    missing
                )
            });
        format!(
            "connected_now={} fresh={} again={} reconnecting={} disconnected={} server_events={} engine={} raw={} logs={} settings={} strats={} strat_snapshots={} schema_events={} schema_kinds={} schema_fields={} strategy_rows={} markets={} trades={} target_trade_packets={} books={} target_book_full={} target_book_update={} market_probe=[{}] order_events={} balances={} transfer_assets={} mask={:#05b} failures={} coin_card_events={} updates={} failures={} last_count={} parse_failed={}{} candles={}",
            self.connected_now,
            self.connected_fresh,
            self.connected_again,
            self.reconnecting,
            self.disconnected,
            self.server_events,
            self.engine_responses,
            self.raw_events,
            self.server_logs,
            self.settings_events,
            self.strategy_events,
            self.strategy_snapshot_events,
            self.strategy_schema_events,
            self.strategy_schema_kinds,
            self.strategy_schema_fields,
            self.strategies_by_id.len(),
            self.market_events,
            self.trades_apply,
            self.target_trade_packets,
            self.orderbook_apply,
            self.target_orderbook_full,
            self.target_orderbook_update,
            self.market_probe_summary(),
            self.order_events,
            self.balance_events,
            self.transfer_asset_events,
            self.transfer_asset_updated_mask,
            self.transfer_asset_failures,
            self.coin_card_events,
            self.coin_card_updates,
            self.coin_card_failures,
            self.coin_card_last_count,
            self.parse_failed,
            parse_failed_detail,
            candles
        )
    }

    fn market_probe_summary(&self) -> String {
        let market_price = self
            .last_market_price
            .map(|p| {
                format!(
                    "market_bid={:.8} market_ask={:.8} mark={:.8}/{}",
                    p.bid, p.ask, p.mark_price, p.mark_price_found
                )
            })
            .unwrap_or_else(|| "market_price=none".to_string());
        format!(
            "market={} idx={:?} book_kind={:?} bid={:?} ask={:?} trade={:?} {} err={}",
            self.market,
            self.market_index,
            self.last_book_kind,
            self.last_book_bid,
            self.last_book_ask,
            self.last_trade_price,
            market_price,
            self.market_invariant_error.as_deref().unwrap_or("none")
        )
    }
}

struct Session {
    client: MoonClient,
    latest_snapshot: Option<Arc<EventDispatcherSnapshot>>,
    stats: Arc<Mutex<SessionStats>>,
    parse_failure_correlations_logged: usize,
}

impl Session {
    fn connect(
        label: &str,
        cfg: &FireConfig,
        keys: ImportedKeys,
        provided_strategy: Option<StrategySnapshot>,
    ) -> Self {
        let stats = Arc::new(Mutex::new(SessionStats {
            label: label.to_string(),
            market: cfg.market.clone(),
            ..Default::default()
        }));
        let initial_strategies: Vec<_> = provided_strategy.iter().cloned().collect();
        if let Some(strategy) = provided_strategy.as_ref() {
            println!(
                "FIRETEST {label}: local strategy snapshot seeded id={} ver={} last_date={}",
                strategy.strategy_id, strategy.strategy_ver, strategy.last_date
            );
        }

        let init = InitConfig {
            mm_orders_subscribe: Some(true),
            subscribe_trades: Some(TradesStreamMode::TradesOnly),
            subscribe_orderbooks: vec![cfg.market.clone()],
            step_timeout: None,
            initial_strategies: Some(InitialStrategies::new(0, initial_strategies)),
        };

        println!(
            "FIRETEST {label}: connecting to {}:{} market={}",
            cfg.host, cfg.port, cfg.market
        );
        let client = MoonClient::connect(
            ClientConfig::new(&cfg.host, cfg.port, keys.master_key, keys.mac_key)
                .with_transport_mode(cfg.mask_ver)
                .with_client_id(rand::random()),
            ConnectConfig::new(init).with_connect_timeout(cfg.connect_timeout),
        )
        .unwrap_or_else(|err| panic!("FIRETEST {label}: MoonClient connect failed: {err}"));
        client
            .debug_reset_err_emu_diagnostics()
            .unwrap_or_else(|err| panic!("FIRETEST {label}: reset diagnostics failed: {err}"));

        let mut session = Self {
            client,
            latest_snapshot: None,
            stats,
            parse_failure_correlations_logged: 0,
        };
        assert!(
            pump_session_until(&mut session, cfg.connect_timeout, "connect ready", |s| {
                let st = s.snapshot();
                st.connected_now && st.connected_fresh > 0 && s.latest_snapshot.is_some()
            }),
            "FIRETEST {label}: MoonClient did not connect within {:?}",
            cfg.connect_timeout
        );
        session
    }

    fn pump(&mut self, duration: Duration) {
        self.drain_queued();
        if !duration.is_zero() {
            std::thread::sleep(duration);
        }
        self.log_new_parse_failure_correlations("immediate");
        self.drain_queued();
        self.refresh_stats_from_dispatcher(false);
    }

    fn drain_queued(&mut self) {
        let mut lifecycle = Vec::new();
        self.client.drain_lifecycle_events_into(&mut lifecycle);
        for event in lifecycle {
            self.record_lifecycle_event(event);
        }

        if let Some(snapshot) = self.client.snapshot() {
            self.latest_snapshot = Some(snapshot);
        }
        let snapshot = self.latest_snapshot.clone();

        let mut events = Vec::new();
        self.client.drain_events_into(&mut events);
        for event in events {
            record_event(&self.stats, &event, snapshot.as_deref(), None);
        }
    }

    fn record_lifecycle_event(&self, event: LifecycleEvent) {
        let mut st = self.stats.lock().unwrap();
        match event {
            LifecycleEvent::Connected { fresh: true } => {
                st.connected_now = true;
                st.connected_fresh += 1;
            }
            LifecycleEvent::Connected { fresh: false } => {
                st.connected_now = true;
                st.connected_again += 1;
            }
            LifecycleEvent::Reconnecting => {
                st.connected_now = false;
                st.reconnecting += 1;
            }
            LifecycleEvent::Disconnected => {
                st.connected_now = false;
                st.disconnected += 1;
            }
            LifecycleEvent::ConnectFailed { error } => {
                panic!("FIRETEST {}: MoonClient connect failed: {error}", st.label);
            }
            _ => {}
        }
        println!("LIFECYCLE->{}: {event:?}", st.label);
    }

    fn log_new_parse_failure_correlations(&mut self, label: &str) {
        let (session_label, records, total) = {
            let st = self.stats.lock().unwrap();
            let records = st
                .parse_failures
                .iter()
                .skip(self.parse_failure_correlations_logged)
                .cloned()
                .collect::<Vec<_>>();
            (st.label.clone(), records, st.parse_failures.len())
        };
        if records.is_empty() {
            return;
        }
        let diag = self.client.err_emu_diagnostics_snapshot();
        log_parse_failure_correlations(label, &session_label, &diag, &records);
        self.parse_failure_correlations_logged = total;
    }

    fn request_transfer_assets_refresh(&mut self) {
        self.client
            .balances()
            .refresh_transfer_assets()
            .expect("MoonClient refresh_transfer_assets must queue");
        println!(
            "FIRETEST transfer assets refresh queued kinds=[{}]",
            ExchangeKind::ALL
                .into_iter()
                .map(ExchangeKind::name)
                .collect::<Vec<_>>()
                .join(",")
        );
    }

    fn request_coin_card_candles(&mut self, market: &str, kind: DeepHistoryKind) {
        self.client
            .candles()
            .request_coin_card(market, kind)
            .expect("MoonClient request_coin_card_candles must queue");
        println!(
            "FIRETEST non-blocking CoinCard candles queued market={} kind={kind:?}",
            market
        );
    }

    fn coin_card_candles_count(&self, market: &str, kind: DeepHistoryKind) -> usize {
        self.latest_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.coin_card_candles().get(market, kind))
            .map(|rows| rows.len())
            .unwrap_or(0)
    }

    fn state_snapshot(&self) -> Arc<EventDispatcherSnapshot> {
        self.latest_snapshot.as_ref().cloned().unwrap_or_else(|| {
            self.client
                .snapshot()
                .expect("MoonClient snapshot is not ready")
        })
    }

    fn maybe_state_snapshot(&self) -> Option<Arc<EventDispatcherSnapshot>> {
        self.latest_snapshot
            .as_ref()
            .cloned()
            .or_else(|| self.client.snapshot())
    }

    fn assert_coin_card_candles_healthy(&self, market: &str, kind: DeepHistoryKind) {
        let count = self.coin_card_candles_count(market, kind);
        assert!(
            count >= FIRETEST_MIN_COIN_CARD_CANDLES,
            "FireTest non-blocking CoinCard candles returned too few rows for {market} {kind:?}: got {count}, expected at least {}",
            FIRETEST_MIN_COIN_CARD_CANDLES
        );
        println!(
            "OK: non-blocking CoinCard candles market={} kind={kind:?} count={}",
            market, count
        );
    }

    fn refresh_stats_from_dispatcher(&self, log_changes: bool) {
        let Some(snapshot) = self.maybe_state_snapshot() else {
            return;
        };
        let mut st = self.stats.lock().unwrap();
        let event_no = st.server_events;
        sync_market_probe_from_dispatcher(&mut st, event_no, snapshot.as_ref(), log_changes);
        record_order_state_snapshot(&mut st, snapshot.as_ref());

        if let Some(settings) = snapshot.settings().client_settings.clone() {
            st.last_settings = Some(settings);
        }
        if let Some(state) = snapshot.markets().trade_state(&st.market) {
            if state.last_trade_price > 0.0 {
                st.last_trade_price = Some(state.last_trade_price);
                st.target_trade_packets = st.target_trade_packets.max(st.trades_apply);
            }
        }
        if let (Some(market_index), Some(book_kind)) = (st.market_index, st.last_book_kind) {
            if let Some(kind) = OrderBookKind::from_u8(book_kind) {
                if let Some(top) = snapshot.order_books().top_of_book(market_index, kind) {
                    if let (Some(bid), Some(ask)) = (top.bid, top.ask) {
                        st.last_book_bid = Some(bid.rate);
                        st.last_book_ask = Some(ask.rate);
                    }
                }
            }
        }
    }

    fn snapshot(&self) -> SessionStats {
        self.stats.lock().unwrap().clone()
    }

    fn protocol_summary(&self) -> String {
        let m = self.client.protocol_metrics_snapshot();
        let market_apply = self
            .maybe_state_snapshot()
            .as_ref()
            .and_then(|snapshot| snapshot.markets().last_markets_list_apply_timing())
            .map(|t| {
                format!(
                    " get_markets_list_apply(total={}us markets={}us index={}us corr={}us ref={}us payload={} markets={} corr={})",
                    t.total_ns / 1_000,
                    t.market_loop_ns / 1_000,
                    t.index_rebuild_ns / 1_000,
                    t.corr_loop_ns / 1_000,
                    t.ref_passes_ns / 1_000,
                    t.payload_len,
                    t.market_count,
                    t.corr_count
                )
            })
            .unwrap_or_default();
        format!(
            "recv={} reader_cpu(avg/max={}us/{}us max_src={} >100us/>1ms/>5ms={}/{}/{}) reader_wait(count={} avg/max={}us/{}us max_src={}) writer_cpu(avg/max={}us/{}us >100us/>1ms/>5ms={}/{}/{}) active_dispatch(avg/max={}us/{}us max_src={} events={} actions={} >100us/>1ms/>5ms={}/{}/{}) app_enqueue(avg/max={}us/{}us max_src={} events={} mode={} >100us/>1ms/>5ms={}/{}/{}) writer_tick_wall(count={} avg/max={}us/{}us) send_max={}us public_events={}{}",
            m.recv_count,
            avg_us(m.reader_protocol_ns, m.reader_protocol_count),
            m.reader_protocol_max_ns / 1_000,
            metric_cmd_label(
                m.reader_protocol_max_cmd,
                u8::MAX,
                m.reader_protocol_max_payload_len
            ),
            m.reader_protocol_over_100us,
            m.reader_protocol_over_1ms,
            m.reader_protocol_over_5ms,
            m.reader_protocol_wait_count,
            avg_us(m.reader_protocol_wait_ns, m.reader_protocol_wait_count),
            m.reader_protocol_wait_max_ns / 1_000,
            metric_cmd_label(
                m.reader_protocol_wait_max_cmd,
                u8::MAX,
                m.reader_protocol_wait_max_payload_len
            ),
            avg_us(m.writer_cpu_ns, m.writer_cpu_count),
            m.writer_cpu_max_ns / 1_000,
            m.writer_cpu_over_100us,
            m.writer_cpu_over_1ms,
            m.writer_cpu_over_5ms,
            avg_us(m.active_dispatch_ns, m.active_dispatch_count),
            m.active_dispatch_max_ns / 1_000,
            metric_cmd_label(
                m.active_dispatch_max_cmd,
                m.active_dispatch_max_api_method,
                m.active_dispatch_max_payload_len
            ),
            m.active_dispatch_max_events,
            m.active_dispatch_max_actions,
            m.active_dispatch_over_100us,
            m.active_dispatch_over_1ms,
            m.active_dispatch_over_5ms,
            avg_us(m.app_enqueue_ns, m.app_enqueue_count),
            m.app_enqueue_max_ns / 1_000,
            metric_cmd_label(
                m.app_enqueue_max_cmd,
                m.app_enqueue_max_api_method,
                m.app_enqueue_max_payload_len
            ),
            m.app_enqueue_max_events,
            metric_app_mode_label(m.app_enqueue_max_mode),
            m.app_enqueue_over_100us,
            m.app_enqueue_over_1ms,
            m.app_enqueue_over_5ms,
            m.writer_tick_count,
            avg_us(m.writer_tick_ns, m.writer_tick_count),
            m.writer_tick_max_ns / 1_000,
            m.send_phase_max_ns / 1_000,
            m.public_event_queue_len,
            market_apply
        )
    }

    fn emit_active_lib_report(&mut self, profile: FireProfile, started_at: Instant) {
        self.drain_queued();
        let now_time = delphi_now_raw_for_test();
        self.refresh_stats_from_dispatcher(false);

        let snapshot = self.state_snapshot();
        let st = self.snapshot();
        let elapsed = started_at.elapsed().as_secs_f64();
        let update_count = st.engine_method_count(EngineMethod::UpdateMarketsList);
        println!(
            "FIRETEST ActiveLib report profile={} session={} elapsed={:.2}s update_markets={} balance_events={} snapshots={} increments={} orders_seen={} current_orders={} order_event_kinds=[{}]",
            profile.as_str(),
            st.label,
            elapsed,
            update_count,
            st.balance_events,
            st.balance_snapshot_events,
            st.balance_incremental_events,
            st.order_status_by_uid.len(),
            snapshot.orders().len(),
            format_count_map(&st.order_event_kinds),
        );

        for market in ACTIVE_LIB_REPORT_MARKETS {
            self.emit_active_lib_market_report(profile, snapshot.as_ref(), market, now_time);
        }
        self.emit_balance_asset_report(snapshot.as_ref());
        self.emit_order_report(snapshot.as_ref(), &st);
    }

    fn emit_active_lib_market_report(
        &self,
        profile: FireProfile,
        snapshot: &EventDispatcherSnapshot,
        market: &str,
        now_time: f64,
    ) {
        let Some(readers) = snapshot.market_history_readers(market) else {
            println!(
                "FIRETEST ActiveLib market={market}: no retained readers; reason=market not retained by trades storage scope"
            );
            return;
        };

        let mut last_prices = Vec::new();
        if let Some(reader) = readers.last_prices.as_ref() {
            reader.copy_last(reader.capacity(), &mut last_prices);
        }
        let mut mark_prices = Vec::new();
        if let Some(reader) = readers.mark_prices.as_ref() {
            reader.copy_last(reader.capacity(), &mut mark_prices);
        }
        let mut futures_25 = Vec::new();
        let mut futures_60 = Vec::new();
        let mut futures_all = Vec::new();
        if let Some(reader) = readers.futures_trades.as_ref() {
            reader.copy_last(reader.capacity(), &mut futures_all);
            let from_25 = now_time - 25.0 / 86_400.0;
            let from_60 = now_time - 60.0 / 86_400.0;
            reader.copy_time_range(
                from_25,
                now_time + 1.0 / 86_400.0,
                reader.capacity(),
                &mut futures_25,
            );
            reader.copy_time_range(
                from_60,
                now_time + 1.0 / 86_400.0,
                reader.capacity(),
                &mut futures_60,
            );
        }
        let mut spot_all = Vec::new();
        if let Some(reader) = readers.spot_trades.as_ref() {
            reader.copy_last(reader.capacity(), &mut spot_all);
        }

        let last_stats = price_line_stats(last_prices.iter().map(|p| (p.real_time, p.current)));
        let mark_stats = price_line_stats(mark_prices.iter().map(|p| (p.real_time, p.current)));
        let last_delta_1m = price_delta_for_window(
            last_prices.iter().map(|p| (p.real_time, p.current)),
            now_time,
            60.0,
        );
        let last_delta_1h = price_delta_for_window(
            last_prices.iter().map(|p| (p.real_time, p.current)),
            now_time,
            3600.0,
        );
        let mark_delta_1m = price_delta_for_window(
            mark_prices.iter().map(|p| (p.real_time, p.current)),
            now_time,
            60.0,
        );
        let manual_25 = trade_volume(&futures_25);
        let manual_60 = trade_volume(&futures_60);
        let derived = snapshot.market_history_derived_snapshot(market, now_time);
        let price = snapshot.markets().price(market);
        let handle = snapshot.markets().get(market);
        let balance = handle.as_ref().map(|handle| handle.balance_position());

        println!(
            "FIRETEST ActiveLib market={market} LastPrice count={} expected_by_updates~{} span={:.2}s expected_by_2s_span~{} min={:.8} max={:.8} delta_all={:.4}% delta_1m={:.4}% delta_1h={:.4}% values=[{}]",
            last_stats.count,
            self.snapshot().engine_method_count(EngineMethod::UpdateMarketsList),
            last_stats.span_secs,
            expected_price_points_for_span(last_stats),
            last_stats.min,
            last_stats.max,
            last_stats.delta_percent,
            last_delta_1m,
            last_delta_1h,
            format_last_price_values(&last_prices),
        );
        println!(
            "FIRETEST ActiveLib market={market} MarkPrice count={} span={:.2}s expected_by_2s_span~{} min={:.8} max={:.8} delta_all={:.4}% delta_1m={:.4}% values=[{}]",
            mark_stats.count,
            mark_stats.span_secs,
            expected_price_points_for_span(mark_stats),
            mark_stats.min,
            mark_stats.max,
            mark_stats.delta_percent,
            mark_delta_1m,
            format_mark_price_values(&mark_prices),
        );

        if let (Some(last), Some(mark)) = (last_prices.last(), mark_prices.last()) {
            let rel = rel_diff(f64::from(last.current), f64::from(mark.current));
            println!(
                "FIRETEST ActiveLib market={market} LastPrice_vs_MarkPrice last={:.8} mark={:.8} rel_diff={:.4}%",
                last.current,
                mark.current,
                rel * 100.0
            );
            assert!(
                rel <= PRICE_NEIGHBORHOOD_PCT,
                "ActiveLib {market}: MarkPrice diverged from LastPrice by {:.4}%",
                rel * 100.0
            );
        }

        let Some(derived) = derived else {
            panic!("ActiveLib {market}: missing derived snapshot");
        };
        println!(
            "FIRETEST ActiveLib market={market} trades retained futures={} spot={} manual_vol_25s={:.4} manual_vol_60s={:.4} active_vol_1m={:.4} active_vol_3m={:.4} active_vol_5m={:.4}",
            futures_all.len(),
            spot_all.len(),
            manual_25,
            manual_60,
            derived.trade_volumes.one_minute.total_value(),
            derived.trade_volumes.three_minutes.total_value(),
            derived.trade_volumes.five_minutes.total_value(),
        );
        println!(
            "FIRETEST ActiveLib market={market} deltas trade(1m={:.4}% 5m={:.4}%) last_price(1m={:.4}% 5m={:.4}% 15m={:.4}% 30m={:.4}% 1h={:.4}%) candle(5m={:.4}% 15m={:.4}% 30m={:.4}% 1h={:.4}% 2h={:.4}% 3h={:.4}% 24h={:.4}% 72h={:.4}%) combined(1m={:.4}% 5m={:.4}% 15m={:.4}% 30m={:.4}% 1h={:.4}% 2h={:.4}% 3h={:.4}% 24h={:.4}% 72h={:.4}%)",
            derived.trade_deltas.one_minute,
            derived.trade_deltas.five_minutes,
            derived.last_price_deltas.one_minute,
            derived.last_price_deltas.five_minutes,
            derived.last_price_deltas.fifteen_minutes,
            derived.last_price_deltas.thirty_minutes,
            derived.last_price_deltas.one_hour,
            derived.candle_deltas.five_minutes,
            derived.candle_deltas.fifteen_minutes,
            derived.candle_deltas.thirty_minutes,
            derived.candle_deltas.one_hour,
            derived.candle_deltas.two_hours,
            derived.candle_deltas.three_hours,
            derived.candle_deltas.twenty_four_hours,
            derived.candle_deltas.seventy_two_hours,
            derived.deltas.one_minute,
            derived.deltas.five_minutes,
            derived.deltas.fifteen_minutes,
            derived.deltas.thirty_minutes,
            derived.deltas.one_hour,
            derived.deltas.two_hours,
            derived.deltas.three_hours,
            derived.deltas.twenty_four_hours,
            derived.deltas.seventy_two_hours,
        );
        println!(
            "FIRETEST ActiveLib market={market} volumes candle(5m={:.4} 15m={:.4} 30m={:.4} 1h={:.4} 2h={:.4} 3h={:.4} 24h={:.4} 72h={:.4}) trade_buy_sell_1m=({:.4}/{:.4}) trade_buy_sell_5m=({:.4}/{:.4})",
            derived.candle_volumes.five_minutes,
            derived.candle_volumes.fifteen_minutes,
            derived.candle_volumes.thirty_minutes,
            derived.candle_volumes.one_hour,
            derived.candle_volumes.two_hours,
            derived.candle_volumes.three_hours,
            derived.candle_volumes.twenty_four_hours,
            derived.candle_volumes.seventy_two_hours,
            derived.trade_volumes.one_minute.buy_value,
            derived.trade_volumes.one_minute.sell_value,
            derived.trade_volumes.five_minutes.buy_value,
            derived.trade_volumes.five_minutes.sell_value,
        );

        let zero_reason = active_lib_zero_reason(profile, last_stats, futures_all.len());
        let zero_fields = active_lib_zero_fields(&derived, profile);
        if !zero_fields.is_empty() {
            println!(
                "FIRETEST ActiveLib market={market} ZERO_FIELDS [{}] reason={zero_reason}",
                zero_fields.join(",")
            );
        }

        assert!(
            last_stats.count > 0,
            "ActiveLib {market}: LastPrice history is empty"
        );
        assert!(
            mark_stats.count > 0,
            "ActiveLib {market}: MarkPrice history is empty"
        );
        assert!(
            last_stats.delta_percent <= 10.0,
            "ActiveLib {market}: LastPrice delta looks insane: {:.4}%",
            last_stats.delta_percent
        );
        assert!(
            mark_stats.delta_percent <= 10.0,
            "ActiveLib {market}: MarkPrice delta looks insane: {:.4}%",
            mark_stats.delta_percent
        );
        assert!(
            manual_25 <= manual_60 + EPS,
            "ActiveLib {market}: manual 25s volume exceeds 60s volume: {manual_25} > {manual_60}"
        );
        if manual_60 > EPS {
            assert!(
                derived.trade_volumes.one_minute.total_value() > EPS,
                "ActiveLib {market}: manual 60s trade volume is non-zero but active 1m volume is zero"
            );
        }

        if let Some(price) = price {
            let funding_hours = (price.funding_time - now_time) * 24.0;
            println!(
                "FIRETEST ActiveLib market={market} funding_rate={:.8} funding_time={:.8} funding_hours_from_now={:.3} mark_current={:.8}/{} bid={:.8} ask={:.8}",
                price.funding_rate,
                price.funding_time,
                funding_hours,
                price.mark_price,
                price.mark_price_found,
                price.bid,
                price.ask,
            );
            assert!(
                price.funding_time <= EPS || funding_hours.abs() <= 12.0,
                "ActiveLib {market}: funding time is outside 12h window: {funding_hours:.3}h"
            );
        }
        if let Some(balance) = balance {
            println!(
                "FIRETEST ActiveLib market={market} balance init={:.8} locked={:.8} pos_size={:.8} pos_price={:.8} liq={:.8} leverage={} asset={:.8}/{:.8} pnl={:.8} epoch={}",
                balance.initial_balance,
                balance.locked_balance,
                balance.pos_size,
                balance.pos_price,
                balance.liq_price,
                balance.leverage_x,
                balance.asset_balance,
                balance.asset_balance_full,
                balance.total_profit(),
                balance.last_balance_epoch,
            );
        }
    }

    fn emit_balance_asset_report(&self, snapshot: &EventDispatcherSnapshot) {
        let balances = snapshot.balances();
        println!(
            "FIRETEST ActiveLib balances global btc_total={:.8} btc_locked={:.8} btc_full={:.8} special_coin={:.8} total_pnl={:.8} rows={}",
            balances.global.btc_balance_total,
            balances.global.btc_balance_locked,
            balances.global.btc_balance_full,
            balances.global.special_coin_balance,
            balances.global.total_pnl,
            snapshot.markets().market_count(),
        );
        assert!(
            balances.global.btc_balance_total.abs()
                + balances.global.btc_balance_full.abs()
                + balances.global.special_coin_balance.abs()
                > EPS,
            "ActiveLib global balances are zero"
        );

        let mut assets = snapshot
            .markets()
            .iter()
            .filter_map(|handle| {
                handle.with(|market| {
                    let amount = market
                        .asset_balance_full
                        .abs()
                        .max(market.asset_balance.abs());
                    (amount > EPS).then(|| {
                        (
                            market.bn_market_currency.clone(),
                            market.bn_market_name.clone(),
                            market.asset_balance,
                            market.asset_balance_full,
                        )
                    })
                })
            })
            .collect::<Vec<_>>();
        assets.sort_by(|a, b| b.3.abs().total_cmp(&a.3.abs()));
        let preview = assets
            .iter()
            .take(12)
            .map(|(asset, market, bal, full)| format!("{asset}:{market}:{bal:.8}/{full:.8}"))
            .collect::<Vec<_>>()
            .join(" ");
        println!(
            "FIRETEST ActiveLib assets nonzero_count={} preview=[{}]",
            assets.len(),
            preview
        );

        let mut total_transfer_nonzero = 0usize;
        for (kind, rows) in snapshot.transfer_assets().iter() {
            let nonzero = rows
                .iter()
                .filter(|asset| asset.amount.abs().max(asset.total.abs()) > EPS)
                .count();
            total_transfer_nonzero += nonzero;
            let preview = rows
                .iter()
                .take(16)
                .map(|asset| format!("{}:{:.8}/{:.8}", asset.currency, asset.amount, asset.total))
                .collect::<Vec<_>>()
                .join(" ");
            println!(
                "FIRETEST ActiveLib transfer_assets kind={} revision={} rows={} nonzero={} preview=[{}]",
                kind.name(),
                snapshot.transfer_assets().kind_revision(kind),
                rows.len(),
                nonzero,
                preview
            );
            assert!(
                snapshot.transfer_assets().kind_revision(kind) > 0,
                "ActiveLib transfer assets for {} were not refreshed",
                kind.name()
            );
        }
        assert!(
            total_transfer_nonzero > 0,
            "ActiveLib transfer assets were refreshed but all amount/total values are zero"
        );
    }

    fn emit_order_report(&self, snapshot: &EventDispatcherSnapshot, st: &SessionStats) {
        let mut statuses = HashMap::<String, u64>::new();
        for order in snapshot.orders().iter() {
            *statuses.entry(format!("{:?}", order.status)).or_default() += 1;
        }
        println!(
            "FIRETEST ActiveLib orders seen_total={} current={} statuses=[{}] events=[{}] markets_seen=[{}]",
            st.order_status_by_uid.len(),
            snapshot.orders().len(),
            format_count_map(&statuses),
            format_count_map(&st.order_event_kinds),
            format_order_market_preview(st),
        );
    }

    fn strategy_snapshot(&self, strategy_id: u64) -> Option<StrategySnapshot> {
        self.maybe_state_snapshot()
            .and_then(|snapshot| snapshot.strategy_snapshot(strategy_id).cloned())
    }

    fn send_strategy_snapshot_batch(&mut self, strategies: &[StrategySnapshot]) {
        self.client
            .strategies()
            .send_snapshot_batch(strategies.to_vec())
            .expect("MoonClient strategy snapshot batch must queue");
    }

    fn send_new_order(
        &mut self,
        market: &str,
        is_short: bool,
        price: f64,
        strat_id: u64,
        order_size: f64,
    ) -> u64 {
        let ticket = self
            .client
            .trade()
            .new_order(
                moonproto::NewOrderParams::new(
                    market,
                    if is_short {
                        moonproto::OrderSide::Short
                    } else {
                        moonproto::OrderSide::Long
                    },
                    price,
                    order_size,
                )
                .with_strategy_id(strat_id),
            )
            .expect("MoonClient new_order must queue");
        ticket.request_uid
    }

    fn replace_order(&mut self, uid: u64, new_price: f64) -> bool {
        self.client.orders().move_order(uid, new_price).is_ok()
    }

    fn cancel_order(&mut self, uid: u64) -> bool {
        self.client.orders().cancel(uid).is_ok()
    }

    fn panic_sell_order(&mut self, uid: u64, turn_on: bool) -> bool {
        self.client.orders().turn_panic_sell(uid, turn_on).is_ok()
    }

    fn request_candles_snapshot(&mut self) {
        let mut st = self.stats.lock().unwrap();
        st.candles_requested = true;
        println!(
            "FIRETEST {}: full candles snapshot is maintained by MoonClient after trades subscription",
            st.label
        );
    }
}

fn write_strategy_info_dump(
    profile: FireProfile,
    cfg: &FireConfig,
    sessions: &[(&str, &Session)],
) -> PathBuf {
    let path = std::env::var_os("MOONPROTO_FIRETEST_STRATEGY_DUMP")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join(format!("firetest_strategy_info_{}.txt", profile.as_str()))
        });
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap_or_else(|err| {
            panic!(
                "cannot create strategy dump dir {}: {err}",
                parent.display()
            )
        });
    }

    let mut out = String::new();
    writeln!(
        out,
        "FireTest strategy dump\nprofile={}\nserver={}:{}\nmarket={}\n",
        profile.as_str(),
        cfg.host,
        cfg.port,
        cfg.market
    )
    .unwrap();

    for (label, session) in sessions {
        append_session_strategy_dump(&mut out, label, session);
    }

    fs::write(&path, out)
        .unwrap_or_else(|err| panic!("cannot write strategy dump {}: {err}", path.display()));
    println!("FIRETEST strategy info dump: {}", path.display());
    path
}

fn write_strategy_raw_dump(
    label: &str,
    event_no: u64,
    kind: &str,
    server_epoch: u64,
    raw_data: &[u8],
) -> Option<PathBuf> {
    let dir = std::env::var_os("MOONPROTO_FIRETEST_STRATEGY_RAW_DUMP_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("firetest_strategy_raw")
        });
    if let Err(err) = fs::create_dir_all(&dir) {
        eprintln!(
            "WARN: cannot create strategy raw dump dir {}: {err}",
            dir.display()
        );
        return None;
    }

    let file_name = format!(
        "{}_strat_{}_event_{:06}_epoch_{}_raw_{}.bin",
        sanitize_file_component(label),
        sanitize_file_component(kind),
        event_no,
        server_epoch,
        raw_data.len()
    );
    let path = dir.join(file_name);
    match fs::write(&path, raw_data) {
        Ok(()) => Some(path),
        Err(err) => {
            eprintln!(
                "WARN: failed to write strategy raw dump label={label} kind={kind} epoch={server_epoch} len={} err={err}",
                raw_data.len()
            );
            None
        }
    }
}

fn append_session_strategy_dump(out: &mut String, label: &str, session: &Session) {
    let stats = session.snapshot();
    let snapshot = session.state_snapshot();
    let strats = snapshot.strats();
    writeln!(
        out,
        "## Session {label}\nsummary={}\nschema_revision={} schema_failures={} schema_error={}\n",
        stats.summary(),
        strats.strategy_schema_revision(),
        strats.strategy_schema_failures(),
        strats.strategy_schema_last_error().unwrap_or("none")
    )
    .unwrap();

    if let Some(schema) = strats.strategy_schema() {
        append_schema_dump(
            out,
            strats.strategy_schema_raw().map_or(0, |raw| raw.len()),
            schema,
        );
    } else {
        writeln!(out, "Schema: <missing>\n").unwrap();
    }

    let mut snapshots = snapshot.strategy_snapshot_vec();
    snapshots.sort_by_key(|s| s.strategy_id);
    writeln!(out, "Strategies: count={}", snapshots.len()).unwrap();
    for strategy in snapshots {
        let kind_name = strats
            .strategy_schema()
            .and_then(|schema| schema.kind_name(strategy.kind))
            .unwrap_or("?");
        writeln!(
            out,
            "- id={} ver={} last_date={} checked={} kind={}:{} path={} fields={}",
            strategy.strategy_id,
            strategy.strategy_ver,
            strategy.last_date,
            strategy.checked,
            strategy.kind,
            kind_name,
            strategy.path,
            strategy.fields.len()
        )
        .unwrap();
        let mut fields: Vec<_> = strategy.fields.iter().collect();
        fields.sort_by(|a, b| a.0.cmp(b.0));
        for (name, value) in fields {
            writeln!(out, "    {} = {}", name, field_value_text(value)).unwrap();
        }
    }
    writeln!(out).unwrap();
}

fn append_schema_dump(out: &mut String, raw_len: usize, schema: &StrategySchema) {
    writeln!(
        out,
        "Schema: raw={} version={} kinds={} fields={}",
        raw_len,
        schema.format_version,
        schema.kinds.len(),
        schema.fields.len()
    )
    .unwrap();
    writeln!(out, "Kinds:").unwrap();
    for kind in &schema.kinds {
        writeln!(out, "- {} {}", kind.ordinal, kind.name).unwrap();
    }

    writeln!(out, "Chapters/Layout markers:").unwrap();
    for field in &schema.fields {
        match &field.layout {
            StrategyFieldLayout::ChapterClass { value, chapter } => {
                writeln!(
                    out,
                    "- field={} chapter_class={} chapter={}",
                    field.name, value, chapter
                )
                .unwrap();
            }
            StrategyFieldLayout::FilterClass(value) => {
                writeln!(out, "- field={} filter_class={}", field.name, value).unwrap();
            }
            StrategyFieldLayout::Comment(value) => {
                writeln!(out, "- field={} comment={}", field.name, value).unwrap();
            }
            StrategyFieldLayout::None => {}
        }
    }

    writeln!(out, "Fields:").unwrap();
    for field in &schema.fields {
        let visible = field
            .visible_kind_ordinals
            .iter()
            .map(|ord| format!("{}:{}", ord, schema.kind_name(*ord).unwrap_or("?")))
            .collect::<Vec<_>>()
            .join(",");
        writeln!(
            out,
            "- {} type={}({}) ui={} flags=0x{:02X} default={} visible=[{}] layout={} static_picklist={} dynamic_picklist={}",
            field.name,
            field.raw_type_id(),
            field.type_id.name(),
            ui_kind_text(field.ui_kind),
            field.raw_flags(),
            field
                .default_value
                .as_ref()
                .map(field_value_text)
                .unwrap_or_else(|| "none".to_string()),
            visible,
            layout_text(&field.layout),
            field
                .static_picklist_raw()
                .map(|raw| short_text(raw, 220))
                .unwrap_or_else(|| "none".to_string()),
            field
                .dynamic_picklist
                .as_ref()
                .map(dynamic_picklist_text)
                .unwrap_or_else(|| "none".to_string())
        )
        .unwrap();
    }
    writeln!(out).unwrap();
}

fn ui_kind_text(kind: StrategyFieldUiKind) -> &'static str {
    match kind {
        StrategyFieldUiKind::Edit => "edit",
        StrategyFieldUiKind::Checkbox => "checkbox",
        StrategyFieldUiKind::Combo => "combo",
        StrategyFieldUiKind::Color => "color",
        StrategyFieldUiKind::Unknown(_) => "unknown",
    }
}

fn layout_text(layout: &StrategyFieldLayout) -> String {
    match layout {
        StrategyFieldLayout::None => "none".to_string(),
        StrategyFieldLayout::Comment(value) => format!("comment({})", short_text(value, 120)),
        StrategyFieldLayout::FilterClass(value) => {
            format!("filter_class({})", short_text(value, 120))
        }
        StrategyFieldLayout::ChapterClass { value, chapter } => format!(
            "chapter_class(value={}, chapter={})",
            short_text(value, 120),
            short_text(chapter, 120)
        ),
    }
}

fn dynamic_picklist_text(value: &StrategyDynamicPicklist) -> String {
    match value {
        StrategyDynamicPicklist::HookStrategies => "hook_strategies".to_string(),
        StrategyDynamicPicklist::AllStrategies => "all_strategies".to_string(),
        StrategyDynamicPicklist::FieldName(name) => format!("field_name({name})"),
    }
}

fn field_value_text(value: &FieldValue) -> String {
    match value {
        FieldValue::Bool(v) => v.to_string(),
        FieldValue::Int32(v) => v.to_string(),
        FieldValue::Int64(v) => v.to_string(),
        FieldValue::Double(v) => format!("{v:.12}"),
        FieldValue::String(v) => format!("{:?}", short_text(v, 260)),
        FieldValue::Byte(v) => v.to_string(),
        FieldValue::Word(v) => v.to_string(),
        FieldValue::UInt32(v) => v.to_string(),
        FieldValue::UInt64(v) => v.to_string(),
        FieldValue::Single(v) => format!("{v:.7}"),
    }
}

#[derive(Debug, Clone, Copy)]
struct PriceLineStats {
    count: usize,
    min: f64,
    max: f64,
    delta_percent: f64,
    span_secs: f64,
}

fn price_line_stats<I>(rows: I) -> PriceLineStats
where
    I: IntoIterator<Item = (f64, f32)>,
{
    let mut count = 0usize;
    let mut min = f64::INFINITY;
    let mut max = 0.0f64;
    let mut first_time = 0.0f64;
    let mut last_time = 0.0f64;
    for (time, price) in rows {
        if price <= 0.0 {
            continue;
        }
        let price = f64::from(price);
        if count == 0 {
            first_time = time;
        }
        last_time = time;
        count += 1;
        min = min.min(price);
        max = max.max(price);
    }
    let delta_percent = if count > 0 && min > 0.0 && max >= min {
        (max / min - 1.0) * 100.0
    } else {
        0.0
    };
    PriceLineStats {
        count,
        min: if count > 0 { min } else { 0.0 },
        max,
        delta_percent,
        span_secs: if count > 1 {
            ((last_time - first_time) * 86_400.0).max(0.0)
        } else {
            0.0
        },
    }
}

fn expected_price_points_for_span(stats: PriceLineStats) -> usize {
    if stats.count == 0 {
        0
    } else {
        (stats.span_secs / 2.0).floor() as usize + 1
    }
}

fn price_delta_for_window<I>(rows: I, now_time: f64, window_seconds: f64) -> f64
where
    I: IntoIterator<Item = (f64, f32)>,
{
    let from_time = now_time - window_seconds / 86_400.0;
    price_line_stats(
        rows.into_iter()
            .filter(|(time, _)| *time >= from_time && *time <= now_time),
    )
    .delta_percent
}

fn format_last_price_values(rows: &[LastPricePoint]) -> String {
    rows.iter()
        .map(|p| format!("{:.8}", p.current))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_mark_price_values(rows: &[MarkPricePoint]) -> String {
    rows.iter()
        .map(|p| format!("{:.8}", p.current))
        .collect::<Vec<_>>()
        .join(",")
}

fn trade_volume(rows: &[TradeHistoryRow]) -> f64 {
    rows.iter().map(|row| f64::from(row.traded_value())).sum()
}

fn rel_diff(a: f64, b: f64) -> f64 {
    let denom = a.abs().max(b.abs()).max(EPS);
    (a - b).abs() / denom
}

fn active_lib_zero_reason(
    profile: FireProfile,
    last_stats: PriceLineStats,
    futures_count: usize,
) -> String {
    if last_stats.count == 0 {
        "LastPrice storage did not receive UpdateMarketsList rows".to_string()
    } else if futures_count == 0 {
        "TradesStream retained no futures rows for this market".to_string()
    } else if profile == FireProfile::Quick {
        format!(
            "quick profile captured only {:.2}s of retained price line; zero deltas can be a real flat price sample, and long candle windows may be absent",
            last_stats.span_secs
        )
    } else {
        "possible quiet market or ActiveLib derived-state bug; inspect fields".to_string()
    }
}

fn active_lib_zero_fields(
    derived: &moonproto::state::MarketDerivedSnapshot,
    profile: FireProfile,
) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if derived.trade_volumes.one_minute.total_value() <= EPS {
        fields.push("trade_vol_1m");
    }
    if derived.trade_volumes.five_minutes.total_value() <= EPS {
        fields.push("trade_vol_5m");
    }
    if derived.trade_deltas.one_minute.abs() <= EPS {
        fields.push("trade_delta_1m");
    }
    if derived.trade_deltas.five_minutes.abs() <= EPS {
        fields.push("trade_delta_5m");
    }
    if derived.last_price_deltas.one_minute.abs() <= EPS {
        fields.push("last_price_delta_1m");
    }
    if derived.last_price_deltas.one_hour.abs() <= EPS {
        fields.push("last_price_delta_1h");
    }
    if profile == FireProfile::Full {
        if derived.candle_volumes.one_hour <= EPS {
            fields.push("candle_vol_1h");
        }
        if derived.candle_volumes.twenty_four_hours <= EPS {
            fields.push("candle_vol_24h");
        }
        if derived.candle_deltas.one_hour.abs() <= EPS {
            fields.push("candle_delta_1h");
        }
        if derived.deltas.one_hour.abs() <= EPS {
            fields.push("combined_delta_1h");
        }
    }
    fields
}

fn format_count_map<K>(map: &HashMap<K, u64>) -> String
where
    K: ToString + Eq + std::hash::Hash,
{
    let mut rows = map
        .iter()
        .map(|(key, value)| format!("{}={}", key.to_string(), value))
        .collect::<Vec<_>>();
    rows.sort();
    rows.join(" ")
}

fn format_order_market_preview(st: &SessionStats) -> String {
    let mut markets = HashMap::<String, u64>::new();
    for market in st.order_market_by_uid.values() {
        *markets.entry(market.clone()).or_default() += 1;
    }
    format_count_map(&markets)
}

fn avg_us(total_ns: u64, count: u64) -> u64 {
    if count == 0 {
        0
    } else {
        total_ns / count / 1_000
    }
}

fn metric_cmd_label(cmd: u8, api_method: u8, payload_len: u64) -> String {
    if cmd == u8::MAX {
        format!("pre-cmd payload={payload_len}")
    } else {
        let c = Command::from_byte(cmd);
        if c == Command::API && api_method != u8::MAX {
            let method = EngineMethod::from_byte(api_method);
            format!(
                "{}({}) method={}({}) payload={payload_len}",
                c.name(),
                c.to_byte(),
                method.name(),
                method.to_byte()
            )
        } else {
            format!("{}({}) payload={payload_len}", c.name(), c.to_byte())
        }
    }
}

fn metric_app_mode_label(mode: u8) -> &'static str {
    match mode {
        1 => "callback",
        2 => "state",
        3 => "queue",
        4 => "worker",
        _ => "none",
    }
}

fn protocol_metrics_summary(m: &ProtocolMetricsSnapshot) -> String {
    format!(
        "recv={} reader_cpu(avg/max={}us/{}us max_src={} >100us/>1ms/>5ms={}/{}/{}) reader_wait(count={} avg/max={}us/{}us max_src={}) writer_cpu(avg/max={}us/{}us >100us/>1ms/>5ms={}/{}/{}) active_dispatch(avg/max={}us/{}us max_src={} events={} actions={} >100us/>1ms/>5ms={}/{}/{}) app_enqueue(avg/max={}us/{}us max_src={} events={} mode={} >100us/>1ms/>5ms={}/{}/{}) writer_tick_wall(count={} avg/max={}us/{}us) send_max={}us public_events={}",
        m.recv_count,
        avg_us(m.reader_protocol_ns, m.reader_protocol_count),
        m.reader_protocol_max_ns / 1_000,
        metric_cmd_label(
            m.reader_protocol_max_cmd,
            u8::MAX,
            m.reader_protocol_max_payload_len
        ),
        m.reader_protocol_over_100us,
        m.reader_protocol_over_1ms,
        m.reader_protocol_over_5ms,
        m.reader_protocol_wait_count,
        avg_us(m.reader_protocol_wait_ns, m.reader_protocol_wait_count),
        m.reader_protocol_wait_max_ns / 1_000,
        metric_cmd_label(
            m.reader_protocol_wait_max_cmd,
            u8::MAX,
            m.reader_protocol_wait_max_payload_len
        ),
        avg_us(m.writer_cpu_ns, m.writer_cpu_count),
        m.writer_cpu_max_ns / 1_000,
        m.writer_cpu_over_100us,
        m.writer_cpu_over_1ms,
        m.writer_cpu_over_5ms,
        avg_us(m.active_dispatch_ns, m.active_dispatch_count),
        m.active_dispatch_max_ns / 1_000,
        metric_cmd_label(
            m.active_dispatch_max_cmd,
            m.active_dispatch_max_api_method,
            m.active_dispatch_max_payload_len
        ),
        m.active_dispatch_max_events,
        m.active_dispatch_max_actions,
        m.active_dispatch_over_100us,
        m.active_dispatch_over_1ms,
        m.active_dispatch_over_5ms,
        avg_us(m.app_enqueue_ns, m.app_enqueue_count),
        m.app_enqueue_max_ns / 1_000,
        metric_cmd_label(
            m.app_enqueue_max_cmd,
            m.app_enqueue_max_api_method,
            m.app_enqueue_max_payload_len
        ),
        m.app_enqueue_max_events,
        metric_app_mode_label(m.app_enqueue_max_mode),
        m.app_enqueue_over_100us,
        m.app_enqueue_over_1ms,
        m.app_enqueue_over_5ms,
        m.writer_tick_count,
        avg_us(m.writer_tick_ns, m.writer_tick_count),
        m.writer_tick_max_ns / 1_000,
        m.send_phase_max_ns / 1_000,
        m.public_event_queue_len,
    )
}

fn delphi_now_raw_for_test() -> f64 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    25_569.0 + secs / 86_400.0
}

fn record_event(
    stats: &Arc<Mutex<SessionStats>>,
    event: &Event,
    dispatcher: Option<&EventDispatcherSnapshot>,
    candles_snapshot_tx: Option<&mpsc::Sender<Vec<RequestCandlesMarket>>>,
) {
    let mut st = stats.lock().unwrap();
    st.server_events += 1;
    let event_no = st.server_events;
    if let Some(dispatcher) = dispatcher {
        sync_market_probe_from_dispatcher(&mut st, event_no, dispatcher, false);
    }
    match event {
        Event::Order(ev) => {
            st.order_events += 1;
            *st.order_event_kinds
                .entry(order_event_kind(ev))
                .or_default() += 1;
            if let OrderEvent::Ignored { uid, reason } = ev {
                st.order_ignored_by_uid.insert(*uid, *reason);
            }
            if let Some(dispatcher) = dispatcher {
                record_order_state_snapshot(&mut st, dispatcher);
            }
            log_server_event(&st, event_no, format!("Order {ev:?}"));
        }
        Event::Balance(ev) => {
            st.balance_events += 1;
            match ev {
                BalanceEvent::SnapshotApplied { .. } => st.balance_snapshot_events += 1,
                BalanceEvent::IncrementalApplied { .. } => st.balance_incremental_events += 1,
                BalanceEvent::Ignored { .. } | BalanceEvent::EpochStale { .. } => {}
            }
            log_server_event(&st, event_no, format!("Balance {ev:?}"));
        }
        Event::TransferAssets(ev) => {
            st.transfer_asset_events += 1;
            match ev {
                moonproto::TransferAssetsEvent::Updated {
                    kind,
                    count,
                    nonzero_count,
                    revision,
                    ..
                } => {
                    st.transfer_asset_updated_mask |= 1 << kind.to_byte();
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "TransferAssets kind={} count={} nonzero={} revision={}",
                            kind.name(),
                            count,
                            nonzero_count,
                            revision
                        ),
                    );
                }
                moonproto::TransferAssetsEvent::RefreshCompleted {
                    request_id,
                    requested,
                    updated,
                    failed,
                    revision,
                } => {
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "TransferAssets refresh-complete id={} requested={} updated={} failed={} revision={}",
                            request_id, requested, updated, failed, revision
                        ),
                    );
                }
                moonproto::TransferAssetsEvent::UpdateFailed { kind, error, .. } => {
                    st.transfer_asset_failures += 1;
                    log_server_event(
                        &st,
                        event_no,
                        format!("TransferAssets kind={} failed={}", kind.name(), error),
                    );
                }
                moonproto::TransferAssetsEvent::TransferApplied {
                    asset,
                    qty,
                    from,
                    to,
                    revision,
                    ..
                } => {
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "TransferAssets local-transfer asset={} qty={:.8} from={} to={} revision={}",
                            asset,
                            qty,
                            from.name(),
                            to.name(),
                            revision
                        ),
                    );
                }
            }
        }
        Event::CoinCardCandles(ev) => {
            st.coin_card_events += 1;
            match ev {
                moonproto::CoinCardCandlesEvent::Updated {
                    market,
                    kind,
                    request_uid,
                    count,
                    revision,
                } => {
                    st.coin_card_updates += 1;
                    st.coin_card_last_count = *count;
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "CoinCardCandles market={} kind={:?} uid={} count={} revision={}",
                            market, kind, request_uid, count, revision
                        ),
                    );
                }
                moonproto::CoinCardCandlesEvent::UpdateFailed {
                    market,
                    kind,
                    request_uid,
                    error,
                } => {
                    st.coin_card_failures += 1;
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "CoinCardCandles failed market={} kind={:?} uid={:?} error={}",
                            market, kind, request_uid, error
                        ),
                    );
                }
            }
        }
        Event::CandlesSnapshot(ev) => match ev {
            moonproto::state::CandlesSnapshotEvent::Ready {
                request_uid,
                summary,
            } => {
                st.candles_complete = Some(CandlesSnapshotSummary {
                    uid: *request_uid,
                    zipped_bytes: 0,
                    markets: summary.retained_markets,
                    candles: summary.retained_candles,
                    market_preview: format!(
                        "received_markets={} received_candles={}",
                        summary.received_markets, summary.received_candles
                    ),
                });
                log_server_event(
                    &st,
                    event_no,
                    format!(
                        "CandlesSnapshot Ready uid={} summary={summary:?}",
                        request_uid
                    ),
                );
            }
            moonproto::state::CandlesSnapshotEvent::Failed { request_uid, error } => {
                st.parse_failed += 1;
                log_server_event(
                    &st,
                    event_no,
                    format!(
                        "CandlesSnapshot Failed uid={:?} error={}",
                        request_uid, error
                    ),
                );
            }
        },
        Event::Markets(ev) => {
            st.market_events += 1;
            if let Some(dispatcher) = dispatcher {
                sync_market_probe_from_dispatcher(&mut st, event_no, dispatcher, true);
            }
            log_server_event(
                &st,
                event_no,
                format!("Markets {ev:?}; {}", st.market_probe_summary()),
            );
        }
        Event::Settings(SettingsEvent::ClientSettingsUpdated) => {
            st.settings_events += 1;
            if let Some(dispatcher) = dispatcher {
                st.last_settings = dispatcher.settings().client_settings.clone();
            }
            if let Some(settings) = &st.last_settings {
                log_server_event(
                    &st,
                    event_no,
                    format!(
                        "UI ClientSettings uid={} x_sell={} emu_mode={} fixed_sell_mode={} fixed_sell_price={:.8} trailing_drop={:.8}",
                        settings.uid,
                        settings.x_sell,
                        settings.emu_mode,
                        settings.fixed_sell_mode,
                        settings.fixed_sell_price,
                        settings.trailing_drop
                    ),
                );
            } else {
                log_server_event(
                    &st,
                    event_no,
                    "UI ClientSettingsUpdated; state snapshot is refreshed after pump",
                );
            }
        }
        Event::Settings(other) => {
            log_server_event(&st, event_no, format!("UI {other:?}"));
        }
        Event::Strat(StratEvent::SnapshotFull {
            server_epoch,
            raw_data,
        }) => {
            record_strategy_snapshot(&mut st, event_no, "SnapshotFull", *server_epoch, raw_data);
        }
        Event::Strat(StratEvent::SnapshotPartial {
            server_epoch,
            raw_data,
        }) => {
            record_strategy_snapshot(
                &mut st,
                event_no,
                "SnapshotPartial",
                *server_epoch,
                raw_data,
            );
        }
        Event::Strat(StratEvent::SchemaApplied {
            raw_len,
            format_version,
            kind_count,
            field_count,
        }) => {
            st.strategy_events += 1;
            st.strategy_schema_events += 1;
            st.strategy_schema_kinds = *kind_count;
            st.strategy_schema_fields = *field_count;
            log_server_event(
                &st,
                event_no,
                format!(
                    "Strat SchemaApplied raw={} version={} kinds={} fields={}",
                    raw_len, format_version, kind_count, field_count
                ),
            );
        }
        Event::Strat(StratEvent::SchemaParseFailed { raw_len }) => {
            st.strategy_events += 1;
            st.parse_failed += 1;
            log_server_event(
                &st,
                event_no,
                format!("Strat SchemaParseFailed raw={raw_len}"),
            );
        }
        Event::Strat(other) => {
            log_server_event(&st, event_no, format!("Strat {other:?}"));
        }
        Event::Trade(TradesEvent::Applied {
            packet_num,
            base_time,
        }) => {
            st.trades_apply += 1;
            let target_trade_price = dispatcher.and_then(|dispatcher| {
                dispatcher
                    .markets()
                    .trade_state(&st.market)
                    .and_then(|state| {
                        (state.last_trade_price > 0.0).then_some(state.last_trade_price)
                    })
            });
            if let Some(price) = target_trade_price {
                st.target_trade_packets += 1;
                st.last_trade_price = Some(price);
                if st.target_trade_packets <= 5 || st.target_trade_packets.is_power_of_two() {
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "TradesStream target market={} idx={:?} packet_num={} source=market_tail/SeqRing last_price={:.8}",
                            st.market, st.market_index, packet_num, price
                        ),
                    );
                }
            }
            if should_log_stream_count(st.trades_apply) {
                log_server_event(
                    &st,
                    event_no,
                    format!(
                        "TradesStream Applied #{} packet_num={} base_time={:.8}; rows are retained in SeqRing/storage",
                        st.trades_apply,
                        packet_num,
                        base_time,
                    ),
                );
            }
        }
        Event::Trade(other) => {
            log_server_event(&st, event_no, format!("TradesStream {other:?}"));
        }
        Event::OrderBook(OrderBookEvent::Apply {
            market_index,
            market_name,
            kind,
            is_full,
            seq,
            top,
            buys,
            sells,
        }) => {
            st.orderbook_apply += 1;
            let raw_kind = kind.as_u8();
            if st.market_index == Some(*market_index) {
                if *is_full {
                    st.target_orderbook_full += 1;
                } else {
                    st.target_orderbook_update += 1;
                }
                st.last_book_kind = Some(raw_kind);
                if let (Some(bid), Some(ask)) = (top.bid, top.ask) {
                    st.last_book_bid = Some(bid.rate);
                    st.last_book_ask = Some(ask.rate);
                    if bid.rate <= 0.0 || ask.rate <= 0.0 || ask.rate <= bid.rate {
                        st.market_invariant_error = Some(format!(
                            "bad book top for {} kind={raw_kind}: bid={:.8} ask={:.8}",
                            st.market, bid.rate, ask.rate
                        ));
                    }
                } else if dispatcher.is_some() {
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "OrderBook target top not complete yet market={} idx={} kind={} top_bid={:?} top_ask={:?}",
                            st.market,
                            market_index,
                            raw_kind,
                            top.bid.map(|level| level.rate),
                            top.ask.map(|level| level.rate)
                        ),
                    );
                }
                if st.target_orderbook_full + st.target_orderbook_update <= 8
                    || (st.target_orderbook_full + st.target_orderbook_update).is_power_of_two()
                {
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "OrderBook target market={} event_market={:?} idx={} kind={:?} raw_kind={} full={} seq={} top_bid={:?} top_ask={:?}",
                            st.market,
                            market_name,
                            market_index,
                            kind,
                            raw_kind,
                            is_full,
                            seq,
                            st.last_book_bid,
                            st.last_book_ask
                        ),
                    );
                }
            }
            if should_log_stream_count(st.orderbook_apply) {
                let top_buy = buys
                    .first()
                    .map(|l| format!("{:.8}@{:.8}", l.quantity, l.rate))
                    .unwrap_or_else(|| "none".to_string());
                let top_sell = sells
                    .first()
                    .map(|l| format!("{:.8}@{:.8}", l.quantity, l.rate))
                    .unwrap_or_else(|| "none".to_string());
                log_server_event(
                    &st,
                    event_no,
                    format!(
                        "OrderBook Apply #{} market_index={} kind={} full={} seq={} buys={} sells={} top_buy={} top_sell={}",
                        st.orderbook_apply,
                        market_index,
                        raw_kind,
                        is_full,
                        seq,
                        buys.len(),
                        sells.len(),
                        top_buy,
                        top_sell
                    ),
                );
            }
        }
        Event::OrderBook(other) => {
            log_server_event(&st, event_no, format!("OrderBook {other:?}"));
        }
        Event::EngineResponse(resp) => {
            record_engine_response(&mut st, event_no, resp, candles_snapshot_tx);
        }
        Event::Arb(arb) => {
            log_server_event(&st, event_no, format!("Arb {}", arb_summary(arb)));
        }
        Event::ServerLog { time, msg } => {
            st.server_logs += 1;
            if let Some((request_uid, server_uid)) = parse_new_order_log_mapping(msg) {
                st.order_uid_by_request.insert(request_uid, server_uid);
            }
            log_server_event(
                &st,
                event_no,
                format!("LogMsg time={time:.8} msg={}", short_text(msg, 220)),
            );
        }
        Event::Raw { cmd, payload } => {
            st.raw_events += 1;
            log_server_event(
                &st,
                event_no,
                format!(
                    "Raw cmd={cmd:?} len={} head={}",
                    payload.len(),
                    hex_preview(payload, 32)
                ),
            );
        }
        Event::ParseFailed { cmd, len, payload } => {
            st.parse_failed += 1;
            let hash = fnv1a64(payload);
            let dump = write_parse_failed_dump(&st.label, event_no, *cmd, payload);
            st.parse_failures.push(ParseFailureRecord {
                event_no,
                cmd: *cmd,
                len: *len,
                hash,
                dump: dump.clone(),
            });
            let dump_suffix = dump
                .as_ref()
                .map(|path| format!(" dump={}", path.display()))
                .unwrap_or_else(|| " dump=<write-failed>".to_string());
            log_server_event(
                &st,
                event_no,
                format!(
                    "ParseFailed cmd={cmd:?} len={len} hash={:016X} head={}{}",
                    hash,
                    hex_preview(payload, 32),
                    dump_suffix
                ),
            );
        }
        other => {
            log_server_event(&st, event_no, format!("{other:?}"));
        }
    }
}

fn sync_market_probe_from_dispatcher(
    st: &mut SessionStats,
    event_no: u64,
    dispatcher: &EventDispatcherSnapshot,
    log_changes: bool,
) {
    let old_index = st.market_index;
    st.market_index = dispatcher.markets().market_index_by_name(&st.market);
    if log_changes && st.market_index != old_index {
        log_server_event(
            st,
            event_no,
            format!(
                "Market index resolved market={} old={old_index:?} new={:?}",
                st.market, st.market_index
            ),
        );
    }
    if let Some(price) = dispatcher.markets().price(&st.market) {
        st.last_market_price = Some(MarketProbePrice::from(&price));
        if price.bid <= 0.0 || price.ask <= 0.0 || price.ask < price.bid {
            st.market_invariant_error = Some(format!(
                "bad UpdateMarketsList price for {}: bid={:.8} ask={:.8}",
                st.market, price.bid, price.ask
            ));
        }
    }
}

fn record_order_state_snapshot(st: &mut SessionStats, dispatcher: &EventDispatcherSnapshot) {
    for order in dispatcher.orders().iter() {
        st.order_status_by_uid.insert(order.uid, order.status);
        st.order_market_by_uid
            .insert(order.uid, order.market_name.clone());
        st.order_sell_reason_by_uid
            .insert(order.uid, order.sell_reason().description().to_string());
    }
}

fn order_event_kind(ev: &OrderEvent) -> &'static str {
    match ev {
        OrderEvent::Created(_) => "Created",
        OrderEvent::Updated(_) => "Updated",
        OrderEvent::Removed(_) => "Removed",
        OrderEvent::BulkReplaced { .. } => "BulkReplaced",
        OrderEvent::TracePoint { .. } => "TracePoint",
        OrderEvent::CorridorChanged(_) => "CorridorChanged",
        OrderEvent::VStopChanged(_) => "VStopChanged",
        OrderEvent::StopsChanged(_) => "StopsChanged",
        OrderEvent::Snapshot => "Snapshot",
        OrderEvent::Ignored { .. } => "Ignored",
    }
}

fn parse_new_order_log_mapping(msg: &str) -> Option<(u64, u64)> {
    let request_marker = "request <";
    let arrow_marker = "=> <";
    let req_start = msg.find(request_marker)? + request_marker.len();
    let req_end = msg[req_start..].find('>')? + req_start;
    let arrow_start = msg[req_end..].find(arrow_marker)? + req_end + arrow_marker.len();
    let server_end = msg[arrow_start..].find('>')? + arrow_start;
    let request_uid = msg[req_start..req_end].trim().parse::<u64>().ok()?;
    let server_uid = msg[arrow_start..server_end].trim().parse::<u64>().ok()?;
    Some((request_uid, server_uid))
}

fn record_strategy_snapshot(
    st: &mut SessionStats,
    event_no: u64,
    kind: &str,
    server_epoch: u64,
    raw_data: &[u8],
) {
    st.strategy_events += 1;
    st.strategy_snapshot_events += 1;
    let raw_dump = write_strategy_raw_dump(&st.label, event_no, kind, server_epoch, raw_data);
    let raw_dump_suffix = raw_dump
        .as_ref()
        .map(|path| format!(" dump={}", path.display()))
        .unwrap_or_else(|| " dump=<write-failed>".to_string());
    if let Some(batch) = parse_strategy_batch(raw_data) {
        let ids_preview = batch
            .strategies
            .iter()
            .take(6)
            .map(|s| s.strategy_id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let count = batch.strategies.len();
        for strategy in batch.strategies {
            st.strategies_by_id.insert(strategy.strategy_id, strategy);
        }
        log_server_event(
            st,
            event_no,
            format!(
                "Strat {kind} epoch={} raw={} strategies={} ids=[{}]{}",
                server_epoch,
                raw_data.len(),
                count,
                ids_preview,
                raw_dump_suffix
            ),
        );
    } else {
        st.parse_failed += 1;
        log_server_event(
            st,
            event_no,
            format!(
                "Strat {kind} epoch={} raw={} parse_failed head={}{}",
                server_epoch,
                raw_data.len(),
                hex_preview(raw_data, 32),
                raw_dump_suffix
            ),
        );
    }
}

fn record_engine_response(
    st: &mut SessionStats,
    event_no: u64,
    resp: &EngineResponse,
    candles_snapshot_tx: Option<&mpsc::Sender<Vec<RequestCandlesMarket>>>,
) {
    st.engine_responses += 1;
    *st.engine_method_counts
        .entry(resp.method.to_byte())
        .or_insert(0) += 1;
    let mut detail = format!(
        "EngineResponse #{} uid={} method={:?} success={} error_code={} error_msg={} data_len={} head={}",
        st.engine_responses,
        resp.request_uid,
        resp.method,
        resp.success,
        resp.error_code,
        short_text(&resp.error_msg, 120),
        resp.data.len(),
        hex_preview(&resp.data, 24)
    );

    if resp.method == EngineMethod::RequestCandlesData {
        st.candles_chunks += 1;
        if let Some((chunk_index, chunk_total, payload_len)) = candles_chunk_info(&resp.data) {
            st.candles_payload_bytes += payload_len;
            if st.candles_seen_chunks.len() != chunk_total {
                st.candles_seen_chunks.clear();
                st.candles_seen_chunks.resize(chunk_total, false);
            }
            if let Some(seen) = st.candles_seen_chunks.get_mut(chunk_index) {
                *seen = true;
            }
            detail.push_str(&format!(
                " candle_chunk={}/{} payload_len={} seen_missing=[{}]",
                chunk_index + 1,
                chunk_total,
                payload_len,
                missing_chunk_indexes(&st.candles_seen_chunks)
            ));
        } else {
            detail.push_str(" candle_chunk=malformed");
        }

        if st.candles_requested && resp.success {
            let before = st.candles_aggregator.progress();
            let merged = st.candles_aggregator.on_chunk(&resp.data);
            let after = st.candles_aggregator.progress();
            st.candles_last_progress = after;
            if merged.is_none() && before == after {
                st.candles_ignored += 1;
                detail.push_str(" candle_state=ignored_or_duplicate");
            } else if let Some(zipped_data) = merged {
                match parse_request_candles_data_response(&zipped_data) {
                    Some(markets) => {
                        let candles = markets.iter().map(|m| m.candles_5m.len()).sum();
                        let market_preview = markets
                            .iter()
                            .take(8)
                            .map(|m| format!("{}:{}", m.market_name, m.candles_5m.len()))
                            .collect::<Vec<_>>()
                            .join(",");
                        let summary = CandlesSnapshotSummary {
                            uid: resp.request_uid,
                            zipped_bytes: zipped_data.len(),
                            markets: markets.len(),
                            candles,
                            market_preview,
                        };
                        detail.push_str(&format!(" candle_complete {}", summary.summary()));
                        st.candles_complete = Some(summary);
                        if let Some(tx) = candles_snapshot_tx {
                            let _ = tx.send(markets);
                        }
                    }
                    None => {
                        st.parse_failed += 1;
                        detail.push_str(&format!(
                            " candle_complete parse_failed zipped={}",
                            zipped_data.len()
                        ));
                    }
                }
            } else {
                let (received, total) = st.candles_last_progress;
                detail.push_str(&format!(" candle_progress={received}/{total}"));
            }
        }
    }

    log_server_event(st, event_no, detail);
}

fn arb_summary(event: &moonproto::ArbEvent) -> String {
    match event {
        moonproto::ArbEvent::PricesApplied {
            uid,
            version,
            market_blocks,
            price_items,
            applied_prices,
        } => {
            format!(
                "PricesApplied uid={} version={} market_blocks={} price_items={} applied_prices={}",
                uid, version, market_blocks, price_items, applied_prices
            )
        }
        moonproto::ArbEvent::IsolationApplied {
            uid,
            version,
            entries,
            applied_entries,
        } => {
            format!(
                "IsolationApplied uid={} version={} entries={} applied_entries={}",
                uid, version, entries, applied_entries
            )
        }
    }
}

fn candles_chunk_info(data: &[u8]) -> Option<(usize, usize, usize)> {
    if data.len() < 4 {
        return None;
    }
    let chunk_index = u16::from_le_bytes([data[0], data[1]]) as usize;
    let chunk_total = u16::from_le_bytes([data[2], data[3]]) as usize;
    Some((chunk_index, chunk_total, data.len() - 4))
}

fn missing_chunk_indexes(seen_chunks: &[bool]) -> String {
    if seen_chunks.is_empty() {
        return String::new();
    }
    let missing = seen_chunks
        .iter()
        .enumerate()
        .filter_map(|(idx, seen)| (!*seen).then_some((idx + 1).to_string()))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        "none".to_string()
    } else {
        missing.join(",")
    }
}

fn should_log_stream_count(count: u64) -> bool {
    count <= 10 || count.is_power_of_two()
}

fn log_server_event(st: &SessionStats, event_no: u64, detail: impl AsRef<str>) {
    println!("SERVER->{} #{event_no}: {}", st.label, detail.as_ref());
}

fn short_text(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    out
}

fn hex_preview(data: &[u8], max_len: usize) -> String {
    let mut out = data
        .iter()
        .take(max_len)
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(" ");
    if data.len() > max_len {
        out.push_str(" ...");
    }
    out
}

fn write_parse_failed_dump(
    label: &str,
    event_no: u64,
    cmd: Command,
    payload: &[u8],
) -> Option<std::path::PathBuf> {
    let name = format!(
        "moonproto_firetest_parse_failed_{}_{}_{}_{:06}.bin",
        sanitize_file_component(label),
        sanitize_file_component(&format!("{cmd:?}")),
        payload.len(),
        event_no
    );
    let path = std::env::temp_dir().join(name);
    match std::fs::write(&path, payload) {
        Ok(()) => Some(path),
        Err(err) => {
            eprintln!(
                "WARN: failed to write ParseFailed dump label={label} cmd={cmd:?} len={} err={err}",
                payload.len()
            );
            None
        }
    }
}

fn sanitize_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn pump_pair_for(a: &mut Session, b: &mut Session, duration: Duration) {
    let start = Instant::now();
    while start.elapsed() < duration {
        a.pump(PUMP_SLICE);
        b.pump(PUMP_SLICE);
    }
}

fn pump_session_until<F>(
    session: &mut Session,
    timeout: Duration,
    label: &str,
    mut predicate: F,
) -> bool
where
    F: FnMut(&Session) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        session.pump(PUMP_SLICE);
        if predicate(session) {
            println!("OK: {label} after {:.2}s", start.elapsed().as_secs_f64());
            return true;
        }
    }
    let stats = session.snapshot();
    eprintln!(
        "FIRETEST TIMEOUT {label}: session=[{}] metrics=[{}]",
        stats.summary(),
        session.protocol_summary(),
    );
    false
}

fn pump_pair_until<F>(
    a: &mut Session,
    b: &mut Session,
    timeout: Duration,
    label: &str,
    mut predicate: F,
) -> bool
where
    F: FnMut(&SessionStats, &SessionStats) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        a.pump(PUMP_SLICE);
        b.pump(PUMP_SLICE);
        let a_stats = a.snapshot();
        let b_stats = b.snapshot();
        if predicate(&a_stats, &b_stats) {
            println!("OK: {label} after {:.2}s", start.elapsed().as_secs_f64());
            return true;
        }
    }

    let a_stats = a.snapshot();
    let b_stats = b.snapshot();
    eprintln!(
        "FIRETEST TIMEOUT {label}: A=[{}] A.metrics=[{}] B=[{}] B.metrics=[{}]",
        a_stats.summary(),
        a.protocol_summary(),
        b_stats.summary(),
        b.protocol_summary()
    );
    log_err_emu_pair(label, a, b);
    false
}

fn pump_pair_until_sessions<F>(
    a: &mut Session,
    b: &mut Session,
    timeout: Duration,
    label: &str,
    mut predicate: F,
) -> bool
where
    F: FnMut(&Session, &Session) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        a.pump(PUMP_SLICE);
        b.pump(PUMP_SLICE);
        if predicate(a, b) {
            println!("OK: {label} after {:.2}s", start.elapsed().as_secs_f64());
            return true;
        }
    }

    let a_stats = a.snapshot();
    let b_stats = b.snapshot();
    eprintln!(
        "FIRETEST TIMEOUT {label}: A=[{}] A.metrics=[{}] B=[{}] B.metrics=[{}]",
        a_stats.summary(),
        a.protocol_summary(),
        b_stats.summary(),
        b.protocol_summary()
    );
    log_err_emu_pair(label, a, b);
    false
}

fn has_nonblocking_api_refresh(st: &SessionStats) -> bool {
    has_transfer_assets_refresh(st) && has_coin_card_candles(st)
}

fn request_nonblocking_api_refresh(session: &mut Session, cfg: &FireConfig) {
    session.request_transfer_assets_refresh();
    session.request_coin_card_candles(&cfg.market, FIRETEST_COIN_CARD_KIND);
}

fn pump_pair_until_nonblocking_api_refresh(
    a: &mut Session,
    b: &mut Session,
    cfg: &FireConfig,
    timeout: Duration,
) -> bool {
    request_nonblocking_api_refresh(a, cfg);
    request_nonblocking_api_refresh(b, cfg);
    let start = Instant::now();
    let mut next_retry = start + Duration::from_secs(2);
    let mut attempts = 1u32;
    while start.elapsed() < timeout {
        a.pump(PUMP_SLICE);
        b.pump(PUMP_SLICE);
        let a_ok = has_nonblocking_api_refresh(&a.snapshot());
        let b_ok = has_nonblocking_api_refresh(&b.snapshot());
        if a_ok && b_ok {
            println!(
                "OK: non-blocking transfer assets + CoinCard candles after {:.2}s attempts={attempts}",
                start.elapsed().as_secs_f64()
            );
            return true;
        }
        if Instant::now() >= next_retry {
            attempts += 1;
            if !a_ok {
                println!(
                    "FIRETEST retry non-blocking API for A after {:.2}s",
                    start.elapsed().as_secs_f64()
                );
                request_nonblocking_api_refresh(a, cfg);
            }
            if !b_ok {
                println!(
                    "FIRETEST retry non-blocking API for B after {:.2}s",
                    start.elapsed().as_secs_f64()
                );
                request_nonblocking_api_refresh(b, cfg);
            }
            next_retry += Duration::from_secs(2);
        }
    }

    let a_stats = a.snapshot();
    let b_stats = b.snapshot();
    eprintln!(
        "FIRETEST TIMEOUT non-blocking transfer assets + CoinCard candles: A=[{}] A.metrics=[{}] B=[{}] B.metrics=[{}]",
        a_stats.summary(),
        a.protocol_summary(),
        b_stats.summary(),
        b.protocol_summary()
    );
    log_err_emu_pair("non-blocking transfer assets + CoinCard candles", a, b);
    false
}

fn has_initial_health(st: &SessionStats) -> bool {
    st.connected_now
        && st.strategy_snapshot_events > 0
        && st.strategy_schema_events > 0
        && st.strategy_schema_kinds > 0
        && st.strategy_schema_fields > 0
        && st.trades_apply > 0
        && st.orderbook_apply > 0
        && st.parse_failed == 0
}

fn has_transfer_assets_refresh(st: &SessionStats) -> bool {
    st.transfer_asset_updated_mask == 0b111 && st.transfer_asset_failures == 0
}

fn has_coin_card_candles(st: &SessionStats) -> bool {
    st.coin_card_updates > 0
        && st.coin_card_failures == 0
        && st.coin_card_last_count >= FIRETEST_MIN_COIN_CARD_CANDLES
}

fn has_market_consistency(st: &SessionStats) -> bool {
    if !st.connected_now || st.parse_failed != 0 || st.market_invariant_error.is_some() {
        return false;
    }
    if st.market_index.is_none() {
        return false;
    }
    let Some(bid) = st.last_book_bid else {
        return false;
    };
    let Some(ask) = st.last_book_ask else {
        return false;
    };
    let Some(trade_price) = st.last_trade_price else {
        return false;
    };
    let Some(market_price) = st.last_market_price else {
        return false;
    };
    !st.market.is_empty()
        && st.target_orderbook_full > 0
        && st.target_orderbook_update > 0
        && st.target_trade_packets > 0
        && bid > 0.0
        && ask > bid
        && market_price.bid > 0.0
        && market_price.ask >= market_price.bid
        && price_near_envelope(trade_price, bid, ask)
        && price_near_envelope(market_price.bid, bid, ask)
        && price_near_envelope(market_price.ask, bid, ask)
}

fn price_near_envelope(price: f64, bid: f64, ask: f64) -> bool {
    if price <= 0.0 || bid <= 0.0 || ask <= 0.0 || ask < bid {
        return false;
    }
    let lower = bid * (1.0 - PRICE_NEIGHBORHOOD_PCT);
    let upper = ask * (1.0 + PRICE_NEIGHBORHOOD_PCT);
    price >= lower && price <= upper
}

fn log_err_emu_pair(label: &str, a: &Session, b: &Session) {
    let a_stats = a.snapshot();
    let b_stats = b.snapshot();
    log_err_emu_snapshot(
        label,
        "A",
        &a.client.err_emu_diagnostics_snapshot(),
        &a_stats.parse_failures,
    );
    log_err_emu_snapshot(
        label,
        "B",
        &b.client.err_emu_diagnostics_snapshot(),
        &b_stats.parse_failures,
    );
}

fn log_protocol_cpu_pair(label: &str, a: &Session, b: &Session) {
    println!("FIRETEST CPU {label} A: {}", a.protocol_summary());
    println!("FIRETEST CPU {label} B: {}", b.protocol_summary());
}

fn log_err_emu_snapshot(
    label: &str,
    session: &str,
    diag: &ErrEmuDiagnostics,
    parse_failures: &[ParseFailureRecord],
) {
    if diag.valid_packets == 0 {
        eprintln!(
            "FIRETEST ErrEmu {label} {session}: no packets counted while err_emu was enabled"
        );
        return;
    }
    let actual_drop = diag.dropped_packets as f64 / diag.valid_packets.max(1) as f64 * 100.0;
    eprintln!(
        "FIRETEST ErrEmu {label} {session}: configured={} rx_valid={} rx_delivered={} rx_dropped={} rx_actual_drop={:.2}% tx_sent={} tx_blackholed={}",
        diag.configured_rate,
        diag.valid_packets,
        diag.delivered_packets,
        diag.dropped_packets,
        actual_drop,
        diag.outgoing_packets,
        diag.outgoing_blackholed_packets
    );
    for raw in [
        Command::Sliced.to_byte(),
        Command::SlicedACK.to_byte(),
        Command::UI.to_byte(),
        Command::API.to_byte(),
        Command::WhoAreYou.to_byte(),
        Command::Fine.to_byte(),
        Command::WrongHello.to_byte(),
        Command::WantNewHello.to_byte(),
        Command::NeedHelloAgain.to_byte(),
        Command::Ping.to_byte(),
    ] {
        if let Some(cmd) = diag.by_cmd.iter().find(|cmd| cmd.raw_cmd == raw) {
            let cmd_drop = cmd.dropped_packets as f64 / cmd.valid_packets.max(1) as f64 * 100.0;
            eprintln!(
                "FIRETEST ErrEmu {label} {session}: cmd={:?}/{} valid={} delivered={} dropped={} actual_drop={:.2}%",
                Command::from_byte(raw),
                raw,
                cmd.valid_packets,
                cmd.delivered_packets,
                cmd.dropped_packets,
                cmd_drop
            );
        }
    }

    for raw in [
        Command::Hello.to_byte(),
        Command::HelloAgain.to_byte(),
        Command::ImFriend.to_byte(),
        Command::LogOff.to_byte(),
        Command::Ping.to_byte(),
        Command::SlicedACK.to_byte(),
    ] {
        if let Some(cmd) = diag.outgoing_by_cmd.iter().find(|cmd| cmd.raw_cmd == raw) {
            eprintln!(
                "FIRETEST ErrEmu {label} {session}: tx_cmd={:?}/{} sent={}",
                Command::from_byte(raw),
                raw,
                cmd.valid_packets,
            );
        }
        if let Some(cmd) = diag
            .outgoing_blackholed_by_cmd
            .iter()
            .find(|cmd| cmd.raw_cmd == raw)
        {
            eprintln!(
                "FIRETEST ErrEmu {label} {session}: tx_cmd={:?}/{} blackholed={}",
                Command::from_byte(raw),
                raw,
                cmd.valid_packets,
            );
        }
    }

    log_parse_failure_correlations(label, session, diag, parse_failures);

    let candidates: Vec<_> = diag
        .sliced
        .iter()
        .filter(|dg| is_sliced_response_candidate(dg))
        .collect();
    if candidates.is_empty() {
        eprintln!(
            "FIRETEST ErrEmu {label} {session}: no observed Sliced API/UI response datagrams"
        );
    } else {
        let total = candidates.len();
        for (idx, dg) in candidates.iter().enumerate() {
            if idx == 8 && total > 24 {
                eprintln!(
                    "FIRETEST ErrEmu {label} {session}: ... skipped {} middle Sliced candidates ...",
                    total - 24
                );
            }
            if idx >= 8 && idx + 16 < total {
                continue;
            }
            eprintln!(
                "FIRETEST ErrEmu {label} {session}: Sliced candidate {}/{}: {}",
                idx + 1,
                total,
                describe_sliced_candidate(diag.configured_rate, dg)
            );
        }
    }
}

fn log_parse_failure_correlations(
    label: &str,
    session: &str,
    diag: &ErrEmuDiagnostics,
    parse_failures: &[ParseFailureRecord],
) {
    for pf in parse_failures.iter().rev().take(8).rev() {
        let dump = pf
            .dump
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<none>".to_string());
        eprintln!(
            "FIRETEST ErrEmu {label} {session}: parse_failed event={} cmd={:?} len={} hash={:016X} dump={}",
            pf.event_no, pf.cmd, pf.len, pf.hash, dump
        );
        let matches = diag
            .sliced
            .iter()
            .filter(|dg| dg.completed_payload_hash == Some(pf.hash))
            .collect::<Vec<_>>();
        if matches.is_empty() {
            eprintln!(
                "FIRETEST ErrEmu {label} {session}: parse_failed hash {:016X} has no completed Sliced match in current diagnostics window",
                pf.hash
            );
        } else {
            for dg in matches {
                eprintln!(
                    "FIRETEST ErrEmu {label} {session}: parse_failed hash {:016X} matched {}",
                    pf.hash,
                    describe_sliced_candidate(diag.configured_rate, dg)
                );
            }
        }
    }
}

fn is_sliced_response_candidate(dg: &ErrEmuSlicedDatagramDiagnostics) -> bool {
    let completed_settings =
        dg.completed_cmd == Some(Command::UI.to_byte()) && dg.completed_ui_cmd_id == Some(1);
    let completed_orderbook = dg.completed_cmd == Some(Command::OrderBook.to_byte());
    let block0_ui = dg
        .block0_wire_cmd
        .map(|cmd| Command::from_byte(cmd & 0x7F) == Command::UI)
        .unwrap_or(false);
    let block0_orderbook = dg
        .block0_wire_cmd
        .map(|cmd| Command::from_byte(cmd & 0x7F) == Command::OrderBook)
        .unwrap_or(false);
    let block0_known_settings = dg.block0_ui_cmd_id == Some(1);
    let completed_api = dg.completed_cmd == Some(Command::API.to_byte());
    let completed_strat = dg.completed_cmd == Some(Command::Strat.to_byte());
    let block0_api = dg
        .block0_wire_cmd
        .map(|cmd| Command::from_byte(cmd & 0x7F) == Command::API)
        .unwrap_or(false);
    let block0_strat = dg
        .block0_wire_cmd
        .map(|cmd| Command::from_byte(cmd & 0x7F) == Command::Strat)
        .unwrap_or(false);
    completed_api
        || block0_api
        || completed_strat
        || block0_strat
        || completed_orderbook
        || block0_orderbook
        || completed_settings
        || (block0_ui && (block0_known_settings || dg.block0_ui_cmd_id.is_none()))
}

fn describe_sliced_candidate(configured_rate: u8, dg: &ErrEmuSlicedDatagramDiagnostics) -> String {
    let missing = dg.missing_blocks();
    let missing_preview = preview_u8(&missing, 24);
    let observed_attempts = dg.delivered_packets + dg.dropped_packets;
    let p = configured_rate as f64 / 100.0;
    let pure_err_emu_p = if missing.is_empty() {
        Some(0.0)
    } else {
        let mut acc = 1.0f64;
        let mut attributable = true;
        for block in &missing {
            let drops = dg.block_drop_count(*block);
            if drops == 0 {
                attributable = false;
                break;
            }
            acc *= p.powi(drops.min(i32::MAX as u64) as i32);
        }
        attributable.then_some(acc)
    };
    let pure_err = pure_err_emu_p
        .map(|v| format!("{:.8}%", v * 100.0))
        .unwrap_or_else(|| "not attributable to observed ErrEmu drops".to_string());
    let payload_head = dg
        .completed_payload_head
        .map(|head| hex_preview(&head[..dg.completed_payload_head_len.min(head.len())], 8))
        .unwrap_or_else(|| "none".to_string());
    let payload_hash = dg
        .completed_payload_hash
        .map(|hash| format!("{hash:016X}"))
        .unwrap_or_else(|| "none".to_string());
    let observation = if dg.completed_cmd.is_some() && !missing.is_empty() {
        " observation=incomplete-window"
    } else {
        ""
    };
    let orderbook = if dg.completed_cmd == Some(Command::OrderBook.to_byte()) {
        format!(
            " orderbook_market={:?} orderbook_kind={:?} orderbook_seq={:?} orderbook_full={:?} orderbook_buys={:?} orderbook_sells={:?}",
            dg.completed_orderbook_market_index,
            dg.completed_orderbook_kind,
            dg.completed_orderbook_seq,
            dg.completed_orderbook_is_full,
            dg.completed_orderbook_buys,
            dg.completed_orderbook_sells
        )
    } else {
        String::new()
    };
    format!(
        "Sliced d={} blocks={}/{} attempts={} delivered_packets={} dropped_packets={} wire_cmd={:?} ui_cmd={:?} complete_cmd={:?} complete_ui={:?} complete_strat_cmd={:?} complete_strat_uid={:?} complete_api_method={:?} complete_api_uid={:?} complete_api_success={:?}{} payload_len={:?} payload_hash={} payload_head={} missing=[{}] pure_err_emu_fail_p={}{}",
        dg.datagram_num,
        dg.delivered_unique_blocks(),
        dg.blocks_count,
        observed_attempts,
        dg.delivered_packets,
        dg.dropped_packets,
        dg.block0_wire_cmd.map(Command::from_byte),
        dg.block0_ui_cmd_id,
        dg.completed_cmd.map(Command::from_byte),
        dg.completed_ui_cmd_id,
        dg.completed_strat_cmd_id,
        dg.completed_strat_uid,
        dg.completed_api_method.map(EngineMethod::from_byte),
        dg.completed_api_uid,
        dg.completed_api_success,
        orderbook,
        dg.completed_payload_len,
        payload_hash,
        payload_head,
        missing_preview,
        pure_err,
        observation,
    )
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

fn preview_u8(values: &[u8], limit: usize) -> String {
    if values.is_empty() {
        return "none".to_string();
    }
    let mut out: Vec<String> = values
        .iter()
        .take(limit)
        .map(|value| value.to_string())
        .collect();
    if values.len() > limit {
        out.push("...".to_string());
    }
    out.join(",")
}

fn select_field(strategy: &StrategySnapshot, preferred: &str) -> String {
    try_select_field(strategy, preferred).unwrap_or_else(|| {
        panic!(
            "strategy_id={} has neither string field `{preferred}` nor any fallback string field",
            strategy.strategy_id
        )
    })
}

fn try_select_field(strategy: &StrategySnapshot, preferred: &str) -> Option<String> {
    if matches!(strategy.fields.get(preferred), Some(FieldValue::String(_))) {
        return Some(preferred.to_string());
    }
    strategy
        .fields
        .iter()
        .find_map(|(name, value)| matches!(value, FieldValue::String(_)).then(|| name.to_string()))
}

fn strategy_field_string<'a>(
    stats: &'a SessionStats,
    strategy_id: u64,
    field: &str,
) -> Option<&'a str> {
    match stats
        .strategies_by_id
        .get(&strategy_id)
        .and_then(|s| s.fields.get(field))
    {
        Some(FieldValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn with_strategy_string(
    mut strategy: StrategySnapshot,
    field: &str,
    value: String,
    version_bump: i32,
) -> StrategySnapshot {
    strategy.strategy_ver = strategy.strategy_ver.saturating_add(version_bump.max(1));
    strategy.last_date = now_epoch_ms();
    strategy.fields.insert(field, FieldValue::String(value));
    strategy
}

fn firetest_strategy(cfg: &FireConfig) -> StrategySnapshot {
    let strategy_id = cfg.strategy_id.unwrap_or(FIRETEST_STRATEGY_ID);
    let mut fields = StrategyFields::new();
    fields.insert(
        "StrategyName",
        FieldValue::String("MoonProto FireTest".to_string()),
    );
    fields.insert(
        "Comment",
        FieldValue::String("firetest-initial".to_string()),
    );
    fields.insert("AcceptCommands", FieldValue::Bool(true));
    fields.insert("OrderSize", FieldValue::Double(0.0));
    StrategySnapshot {
        strategy_id,
        strategy_ver: 1,
        last_date: now_epoch_ms(),
        checked: false,
        kind: StrategyKind::TELEGRAM.to_byte(),
        path: "FireTest".into(),
        fields,
    }
}

fn assert_strategy_field_visible_for_firetest(
    session: &Session,
    cfg: &FireConfig,
    strategy: &StrategySnapshot,
) {
    let snapshot = session.state_snapshot();
    let schema = snapshot
        .strats()
        .strategy_schema()
        .expect("FireTest strategy schema must be loaded before local strategy mutation");
    let field = schema.field(&cfg.strategy_field).unwrap_or_else(|| {
        panic!(
            "FireTest strategy_field `{}` is absent from schema",
            cfg.strategy_field
        )
    });
    assert!(
        field.visible_for_kind(strategy.kind),
        "FireTest strategy_field `{}` is not visible for kind {}:{}; choose a schema-visible field/kind pair",
        cfg.strategy_field,
        strategy.kind,
        schema.kind_name(strategy.kind).unwrap_or("?")
    );
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis() as u64
}

struct ErrEmuGuard {
    active: bool,
}

impl ErrEmuGuard {
    fn set(percent: u8) -> Self {
        set_err_emu(percent);
        println!("FIRETEST: client-side err_emu={percent}% enabled before connect");
        Self {
            active: percent > 0,
        }
    }

    fn set_for_gate(&mut self, percent: u8, gate: &str) {
        set_err_emu(percent);
        self.active = percent > 0;
        println!("FIRETEST: client-side err_emu={percent}% enabled for {gate}");
    }

    fn reset(&mut self, gate: &str) {
        if self.active {
            set_err_emu(0);
            self.active = false;
            println!("FIRETEST: client-side err_emu reset to 0% after {gate}");
        }
    }
}

impl Drop for ErrEmuGuard {
    fn drop(&mut self) {
        if self.active {
            set_err_emu(0);
            println!("FIRETEST: client-side err_emu reset to 0%");
        }
    }
}

fn log_high_loss_recovery_math() {
    let service_drop = (FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT / 2) as f64 / 100.0;
    let service_deliver = 1.0 - service_drop;
    let client_only_reconnect_attempt = service_deliver;
    let both_sides_reconnect_attempt = service_deliver * service_deliver;
    let client_only_fail =
        (1.0 - client_only_reconnect_attempt).powi(FIRETEST_RECONNECT_MATH_ATTEMPTS);
    let both_sides_fail =
        (1.0 - both_sides_reconnect_attempt).powi(FIRETEST_RECONNECT_MATH_ATTEMPTS);

    println!(
        "FIRETEST high-loss math: err_emu={}%, Delphi service drop={}%, delivery={:.2}%; reconnect attempt p(client-side)={:.2}%, fail after {} attempts={:.6}%; p(client+server)={:.2}%, fail after {} attempts={:.6}%",
        FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT,
        FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT / 2,
        service_deliver * 100.0,
        client_only_reconnect_attempt * 100.0,
        FIRETEST_RECONNECT_MATH_ATTEMPTS,
        client_only_fail * 100.0,
        both_sides_reconnect_attempt * 100.0,
        FIRETEST_RECONNECT_MATH_ATTEMPTS,
        both_sides_fail * 100.0,
    );
}

fn log_sliced_recovery_math(label: &str, err_emu_percent: u8) {
    let p = err_emu_percent.min(100) as f64 * 0.01;
    let attempts = FIRETEST_SLICED_MAX_RETRIES + 1;
    let one_block_fail = p.powi(attempts);
    let any_block_fail = |blocks: i32| 1.0 - (1.0 - one_block_fail).powi(blocks);
    let request_response_fail = 1.0 - (1.0 - one_block_fail).powi(2);
    let startup_engine_api_count = 4;
    let any_startup_api_fail = 1.0 - (1.0 - request_response_fail).powi(startup_engine_api_count);
    println!(
        "FIRETEST Sliced recovery math {label}: err_emu={}%, MaxRetries={} -> attempts={} per block; pure client-drop fail probability: 1-block={:.8}%, EngineAPI request+response={:.8}%, any of {} startup EngineAPI pairs={:.8}%, 6-block={:.8}%, 32-block={:.8}%, 255-block={:.8}%. If one startup Engine API/Sliced response times out, do not blame randomness unless diagnostics show every attempt for a missing block was actually dropped.",
        err_emu_percent,
        FIRETEST_SLICED_MAX_RETRIES,
        attempts,
        one_block_fail * 100.0,
        request_response_fail * 100.0,
        startup_engine_api_count,
        any_startup_api_fail * 100.0,
        any_block_fail(6) * 100.0,
        any_block_fail(32) * 100.0,
        any_block_fail(255) * 100.0,
    );
}

fn quick_profile_config(cfg: &FireConfig) -> FireConfig {
    let mut quick = cfg.clone();
    quick.connect_timeout = quick
        .connect_timeout
        .min(Duration::from_secs(QUICK_CONNECT_TIMEOUT_SECS));
    quick.wait = quick
        .wait
        .min(Duration::from_secs(QUICK_STREAM_TIMEOUT_SECS));
    quick
}

#[derive(Default)]
struct MoonClientPathStats {
    lifecycle_connected: bool,
    lifecycle_ready: bool,
    lifecycle_connected_at_s: Option<f64>,
    lifecycle_ready_at_s: Option<f64>,
    first_schema_at_s: Option<f64>,
    first_trade_at_s: Option<f64>,
    first_orderbook_at_s: Option<f64>,
    first_transfer_done_at_s: Option<f64>,
    first_coin_card_at_s: Option<f64>,
    candles_ready_at_s: Option<f64>,
    first_market_price_at_s: Option<f64>,
    first_retained_history_at_s: Option<f64>,
    init_step_at_s: HashMap<&'static str, f64>,
    engine_method_counts: HashMap<u8, u64>,
    engine_method_first_at_s: HashMap<u8, f64>,
    parse_failed: u64,
    strategy_snapshot_events: u64,
    strategy_schema_events: u64,
    strategy_schema_kinds: usize,
    strategy_schema_fields: usize,
    trades_apply: u64,
    orderbook_apply: u64,
    target_orderbook_full: u64,
    target_orderbook_update: u64,
    transfer_asset_updated_mask: u8,
    transfer_asset_failures: u64,
    transfer_asset_refresh_done: bool,
    coin_card_updates: u64,
    coin_card_failures: u64,
    coin_card_last_count: usize,
    candles_ready: bool,
    candles_markets: usize,
    candles_rows: usize,
    market_index: Option<u16>,
    last_book_bid: Option<f64>,
    last_book_ask: Option<f64>,
    last_trade_price: Option<f64>,
    last_market_price: Option<MarketProbePrice>,
    retained_last_prices: usize,
    retained_futures_trades: usize,
    retained_spot_trades: usize,
    derived_trade_vol_1m: f64,
    derived_trade_vol_5m: f64,
    derived_candle_vol_1h: f64,
    market_invariant_error: Option<String>,
}

impl MoonClientPathStats {
    fn engine_method_count(&self, method: EngineMethod) -> u64 {
        self.engine_method_counts
            .get(&(method.to_byte()))
            .copied()
            .unwrap_or(0)
    }

    fn has_engine_method(&self, method: EngineMethod) -> bool {
        self.engine_method_count(method) > 0
    }

    fn has_init_step(&self, step: &'static str) -> bool {
        self.init_step_at_s.contains_key(step)
    }

    fn record_lifecycle(&mut self, event: &LifecycleEvent, elapsed_s: f64) {
        match event {
            LifecycleEvent::Connected { fresh: true } => {
                self.lifecycle_connected = true;
                self.lifecycle_connected_at_s.get_or_insert(elapsed_s);
            }
            LifecycleEvent::Ready => {
                self.lifecycle_ready = true;
                self.lifecycle_ready_at_s.get_or_insert(elapsed_s);
            }
            LifecycleEvent::InitStepCompleted { step, elapsed_ms } => {
                self.init_step_at_s
                    .entry(*step)
                    .or_insert((*elapsed_ms as f64) * 0.001);
            }
            LifecycleEvent::ConnectFailed { error } => {
                self.market_invariant_error = Some(format!("MoonClient connect failed: {error}"));
            }
            _ => {}
        }
    }

    fn record_event(&mut self, event: &Event, target_market: &str, elapsed_s: f64) {
        match event {
            Event::EngineResponse(resp) => {
                *self
                    .engine_method_counts
                    .entry(resp.method.to_byte())
                    .or_insert(0) += 1;
                self.engine_method_first_at_s
                    .entry(resp.method.to_byte())
                    .or_insert(elapsed_s);
            }
            Event::ParseFailed { .. } => {
                self.parse_failed += 1;
            }
            Event::Strat(StratEvent::SnapshotFull { .. })
            | Event::Strat(StratEvent::SnapshotPartial { .. }) => {
                self.strategy_snapshot_events += 1;
            }
            Event::Strat(StratEvent::SchemaApplied {
                kind_count,
                field_count,
                ..
            }) => {
                self.first_schema_at_s.get_or_insert(elapsed_s);
                self.strategy_schema_events += 1;
                self.strategy_schema_kinds = self.strategy_schema_kinds.max(*kind_count);
                self.strategy_schema_fields = self.strategy_schema_fields.max(*field_count);
            }
            Event::Strat(StratEvent::SchemaParseFailed { .. }) => {
                self.parse_failed += 1;
            }
            Event::Trade(TradesEvent::Applied { .. }) => {
                self.first_trade_at_s.get_or_insert(elapsed_s);
                self.trades_apply += 1;
            }
            Event::OrderBook(OrderBookEvent::Apply {
                market_name,
                kind,
                is_full,
                top,
                ..
            }) if market_name.as_deref() == Some(target_market) => {
                self.first_orderbook_at_s.get_or_insert(elapsed_s);
                self.orderbook_apply += 1;
                if *is_full {
                    self.target_orderbook_full += 1;
                } else {
                    self.target_orderbook_update += 1;
                }
                if let (Some(bid), Some(ask)) = (top.bid, top.ask) {
                    self.last_book_bid = Some(bid.rate);
                    self.last_book_ask = Some(ask.rate);
                    if bid.rate <= 0.0 || ask.rate <= 0.0 || ask.rate <= bid.rate {
                        let raw_kind = kind.as_u8();
                        self.market_invariant_error = Some(format!(
                            "bad MoonClient book top for {target_market} kind={raw_kind}: bid={:.8} ask={:.8}",
                            bid.rate, ask.rate
                        ));
                    }
                }
            }
            Event::TransferAssets(ev) => match ev {
                moonproto::TransferAssetsEvent::Updated { kind, .. } => {
                    self.transfer_asset_updated_mask |= 1 << kind.as_index();
                }
                moonproto::TransferAssetsEvent::RefreshCompleted {
                    updated, failed, ..
                } => {
                    self.first_transfer_done_at_s.get_or_insert(elapsed_s);
                    self.transfer_asset_refresh_done = *updated == ExchangeKind::ALL.len();
                    self.transfer_asset_failures += *failed as u64;
                }
                moonproto::TransferAssetsEvent::UpdateFailed { .. } => {
                    self.transfer_asset_failures += 1;
                }
                moonproto::TransferAssetsEvent::TransferApplied { .. } => {}
            },
            Event::CoinCardCandles(ev) => match ev {
                moonproto::CoinCardCandlesEvent::Updated { count, .. } => {
                    self.first_coin_card_at_s.get_or_insert(elapsed_s);
                    self.coin_card_updates += 1;
                    self.coin_card_last_count = *count;
                }
                moonproto::CoinCardCandlesEvent::UpdateFailed { .. } => {
                    self.coin_card_failures += 1;
                }
            },
            Event::CandlesSnapshot(ev) => match ev {
                moonproto::state::CandlesSnapshotEvent::Ready { summary, .. } => {
                    self.candles_ready_at_s.get_or_insert(elapsed_s);
                    self.candles_ready = true;
                    self.candles_markets = summary.retained_markets;
                    self.candles_rows = summary.retained_candles;
                }
                moonproto::state::CandlesSnapshotEvent::Failed { error, .. } => {
                    self.market_invariant_error =
                        Some(format!("MoonClient auto candles failed: {error}"));
                }
            },
            _ => {}
        }
    }

    fn refresh_from_snapshot(
        &mut self,
        snapshot: &EventDispatcherSnapshot,
        target_market: &str,
        elapsed_s: f64,
    ) {
        self.market_index = snapshot.markets().market_index_by_name(target_market);
        if let Some(price) = snapshot.markets().price(target_market) {
            self.first_market_price_at_s.get_or_insert(elapsed_s);
            self.last_market_price = Some(MarketProbePrice::from(&price));
            if price.bid <= 0.0 || price.ask <= 0.0 || price.ask < price.bid {
                self.market_invariant_error = Some(format!(
                    "bad MoonClient UpdateMarketsList price for {target_market}: bid={:.8} ask={:.8}",
                    price.bid, price.ask
                ));
            }
        }
        if let Some(state) = snapshot.markets().trade_state(target_market) {
            if state.last_trade_price > 0.0 {
                self.last_trade_price = Some(state.last_trade_price);
            }
        }
        if let Some(top) = snapshot.top_of_book(target_market, OrderBookKind::Futures) {
            if let (Some(bid), Some(ask)) = (top.bid, top.ask) {
                self.last_book_bid = Some(bid.rate);
                self.last_book_ask = Some(ask.rate);
            }
        }
        if let Some(readers) = snapshot.market_history_readers(target_market) {
            if let Some(reader) = readers.last_prices.as_ref() {
                self.retained_last_prices = reader.bounds().len;
            }
            if let Some(reader) = readers.futures_trades.as_ref() {
                self.retained_futures_trades = reader.bounds().len;
            }
            if let Some(reader) = readers.spot_trades.as_ref() {
                self.retained_spot_trades = reader.bounds().len;
            }
            if self.retained_last_prices + self.retained_futures_trades + self.retained_spot_trades
                > 0
            {
                self.first_retained_history_at_s.get_or_insert(elapsed_s);
            }
        }
        if let Some(derived) =
            snapshot.market_history_derived_snapshot(target_market, delphi_now_raw_for_test())
        {
            self.derived_trade_vol_1m = derived.trade_volumes.one_minute.total_value();
            self.derived_trade_vol_5m = derived.trade_volumes.five_minutes.total_value();
            self.derived_candle_vol_1h = derived.candle_volumes.one_hour;
        }
        if let Some(candles) = snapshot
            .coin_card_candles()
            .get(target_market, FIRETEST_COIN_CARD_KIND)
        {
            self.coin_card_last_count = self.coin_card_last_count.max(candles.len());
        }
    }

    fn healthy(&self, require_auto_candles: bool) -> bool {
        if self.parse_failed != 0 || self.market_invariant_error.is_some() {
            return false;
        }
        let Some(bid) = self.last_book_bid else {
            return false;
        };
        let Some(ask) = self.last_book_ask else {
            return false;
        };
        let Some(trade_price) = self.last_trade_price else {
            return false;
        };
        let Some(market_price) = self.last_market_price else {
            return false;
        };
        // Mandatory Init is a lifecycle/state contract. Some Delphi-style
        // pending steps (notably GetMarketsList) are applied by the owner after
        // response delivery and are not required to surface as raw EngineResponse
        // events in the public UI stream.
        self.strategy_snapshot_events > 0
            && self.lifecycle_connected
            && self.lifecycle_ready
            && self.strategy_schema_events > 0
            && self.strategy_schema_kinds > 0
            && self.strategy_schema_fields > 0
            && self.has_init_step("BaseCheck")
            && self.has_init_step("AuthCheck")
            && self.has_init_step("GetMarketsList")
            && self.has_init_step("UpdateMarketsList")
            && self.has_engine_method(EngineMethod::SubscribeAllTrades)
            && self.has_engine_method(EngineMethod::SubscribeOrderBook)
            && self.trades_apply > 0
            && self.orderbook_apply > 0
            && self.target_orderbook_full > 0
            && self.target_orderbook_update > 0
            && self.transfer_asset_updated_mask == 0b111
            && self.transfer_asset_failures == 0
            && self.transfer_asset_refresh_done
            && self.coin_card_updates > 0
            && self.coin_card_failures == 0
            && self.coin_card_last_count >= FIRETEST_MIN_COIN_CARD_CANDLES
            && self.retained_last_prices > 0
            && self.retained_futures_trades + self.retained_spot_trades > 0
            && (!require_auto_candles || (self.candles_ready && self.candles_rows > 0))
            && bid > 0.0
            && ask > bid
            && market_price.bid > 0.0
            && market_price.ask >= market_price.bid
            && price_near_envelope(trade_price, bid, ask)
            && price_near_envelope(market_price.bid, bid, ask)
            && price_near_envelope(market_price.ask, bid, ask)
    }

    fn summary(&self) -> String {
        let method_at = |method: EngineMethod| {
            self.engine_method_first_at_s
                .get(&method.to_byte())
                .copied()
        };
        let init_at = |step: &'static str| self.init_step_at_s.get(step).copied();
        format!(
            "phase connected_at={:?}s ready_at={:?}s init_step BaseCheck={:?}s AuthCheck={:?}s GetMarketsList={:?}s GetMarketsIndexes={:?}s UpdateMarketsList={:?}s StrategySchema={:?}s PostInitFlush={:?}s StartupSnapshot={:?}s StartupEvents={:?}s engine_event_at BaseCheck={:?}s AuthCheck={:?}s GetMarketsList={:?}s GetMarketsIndexes={:?}s UpdateMarketsList={:?}s SubscribeAllTrades={:?}s SubscribeOrderBook={:?}s schema_event_at={:?}s price_at={:?}s trade_at={:?}s book_at={:?}s retained_at={:?}s transfer_done_at={:?}s coin_card_at={:?}s candles_ready_at={:?}s lifecycle connected={} ready={} methods BaseCheck={} AuthCheck={} GetMarketsList={} GetMarketsIndexes={} UpdateMarketsList={} SubscribeAllTrades={} SubscribeOrderBook={} strats={} schemas={} schema_kinds={} schema_fields={} trades={} books={} full={} update={} bid={:?} ask={:?} trade={:?} market_price={:?} transfer_mask={:#05b} transfer_done={} transfer_fail={} coin_card_updates={} coin_card_count={} candles_ready={} candles_markets={} candles_rows={} retained_last={} retained_trades={}/{} derived_vol_1m={:.4} derived_vol_5m={:.4} candle_vol_1h={:.4} parse_failed={} err={}",
            self.lifecycle_connected_at_s,
            self.lifecycle_ready_at_s,
            init_at("BaseCheck"),
            init_at("AuthCheck"),
            init_at("GetMarketsList"),
            init_at("GetMarketsIndexes"),
            init_at("UpdateMarketsList"),
            init_at("StrategySchema"),
            init_at("PostInitFlush"),
            init_at("StartupSnapshot"),
            init_at("StartupEvents"),
            method_at(EngineMethod::BaseCheck),
            method_at(EngineMethod::AuthCheck),
            method_at(EngineMethod::GetMarketsList),
            method_at(EngineMethod::GetMarketsIndexes),
            method_at(EngineMethod::UpdateMarketsList),
            method_at(EngineMethod::SubscribeAllTrades),
            method_at(EngineMethod::SubscribeOrderBook),
            self.first_schema_at_s,
            self.first_market_price_at_s,
            self.first_trade_at_s,
            self.first_orderbook_at_s,
            self.first_retained_history_at_s,
            self.first_transfer_done_at_s,
            self.first_coin_card_at_s,
            self.candles_ready_at_s,
            self.lifecycle_connected,
            self.lifecycle_ready,
            self.engine_method_count(EngineMethod::BaseCheck),
            self.engine_method_count(EngineMethod::AuthCheck),
            self.engine_method_count(EngineMethod::GetMarketsList),
            self.engine_method_count(EngineMethod::GetMarketsIndexes),
            self.engine_method_count(EngineMethod::UpdateMarketsList),
            self.engine_method_count(EngineMethod::SubscribeAllTrades),
            self.engine_method_count(EngineMethod::SubscribeOrderBook),
            self.strategy_snapshot_events,
            self.strategy_schema_events,
            self.strategy_schema_kinds,
            self.strategy_schema_fields,
            self.trades_apply,
            self.orderbook_apply,
            self.target_orderbook_full,
            self.target_orderbook_update,
            self.last_book_bid,
            self.last_book_ask,
            self.last_trade_price,
            self.last_market_price.map(|p| (p.bid, p.ask, p.mark_price, p.mark_price_found)),
            self.transfer_asset_updated_mask,
            self.transfer_asset_refresh_done,
            self.transfer_asset_failures,
            self.coin_card_updates,
            self.coin_card_last_count,
            self.candles_ready,
            self.candles_markets,
            self.candles_rows,
            self.retained_last_prices,
            self.retained_futures_trades,
            self.retained_spot_trades,
            self.derived_trade_vol_1m,
            self.derived_trade_vol_5m,
            self.derived_candle_vol_1h,
            self.parse_failed,
            self.market_invariant_error.as_deref().unwrap_or("none")
        )
    }
}

fn run_moonclient_public_smoke(
    label: &str,
    cfg: &FireConfig,
    keys: ImportedKeys,
    startup_timeout: Duration,
    stream_after_ready: Duration,
    require_auto_candles: bool,
) -> MoonClientPathStats {
    let init = InitConfig {
        mm_orders_subscribe: Some(true),
        subscribe_trades: Some(TradesStreamMode::TradesOnly),
        subscribe_orderbooks: vec![cfg.market.clone()],
        step_timeout: None,
        initial_strategies: Some(InitialStrategies::new(0, Vec::new())),
    };
    let client = MoonClient::connect(
        ClientConfig::new(&cfg.host, cfg.port, keys.master_key, keys.mac_key)
            .with_transport_mode(cfg.mask_ver)
            .with_client_id(rand::random()),
        ConnectConfig::new(init).with_connect_timeout(cfg.connect_timeout),
    )
    .unwrap_or_else(|err| panic!("FIRETEST {label}: MoonClient connect failed: {err}"));

    client
        .balances()
        .refresh_transfer_assets()
        .unwrap_or_else(|err| panic!("FIRETEST {label}: refresh_transfer_assets failed: {err}"));
    client
        .candles()
        .request_coin_card(&cfg.market, FIRETEST_COIN_CARD_KIND)
        .unwrap_or_else(|err| panic!("FIRETEST {label}: request_coin_card_candles failed: {err}"));

    let start = Instant::now();
    let startup_deadline = start + startup_timeout;
    let mut ready_at: Option<Instant> = None;
    let mut stats = MoonClientPathStats::default();
    loop {
        let elapsed_s = start.elapsed().as_secs_f64();
        for event in client.drain_lifecycle_events() {
            stats.record_lifecycle(&event, elapsed_s);
        }
        if stats.lifecycle_ready && ready_at.is_none() {
            ready_at = Some(Instant::now());
        }
        for event in client.drain_events() {
            stats.record_event(&event, &cfg.market, elapsed_s);
        }
        if let Some(snapshot) = client.snapshot() {
            stats.refresh_from_snapshot(&snapshot, &cfg.market, elapsed_s);
        }
        if stats.healthy(require_auto_candles) {
            break;
        }
        let deadline = ready_at.map_or(startup_deadline, |ready| ready + stream_after_ready);
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(PUMP_SLICE);
    }
    let elapsed_s = start.elapsed().as_secs_f64();
    for event in client.drain_lifecycle_events() {
        stats.record_lifecycle(&event, elapsed_s);
    }
    for event in client.drain_events() {
        stats.record_event(&event, &cfg.market, elapsed_s);
    }
    if let Some(snapshot) = client.snapshot() {
        stats.refresh_from_snapshot(&snapshot, &cfg.market, elapsed_s);
    }
    let healthy = stats.healthy(require_auto_candles);
    let slow_startup = stats
        .lifecycle_ready_at_s
        .is_some_and(|ready| ready > FIRETEST_SLOW_STARTUP_DIAG_SECS)
        || stats
            .init_step_at_s
            .get("BaseCheck")
            .is_some_and(|base| *base > FIRETEST_SLOW_STARTUP_DIAG_SECS);
    if !healthy || slow_startup {
        eprintln!(
            "FIRETEST {label}: MoonClient diagnostics reason={} summary={}",
            if healthy { "slow-startup" } else { "failure" },
            stats.summary()
        );
        log_err_emu_snapshot(label, "public", &client.err_emu_diagnostics_snapshot(), &[]);
    }
    let _ = client.disconnect();
    let _ = client.wait_finished();

    assert!(
        healthy,
        "FIRETEST {label}: MoonClient public path did not reach health within startup={startup_timeout:?} + stream_after_ready={stream_after_ready:?}: {}",
        stats.summary()
    );
    println!(
        "FIRETEST CPU {label}: {}",
        protocol_metrics_summary(&client.protocol_metrics_snapshot())
    );
    println!(
        "OK: FIRETEST {label}: MoonClient public path healthy after {:.2}s [{}]",
        start.elapsed().as_secs_f64(),
        stats.summary()
    );
    stats
}

fn run_quick_fire_test(cfg: &FireConfig, keys: ImportedKeys) {
    let start = Instant::now();
    let cfg = quick_profile_config(cfg);
    let err_emu_percent = firetest_err_emu_percent();
    let _err_emu = ErrEmuGuard::set(err_emu_percent);

    println!(
        "FIRETEST quick target: <= {}s; MoonClient public path, err_emu={}%, connect_timeout={:?}, stream_timeout={:?}",
        QUICK_TOTAL_TARGET_SECS,
        err_emu_percent,
        cfg.connect_timeout,
        cfg.wait
    );
    log_sliced_recovery_math("quick startup", err_emu_percent);

    let public_path_startup_timeout = cfg.connect_timeout + cfg.wait;
    let _stats = run_moonclient_public_smoke(
        "quick/public",
        &cfg,
        keys,
        public_path_startup_timeout,
        cfg.wait,
        false,
    );
    assert!(
        start.elapsed() <= Duration::from_secs(QUICK_TOTAL_TARGET_SECS),
        "quick FireTest exceeded {}s target: {:.2}s",
        QUICK_TOTAL_TARGET_SECS,
        start.elapsed().as_secs_f64()
    );
    println!(
        "FIRETEST_QUICK_PASS after {:.2}s",
        start.elapsed().as_secs_f64()
    );
}

fn request_settings_until(session: &mut Session, timeout: Duration) -> ClientSettingsCommand {
    let start = Instant::now();
    let before_events = session.snapshot().settings_events;
    let mut attempts = 0u32;
    let mut next_retry = start;
    loop {
        if Instant::now() >= next_retry {
            attempts += 1;
            session
                .client
                .settings()
                .refresh()
                .expect("MoonClient request_client_settings must queue");
            next_retry = Instant::now() + Duration::from_secs(2);
        }
        session.pump(PUMP_SLICE);
        let st = session.snapshot();
        if let Some(settings) = st.last_settings.clone() {
            if st.settings_events > before_events || before_events == 0 {
                println!(
                    "OK: settings snapshot uid={} attempts={} after {:.2}s",
                    settings.uid,
                    attempts,
                    start.elapsed().as_secs_f64()
                );
                return settings;
            }
        }
        assert!(
            start.elapsed() < timeout,
            "settings request failed after {attempts} non-blocking attempts within {timeout:?}"
        );
    }
}

fn request_balance_until(session: &mut Session, timeout: Duration) {
    let start = Instant::now();
    let before = session.snapshot();
    let mut attempts = 0u32;
    let mut next_retry = start;
    loop {
        if Instant::now() >= next_retry {
            attempts += 1;
            session
                .client
                .balances()
                .refresh()
                .expect("MoonClient request_balance_snapshot must queue");
            next_retry = Instant::now() + Duration::from_secs(2);
        }
        session.pump(PUMP_SLICE);
        let st = session.snapshot();
        if st.balance_events > before.balance_events
            || st.balance_snapshot_events > before.balance_snapshot_events
        {
            println!(
                "OK: high-loss balance stream refresh events={} snapshots={} attempts={} after {:.2}s",
                st.balance_events - before.balance_events,
                st.balance_snapshot_events - before.balance_snapshot_events,
                attempts,
                start.elapsed().as_secs_f64()
            );
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "high-loss balance refresh produced no Balance event after {attempts} non-blocking attempts within {timeout:?}"
        );
    }
}

fn request_orders_until(session: &mut Session, timeout: Duration) {
    let start = Instant::now();
    let before = session.snapshot();
    let mut attempts = 0u32;
    let mut next_retry = start;
    loop {
        if Instant::now() >= next_retry {
            attempts += 1;
            session
                .client
                .orders()
                .request_snapshot()
                .expect("MoonClient request_order_snapshot must queue");
            next_retry = Instant::now() + Duration::from_secs(2);
        }
        session.pump(PUMP_SLICE);
        let st = session.snapshot();
        if st.order_events > before.order_events
            || st.order_status_by_uid.len() > before.order_status_by_uid.len()
        {
            println!(
                "OK: high-loss order refresh events_delta={} current_seen={} attempts={} after {:.2}s",
                st.order_events - before.order_events,
                st.order_status_by_uid.len(),
                attempts,
                start.elapsed().as_secs_f64()
            );
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "high-loss order refresh produced no order state after {attempts} non-blocking attempts within {timeout:?}"
        );
    }
}

fn request_engine_until(session: &mut Session, method: EngineMethod, timeout: Duration) {
    let start = Instant::now();
    let mut attempts = 0u32;
    let mut next_retry = start;
    let before_account_revision = session
        .maybe_state_snapshot()
        .map(|snapshot| snapshot.account().revision())
        .unwrap_or(0);
    loop {
        if Instant::now() >= next_retry {
            attempts += 1;
            match method {
                EngineMethod::CheckAPIExpirationTime => session
                    .client
                    .account()
                    .refresh_api_expiration_time()
                    .expect("MoonClient refresh_api_expiration_time must queue"),
                EngineMethod::QueryHedgeMode => session
                    .client
                    .account()
                    .refresh_hedge_mode()
                    .expect("MoonClient refresh_hedge_mode must queue"),
                _ => panic!("FireTest non-blocking Engine API gate does not support {method:?}"),
            }
            next_retry = Instant::now() + Duration::from_secs(2);
        }
        session.pump(PUMP_SLICE);
        if let Some(snapshot) = session.maybe_state_snapshot() {
            let account = snapshot.account();
            let ok = match method {
                EngineMethod::CheckAPIExpirationTime => account.api_expiration().is_some(),
                EngineMethod::QueryHedgeMode => account.hedge_mode().is_some(),
                _ => false,
            };
            if ok && account.revision() > before_account_revision {
                println!(
                    "OK: high-loss Engine API {:?} non-blocking account refresh attempts={} after {:.2}s",
                    method,
                    attempts,
                    start.elapsed().as_secs_f64()
                );
                return;
            }
        }
        if start.elapsed() >= timeout {
            panic!(
                "high-loss Engine API {:?} failed after {} non-blocking attempts within {:?}",
                method, attempts, timeout
            );
        }
    }
}

fn run_high_loss_simple_ops_gate(
    a: &mut Session,
    b: &mut Session,
    err_emu: &mut ErrEmuGuard,
    timeout: Duration,
) {
    // Do not "fix" this by disabling err_emu as flaky random. Delphi halves
    // MoonProtoErrEmu for service/handshake packets, so at 50% configured loss
    // reconnect service delivery is still 75%. A client-side-only reconnect
    // attempt needs one incoming Fine, so 10 attempts fail with 0.25^10 =
    // 0.000095%. Even if both client and server apply 50% ErrEmu, one attempt is
    // 0.75*0.75 = 56.25%, and 10 attempts fail with ~0.0257%. If this gate fails
    // consistently, it is a protocol/reconnect bug, not "FireTest randomness".
    a.client
        .debug_reset_err_emu_diagnostics()
        .expect("reset A err_emu diagnostics");
    b.client
        .debug_reset_err_emu_diagnostics()
        .expect("reset B err_emu diagnostics");
    err_emu.set_for_gate(
        FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT,
        "50% simple operations/reconnect gate",
    );
    log_high_loss_recovery_math();

    request_engine_until(a, EngineMethod::CheckAPIExpirationTime, timeout);
    request_engine_until(a, EngineMethod::QueryHedgeMode, timeout);
    let _settings = request_settings_until(a, timeout);
    request_balance_until(a, timeout);
    request_orders_until(a, timeout);

    let before_streams = a.snapshot();
    let streams_ok = pump_pair_until(a, b, timeout, "high-loss live streams", |a, _| {
        a.trades_apply > before_streams.trades_apply
            && a.orderbook_apply > before_streams.orderbook_apply
            && a.parse_failed == before_streams.parse_failed
    });
    if !streams_ok {
        let after = a.snapshot();
        log_err_emu_pair("high-loss live streams failure", a, b);
        panic!(
            "client A high-loss stream gate failed under err_emu={} within {:?}: before=[{}] after=[{}]",
            FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT,
            timeout,
            before_streams.summary(),
            after.summary()
        );
    }
    println!("OK: high-loss live streams delivered");

    let before_blackhole = a.snapshot();
    a.client
        .debug_set_outgoing_blackhole(true)
        .expect("enable outgoing blackhole");
    let disconnected = pump_pair_until(
        a,
        b,
        timeout,
        "high-loss forced reconnect detection",
        |a, _| a.reconnecting > before_blackhole.reconnecting,
    );
    a.client
        .debug_set_outgoing_blackhole(false)
        .expect("disable outgoing blackhole");
    assert!(
        disconnected,
        "client A did not enter reconnecting state under err_emu={} within {:?}",
        FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT, timeout
    );

    let before_reconnect = a.snapshot();
    let reconnected = pump_pair_until(a, b, timeout, "high-loss reconnect", |a, _| {
        a.connected_again > before_reconnect.connected_again
    });
    assert!(
        reconnected && a.snapshot().connected_now,
        "client A did not reconnect under err_emu={} within {:?}",
        FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT,
        timeout
    );
    println!("OK: high-loss reconnect completed");

    let after_reconnect = a.snapshot();
    let streams_after_reconnect_ok = pump_pair_until(
        a,
        b,
        timeout,
        "high-loss streams after reconnect",
        |a, _| {
            a.trades_apply > after_reconnect.trades_apply
                && a.orderbook_apply > after_reconnect.orderbook_apply
                && a.parse_failed == after_reconnect.parse_failed
        },
    );
    if !streams_after_reconnect_ok {
        let after = a.snapshot();
        log_err_emu_pair("high-loss streams after reconnect failure", a, b);
        panic!(
            "client A high-loss post-reconnect stream gate failed within {:?}: before=[{}] after=[{}]",
            timeout,
            after_reconnect.summary(),
            after.summary()
        );
    }
    println!("OK: high-loss streams after reconnect delivered");
    log_err_emu_pair("high-loss simple ops gate", a, b);
}

fn ensure_server_emulator_mode(
    cfg: &FireConfig,
    a: &mut Session,
    b: &mut Session,
) -> Option<ClientSettingsCommand> {
    let original = request_settings_until(a, cfg.connect_timeout);
    if original.emu_mode {
        println!("OK: server emulator mode is already enabled");
        return None;
    }

    let mut enabled = original.clone();
    enabled.emu_mode = true;
    println!("FIRETEST order flow: enabling server emulator mode through UI settings");
    a.client
        .settings()
        .send(enabled.clone())
        .expect("MoonClient send_settings must queue");
    assert!(
        pump_pair_until(a, b, cfg.connect_timeout, "enable emulator mode", |a, b| {
            a.last_settings
                .as_ref()
                .map(|settings| settings.emu_mode)
                .unwrap_or(false)
                && b.last_settings
                    .as_ref()
                    .map(|settings| settings.emu_mode)
                    .unwrap_or(false)
        }),
        "server emulator mode was not confirmed within {:?}",
        cfg.connect_timeout
    );
    Some(original)
}

fn ensure_server_real_order_mode(
    cfg: &FireConfig,
    a: &mut Session,
    b: &mut Session,
) -> Option<ClientSettingsCommand> {
    let original = request_settings_until(a, cfg.connect_timeout);
    if !original.emu_mode {
        println!("OK: server emulator mode is already disabled");
        return None;
    }

    let mut real = original.clone();
    real.emu_mode = false;
    println!("FIRETEST real order cancel: disabling server emulator mode through UI settings");
    a.client
        .settings()
        .send(real.clone())
        .expect("MoonClient send_settings must queue");
    assert!(
        pump_pair_until(
            a,
            b,
            cfg.connect_timeout,
            "disable emulator mode",
            |a, b| {
                a.last_settings
                    .as_ref()
                    .map(|settings| !settings.emu_mode)
                    .unwrap_or(false)
                    && b.last_settings
                        .as_ref()
                        .map(|settings| !settings.emu_mode)
                        .unwrap_or(false)
            }
        ),
        "server emulator mode=false was not confirmed within {:?}",
        cfg.connect_timeout
    );
    Some(original)
}

fn restore_server_emulator_mode(
    cfg: &FireConfig,
    a: &mut Session,
    b: &mut Session,
    original: Option<ClientSettingsCommand>,
) {
    let Some(original) = original else {
        return;
    };
    println!(
        "FIRETEST order flow: restoring server emulator mode to {}",
        original.emu_mode
    );
    a.client
        .settings()
        .send(original.clone())
        .expect("MoonClient send_settings must queue");
    assert!(
        pump_pair_until(
            a,
            b,
            cfg.connect_timeout,
            "restore emulator mode",
            |a, b| {
                a.last_settings
                    .as_ref()
                    .map(|settings| settings.emu_mode == original.emu_mode)
                    .unwrap_or(false)
                    && b.last_settings
                        .as_ref()
                        .map(|settings| settings.emu_mode == original.emu_mode)
                        .unwrap_or(false)
            }
        ),
        "server emulator mode was not restored within {:?}",
        cfg.connect_timeout
    );
}

#[derive(Clone, Copy, Debug)]
struct MarketBalanceProbe {
    initial_balance: f64,
    locked_balance: f64,
    pos_size: f64,
    pos_price: f64,
    asset_balance: f64,
    asset_balance_full: f64,
    total_profit: f64,
    balance_hash: u64,
    epoch: u16,
}

impl MarketBalanceProbe {
    fn summary(self) -> String {
        format!(
            "init={:.8} locked={:.8} pos={:.8}@{:.8} asset={:.8}/{:.8} pnl={:.8} hash={} epoch={}",
            self.initial_balance,
            self.locked_balance,
            self.pos_size,
            self.pos_price,
            self.asset_balance,
            self.asset_balance_full,
            self.total_profit,
            self.balance_hash,
            self.epoch
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct GlobalBalanceProbe {
    btc_balance_total: f64,
    btc_balance_locked: f64,
    btc_balance_full: f64,
    special_coin_balance: f64,
    total_pnl: f64,
}

impl GlobalBalanceProbe {
    fn summary(self) -> String {
        format!(
            "btc_total={:.8} btc_locked={:.8} btc_full={:.8} special_coin={:.8} total_pnl={:.8}",
            self.btc_balance_total,
            self.btc_balance_locked,
            self.btc_balance_full,
            self.special_coin_balance,
            self.total_pnl
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct ActiveBalanceProbe {
    market: MarketBalanceProbe,
    global: GlobalBalanceProbe,
}

impl ActiveBalanceProbe {
    fn summary(self) -> String {
        format!(
            "market=[{}] global=[{}]",
            self.market.summary(),
            self.global.summary()
        )
    }
}

fn active_balance_probe(session: &Session, market: &str) -> ActiveBalanceProbe {
    let snapshot = session.state_snapshot();
    let handle = snapshot
        .markets()
        .get(market)
        .unwrap_or_else(|| panic!("market {market} is not present in ActiveLib MarketsState"));
    let pos = handle.balance_position();
    let global = snapshot.balances().global();
    ActiveBalanceProbe {
        market: MarketBalanceProbe {
            initial_balance: pos.initial_balance,
            locked_balance: pos.locked_balance,
            pos_size: pos.pos_size,
            pos_price: pos.pos_price,
            asset_balance: pos.asset_balance,
            asset_balance_full: pos.asset_balance_full,
            total_profit: pos.total_profit(),
            balance_hash: pos.balance_hash,
            epoch: pos.last_balance_epoch,
        },
        global: GlobalBalanceProbe {
            btc_balance_total: global.btc_balance_total,
            btc_balance_locked: global.btc_balance_locked,
            btc_balance_full: global.btc_balance_full,
            special_coin_balance: global.special_coin_balance,
            total_pnl: global.total_pnl,
        },
    }
}

fn market_live_ask(session: &Session, market: &str) -> Option<f64> {
    session
        .maybe_state_snapshot()
        .and_then(|snapshot| snapshot.markets().price(market))
        .and_then(|price| {
            [price.ask, price.last_ask, price.bid, price.mark_price]
                .into_iter()
                .find(|value| value.is_finite() && *value > EPS)
        })
}

fn order_uid_for_request_or_new_market(
    st: &SessionStats,
    before_uids: &[u64],
    request_uid: u64,
    market: &str,
) -> Option<u64> {
    (request_uid != 0)
        .then(|| st.order_uid_by_request.get(&request_uid).copied())
        .flatten()
        .or_else(|| {
            st.order_status_by_uid.iter().find_map(|(uid, _)| {
                (!before_uids.contains(uid)
                    && st
                        .order_market_by_uid
                        .get(uid)
                        .map(|seen| seen == market)
                        .unwrap_or(false))
                .then_some(*uid)
            })
        })
}

fn cancel_if_known(
    session: &mut Session,
    before_uids: &[u64],
    request_uid: u64,
    market: &str,
) -> Option<u64> {
    let st = session.snapshot();
    let uid = order_uid_for_request_or_new_market(&st, before_uids, request_uid, market)?;
    if session.cancel_order(uid) {
        println!("FIRETEST real order cancel: cleanup cancel queued uid={uid}");
    } else {
        println!("FIRETEST real order cancel: cleanup cancel gate refused uid={uid}");
    }
    Some(uid)
}

fn order_is_present(session: &Session, uid: u64) -> bool {
    session
        .maybe_state_snapshot()
        .map(|snapshot| snapshot.orders().iter().any(|order| order.uid == uid))
        .unwrap_or(false)
}

fn assert_balance_stream_seen(session: &Session) {
    let st = session.snapshot();
    let snapshot = session.state_snapshot();
    let global = snapshot.balances().global();
    assert!(
        st.balance_events >= 2 && st.balance_snapshot_events >= 1,
        "FireTest balance stream: session {} saw too few balance events: total={} snapshots={} increments={}",
        st.label,
        st.balance_events,
        st.balance_snapshot_events,
        st.balance_incremental_events
    );
    assert!(
        global.btc_balance_total.abs()
            + global.btc_balance_full.abs()
            + global.special_coin_balance.abs()
            > EPS,
        "FireTest balance stream: session {} has zero global balances after balance events",
        st.label
    );
    println!(
        "OK: balance stream session={} events={} snapshots={} increments={} global=[btc_total={:.8} btc_locked={:.8} btc_full={:.8} special_coin={:.8}]",
        st.label,
        st.balance_events,
        st.balance_snapshot_events,
        st.balance_incremental_events,
        global.btc_balance_total,
        global.btc_balance_locked,
        global.btc_balance_full,
        global.special_coin_balance
    );
}

fn run_order_lifecycle_gate(cfg: &FireConfig, a: &mut Session, b: &mut Session) {
    let restore_emu_mode = ensure_server_emulator_mode(cfg, a, b);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_order_lifecycle_gate_body(cfg, a, b);
    }));
    restore_server_emulator_mode(cfg, a, b, restore_emu_mode);
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

fn run_real_order_cancel_gate(cfg: &FireConfig, a: &mut Session, b: &mut Session) {
    let restore_emu_mode = ensure_server_real_order_mode(cfg, a, b);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_real_order_cancel_gate_body(a, b);
    }));
    restore_server_emulator_mode(cfg, a, b, restore_emu_mode);
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

fn run_real_order_cancel_gate_body(a: &mut Session, b: &mut Session) {
    let market = FIRETEST_REAL_BALANCE_ORDER_MARKET;
    assert_balance_stream_seen(a);
    assert_balance_stream_seen(b);

    let ask = pump_pair_until_sessions(
        a,
        b,
        FIRETEST_REAL_BALANCE_ORDER_TIMEOUT,
        "SOL live price before real order cancel",
        |a, _| market_live_ask(a, market).is_some(),
    )
    .then(|| market_live_ask(a, market))
    .flatten()
    .unwrap_or_else(|| {
        panic!(
            "FireTest real order cancel: no live ask/price for {market} within {:?}",
            FIRETEST_REAL_BALANCE_ORDER_TIMEOUT
        )
    });
    let price = ask * (1.0 - FIRETEST_REAL_BALANCE_ORDER_DISCOUNT);
    let baseline = active_balance_probe(a, market);
    let before_uids = a
        .state_snapshot()
        .orders()
        .iter()
        .map(|order| order.uid)
        .collect::<Vec<_>>();
    let request_uid = a.send_new_order(market, false, price, 0, FIRETEST_ORDER_SIZE_USD);
    println!(
        "FIRETEST real order cancel: sent long request_uid={} market={} ask={:.8} limit={:.8} size_usd={} baseline_balance=[{}]",
        request_uid,
        market,
        ask,
        price,
        FIRETEST_ORDER_SIZE_USD,
        baseline.summary()
    );

    let start = Instant::now();
    let mut server_uid = None;
    let mut logged_waiting_for_local_order = false;
    while start.elapsed() < FIRETEST_REAL_BALANCE_ORDER_TIMEOUT {
        a.pump(PUMP_SLICE);
        b.pump(PUMP_SLICE);
        let st = a.snapshot();
        if server_uid.is_none() {
            server_uid =
                order_uid_for_request_or_new_market(&st, &before_uids, request_uid, market);
        }
        if let Some(uid) = server_uid {
            let local_status = st.order_status_by_uid.get(&uid).copied();
            if local_status.is_none() {
                if !logged_waiting_for_local_order {
                    println!(
                        "FIRETEST real order cancel: server uid={uid} is known, waiting for local ActiveLib order state before cancel"
                    );
                    logged_waiting_for_local_order = true;
                }
                continue;
            }
            assert!(
                matches!(
                    local_status,
                    Some(
                        OrderWorkerStatus::None
                            | OrderWorkerStatus::BuySet
                            | OrderWorkerStatus::SellSet
                    )
                ),
                "real order uid={uid} reached non-cancelable local status {local_status:?} before FireTest cancel"
            );
            assert!(
                a.cancel_order(uid),
                "cancel_order did not pass Delphi local gate for uid={uid} status={local_status:?}"
            );
            println!(
                "FIRETEST real order cancel: server uid={} arrived after {:.2}s; cancel queued immediately",
                uid,
                start.elapsed().as_secs_f64()
            );
            break;
        }
    }

    let Some(server_uid) = server_uid else {
        let _ = cancel_if_known(a, &before_uids, request_uid, market);
        panic!(
            "FireTest real order cancel: server order uid for {market} did not arrive within {:?}",
            FIRETEST_REAL_BALANCE_ORDER_TIMEOUT
        );
    };

    let cancel_start = Instant::now();
    let removed = loop {
        if cancel_start.elapsed() >= FIRETEST_REAL_BALANCE_ORDER_TIMEOUT {
            break false;
        }
        a.pump(PUMP_SLICE);
        b.pump(PUMP_SLICE);
        if !order_is_present(a, server_uid) {
            let current = active_balance_probe(a, market);
            println!(
                "FIRETEST real order cancel: order removed after cancel in {:.2}s uid={} current_balance=[{}]",
                cancel_start.elapsed().as_secs_f64(),
                server_uid,
                current.summary()
            );
            break true;
        }
        if matches!(
            a.snapshot().order_status_by_uid.get(&server_uid).copied(),
            Some(OrderWorkerStatus::BuyDone | OrderWorkerStatus::SellDone)
        ) {
            panic!(
                "FireTest real order cancel: uid={server_uid} unexpectedly filled/closed; limit was 5% below market"
            );
        }
    };
    assert!(
        removed,
        "FireTest real order cancel: {market} order uid={server_uid} was not removed within {:?} after cancel",
        FIRETEST_REAL_BALANCE_ORDER_TIMEOUT
    );
    assert_balance_stream_seen(a);
    assert_balance_stream_seen(b);
    println!("OK: real non-emulator SOL order was created, canceled, and removed; balance stream was observed independently");
}

fn price_matches(a: f64, b: f64) -> bool {
    let tolerance = (b.abs() * 1e-9).max(EPS);
    (a - b).abs() <= tolerance
}

fn format_order_for_debug(order: &Order) -> String {
    format!(
        "uid={} market={} status={:?} pending_buy={:?} buy_price={:.8} sell_price={:.8} bulk_buy={} bulk_sell={} panic={} buy_type={:?} sell_type={:?} buy_mean={:.8} sell_mean={:.8} buy_actual_q={:.8} sell_actual_q={:.8} reason={}",
        order.uid,
        order.market_name,
        order.status,
        order.pending_buy_cond_price,
        order.buy_price,
        order.sell_price,
        order.bulk_replace_buy,
        order.bulk_replace_sell,
        order.panic_sell,
        order.buy_order.order_type,
        order.sell_order.order_type,
        order.buy_order.mean_price,
        order.sell_order.mean_price,
        order.buy_order.actual_q,
        order.sell_order.actual_q,
        order.sell_reason().description()
    )
}

fn describe_session_order(session: &Session, uid: u64) -> String {
    session
        .maybe_state_snapshot()
        .and_then(|snapshot| snapshot.orders().get(uid).map(format_order_for_debug))
        .unwrap_or_else(|| format!("uid={uid} <not present in snapshot>"))
}

fn replace_local_effect_seen(
    order: &Order,
    status_before: OrderWorkerStatus,
    new_price: f64,
) -> bool {
    match status_before {
        OrderWorkerStatus::None => {
            order
                .pending_buy_cond_price
                .map(|price| price_matches(price, new_price))
                .unwrap_or(false)
                || matches!(
                    order.status,
                    OrderWorkerStatus::BuySet
                        | OrderWorkerStatus::SellSet
                        | OrderWorkerStatus::SellDone
                )
        }
        OrderWorkerStatus::BuySet => {
            price_matches(order.buy_price, new_price)
                || order.bulk_replace_buy
                || matches!(
                    order.status,
                    OrderWorkerStatus::SellSet | OrderWorkerStatus::SellDone
                )
        }
        OrderWorkerStatus::SellSet => {
            price_matches(order.sell_price, new_price)
                || order.bulk_replace_sell
                || order.status == OrderWorkerStatus::SellDone
        }
        _ => false,
    }
}

fn wait_order_replace_local_effect(
    a: &mut Session,
    b: &mut Session,
    uid: u64,
    status_before: OrderWorkerStatus,
    new_price: f64,
    timeout: Duration,
) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        a.pump(PUMP_SLICE);
        b.pump(PUMP_SLICE);

        let stats = a.snapshot();
        if let Some(reason) = stats.order_ignored_by_uid.get(&uid).copied() {
            return Err(format!(
                "runtime owner ignored replace uid={uid} reason={reason:?} order=[{}]",
                describe_session_order(a, uid)
            ));
        }

        let snapshot = a.state_snapshot();
        if let Some(order) = snapshot.orders().get(uid) {
            if replace_local_effect_seen(order, status_before, new_price) {
                println!(
                    "OK: order replace local effect after {:.2}s: before={status_before:?} {}",
                    start.elapsed().as_secs_f64(),
                    format_order_for_debug(order)
                );
                return Ok(());
            }
        }
    }

    Err(format!(
        "timeout waiting local replace effect uid={uid} before={status_before:?} new_price={new_price:.8} order=[{}] stats=[{}] metrics=[{}]",
        describe_session_order(a, uid),
        a.snapshot().summary(),
        a.protocol_summary()
    ))
}

fn run_order_lifecycle_gate_body(cfg: &FireConfig, a: &mut Session, b: &mut Session) {
    let before_uids = a
        .state_snapshot()
        .orders()
        .iter()
        .map(|order| order.uid)
        .collect::<Vec<_>>();
    let probe = a.snapshot();
    let ask = probe
        .last_book_ask
        .or_else(|| probe.last_market_price.map(|p| p.ask))
        .expect("market consistency gate must provide an ask price before order flow");
    let initial_price = ask * 0.98;
    let fill_price = ask * 1.01;
    let request_uid = a.send_new_order(
        &cfg.market,
        false,
        initial_price,
        0,
        FIRETEST_ORDER_SIZE_USD,
    );
    println!(
        "FIRETEST order flow: sent long request_uid={} market={} size_usd={} initial_price={:.8} fill_price={:.8}",
        request_uid, cfg.market, FIRETEST_ORDER_SIZE_USD, initial_price, fill_price
    );

    assert!(
        pump_pair_until_sessions(
            a,
            b,
            cfg.connect_timeout,
            "order server tag/new status",
            |a, _| {
                let st = a.snapshot();
                st.order_uid_by_request.contains_key(&request_uid)
                    || st.order_status_by_uid.iter().any(|(uid, _)| {
                        !before_uids.contains(uid)
                            && st
                                .order_market_by_uid
                                .get(uid)
                                .map(|market| market == &cfg.market)
                                .unwrap_or(false)
                    })
            }
        ),
        "new order did not produce server tag/status within {:?}",
        cfg.connect_timeout
    );
    let st = a.snapshot();
    let server_uid = st
        .order_uid_by_request
        .get(&request_uid)
        .copied()
        .or_else(|| {
            st.order_status_by_uid.iter().find_map(|(uid, _)| {
                (!before_uids.contains(uid)
                    && st
                        .order_market_by_uid
                        .get(uid)
                        .map(|market| market == &cfg.market)
                        .unwrap_or(false))
                .then_some(*uid)
            })
        })
        .expect("order server uid must be known after gate");

    assert!(
        pump_pair_until(a, b, cfg.wait, "order waiting buy status", |a, _| {
            matches!(
                a.order_status_by_uid.get(&server_uid).copied(),
                Some(OrderWorkerStatus::None | OrderWorkerStatus::BuySet)
            )
        }),
        "new order uid={} did not stay in waiting buy status",
        server_uid
    );

    let status_before_replace = a
        .state_snapshot()
        .orders()
        .get(server_uid)
        .map(|order| {
            println!(
                "FIRETEST order flow: pre-replace {}",
                format_order_for_debug(order)
            );
            order.status
        })
        .unwrap_or_else(|| {
            panic!(
                "new order uid={} disappeared before replace; stats=[{}]",
                server_uid,
                a.snapshot().summary()
            )
        });
    assert!(
        a.replace_order(server_uid, fill_price),
        "replace_order intent was not queued into runtime for uid={server_uid}"
    );
    wait_order_replace_local_effect(
        a,
        b,
        server_uid,
        status_before_replace,
        fill_price,
        cfg.wait,
    )
    .unwrap_or_else(|err| panic!("FireTest order replace local gate failed: {err}"));
    assert!(
        pump_pair_until(
            a,
            b,
            cfg.connect_timeout,
            "order moved to SellSet",
            |a, _| {
                a.order_status_by_uid.get(&server_uid).copied() == Some(OrderWorkerStatus::SellSet)
            }
        ),
        "order uid={} did not reach SellSet after replace; order=[{}]",
        server_uid,
        describe_session_order(a, server_uid)
    );

    assert!(
        a.panic_sell_order(server_uid, true),
        "panic sell did not pass Delphi local gate for uid={server_uid}"
    );
    assert!(
        pump_pair_until(
            a,
            b,
            cfg.connect_timeout,
            "order closed by panic sell",
            |a, _| {
                a.order_status_by_uid.get(&server_uid).copied() == Some(OrderWorkerStatus::SellDone)
            }
        ),
        "order uid={} did not reach SellDone after PanicSell",
        server_uid
    );

    let snapshot = a.state_snapshot();
    if let Some(order) = snapshot.orders().get(server_uid) {
        let delphi_delta_base =
            delphi_sell_report_delta_base(&order.buy_order, &order.sell_order, false);
        let approx_result_usd = delphi_delta_base;
        println!(
            "OK: order flow uid={} status={:?} reason={} buy_q={:.8} sell_q={:.8} sell_spent={:.8} sell_total={:.8} delphi_delta_base={:?} approx_result_usd={:?}",
            server_uid,
            order.status,
            order.sell_reason().description(),
            order.buy_order.actual_q,
            order.sell_order.actual_q,
            order.sell_order.spent_btc,
            order.sell_order.total_btc,
            delphi_delta_base,
            approx_result_usd
        );
        if let Some(result) = delphi_delta_base {
            assert!(
                result.abs() < FIRETEST_ORDER_SIZE_USD * 0.10,
                "order result looks insane by Delphi sell-report formula: delphi_delta_base={result:.8}"
            );
        }
    } else {
        println!(
            "OK: order flow uid={} reached SellDone and was already removed from active Orders",
            server_uid
        );
    }
}

fn delphi_sell_report_delta_base(
    buy: &OrderCompact,
    sell: &OrderCompact,
    reverse_base_currency: bool,
) -> Option<f64> {
    if sell.spent_btc <= EPS || sell.total_btc <= EPS {
        return None;
    }
    let mut delta = sell.total_btc - sell.spent_btc;
    if sell.is_short.is_true() {
        delta = -delta;
    }
    if reverse_base_currency {
        delta = -delta;
    }
    if (sell.actual_q - buy.actual_q).abs() > EPS {
        delta -= (sell.actual_q - buy.actual_q) * sell.mean_price;
    }
    Some(delta)
}

#[test]
#[ignore = "live MoonBot server required; create ../moonproto.firetest.conf"]
fn fire_test_active_library_health() {
    let start = Instant::now();
    let cfg = FireConfig::load_required();
    let profile = FireProfile::from_env();
    let key_info = parse_key_info(&cfg.key_b64).expect("invalid MoonProto key in FireTest config");
    let keys = key_info.keys;

    println!(
        "FIRETEST config: profile={} path={} key={} server={}:{} mask_ver={} market={} strategy_field={} strategy_id={:?} err_emu={} high_loss_err_emu={} connect_timeout={:?} candles_timeout={:?} high_loss_timeout={:?}",
        profile.as_str(),
        cfg.path.display(),
        key_info.display_name,
        cfg.host,
        cfg.port,
        cfg.mask_ver.to_byte(),
        cfg.market,
        cfg.strategy_field,
        cfg.strategy_id,
        firetest_err_emu_percent(),
        FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT,
        cfg.connect_timeout,
        cfg.candles_timeout,
        cfg.high_loss_timeout
    );

    if profile == FireProfile::Quick {
        run_quick_fire_test(&cfg, keys);
        return;
    }

    assert!(
        cfg.allow_mutation,
        "Full FireTest mutates live server settings/strategies/orders. Set allow_mutation=true in {} only for a test server.",
        cfg.path.display()
    );
    let mut err_emu = ErrEmuGuard::set(firetest_err_emu_percent());
    log_sliced_recovery_math("full initial gate", firetest_err_emu_percent());
    let seeded_strategy = firetest_strategy(&cfg);
    let seeded_strategy_id = seeded_strategy.strategy_id;

    let _public_path_stats = run_moonclient_public_smoke(
        "full/public-smoke",
        &cfg,
        keys,
        cfg.connect_timeout + cfg.wait,
        cfg.candles_timeout,
        true,
    );

    let mut a = Session::connect("A", &cfg, keys, Some(seeded_strategy.clone()));
    let mut b = Session::connect("B", &cfg, keys, Some(seeded_strategy.clone()));
    assert_strategy_field_visible_for_firetest(&a, &cfg, &seeded_strategy);
    assert_strategy_field_visible_for_firetest(&b, &cfg, &seeded_strategy);
    assert!(
        a.strategy_snapshot(seeded_strategy_id).is_some()
            && b.strategy_snapshot(seeded_strategy_id).is_some(),
        "FireTest local strategies must be available through EventDispatcher API before stream checks"
    );
    write_strategy_info_dump(FireProfile::Full, &cfg, &[("A", &a), ("B", &b)]);

    assert!(
        pump_pair_until(&mut a, &mut b, cfg.wait, "initial health", |a, b| {
            has_initial_health(a) && has_initial_health(b)
        }),
        "FireTest initial health failed: both clients must receive trades and configured orderbook within {:?}",
        cfg.wait
    );
    assert!(
        pump_pair_until_nonblocking_api_refresh(&mut a, &mut b, &cfg, cfg.connect_timeout),
        "FireTest non-blocking API refresh failed within {:?}: A=[{}] B=[{}]",
        cfg.connect_timeout,
        a.snapshot().summary(),
        b.snapshot().summary()
    );
    let _a_initial_settings = request_settings_until(&mut a, cfg.connect_timeout);
    let _b_initial_settings = request_settings_until(&mut b, cfg.connect_timeout);
    assert!(
        pump_pair_until(
            &mut a,
            &mut b,
            cfg.wait,
            "market book/trades/UpdateMarketsList consistency",
            |a, b| has_market_consistency(a) && has_market_consistency(b)
        ),
        "FireTest market consistency failed within {:?}: A=[{}] B=[{}]",
        cfg.wait,
        a.snapshot().market_probe_summary(),
        b.snapshot().market_probe_summary()
    );
    println!(
        "OK: market consistency A=[{}] B=[{}]",
        a.snapshot().market_probe_summary(),
        b.snapshot().market_probe_summary()
    );
    log_err_emu_pair("initial health 10% gate", &a, &b);
    log_protocol_cpu_pair("initial health 10% gate", &a, &b);

    a.request_candles_snapshot();
    assert!(
        pump_pair_until(
            &mut a,
            &mut b,
            cfg.candles_timeout,
            "full candles snapshot under err_emu",
            |a, _| a
                .candles_complete
                .as_ref()
                .map(CandlesSnapshotSummary::is_healthy)
                .unwrap_or(false)
                && a.parse_failed == 0
        ),
        "client A did not receive a complete candles snapshot within {:?}",
        cfg.candles_timeout
    );
    if let Some(candles) = a.snapshot().candles_complete {
        println!("OK: full candles snapshot {}", candles.summary());
    }
    log_protocol_cpu_pair("after candles 10% gate", &a, &b);
    run_high_loss_simple_ops_gate(&mut a, &mut b, &mut err_emu, cfg.high_loss_timeout);
    log_protocol_cpu_pair("after high-loss simple ops gate", &a, &b);
    err_emu.reset("high-loss simple ops gate");
    a.client
        .debug_reset_err_emu_diagnostics()
        .expect("reset A err_emu diagnostics");
    b.client
        .debug_reset_err_emu_diagnostics()
        .expect("reset B err_emu diagnostics");

    run_order_lifecycle_gate(&cfg, &mut a, &mut b);
    run_real_order_cancel_gate(&cfg, &mut a, &mut b);

    let a_initial = a.snapshot();
    let original_settings = a_initial
        .last_settings
        .clone()
        .expect("settings were counted but not stored");
    let original_strategy = a
        .strategy_snapshot(seeded_strategy_id)
        .expect("seeded strategy missing from dispatcher state");
    let field = select_field(&original_strategy, &cfg.strategy_field);
    let original_field_value = match original_strategy.fields.get(field.as_str()) {
        Some(FieldValue::String(value)) => value.clone(),
        _ => panic!("selected field `{field}` missing from seeded strategy"),
    };

    let run_id = now_epoch_ms();
    let mutated_field_value = format!("firetest-{run_id}");
    let mutated_strategy = with_strategy_string(
        original_strategy.clone(),
        &field,
        mutated_field_value.clone(),
        1,
    );
    let mut mutated_settings = original_settings.clone();
    mutated_settings.x_sell = if original_settings.x_sell == i32::MAX {
        original_settings.x_sell - 1
    } else {
        original_settings.x_sell + 1
    };

    println!(
        "FIRETEST mutation: strategy_id={} field={} {:?}->{:?}; x_sell {}->{}",
        original_strategy.strategy_id,
        field,
        original_field_value,
        mutated_field_value,
        original_settings.x_sell,
        mutated_settings.x_sell
    );
    a.send_strategy_snapshot_batch(std::slice::from_ref(&mutated_strategy));
    a.client
        .settings()
        .send(mutated_settings.clone())
        .expect("MoonClient send_settings must queue");

    let mutation_seen =
        pump_pair_until(&mut a, &mut b, cfg.wait, "cross-client mutation", |_, b| {
            b.last_settings
                .as_ref()
                .map(|s| s.x_sell == mutated_settings.x_sell)
                .unwrap_or(false)
                && strategy_field_string(b, original_strategy.strategy_id, &field)
                    .map(|value| value == mutated_field_value)
                    .unwrap_or(false)
        });

    let restored_strategy = with_strategy_string(
        original_strategy.clone(),
        &field,
        original_field_value.clone(),
        2,
    );
    a.send_strategy_snapshot_batch(std::slice::from_ref(&restored_strategy));
    a.client
        .settings()
        .send(original_settings.clone())
        .expect("MoonClient send_settings must queue");
    let restored = pump_pair_until(&mut a, &mut b, cfg.wait, "restore mutation", |_, b| {
        b.last_settings
            .as_ref()
            .map(|s| s.x_sell == original_settings.x_sell)
            .unwrap_or(false)
            && strategy_field_string(b, original_strategy.strategy_id, &field)
                .map(|value| value == original_field_value)
                .unwrap_or(false)
    });

    assert!(
        mutation_seen,
        "client B did not receive settings + strategy mutation from client A"
    );
    assert!(
        restored,
        "client B did not receive restoration of settings + strategy mutation"
    );

    let before_blackhole = a.snapshot();
    a.client
        .debug_set_outgoing_blackhole(true)
        .expect("enable outgoing blackhole");
    let disconnect_start = Instant::now();
    let disconnected = pump_pair_until(
        &mut a,
        &mut b,
        cfg.disconnect_timeout,
        "server-side disconnect after outgoing blackhole",
        |a, _| a.reconnecting > before_blackhole.reconnecting,
    );
    let disconnected_after = disconnect_start.elapsed();
    a.client
        .debug_set_outgoing_blackhole(false)
        .expect("disable outgoing blackhole");
    assert!(
        disconnected,
        "client A did not enter reconnecting state within {:?} while outgoing blackhole was enabled",
        cfg.disconnect_timeout
    );
    println!(
        "OK: server/client detected silence after {:.2}s",
        disconnected_after.as_secs_f64()
    );

    let before_reconnect = a.snapshot();
    let reconnected = pump_pair_until(
        &mut a,
        &mut b,
        cfg.reconnect_timeout,
        "automatic reconnect",
        |a, _| a.connected_again > before_reconnect.connected_again,
    );
    assert!(
        reconnected && a.snapshot().connected_now,
        "client A did not reconnect within {:?}",
        cfg.reconnect_timeout
    );

    let after_reconnect = a.snapshot();
    let trades_before = after_reconnect.trades_apply;
    let books_before = after_reconnect.orderbook_apply;
    assert!(
        pump_pair_until(
            &mut a,
            &mut b,
            cfg.wait,
            "streams after reconnect",
            |a, _| { a.trades_apply > trades_before && a.orderbook_apply > books_before }
        ),
        "client A did not receive trades and orderbook after reconnect within {:?}",
        cfg.wait
    );

    pump_pair_for(&mut a, &mut b, Duration::from_millis(200));
    a.assert_coin_card_candles_healthy(&cfg.market, FIRETEST_COIN_CARD_KIND);
    b.assert_coin_card_candles_healthy(&cfg.market, FIRETEST_COIN_CARD_KIND);
    log_protocol_cpu_pair("final", &a, &b);
    write_strategy_info_dump(FireProfile::Full, &cfg, &[("A", &a), ("B", &b)]);
    a.emit_active_lib_report(FireProfile::Full, start);
    println!("FIRETEST_PASS");
}
