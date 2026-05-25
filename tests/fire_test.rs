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
//! allow_mutation = true
//! market = BTCUSDT
//! strategy_field = Comment
//! # strategy_id = 123456789
//! # candles_timeout_secs = 30
//! # high_loss_timeout_secs = 60
//! ```
//!
//! Profiles:
//! - `MOONPROTO_FIRETEST_PROFILE=quick` — one client, <=30s target health gate:
//!   connect/AuthDone/InitDone, BaseCheck/AuthCheck, markets/indexes/update,
//!   retained LastPrice/trades, derived trade snapshot, trades + orderbook
//!   streams, ParseFailed=0, CPU summary.
//! - `MOONPROTO_FIRETEST_PROFILE=full` or unset — the complete destructive
//!   health/stress scenario below. Requires `allow_mutation=true`.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use moonproto::client::{set_err_emu, ErrEmuDiagnostics, ErrEmuSlicedDatagramDiagnostics};
use moonproto::commands::arb::ArbPayload;
use moonproto::commands::candles::{
    parse_request_candles_data_response, CandlesAggregator, RequestCandlesMarket,
};
use moonproto::commands::engine_api::{EngineMethod, EngineResponse};
use moonproto::commands::engine_request;
use moonproto::commands::strategy_schema::{
    StrategyDynamicPicklist, StrategyFieldLayout, StrategyFieldUiKind, StrategySchema,
};
use moonproto::commands::strategy_serializer::{
    parse_strategy_batch, FieldValue, StrategyFields, StrategyKind, StrategySnapshot,
};
use moonproto::commands::trade::{OrderCompact, OrderWorkerStatus};
use moonproto::commands::trades_stream::TradeSection;
use moonproto::commands::ui::ClientSettingsCommand;
use moonproto::events::Event;
use moonproto::protocol::Command;
use moonproto::state::{
    LastPricePoint, MarketHistoryConfig, MarketHistoryWorker, MarketPrice, OrderBookEvent,
    OrderBookKind, SettingsEvent, StratEvent, TradesEvent,
};
use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig, EventDispatcher,
    EventDispatcherSnapshot, ImportedKeys, InitConfig, LifecycleEvent,
};

const FIRETEST_ERR_EMU_PERCENT: u8 = 10;
const FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT: u8 = 50;
const FIRETEST_RECONNECT_MATH_ATTEMPTS: i32 = 10;
const FIRETEST_STRATEGY_ID: u64 = 0xF17E_5737_0000_0001;
const DEFAULT_WAIT_SECS: u64 = 5;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 45;
const DEFAULT_CANDLES_TIMEOUT_SECS: u64 = 90;
const DEFAULT_HIGH_LOSS_TIMEOUT_SECS: u64 = 60;
const DEFAULT_DISCONNECT_TIMEOUT_SECS: u64 = 45;
const DEFAULT_RECONNECT_TIMEOUT_SECS: u64 = 30;
const PUMP_SLICE: Duration = Duration::from_millis(50);
const PRICE_NEIGHBORHOOD_PCT: f64 = 0.05;
const FIRETEST_ORDER_SIZE_USD: f64 = 1000.0;
const EPS: f64 = 1e-9;
const QUICK_CONNECT_TIMEOUT_SECS: u64 = 18;
const QUICK_STREAM_TIMEOUT_SECS: u64 = 8;
const QUICK_TOTAL_TARGET_SECS: u64 = 30;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FireProfile {
    Quick,
    Full,
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

#[derive(Clone)]
struct FireConfig {
    path: PathBuf,
    host: String,
    port: u16,
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

        let server = take_required(&values, "server");
        let (host, port) = parse_server(&server);
        let key_b64 = values
            .get("key")
            .or_else(|| values.get("moonproto_key"))
            .unwrap_or_else(|| panic!("FireTest config missing `key`"))
            .to_string();
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

fn take_required(values: &HashMap<String, String>, key: &str) -> String {
    values
        .get(key)
        .unwrap_or_else(|| panic!("FireTest config missing `{key}`"))
        .to_string()
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
        self.markets > 0 && self.candles > 0 && self.zipped_bytes > 0
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
    order_uid_by_request: HashMap<u64, u64>,
    order_status_by_uid: HashMap<u64, OrderWorkerStatus>,
    order_market_by_uid: HashMap<u64, String>,
    order_sell_reason_by_uid: HashMap<u64, String>,
    parse_failed: u64,
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
            order_uid_by_request: self.order_uid_by_request.clone(),
            order_status_by_uid: self.order_status_by_uid.clone(),
            order_market_by_uid: self.order_market_by_uid.clone(),
            order_sell_reason_by_uid: self.order_sell_reason_by_uid.clone(),
            parse_failed: self.parse_failed,
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

    fn has_engine_method(&self, method: EngineMethod) -> bool {
        self.engine_method_count(method) > 0
    }

    fn summary(&self) -> String {
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
            "connected_now={} fresh={} again={} reconnecting={} disconnected={} server_events={} engine={} raw={} logs={} settings={} strats={} schema_events={} schema_kinds={} schema_fields={} strategy_rows={} markets={} trades={} target_trade_packets={} books={} target_book_full={} target_book_update={} market_probe=[{}] order_events={} parse_failed={} candles={}",
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
            self.parse_failed,
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
    client: Client,
    dispatcher: EventDispatcher,
    history_worker: MarketHistoryWorker,
    candles_snapshot_tx: mpsc::Sender<Vec<RequestCandlesMarket>>,
    candles_snapshot_rx: mpsc::Receiver<Vec<RequestCandlesMarket>>,
    stats: Arc<Mutex<SessionStats>>,
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
        let mut client = Client::new(
            ClientConfig::new(&cfg.host, cfg.port, keys.master_key, keys.mac_key)
                .with_client_id(rand::random()),
        );
        client.reset_err_emu_diagnostics();

        let lc_stats = Arc::clone(&stats);
        let lifecycle_label = label.to_string();
        client.on_lifecycle(Box::new(move |event| {
            let mut st = lc_stats.lock().unwrap();
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
                _ => {}
            }
            println!("LIFECYCLE->{lifecycle_label}: {event:?}");
        }));

        let history_worker = MarketHistoryWorker::spawn(firetest_history_config());
        let (candles_snapshot_tx, candles_snapshot_rx) = mpsc::channel();
        let mut dispatcher = EventDispatcher::new();
        dispatcher.set_market_history_handle(history_worker.handle());
        if let Some(strategy) = provided_strategy.as_ref() {
            dispatcher.set_local_strategies(std::slice::from_ref(strategy));
            println!(
                "FIRETEST {label}: local strategy snapshot seeded id={} ver={} last_date={}",
                strategy.strategy_id, strategy.strategy_ver, strategy.last_date
            );
        }

        let init = InitConfig {
            mm_orders_subscribe: Some(true),
            subscribe_trades: Some(false),
            subscribe_orderbooks: vec![cfg.market.clone()],
            step_timeout: None,
        };

        println!(
            "FIRETEST {label}: connecting to {}:{} market={}",
            cfg.host, cfg.port, cfg.market
        );
        connect_and_init(
            &mut client,
            &mut dispatcher,
            ConnectConfig::new(init).with_connect_timeout(cfg.connect_timeout),
        )
        .unwrap_or_else(|err| panic!("FIRETEST {label}: connect_and_init failed: {err}"));

        let mut session = Self {
            client,
            dispatcher,
            history_worker,
            candles_snapshot_tx,
            candles_snapshot_rx,
            stats,
        };
        session.drain_queued();
        session
    }

    fn pump(&mut self, duration: Duration) {
        let stats = Arc::clone(&self.stats);
        let candles_snapshot_tx = self.candles_snapshot_tx.clone();
        self.client.run_with_dispatcher_state(
            duration,
            &mut self.dispatcher,
            Box::new(move |event, dispatcher| {
                record_event(&stats, event, dispatcher, Some(&candles_snapshot_tx))
            }),
        );
        self.drain_queued();
        self.apply_completed_candles_snapshots();
    }

    fn drain_queued(&mut self) {
        let events = self.dispatcher.take_queued_events();
        let snapshot = self.dispatcher.snapshot();
        for event in events {
            record_event(
                &self.stats,
                &event,
                &snapshot,
                Some(&self.candles_snapshot_tx),
            );
        }
        self.apply_completed_candles_snapshots();
    }

    fn snapshot(&self) -> SessionStats {
        self.stats.lock().unwrap().clone()
    }

    fn protocol_summary(&self) -> String {
        let m = self
            .client
            .protocol_metrics_snapshot_with_dispatcher(&self.dispatcher);
        format!(
            "recv={} reader(avg/max={}us/{}us max_src={} >100us/>1ms/>5ms={}/{}/{}) writer_cpu(avg/max={}us/{}us >100us/>1ms/>5ms={}/{}/{}) active_dispatch(avg/max={}us/{}us max_src={} events={} actions={} >100us/>1ms/>5ms={}/{}/{}) app_enqueue(avg/max={}us/{}us max_src={} events={} mode={} >100us/>1ms/>5ms={}/{}/{}) writer_tick_wall(count={} avg/max={}us/{}us) send_max={}us public_events={}",
            m.recv_count,
            avg_us(m.reader_protocol_ns, m.reader_protocol_count),
            m.reader_protocol_max_ns / 1_000,
            metric_cmd_label(m.reader_protocol_max_cmd, m.reader_protocol_max_payload_len),
            m.reader_protocol_over_100us,
            m.reader_protocol_over_1ms,
            m.reader_protocol_over_5ms,
            avg_us(m.writer_cpu_ns, m.writer_cpu_count),
            m.writer_cpu_max_ns / 1_000,
            m.writer_cpu_over_100us,
            m.writer_cpu_over_1ms,
            m.writer_cpu_over_5ms,
            avg_us(m.active_dispatch_ns, m.active_dispatch_count),
            m.active_dispatch_max_ns / 1_000,
            metric_cmd_label(m.active_dispatch_max_cmd, m.active_dispatch_max_payload_len),
            m.active_dispatch_max_events,
            m.active_dispatch_max_actions,
            m.active_dispatch_over_100us,
            m.active_dispatch_over_1ms,
            m.active_dispatch_over_5ms,
            avg_us(m.app_enqueue_ns, m.app_enqueue_count),
            m.app_enqueue_max_ns / 1_000,
            metric_cmd_label(m.app_enqueue_max_cmd, m.app_enqueue_max_payload_len),
            m.app_enqueue_max_events,
            metric_app_mode_label(m.app_enqueue_max_mode),
            m.app_enqueue_over_100us,
            m.app_enqueue_over_1ms,
            m.app_enqueue_over_5ms,
            m.writer_tick_count,
            avg_us(m.writer_tick_ns, m.writer_tick_count),
            m.writer_tick_max_ns / 1_000,
            m.send_phase_max_ns / 1_000,
            m.public_event_queue_len
        )
    }

    fn target_last_price_tail(&self) -> Option<LastPricePoint> {
        let _ = self.history_worker.flush(0.0);
        let reader = self
            .history_worker
            .readers(&self.snapshot().market)?
            .last_prices?;
        let mut rows = Vec::new();
        reader.copy_last(1, &mut rows);
        rows.into_iter().next()
    }

    fn target_retained_trade_counts(&self) -> (usize, usize) {
        let _ = self.history_worker.flush(0.0);
        let readers = self.history_worker.readers(&self.snapshot().market);
        let futures = readers
            .as_ref()
            .and_then(|readers| readers.futures_trades.as_ref())
            .map(|reader| {
                let mut rows = Vec::new();
                reader.copy_last(64, &mut rows);
                rows.len()
            })
            .unwrap_or(0);
        let spot = readers
            .as_ref()
            .and_then(|readers| readers.spot_trades.as_ref())
            .map(|reader| {
                let mut rows = Vec::new();
                reader.copy_last(64, &mut rows);
                rows.len()
            })
            .unwrap_or(0);
        (futures, spot)
    }

    fn apply_completed_candles_snapshots(&mut self) {
        while let Ok(markets) = self.candles_snapshot_rx.try_recv() {
            let total_candles = markets.iter().map(|m| m.candles_5m.len()).sum::<usize>();
            let target_market = self.snapshot().market;
            let target_parsed = markets
                .iter()
                .find(|m| m.market_name == target_market)
                .map(|m| m.candles_5m.len())
                .unwrap_or(0);
            let applied = self.dispatcher.apply_candles_snapshot(&markets);
            let _ = self.history_worker.flush(0.0);
            let readers = self.history_worker.readers(&target_market);
            let (retained, capacity) = readers
                .as_ref()
                .and_then(|readers| readers.candles_5m.as_ref())
                .map(|reader| (reader.bounds().len, reader.capacity()))
                .unwrap_or((0, 0));
            println!(
                "FIRETEST candles active-storage applied={} parsed_candles={} target={} target_parsed_candles={} target_retained_candles={} capacity={}",
                applied, total_candles, target_market, target_parsed, retained, capacity
            );
            assert!(
                target_parsed > 0,
                "FireTest: parsed candles snapshot did not contain target market {target_market}"
            );
            assert!(
                retained == target_parsed.min(capacity),
                "FireTest: Active Lib retained candles mismatch for {target_market}: parsed={target_parsed} retained={retained} capacity={capacity}"
            );
            let now_time = delphi_now_raw_for_test();
            let derived = self
                .history_worker
                .derived_snapshot(&target_market, now_time)
                .expect("target market should expose derived snapshot after candles apply");
            assert!(
                derived.candle_volumes.one_hour > 0.0,
                "FireTest: retained target candles did not feed one-hour candle volume: {:?}",
                derived.candle_volumes
            );
            println!(
                "FIRETEST candles derived deltas/volumes: combined(15m={:.4}% 1h={:.4}% 24h={:.4}%) candle(15m={:.4}% 1h={:.4}% 24h={:.4}% vol1h={:.2} vol24h={:.2}) trade(1m={:.4}% 5m={:.4}%)",
                derived.deltas.fifteen_minutes,
                derived.deltas.one_hour,
                derived.deltas.twenty_four_hours,
                derived.candle_deltas.fifteen_minutes,
                derived.candle_deltas.one_hour,
                derived.candle_deltas.twenty_four_hours,
                derived.candle_volumes.one_hour,
                derived.candle_volumes.twenty_four_hours,
                derived.trade_deltas.one_minute,
                derived.trade_deltas.five_minutes
            );
        }
    }

    fn strategy_snapshot(&self, strategy_id: u64) -> Option<StrategySnapshot> {
        self.dispatcher.strategy_snapshot(strategy_id).cloned()
    }

    fn send_strategy_snapshot_batch(&mut self, strategies: &[StrategySnapshot]) {
        self.dispatcher.set_local_strategies(strategies);
        let schema = self
            .dispatcher
            .strats()
            .strategy_schema()
            .expect("FireTest init must fetch TStratSchema before sending strategies");
        self.client
            .strat_send_snapshot_batch(0, false, schema, strategies);
    }

    fn send_new_order(
        &mut self,
        market: &str,
        is_short: bool,
        price: f64,
        strat_id: u64,
        order_size: f64,
    ) -> u64 {
        let ctx = self
            .client
            .random_trade_ctx()
            .expect("run BaseCheck before FireTest order flow");
        let request_uid = ctx.uid;
        self.client
            .new_order(ctx, market, is_short, price, strat_id, order_size);
        request_uid
    }

    fn replace_order(&mut self, uid: u64, new_price: f64) -> bool {
        self.client
            .replace_tracked_order(self.dispatcher.orders_mut(), uid, new_price)
    }

    fn panic_sell_order(&mut self, uid: u64, turn_on: bool) -> bool {
        self.client
            .turn_tracked_order_panic_sell(self.dispatcher.orders_mut(), uid, turn_on)
    }

    fn request_candles_snapshot(&mut self) {
        let raw = moonproto::commands::engine_request::request_candles_data();
        let uid = raw
            .get(3..11)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        {
            let mut st = self.stats.lock().unwrap();
            st.candles_requested = true;
            st.candles_chunks = 0;
            st.candles_ignored = 0;
            st.candles_payload_bytes = 0;
            st.candles_seen_chunks.clear();
            st.candles_last_progress = (0, 0);
            st.candles_complete = None;
            st.candles_aggregator.reset();
            println!(
                "FIRETEST {}: request full candles snapshot uid={} payload_len={}",
                st.label,
                uid,
                raw.len()
            );
        }
        self.client.send_api_request(&raw);
    }

    fn remember_settings_snapshot(&self, settings: &ClientSettingsCommand) {
        let mut st = self.stats.lock().unwrap();
        if st.last_settings.as_ref().map(|prev| prev.uid) != Some(settings.uid) {
            st.settings_events += 1;
        }
        st.last_settings = Some(settings.clone());
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

fn append_session_strategy_dump(out: &mut String, label: &str, session: &Session) {
    let stats = session.snapshot();
    let strats = session.dispatcher.strats();
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

    let mut snapshots = session.dispatcher.strategy_snapshot_vec();
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
            field.raw_type_id,
            field.type_id.name(),
            ui_kind_text(field.ui_kind),
            field.raw_flags,
            field
                .default_value
                .as_ref()
                .map(field_value_text)
                .unwrap_or_else(|| "none".to_string()),
            visible,
            layout_text(&field.layout),
            field
                .static_picklist_raw
                .as_deref()
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

fn avg_us(total_ns: u64, count: u64) -> u64 {
    if count == 0 {
        0
    } else {
        total_ns / count / 1_000
    }
}

fn metric_cmd_label(cmd: u8, payload_len: u64) -> String {
    if cmd == u8::MAX {
        format!("pre-cmd payload={payload_len}")
    } else {
        let c = Command::from_byte(cmd);
        format!("{}({}) payload={payload_len}", c.name(), c.to_byte())
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
    dispatcher: &EventDispatcherSnapshot,
    candles_snapshot_tx: Option<&mpsc::Sender<Vec<RequestCandlesMarket>>>,
) {
    let mut st = stats.lock().unwrap();
    st.server_events += 1;
    let event_no = st.server_events;
    sync_market_probe_from_dispatcher(&mut st, event_no, dispatcher, false);
    match event {
        Event::Order(ev) => {
            st.order_events += 1;
            record_order_state_snapshot(&mut st, dispatcher);
            log_server_event(&st, event_no, format!("Order {ev:?}"));
        }
        Event::Markets(ev) => {
            st.market_events += 1;
            sync_market_probe_from_dispatcher(&mut st, event_no, dispatcher, true);
            log_server_event(
                &st,
                event_no,
                format!("Markets {ev:?}; {}", st.market_probe_summary()),
            );
        }
        Event::Settings(SettingsEvent::ClientSettingsUpdated) => {
            st.settings_events += 1;
            st.last_settings = dispatcher.settings().client_settings.clone();
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
                log_server_event(&st, event_no, "UI ClientSettings but state is empty");
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
        Event::Trade(TradesEvent::Apply(pkt)) => {
            st.trades_apply += 1;
            let target_market_index = st.market_index;
            let mut target_trade_price = None;
            let mut target_trades = 0usize;
            if let Some(market_index) = target_market_index {
                for section in &pkt.sections {
                    if let TradeSection::Trades(items) = section {
                        for trade in items {
                            if trade.market_index == market_index && trade.price > 0.0 {
                                target_trade_price = Some(trade.price as f64);
                                target_trades += 1;
                            }
                        }
                    }
                }
            }
            if let Some(price) = target_trade_price {
                st.target_trade_packets += 1;
                st.last_trade_price = Some(price);
                if st.target_trade_packets <= 5 || st.target_trade_packets.is_power_of_two() {
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "TradesStream target market={} idx={:?} packet_num={} trades={} last_price={:.8}",
                            st.market, target_market_index, pkt.packet_num, target_trades, price
                        ),
                    );
                }
            }
            if should_log_stream_count(st.trades_apply) {
                let mut trades = 0usize;
                let mut mm_orders = 0usize;
                let mut liq_orders = 0usize;
                let mut watcher_fills = 0usize;
                for section in &pkt.sections {
                    match section {
                        TradeSection::Trades(items) => trades += items.len(),
                        TradeSection::MMOrders(items) => mm_orders += items.len(),
                        TradeSection::LiqOrders(items) => liq_orders += items.len(),
                        TradeSection::WatcherFills { .. } => watcher_fills += 1,
                    }
                }
                log_server_event(
                    &st,
                    event_no,
                    format!(
                        "TradesStream Apply #{} packet_num={} base_time={:.8} sections={} trades={} mm_orders={} liq={} watcher_fills={}",
                        st.trades_apply,
                        pkt.packet_num,
                        pkt.base_time,
                        pkt.sections.len(),
                        trades,
                        mm_orders,
                        liq_orders,
                        watcher_fills
                    ),
                );
            }
        }
        Event::Trade(other) => {
            log_server_event(&st, event_no, format!("TradesStream {other:?}"));
        }
        Event::OrderBook(OrderBookEvent::Apply {
            market_index,
            book_kind,
            is_full,
            seq,
            buys,
            sells,
        }) => {
            st.orderbook_apply += 1;
            if st.market_index == Some(*market_index) {
                if *is_full {
                    st.target_orderbook_full += 1;
                } else {
                    st.target_orderbook_update += 1;
                }
                st.last_book_kind = Some(*book_kind);
                match OrderBookKind::from_u8(*book_kind)
                    .and_then(|kind| dispatcher.order_books().top_of_book(*market_index, kind))
                {
                    Some(top) => {
                        if let Some(bid) = top.bid {
                            st.last_book_bid = Some(bid.rate);
                        }
                        if let Some(ask) = top.ask {
                            st.last_book_ask = Some(ask.rate);
                        }
                        if let (Some(bid), Some(ask)) = (st.last_book_bid, st.last_book_ask) {
                            if bid <= 0.0 || ask <= 0.0 || ask <= bid {
                                st.market_invariant_error = Some(format!(
                                    "bad book top for {}: bid={bid:.8} ask={ask:.8}",
                                    st.market
                                ));
                            }
                        }
                    }
                    None => {
                        st.market_invariant_error = Some(format!(
                            "orderbook top unavailable for market={} idx={} kind={}",
                            st.market, market_index, book_kind
                        ));
                    }
                }
                if st.target_orderbook_full + st.target_orderbook_update <= 8
                    || (st.target_orderbook_full + st.target_orderbook_update).is_power_of_two()
                {
                    log_server_event(
                        &st,
                        event_no,
                        format!(
                            "OrderBook target market={} idx={} kind={} full={} seq={} top_bid={:?} top_ask={:?}",
                            st.market,
                            market_index,
                            book_kind,
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
                        book_kind,
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
        Event::Arb { uid, payload } => {
            log_server_event(
                &st,
                event_no,
                format!("Arb uid={} {}", uid, arb_summary(payload)),
            );
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
            let dump = write_parse_failed_dump(&st.label, event_no, *cmd, payload);
            let dump_suffix = dump
                .as_ref()
                .map(|path| format!(" dump={}", path.display()))
                .unwrap_or_else(|| " dump=<write-failed>".to_string());
            log_server_event(
                &st,
                event_no,
                format!(
                    "ParseFailed cmd={cmd:?} len={len} head={}{}",
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
        st.last_market_price = Some(MarketProbePrice::from(price));
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
                "Strat {kind} epoch={} raw={} strategies={} ids=[{}]",
                server_epoch,
                raw_data.len(),
                count,
                ids_preview
            ),
        );
    } else {
        st.parse_failed += 1;
        log_server_event(
            st,
            event_no,
            format!(
                "Strat {kind} epoch={} raw={} parse_failed head={}",
                server_epoch,
                raw_data.len(),
                hex_preview(raw_data, 32)
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

fn arb_summary(payload: &ArbPayload) -> String {
    match payload {
        ArbPayload::Price { version, blocks } => {
            let price_items: usize = blocks.iter().map(|b| b.prices.len()).sum();
            let preview = blocks
                .iter()
                .take(8)
                .map(|b| format!("{}:{}", b.market_index, b.prices.len()))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "Price version={} blocks={} price_items={} preview=[{}]",
                version,
                blocks.len(),
                price_items,
                preview
            )
        }
        ArbPayload::Isolation { version, entries } => {
            let preview = entries
                .iter()
                .take(8)
                .map(|e| format!("{}:{}:{}", e.market_index, e.platform_code, e.flags))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "Isolation version={} entries={} preview=[{}]",
                version,
                entries.len(),
                preview
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

fn pump_single_until<F>(
    session: &mut Session,
    timeout: Duration,
    label: &str,
    mut predicate: F,
) -> bool
where
    F: FnMut(&SessionStats) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        session.pump(PUMP_SLICE);
        let stats = session.snapshot();
        if predicate(&stats) {
            println!("OK: {label} after {:.2}s", start.elapsed().as_secs_f64());
            return true;
        }
    }

    let stats = session.snapshot();
    eprintln!(
        "FIRETEST TIMEOUT {label}: A=[{}] A.metrics=[{}]",
        stats.summary(),
        session.protocol_summary(),
    );
    log_err_emu_snapshot(label, "A", &session.client.err_emu_diagnostics_snapshot());
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

fn has_initial_health(st: &SessionStats) -> bool {
    st.connected_now
        && st.strategy_schema_events > 0
        && st.strategy_schema_kinds > 0
        && st.strategy_schema_fields > 0
        && st.trades_apply > 0
        && st.orderbook_apply > 0
        && st.parse_failed == 0
}

fn has_quick_health(st: &SessionStats) -> bool {
    st.connected_now
        && st.parse_failed == 0
        && st.strategy_schema_events > 0
        && st.strategy_schema_kinds > 0
        && st.strategy_schema_fields > 0
        && st.has_engine_method(EngineMethod::BaseCheck)
        && st.has_engine_method(EngineMethod::AuthCheck)
        && st.has_engine_method(EngineMethod::GetMarketsList)
        && st.has_engine_method(EngineMethod::GetMarketsIndexes)
        && st.has_engine_method(EngineMethod::UpdateMarketsList)
        && st.has_engine_method(EngineMethod::SubscribeAllTrades)
        && st.has_engine_method(EngineMethod::SubscribeOrderBook)
        && has_market_consistency(st)
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
    log_err_emu_snapshot(label, "A", &a.client.err_emu_diagnostics_snapshot());
    log_err_emu_snapshot(label, "B", &b.client.err_emu_diagnostics_snapshot());
}

fn log_protocol_cpu_pair(label: &str, a: &Session, b: &Session) {
    println!("FIRETEST CPU {label} A: {}", a.protocol_summary());
    println!("FIRETEST CPU {label} B: {}", b.protocol_summary());
}

fn log_err_emu_snapshot(label: &str, session: &str, diag: &ErrEmuDiagnostics) {
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
        for dg in candidates.iter().rev().take(16).rev() {
            eprintln!(
                "FIRETEST ErrEmu {label} {session}: {}",
                describe_sliced_candidate(diag.configured_rate, dg)
            );
        }
    }
}

fn is_sliced_response_candidate(dg: &ErrEmuSlicedDatagramDiagnostics) -> bool {
    let completed_settings =
        dg.completed_cmd == Some(Command::UI.to_byte()) && dg.completed_ui_cmd_id == Some(1);
    let block0_ui = dg
        .block0_wire_cmd
        .map(|cmd| Command::from_byte(cmd & 0x7F) == Command::UI)
        .unwrap_or(false);
    let block0_known_settings = dg.block0_ui_cmd_id == Some(1);
    let completed_api = dg.completed_cmd == Some(Command::API.to_byte());
    let block0_api = dg
        .block0_wire_cmd
        .map(|cmd| Command::from_byte(cmd & 0x7F) == Command::API)
        .unwrap_or(false);
    completed_api
        || block0_api
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
    format!(
        "Sliced d={} blocks={}/{} attempts={} delivered_packets={} dropped_packets={} wire_cmd={:?} ui_cmd={:?} complete_cmd={:?} complete_ui={:?} complete_api_method={:?} complete_api_uid={:?} complete_api_success={:?} payload_len={:?} missing=[{}] pure_err_emu_fail_p={}",
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
        dg.completed_api_method.map(EngineMethod::from_byte),
        dg.completed_api_uid,
        dg.completed_api_success,
        dg.completed_payload_len,
        missing_preview,
        pure_err,
    )
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
        kind: StrategyKind::TELEGRAM.0,
        path: "FireTest".to_string(),
        fields,
    }
}

fn assert_strategy_field_visible_for_firetest(
    session: &Session,
    cfg: &FireConfig,
    strategy: &StrategySnapshot,
) {
    let schema = session
        .dispatcher
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

fn firetest_history_config() -> MarketHistoryConfig {
    MarketHistoryConfig {
        futures_trades_capacity: 64,
        spot_trades_capacity: 64,
        liquidation_capacity: 0,
        mm_orders_capacity: 0,
        mm_order_companion_capacity: 0,
        last_price_capacity: 64,
        mini_candles_capacity: 0,
        candles_5m_capacity: 512,
        trade_join_capacity: 64,
    }
}

fn run_quick_fire_test(cfg: &FireConfig, keys: ImportedKeys) {
    let start = Instant::now();
    let cfg = quick_profile_config(cfg);
    let _err_emu = ErrEmuGuard::set(FIRETEST_ERR_EMU_PERCENT);

    println!(
        "FIRETEST quick target: <= {}s; one client, err_emu={}%, connect_timeout={:?}, stream_timeout={:?}",
        QUICK_TOTAL_TARGET_SECS,
        FIRETEST_ERR_EMU_PERCENT,
        cfg.connect_timeout,
        cfg.wait
    );

    let mut a = Session::connect("A", &cfg, keys, None);
    assert!(
        a.client.is_authorized(),
        "quick FireTest: connect_and_init returned but client is not AuthDone"
    );
    println!(
        "OK: quick connect/AuthDone/InitDone after {:.2}s",
        start.elapsed().as_secs_f64()
    );

    assert!(
        pump_single_until(&mut a, cfg.wait, "quick streams/API/market health", |a| {
            has_quick_health(a)
        }),
        "quick FireTest did not receive required API methods, market state, trades and orderbook within {:?}: A=[{}]",
        cfg.wait,
        a.snapshot().summary()
    );

    let st = a.snapshot();
    println!(
        "OK: quick methods BaseCheck={} AuthCheck={} GetMarketsList={} GetMarketsIndexes={} UpdateMarketsList={} SubscribeAllTrades={} SubscribeOrderBook={}",
        st.engine_method_count(EngineMethod::BaseCheck),
        st.engine_method_count(EngineMethod::AuthCheck),
        st.engine_method_count(EngineMethod::GetMarketsList),
        st.engine_method_count(EngineMethod::GetMarketsIndexes),
        st.engine_method_count(EngineMethod::UpdateMarketsList),
        st.engine_method_count(EngineMethod::SubscribeAllTrades),
        st.engine_method_count(EngineMethod::SubscribeOrderBook),
    );
    println!(
        "OK: quick market consistency [{}]",
        st.market_probe_summary()
    );
    let last_price = a
        .target_last_price_tail()
        .expect("quick FireTest did not retain LastPrice from UpdateMarketsList");
    assert!(
        last_price.current > 0.0,
        "quick retained LastPrice must be positive, got {:?}",
        last_price
    );
    println!(
        "OK: quick retained LastPrice current={:.8} time={:.8}",
        last_price.current, last_price.real_time
    );
    let (futures_retained, spot_retained) = a.target_retained_trade_counts();
    assert!(
        futures_retained + spot_retained > 0,
        "quick FireTest received target trades but retained no target futures/spot rows"
    );
    println!(
        "OK: quick retained trades futures={} spot={}",
        futures_retained, spot_retained
    );
    let market = a.snapshot().market;
    let derived = a
        .history_worker
        .derived_snapshot(&market, delphi_now_raw_for_test())
        .expect("quick FireTest retained target market must expose derived snapshot");
    if futures_retained > 0 {
        assert!(
            derived.trade_volumes.five_minutes.total_value() > 0.0,
            "quick FireTest retained futures trades but derived trade volume stayed zero: {:?}",
            derived.trade_volumes
        );
    }
    println!(
        "OK: quick derived trade_vol_1m={:.4} trade_vol_5m={:.4} trade_delta_1m={:.4}% trade_delta_5m={:.4}% candle_vol_1h={:.4}",
        derived.trade_volumes.one_minute.total_value(),
        derived.trade_volumes.five_minutes.total_value(),
        derived.trade_deltas.one_minute,
        derived.trade_deltas.five_minutes,
        derived.candle_volumes.one_hour
    );
    log_err_emu_snapshot(
        "quick 10% gate",
        "A",
        &a.client.err_emu_diagnostics_snapshot(),
    );
    println!("FIRETEST CPU quick A: {}", a.protocol_summary());
    write_strategy_info_dump(FireProfile::Quick, &cfg, &[("A", &a)]);

    assert_eq!(st.parse_failed, 0, "quick FireTest saw ParseFailed");
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

fn next_attempt_timeout(start: Instant, total: Duration) -> Duration {
    let elapsed = start.elapsed();
    assert!(
        elapsed < total,
        "high-loss operation exceeded total timeout {:?}",
        total
    );
    total.saturating_sub(elapsed).min(Duration::from_secs(20))
}

fn request_settings_until(session: &mut Session, timeout: Duration) -> ClientSettingsCommand {
    let start = Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let attempt_timeout = next_attempt_timeout(start, timeout);
        match session
            .client
            .request_client_settings(&mut session.dispatcher, attempt_timeout)
        {
            Ok(settings) => {
                session.drain_queued();
                session.remember_settings_snapshot(&settings);
                println!(
                    "OK: settings snapshot uid={} attempts={} after {:.2}s",
                    settings.uid,
                    attempts,
                    start.elapsed().as_secs_f64()
                );
                return settings;
            }
            Err(err) if start.elapsed() < timeout => {
                session.drain_queued();
                println!("FIRETEST settings retry after {err:?}");
            }
            Err(err) => {
                panic!("settings request failed after {attempts} attempts: {err:?}")
            }
        }
    }
}

fn request_balance_until(session: &mut Session, timeout: Duration) {
    let start = Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let attempt_timeout = next_attempt_timeout(start, timeout);
        match session
            .client
            .request_balance_snapshot(&mut session.dispatcher, attempt_timeout)
        {
            Ok(balances) => {
                session.drain_queued();
                println!(
                    "OK: high-loss balance snapshot rows={} attempts={} after {:.2}s",
                    balances.by_market.len(),
                    attempts,
                    start.elapsed().as_secs_f64()
                );
                return;
            }
            Err(err) if start.elapsed() < timeout => {
                session.drain_queued();
                println!("FIRETEST high-loss balance retry after {err:?}");
            }
            Err(err) => {
                panic!("high-loss balance request failed after {attempts} attempts: {err:?}")
            }
        }
    }
}

fn request_orders_until(session: &mut Session, timeout: Duration) {
    let start = Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let attempt_timeout = next_attempt_timeout(start, timeout);
        match session
            .client
            .request_order_snapshot(&mut session.dispatcher, attempt_timeout)
        {
            Ok(orders) => {
                session.drain_queued();
                println!(
                    "OK: high-loss order snapshot rows={} attempts={} after {:.2}s",
                    orders.len(),
                    attempts,
                    start.elapsed().as_secs_f64()
                );
                return;
            }
            Err(err) if start.elapsed() < timeout => {
                session.drain_queued();
                println!("FIRETEST high-loss orders retry after {err:?}");
            }
            Err(err) => panic!("high-loss order request failed after {attempts} attempts: {err:?}"),
        }
    }
}

fn request_engine_until(
    session: &mut Session,
    method: EngineMethod,
    request: Vec<u8>,
    timeout: Duration,
) {
    let start = Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let attempt_timeout = next_attempt_timeout(start, timeout);
        match session.client.request_engine_response(
            &mut session.dispatcher,
            &request,
            attempt_timeout,
        ) {
            Ok(resp) if resp.success && resp.method == method => {
                session.drain_queued();
                println!(
                    "OK: high-loss Engine API {:?} bytes={} attempts={} after {:.2}s",
                    method,
                    resp.data.len(),
                    attempts,
                    start.elapsed().as_secs_f64()
                );
                return;
            }
            Ok(resp) => {
                session.drain_queued();
                panic!(
                    "high-loss Engine API {:?} returned method={:?} success={} code={} msg={}",
                    method, resp.method, resp.success, resp.error_code, resp.error_msg
                );
            }
            Err(err) if start.elapsed() < timeout => {
                session.drain_queued();
                println!("FIRETEST high-loss Engine API {method:?} retry after {err:?}");
            }
            Err(err) => panic!(
                "high-loss Engine API {:?} failed after {} attempts: {:?}",
                method, attempts, err
            ),
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
    a.client.reset_err_emu_diagnostics();
    b.client.reset_err_emu_diagnostics();
    err_emu.set_for_gate(
        FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT,
        "50% simple operations/reconnect gate",
    );
    log_high_loss_recovery_math();

    request_engine_until(
        a,
        EngineMethod::CheckAPIExpirationTime,
        engine_request::check_api_expiration_time(),
        timeout,
    );
    request_engine_until(
        a,
        EngineMethod::QueryHedgeMode,
        engine_request::query_hedge_mode(),
        timeout,
    );
    let _settings = request_settings_until(a, timeout);
    request_balance_until(a, timeout);
    request_orders_until(a, timeout);

    let before_streams = a.snapshot();
    assert!(
        pump_pair_until(a, b, timeout, "high-loss live streams", |a, _| {
            a.trades_apply > before_streams.trades_apply
                && a.orderbook_apply > before_streams.orderbook_apply
                && a.parse_failed == before_streams.parse_failed
        }),
        "client A did not receive trades and orderbook under err_emu={} within {:?}",
        FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT,
        timeout
    );
    println!("OK: high-loss live streams delivered");

    let before_blackhole = a.snapshot();
    a.client.debug_set_outgoing_blackhole(true);
    let disconnected = pump_pair_until(
        a,
        b,
        timeout,
        "high-loss forced reconnect detection",
        |a, _| a.reconnecting > before_blackhole.reconnecting,
    );
    a.client.debug_set_outgoing_blackhole(false);
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
        reconnected && a.client.is_authorized(),
        "client A did not reconnect under err_emu={} within {:?}",
        FIRETEST_HIGH_LOSS_ERR_EMU_PERCENT,
        timeout
    );
    println!("OK: high-loss reconnect completed");

    let after_reconnect = a.snapshot();
    assert!(
        pump_pair_until(
            a,
            b,
            timeout,
            "high-loss streams after reconnect",
            |a, _| {
                a.trades_apply > after_reconnect.trades_apply
                    && a.orderbook_apply > after_reconnect.orderbook_apply
                    && a.parse_failed == after_reconnect.parse_failed
            }
        ),
        "client A did not receive trades/orderbook after high-loss reconnect within {:?}",
        timeout
    );
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
    a.client.ui_send_settings(&enabled);
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
    a.client.ui_send_settings(&original);
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

fn run_order_lifecycle_gate_body(cfg: &FireConfig, a: &mut Session, b: &mut Session) {
    let before_uids = a
        .dispatcher
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

    assert!(
        a.replace_order(server_uid, fill_price),
        "replace_order did not pass Delphi local gate for uid={server_uid}"
    );
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
        "order uid={} did not reach SellSet after replace",
        server_uid
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
                a.order_status_by_uid.get(&server_uid).copied() == Some(OrderWorkerStatus::SelLDone)
            }
        ),
        "order uid={} did not reach SellDone after PanicSell",
        server_uid
    );

    if let Some(order) = a.dispatcher.orders().get(server_uid) {
        let delphi_delta_base =
            delphi_sell_report_delta_base(&order.buy_order, &order.sell_order, false);
        let approx_result_usd =
            if is_usd_like_base(a.client.server_info().base_currency_name.as_deref()) {
                delphi_delta_base
            } else {
                None
            };
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
    if sell.is_short != 0 {
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

fn is_usd_like_base(base_currency_name: Option<&str>) -> bool {
    matches!(
        base_currency_name,
        Some("USD" | "USDT" | "USDC" | "FDUSD" | "TUSD" | "BUSD")
    )
}

#[test]
#[ignore = "live MoonBot server required; create ../moonproto.firetest.conf"]
fn fire_test_active_library_health() {
    let cfg = FireConfig::load_required();
    let profile = FireProfile::from_env();
    let keys = import_key(&cfg.key_b64).expect("invalid MoonProto key in FireTest config");

    println!(
        "FIRETEST config: profile={} path={} server={}:{} market={} strategy_field={} strategy_id={:?} err_emu={} high_loss_err_emu={} connect_timeout={:?} candles_timeout={:?} high_loss_timeout={:?}",
        profile.as_str(),
        cfg.path.display(),
        cfg.host,
        cfg.port,
        cfg.market,
        cfg.strategy_field,
        cfg.strategy_id,
        FIRETEST_ERR_EMU_PERCENT,
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
    let mut err_emu = ErrEmuGuard::set(FIRETEST_ERR_EMU_PERCENT);
    let seeded_strategy = firetest_strategy(&cfg);
    let seeded_strategy_id = seeded_strategy.strategy_id;

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
    a.client.reset_err_emu_diagnostics();
    b.client.reset_err_emu_diagnostics();

    run_order_lifecycle_gate(&cfg, &mut a, &mut b);

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
    a.client.ui_send_settings(&mutated_settings);

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
    a.client.ui_send_settings(&original_settings);
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
    a.client.debug_set_outgoing_blackhole(true);
    let disconnect_start = Instant::now();
    let disconnected = pump_pair_until(
        &mut a,
        &mut b,
        cfg.disconnect_timeout,
        "server-side disconnect after outgoing blackhole",
        |a, _| a.reconnecting > before_blackhole.reconnecting,
    );
    let disconnected_after = disconnect_start.elapsed();
    a.client.debug_set_outgoing_blackhole(false);
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
        reconnected && a.client.is_authorized(),
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
    log_protocol_cpu_pair("final", &a, &b);
    write_strategy_info_dump(FireProfile::Full, &cfg, &[("A", &a), ("B", &b)]);
    println!("FIRETEST_PASS");
}
