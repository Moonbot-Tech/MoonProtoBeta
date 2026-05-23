//! FireTest: live health test for the active MoonProto library.
//!
//! This test is intentionally ignored by default. It talks to a real MoonBot
//! server, enables client-side `err_emu=10%` before connecting, verifies the
//! full chunked candles snapshot, mutates settings/strategy state, verifies
//! cross-client broadcast, then forces a real reconnect and checks that
//! trades/orderbook streams continue.
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
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use moonproto::client::set_err_emu;
use moonproto::commands::arb::ArbPayload;
use moonproto::commands::candles::{parse_request_candles_data_response, CandlesAggregator};
use moonproto::commands::engine_api::{EngineMethod, EngineResponse};
use moonproto::commands::strategy_serializer::{
    parse_strategy_batch, FieldValue, StrategySnapshot,
};
use moonproto::commands::trades_stream::TradeSection;
use moonproto::commands::ui::ClientSettingsCommand;
use moonproto::events::Event;
use moonproto::state::{OrderBookEvent, SettingsEvent, StratEvent, TradesEvent};
use moonproto::{
    connect_and_init, import_key, Client, ClientConfig, ConnectConfig, EventDispatcher,
    ImportedKeys, InitConfig, LifecycleEvent,
};

const FIRETEST_ERR_EMU_PERCENT: u8 = 10;
const FIRETEST_STRATEGY_ID: u64 = 0xF17E_5737_0000_0001;
const DEFAULT_WAIT_SECS: u64 = 5;
const DEFAULT_CANDLES_TIMEOUT_SECS: u64 = 90;
const DEFAULT_DISCONNECT_TIMEOUT_SECS: u64 = 45;
const DEFAULT_RECONNECT_TIMEOUT_SECS: u64 = 30;
const PUMP_SLICE: Duration = Duration::from_millis(50);

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
    candles_timeout: Duration,
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
        let candles_timeout = Duration::from_secs(parse_u64(
            values.get("candles_timeout_secs").map(String::as_str),
            DEFAULT_CANDLES_TIMEOUT_SECS,
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
            candles_timeout,
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

#[derive(Default)]
struct SessionStats {
    label: String,
    connected_now: bool,
    server_events: u64,
    connected_fresh: u64,
    connected_again: u64,
    reconnecting: u64,
    disconnected: u64,
    engine_responses: u64,
    raw_events: u64,
    server_logs: u64,
    settings_events: u64,
    strategy_events: u64,
    trades_apply: u64,
    orderbook_apply: u64,
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
            connected_now: self.connected_now,
            server_events: self.server_events,
            connected_fresh: self.connected_fresh,
            connected_again: self.connected_again,
            reconnecting: self.reconnecting,
            disconnected: self.disconnected,
            engine_responses: self.engine_responses,
            raw_events: self.raw_events,
            server_logs: self.server_logs,
            settings_events: self.settings_events,
            strategy_events: self.strategy_events,
            trades_apply: self.trades_apply,
            orderbook_apply: self.orderbook_apply,
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
            "connected_now={} fresh={} again={} reconnecting={} disconnected={} server_events={} engine={} raw={} logs={} settings={} strats={} strategy_rows={} trades={} books={} parse_failed={} candles={}",
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
            self.strategies_by_id.len(),
            self.trades_apply,
            self.orderbook_apply,
            self.parse_failed,
            candles
        )
    }
}

struct Session {
    client: Client,
    dispatcher: EventDispatcher,
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
            ..Default::default()
        }));
        let mut client = Client::new(
            ClientConfig::new(&cfg.host, cfg.port, keys.master_key, keys.mac_key)
                .with_client_id(rand::random()),
        );

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

        let mut dispatcher = EventDispatcher::new();
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
            ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(15)),
        )
        .unwrap_or_else(|err| panic!("FIRETEST {label}: connect_and_init failed: {err}"));

        let mut session = Self {
            client,
            dispatcher,
            stats,
        };
        session.drain_queued();
        session
    }

    fn pump(&mut self, duration: Duration) {
        let stats = Arc::clone(&self.stats);
        self.client.run_with_dispatcher_state(
            duration,
            &mut self.dispatcher,
            Box::new(move |event, dispatcher| record_event(&stats, event, dispatcher)),
        );
        self.drain_queued();
    }

    fn drain_queued(&mut self) {
        let events = self.dispatcher.take_queued_events();
        for event in events {
            record_event(&self.stats, &event, &self.dispatcher);
        }
    }

    fn snapshot(&self) -> SessionStats {
        self.stats.lock().unwrap().clone()
    }

    fn strategy_snapshot(&self, strategy_id: u64) -> Option<StrategySnapshot> {
        self.dispatcher.strategy_snapshot(strategy_id).cloned()
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
}

fn record_event(stats: &Arc<Mutex<SessionStats>>, event: &Event, dispatcher: &EventDispatcher) {
    let mut st = stats.lock().unwrap();
    st.server_events += 1;
    let event_no = st.server_events;
    match event {
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
        Event::Strat(other) => {
            log_server_event(&st, event_no, format!("Strat {other:?}"));
        }
        Event::Trade(TradesEvent::Apply(pkt)) => {
            st.trades_apply += 1;
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
            record_engine_response(&mut st, event_no, resp);
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
        Event::ParseFailed { cmd, len } => {
            st.parse_failed += 1;
            log_server_event(&st, event_no, format!("ParseFailed cmd={cmd:?} len={len}"));
        }
        other => {
            log_server_event(&st, event_no, format!("{other:?}"));
        }
    }
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

fn record_engine_response(st: &mut SessionStats, event_no: u64, resp: &EngineResponse) {
    st.engine_responses += 1;
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
        "FIRETEST TIMEOUT {label}: A=[{}] B=[{}]",
        a_stats.summary(),
        b_stats.summary()
    );
    false
}

fn has_initial_health(st: &SessionStats) -> bool {
    st.connected_now
        && st.settings_events > 0
        && st.trades_apply > 0
        && st.orderbook_apply > 0
        && st.parse_failed == 0
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
        .find_map(|(name, value)| matches!(value, FieldValue::String(_)).then(|| name.clone()))
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
    strategy
        .fields
        .insert(field.to_string(), FieldValue::String(value));
    strategy
}

fn firetest_strategy(cfg: &FireConfig) -> StrategySnapshot {
    let strategy_id = cfg.strategy_id.unwrap_or(FIRETEST_STRATEGY_ID);
    let mut fields = HashMap::new();
    fields.insert(
        "StrategyName".to_string(),
        FieldValue::String("MoonProto FireTest".to_string()),
    );
    fields.insert(
        "Comment".to_string(),
        FieldValue::String("firetest-initial".to_string()),
    );
    fields.insert("AcceptCommands".to_string(), FieldValue::Bool(true));
    fields.insert("OrderSize".to_string(), FieldValue::Double(0.0));
    StrategySnapshot {
        strategy_id,
        strategy_ver: 1,
        last_date: now_epoch_ms(),
        checked: false,
        kind: 0,
        path: "FireTest".to_string(),
        fields,
    }
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis() as u64
}

struct ErrEmuGuard;

impl ErrEmuGuard {
    fn set(percent: u8) -> Self {
        set_err_emu(percent);
        println!("FIRETEST: client-side err_emu={percent}% enabled before connect");
        Self
    }
}

impl Drop for ErrEmuGuard {
    fn drop(&mut self) {
        set_err_emu(0);
        println!("FIRETEST: client-side err_emu reset to 0%");
    }
}

#[test]
#[ignore = "live MoonBot server required; create ../moonproto.firetest.conf"]
fn fire_test_active_library_health() {
    let cfg = FireConfig::load_required();
    assert!(
        cfg.allow_mutation,
        "FireTest mutates live server settings/strategies. Set allow_mutation=true in {} only for a test server.",
        cfg.path.display()
    );
    let _err_emu = ErrEmuGuard::set(FIRETEST_ERR_EMU_PERCENT);

    println!(
        "FIRETEST config: path={} server={}:{} market={} strategy_field={} strategy_id={:?} err_emu={} candles_timeout={:?}",
        cfg.path.display(),
        cfg.host,
        cfg.port,
        cfg.market,
        cfg.strategy_field,
        cfg.strategy_id,
        FIRETEST_ERR_EMU_PERCENT,
        cfg.candles_timeout
    );
    let keys = import_key(&cfg.key_b64).expect("invalid MoonProto key in FireTest config");
    let seeded_strategy = firetest_strategy(&cfg);
    let seeded_strategy_id = seeded_strategy.strategy_id;

    let mut a = Session::connect("A", &cfg, keys, Some(seeded_strategy.clone()));
    let mut b = Session::connect("B", &cfg, keys, Some(seeded_strategy.clone()));
    assert!(
        a.strategy_snapshot(seeded_strategy_id).is_some()
            && b.strategy_snapshot(seeded_strategy_id).is_some(),
        "FireTest local strategies must be available through EventDispatcher API before stream checks"
    );

    assert!(
        pump_pair_until(&mut a, &mut b, cfg.wait, "initial health", |a, b| {
            has_initial_health(a) && has_initial_health(b)
        }),
        "FireTest initial health failed: both clients must receive settings, trades, and configured orderbook within {:?}",
        cfg.wait
    );

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

    let a_initial = a.snapshot();
    let original_settings = a_initial
        .last_settings
        .clone()
        .expect("settings were counted but not stored");
    let original_strategy = a
        .strategy_snapshot(seeded_strategy_id)
        .expect("seeded strategy missing from dispatcher state");
    let field = select_field(&original_strategy, &cfg.strategy_field);
    let original_field_value = match original_strategy.fields.get(&field) {
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
    a.client
        .strat_send_snapshot_batch(0, false, std::slice::from_ref(&mutated_strategy));
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
    a.client
        .strat_send_snapshot_batch(0, false, std::slice::from_ref(&restored_strategy));
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
    println!("FIRETEST_PASS");
}
