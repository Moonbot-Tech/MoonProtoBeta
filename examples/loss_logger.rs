/// Loss/Recovery Logger — детальное логирование всех событий gap detection / recovery /
/// потерь / connect lifecycle для тестирования под `MoonProtoErrEmu` на сервере.
///
/// Использование (запуск в фоне через cargo run):
///   cargo run --example loss_logger --release -- <key_b64> [ip:port] [logfile.txt]
///
/// Лог: каждое событие с timestamp в `logfile.txt` (по умолчанию `loss_logger.log`).
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use moonproto::client::{Client, ClientConfig, LifecycleEvent};
use moonproto::events::Event;
use moonproto::key_import;
use moonproto::state::order_books::ApplyResult as OBApplyResult;
use moonproto::state::{BalanceEvent, MarketsEvent, OrderBookEvent, SettingsEvent, TradesEvent};
use moonproto::{run_init_sequence, EventDispatcher, InitConfig};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn ts_str(t: i64) -> String {
    // hh:mm:ss.mmm relative to first call
    let secs = t / 1000;
    let ms = (t % 1000) as u32;
    let h = ((secs / 3600) % 24) as u32;
    let m = ((secs / 60) % 60) as u32;
    let s = (secs % 60) as u32;
    format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
}

struct Counters {
    trades_pkts: AtomicU64,
    trades_apply: AtomicU64,
    trades_dup: AtomicU64,
    trades_ooo: AtomicU64,
    gap_detected: AtomicU64,
    gap_filled: AtomicU64,
    bucket_recovered: AtomicU64,
    bucket_lost: AtomicU64,
    ob_pkts: AtomicU64,
    ob_full: AtomicU64,
    ob_diff: AtomicU64,
    ob_stale: AtomicU64,
    ob_cached: AtomicU64,
    ob_no_full_yet: AtomicU64,
    parse_failed: AtomicU64,
    server_logs: AtomicU64,
}

impl Counters {
    fn new() -> Self {
        Self {
            trades_pkts: AtomicU64::new(0),
            trades_apply: AtomicU64::new(0),
            trades_dup: AtomicU64::new(0),
            trades_ooo: AtomicU64::new(0),
            gap_detected: AtomicU64::new(0),
            gap_filled: AtomicU64::new(0),
            bucket_recovered: AtomicU64::new(0),
            bucket_lost: AtomicU64::new(0),
            ob_pkts: AtomicU64::new(0),
            ob_full: AtomicU64::new(0),
            ob_diff: AtomicU64::new(0),
            ob_stale: AtomicU64::new(0),
            ob_cached: AtomicU64::new(0),
            ob_no_full_yet: AtomicU64::new(0),
            parse_failed: AtomicU64::new(0),
            server_logs: AtomicU64::new(0),
        }
    }
}

fn log_line(buf: &mut BufWriter<File>, t0: i64, msg: &str) {
    let _ = writeln!(buf, "[{}] {}", ts_str(now_ms() - t0), msg);
}

fn flush_log(buf: &mut BufWriter<File>) {
    let _ = buf.flush();
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: loss_logger <key_b64> [ip:port] [logfile] [err_emu_pct]");
        eprintln!("  Default: 207.148.91.186:3000, log → loss_logger.log, err_emu = 0");
        std::process::exit(1);
    }

    let key_b64 = &args[1];
    let (ip, port) = if args.len() >= 3 {
        let parts: Vec<&str> = args[2].splitn(2, ':').collect();
        (
            parts[0].to_string(),
            parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(3000u16),
        )
    } else {
        ("207.148.91.186".to_string(), 3000u16)
    };
    let logfile = args
        .get(3)
        .cloned()
        .unwrap_or_else(|| "loss_logger.log".to_string());
    let err_emu_pct: u8 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0u8);

    if err_emu_pct > 0 {
        moonproto::client::set_err_emu(err_emu_pct);
    }

    let file = File::create(&logfile).expect("Failed to open logfile");
    let mut log = BufWriter::new(file);
    let t0 = now_ms();

    log_line(
        &mut log,
        t0,
        &format!("=== Loss Logger started → {}:{} ===", ip, port),
    );
    log_line(&mut log, t0, &format!("Log file: {}", logfile));
    if err_emu_pct > 0 {
        log_line(
            &mut log,
            t0,
            &format!(
                "[ERR-EMU] Client-side packet drop ACTIVE: {}% (service cmds: {}%)",
                err_emu_pct,
                err_emu_pct / 2
            ),
        );
    } else {
        log_line(&mut log, t0, "[ERR-EMU] disabled");
    }

    // NTP sync
    let ntp = moonproto::ntp::get_best_ntp("pool.ntp.org", 4);
    if ntp.synced {
        moonproto::client::set_ntp_offset(ntp.time_offset);
        log_line(
            &mut log,
            t0,
            &format!(
                "[NTP] synced offset={:.1}ms rtt={}ms",
                ntp.time_offset * 1000.0,
                ntp.round_trip_ms
            ),
        );
    } else {
        log_line(&mut log, t0, "[NTP] sync failed, using system clock");
    }

    let keys = key_import::import_key(key_b64).expect("Failed to import key");
    log_line(&mut log, t0, "[KEY] imported");

    // loss_logger — короткий стресс-тест: без NTP, без periodic refresh.
    let cfg = ClientConfig::new(ip, port, keys.master_key, keys.mac_key)
        .without_ntp()
        .with_refresh(moonproto::client::RefreshConfig {
            update_markets_every: None,
            check_tags_every: None,
        });

    let mut client = Client::new(cfg);
    let mut dispatcher = EventDispatcher::new();
    let counters = Arc::new(Counters::new());

    // Lifecycle callback: write to log via a separate channel-less hook.
    // We use an Arc<Mutex<Vec<String>>> queue read by the main callback.
    let lifecycle_queue: Arc<std::sync::Mutex<Vec<LifecycleEvent>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        let lq = lifecycle_queue.clone();
        client.on_lifecycle(Box::new(move |ev| {
            if let Ok(mut q) = lq.lock() {
                q.push(ev);
            }
        }));
    }

    // Phase 1: connect + handshake
    log_line(&mut log, t0, "[PHASE-1] connecting + handshake (10s)...");
    flush_log(&mut log);

    client.run_with_dispatcher(Duration::from_secs(10), &mut dispatcher, Box::new(|_| {}));

    if !client.is_authorized() {
        log_line(&mut log, t0, "[FAIL] not authorized after 10s, exiting");
        flush_log(&mut log);
        return;
    }
    log_line(
        &mut log,
        t0,
        &format!("[AUTH] OK, ping_count={}", client.ping_count()),
    );

    // Drain lifecycle events accumulated during phase 1
    if let Ok(mut q) = lifecycle_queue.lock() {
        for ev in q.drain(..) {
            log_line(&mut log, t0, &format!("[LIFECYCLE] {:?}", ev));
        }
    }

    log_line(
        &mut log,
        t0,
        "[INIT] BaseCheck/AuthCheck/GetMarketsList + trades subscription",
    );
    let init = InitConfig {
        base_check: true,
        auth_check: true,
        fetch_markets: true,
        subscribe_trades: Some(false),
        ..Default::default()
    };
    match run_init_sequence(&mut client, &mut dispatcher, init) {
        Ok(result) => {
            log_line(
                &mut log,
                t0,
                &format!(
                    "[INIT] ok base={} auth={} markets_bytes={} trades_subscribed={} errors={:?}",
                    result.base_check_ok,
                    result.auth_check_ok,
                    result.markets_response_bytes,
                    result.trades_subscribed,
                    result.errors
                ),
            );
        }
        Err(err) => {
            log_line(&mut log, t0, &format!("[INIT-ERR] {err}"));
        }
    }
    // Empty market list is the protocol-level "all orderbooks" request. The normal
    // typed subscription API is per-market and registry-aware; this logger is a
    // stress tool, so it intentionally uses the raw Engine API wrapper here.
    let _ = client.api_subscribe_order_book(&[]);
    flush_log(&mut log);

    // Phase 2: long-running logger
    log_line(
        &mut log,
        t0,
        "[PHASE-2] logging started (running until killed)...",
    );
    flush_log(&mut log);

    let last_stats_ms = Arc::new(AtomicI64::new(0));
    let last_tick_ms = Arc::new(AtomicI64::new(0));
    let lq_cb = lifecycle_queue.clone();
    let counters_cb = counters.clone();
    let last_stats_cb = last_stats_ms.clone();
    let last_tick_cb = last_tick_ms.clone();
    let logfile_for_cb = logfile.clone();
    let log_path_cb = logfile.clone();

    // We can't share BufWriter directly across closures, so re-open file in append mode
    // for the long-running callback (single thread anyway, main loop is single).
    let mut log2 = BufWriter::new(
        std::fs::OpenOptions::new()
            .append(true)
            .open(&logfile)
            .expect("Failed to open logfile for append"),
    );
    let _ = (logfile_for_cb, log_path_cb);

    // Run for 24 hours max safety; user kills with SIGINT/ctrl-C
    client.run_with_dispatcher(
        Duration::from_secs(24 * 3600),
        &mut dispatcher,
        Box::new(move |event| {
            let now = now_ms();

            // Drain lifecycle events from callback queue
            if let Ok(mut q) = lq_cb.lock() {
                for ev in q.drain(..) {
                    log_line(&mut log2, t0, &format!("[LIFECYCLE] {:?}", ev));
                }
            }

            let mut events_to_log: Vec<String> = Vec::new();
            match event {
                // audit_rust_quality #11: Event::Trades(Vec) → Event::Trade(single).
                // Каждое TradesEvent теперь приходит отдельным events. trades_pkts
                // counter переименован семантически — теперь это сумма всех TradesEvent
                // (включая Duplicate/GapDetected), а не количество пакетов с сервера.
                Event::Trade(tev) => {
                    counters_cb.trades_pkts.fetch_add(1, Ordering::Relaxed);
                    {
                        match tev {
                            TradesEvent::Apply(pkt) => {
                                counters_cb.trades_apply.fetch_add(1, Ordering::Relaxed);
                                let _ = pkt;
                            }
                            TradesEvent::GapDetected { start, end } => {
                                counters_cb.gap_detected.fetch_add(1, Ordering::Relaxed);
                                let n = end.wrapping_sub(*start).wrapping_add(1);
                                events_to_log.push(format!(
                                    "[GAP-DETECTED] [{}..{}] missing {} packets",
                                    start, end, n
                                ));
                            }
                            TradesEvent::GapFilled {
                                packet_num,
                                bucket_seq_range,
                            } => {
                                counters_cb.gap_filled.fetch_add(1, Ordering::Relaxed);
                                events_to_log.push(format!(
                                    "[GAP-FILLED] pkt={} (in bucket [{}..{}])",
                                    packet_num, bucket_seq_range.0, bucket_seq_range.1
                                ));
                            }
                            TradesEvent::BucketClosed {
                                start,
                                end,
                                all_received,
                                retry_count,
                            } => {
                                if *all_received {
                                    counters_cb.bucket_recovered.fetch_add(1, Ordering::Relaxed);
                                    events_to_log.push(format!(
                                        "[BUCKET-RECOVERED] [{}..{}] retry={} ALL recovered",
                                        start, end, retry_count
                                    ));
                                } else {
                                    counters_cb.bucket_lost.fetch_add(1, Ordering::Relaxed);
                                    events_to_log.push(format!(
                                        "[BUCKET-LOST] [{}..{}] retry={}/3 PACKETS LOST",
                                        start, end, retry_count
                                    ));
                                }
                            }
                            TradesEvent::Duplicate => {
                                counters_cb.trades_dup.fetch_add(1, Ordering::Relaxed);
                            }
                            TradesEvent::OutOfOrder { packet_num } => {
                                counters_cb.trades_ooo.fetch_add(1, Ordering::Relaxed);
                                events_to_log.push(format!(
                                    "[TRADES-OOO] pkt={} (stale/no bucket)",
                                    packet_num
                                ));
                            }
                        }
                    }
                }
                Event::OrderBook(obev) => {
                    counters_cb.ob_pkts.fetch_add(1, Ordering::Relaxed);
                    match obev {
                        OrderBookEvent::Apply {
                            market_index,
                            book_kind,
                            is_full,
                            seq,
                            ..
                        } => {
                            if *is_full {
                                counters_cb.ob_full.fetch_add(1, Ordering::Relaxed);
                                events_to_log.push(format!(
                                    "[OB-FULL] mkt={} bk={} seq={}",
                                    market_index, book_kind, seq
                                ));
                            } else {
                                counters_cb.ob_diff.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        OrderBookEvent::Ignored {
                            market_index,
                            book_kind,
                            seq,
                            reason,
                        } => {
                            match reason {
                                OBApplyResult::Stale => {
                                    counters_cb.ob_stale.fetch_add(1, Ordering::Relaxed);
                                    // Не логируем каждый stale — может быть шум
                                }
                                OBApplyResult::Cached => {
                                    counters_cb.ob_cached.fetch_add(1, Ordering::Relaxed);
                                    events_to_log.push(format!(
                                        "[OB-CACHED] mkt={} bk={} seq={} (gap, awaiting)",
                                        market_index, book_kind, seq
                                    ));
                                }
                                OBApplyResult::NoFullYet => {
                                    counters_cb.ob_no_full_yet.fetch_add(1, Ordering::Relaxed);
                                    events_to_log.push(format!(
                                        "[OB-NO-FULL] mkt={} bk={} seq={} (diff before first full)",
                                        market_index, book_kind, seq
                                    ));
                                }
                                _ => {}
                            }
                        }
                        OrderBookEvent::RequestFullNeeded { .. } => {}
                    }
                }
                Event::Balance(bev) => match bev {
                    BalanceEvent::SnapshotApplied { count, epoch } => {
                        events_to_log
                            .push(format!("[BAL-SNAPSHOT] {} items, epoch={}", count, epoch));
                    }
                    BalanceEvent::IncrementalApplied {
                        count,
                        epoch,
                        global_changed,
                    } if (*count > 0 || *global_changed) => {
                        events_to_log.push(format!(
                            "[BAL-INC] {} items, epoch={}, global_changed={}",
                            count, epoch, global_changed
                        ));
                    }
                    BalanceEvent::EpochStale { incoming, last } => {
                        events_to_log.push(format!(
                            "[BAL-STALE] incoming={} last={} (REJECTED)",
                            incoming, last
                        ));
                    }
                    _ => {}
                },
                Event::Strat(sev) => {
                    events_to_log.push(format!("[STRAT] {:?}", sev));
                }
                Event::Settings(sev) => match sev {
                    SettingsEvent::ClientSettingsUpdated => {
                        events_to_log.push("[SETTINGS] ClientSettings updated".to_string());
                    }
                    other => events_to_log.push(format!("[SETTINGS] {:?}", other)),
                },
                Event::Markets(mev) => match mev {
                    MarketsEvent::MarketsListReplaced { count, corr_count } => {
                        events_to_log.push(format!(
                            "[MARKETS] list applied: {} + {} corr",
                            count, corr_count
                        ));
                    }
                    MarketsEvent::IndexesUpdated { count } => {
                        events_to_log.push(format!("[MARKETS] indexes applied: {}", count));
                    }
                    _ => {}
                },
                Event::EngineResponse(resp) if !resp.success => {
                    events_to_log.push(format!(
                        "[API-ERR] uid={} method={:?} code={} msg={}",
                        resp.request_uid, resp.method, resp.error_code, resp.error_msg
                    ));
                }
                Event::ServerLog { msg, .. } => {
                    counters_cb.server_logs.fetch_add(1, Ordering::Relaxed);
                    let trimmed = msg.trim();
                    // Heuristic — log only "interesting" server messages
                    if trimmed.len() < 200 && !trimmed.is_empty() {
                        events_to_log.push(format!("[SRV] {}", trimmed));
                    }
                }
                Event::ParseFailed { cmd, len } => {
                    counters_cb.parse_failed.fetch_add(1, Ordering::Relaxed);
                    events_to_log.push(format!("[PARSE-FAIL] cmd={:?} len={}", cmd, len));
                }
                _ => {}
            }

            // Write all collected log lines
            for line in &events_to_log {
                log_line(&mut log2, t0, line);
            }

            // Periodic stats every 10 seconds
            let last_stats = last_stats_cb.load(Ordering::Relaxed);
            if (now - last_stats).abs() > 10_000 {
                last_stats_cb.store(now, Ordering::Relaxed);
                log_line(
                    &mut log2,
                    t0,
                    &format!(
                        "[STATS] trades_apply={} gap_det={} gap_fill={} bkt_rec={} bkt_lost={} | \
                         ob_full={} ob_diff={} ob_cached={} | \
                         parse_fail={}",
                        counters_cb.trades_apply.load(Ordering::Relaxed),
                        counters_cb.gap_detected.load(Ordering::Relaxed),
                        counters_cb.gap_filled.load(Ordering::Relaxed),
                        counters_cb.bucket_recovered.load(Ordering::Relaxed),
                        counters_cb.bucket_lost.load(Ordering::Relaxed),
                        counters_cb.ob_full.load(Ordering::Relaxed),
                        counters_cb.ob_diff.load(Ordering::Relaxed),
                        counters_cb.ob_cached.load(Ordering::Relaxed),
                        counters_cb.parse_failed.load(Ordering::Relaxed),
                    ),
                );
                flush_log(&mut log2);
            }

            // Throttle: flush at most every 200ms when there were events
            let last_tick = last_tick_cb.load(Ordering::Relaxed);
            if !events_to_log.is_empty() && (now - last_tick).abs() > 200 {
                last_tick_cb.store(now, Ordering::Relaxed);
                flush_log(&mut log2);
            }
        }),
    );

    // After client.run returns
    client.disconnect();
    log_line(&mut log, t0, "[DONE] client.run returned, disconnecting");
    log_line(
        &mut log,
        t0,
        &format!(
            "[DONE-STATS] auth={:?} ping={} sent={}B recv={}B",
            client.auth_status(),
            client.ping_count(),
            client.total_sent(),
            client.total_recv()
        ),
    );

    // Final counter dump
    log_line(
        &mut log,
        t0,
        &format!(
            "[FINAL] trades_apply={} gap_det={} gap_fill={} bkt_rec={} bkt_lost={} | \
             ob_full={} ob_diff={} ob_cached={} ob_no_full_yet={} | \
             parse_fail={} srv_logs={}",
            counters.trades_apply.load(Ordering::Relaxed),
            counters.gap_detected.load(Ordering::Relaxed),
            counters.gap_filled.load(Ordering::Relaxed),
            counters.bucket_recovered.load(Ordering::Relaxed),
            counters.bucket_lost.load(Ordering::Relaxed),
            counters.ob_full.load(Ordering::Relaxed),
            counters.ob_diff.load(Ordering::Relaxed),
            counters.ob_cached.load(Ordering::Relaxed),
            counters.ob_no_full_yet.load(Ordering::Relaxed),
            counters.parse_failed.load(Ordering::Relaxed),
            counters.server_logs.load(Ordering::Relaxed),
        ),
    );
    flush_log(&mut log);
}
