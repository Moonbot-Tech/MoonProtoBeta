use crate::commands::candles::{CandlesAggregator, RequestCandlesMarket};
use crate::commands::engine_api::EngineMethod;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::thread;
// =============================================================================
//  Full candles snapshot collector
// =============================================================================

/// Parsed result returned by the internal full-candles snapshot collector.
///
/// The server answers `RequestCandlesData` with several `EngineResponse` chunks
/// sharing one `request_uid`. The library aggregates those chunks through
/// [`CandlesAggregator`] and hands parsed market entries to Active Lib.
#[derive(Debug, Clone)]
pub(crate) struct MergedCandles {
    /// Request UID used by diagnostics to correlate the chunked response.
    #[cfg(any(test, feature = "diagnostics"))]
    pub uid: u64,
    /// Parsed market entries from the zipped stream.
    pub markets: Vec<RequestCandlesMarket>,
}

/// Internal state for a partially assembled full-candles snapshot.
pub(crate) struct PartialCandles {
    pub(crate) aggregator: CandlesAggregator,
    /// Completion sender used by the persistent candles parse worker after the
    /// aggregator returns the merged zipped stream.
    pub(crate) sender: mpsc::Sender<MergedCandles>,
}

pub(crate) struct CandlesParseQueue {
    tx: Option<mpsc::Sender<CandlesParseJob>>,
}

struct CandlesParseJob {
    uid: u64,
    zipped_data: Vec<u8>,
    sender: mpsc::Sender<MergedCandles>,
}

impl CandlesParseQueue {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel::<CandlesParseJob>();
        match thread::Builder::new()
            .name("moonproto-candles-parse".to_string())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                        parse_and_send_candles(job.uid, job.zipped_data, job.sender)
                    })) {
                        log::error!(
                            target: "moonproto::client",
                            "moonproto-candles-parse panicked: {}",
                            panic_payload_message(payload.as_ref())
                        );
                    }
                }
            }) {
            Ok(_) => Self { tx: Some(tx) },
            Err(err) => {
                log::warn!(target: "moonproto::client",
                    "failed to spawn persistent candles parse worker: {err}; completed full-candles snapshots will parse inline");
                Self { tx: None }
            }
        }
    }

    pub(crate) fn submit(
        &self,
        uid: u64,
        zipped_data: Vec<u8>,
        sender: mpsc::Sender<MergedCandles>,
    ) {
        let job = CandlesParseJob {
            uid,
            zipped_data,
            sender,
        };
        if let Some(tx) = &self.tx {
            match tx.send(job) {
                Ok(()) => return,
                Err(err) => {
                    let job = err.0;
                    log::warn!(target: "moonproto::client",
                        "persistent candles parse worker stopped; parsing uid={uid} inline");
                    parse_and_send_candles(job.uid, job.zipped_data, job.sender);
                    return;
                }
            }
        }
        parse_and_send_candles(job.uid, job.zipped_data, job.sender);
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(value) = payload.downcast_ref::<&'static str>() {
        (*value).to_string()
    } else if let Some(value) = payload.downcast_ref::<String>() {
        value.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn parse_and_send_candles(uid: u64, zipped_data: Vec<u8>, sender: mpsc::Sender<MergedCandles>) {
    let markets = crate::commands::candles::parse_request_candles_data_response(&zipped_data)
        .unwrap_or_else(|| {
            log::warn!(target: "moonproto::client",
                "candles aggregator merged but strict parse failed for uid={} ({} bytes); trying Delphi partial apply",
                uid,
                zipped_data.len()
            );
            crate::commands::candles::parse_request_candles_data_response_partial(&zipped_data)
                .unwrap_or_default()
        });
    let _ = sender.send(MergedCandles {
        #[cfg(any(test, feature = "diagnostics"))]
        uid,
        markets,
    });
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EngineResponseMeta {
    pub(crate) request_uid: u64,
    pub(crate) method: EngineMethod,
    pub(crate) success: bool,
}
