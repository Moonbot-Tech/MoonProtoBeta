use crate::protocol::control::{PingMemoryInfo, PingTelemetry};
use std::sync::atomic::{AtomicU16, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use sysinfo::{get_current_pid, ProcessRefreshKind, ProcessesToUpdate, System};

const CPU_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const MEMORY_SAMPLE_EVERY: u8 = 5;

static LOCAL_NODE_TELEMETRY: OnceLock<Arc<LocalNodeTelemetryCache>> = OnceLock::new();

/// Reader-side view of process telemetry sampled by one process-wide worker.
/// `Ping` handling only performs atomic loads; OS/sysinfo calls stay off the
/// protocol hot path.
pub(super) struct LocalNodeTelemetryReader {
    cache: Arc<LocalNodeTelemetryCache>,
}

struct LocalNodeTelemetryCache {
    moment_cpu_percent: AtomicU8,
    total_cpu_percent: AtomicU8,
    used_memory_mb: AtomicU16,
    free_physical_memory_mb: AtomicU16,
    cores: AtomicU8,
}

impl LocalNodeTelemetryReader {
    pub(super) fn new() -> Self {
        Self {
            cache: Arc::clone(LOCAL_NODE_TELEMETRY.get_or_init(start_sampler)),
        }
    }

    #[inline]
    pub(super) fn sample(&self, include_memory: bool) -> PingTelemetry {
        self.cache.load(include_memory)
    }
}

impl LocalNodeTelemetryCache {
    fn new(initial: PingTelemetry) -> Self {
        let memory = initial.memory.unwrap_or(PingMemoryInfo {
            used_memory_mb: 0,
            free_physical_memory_mb: 0,
            cores: 0,
        });
        Self {
            moment_cpu_percent: AtomicU8::new(initial.moment_cpu_percent),
            total_cpu_percent: AtomicU8::new(initial.total_cpu_percent),
            used_memory_mb: AtomicU16::new(memory.used_memory_mb),
            free_physical_memory_mb: AtomicU16::new(memory.free_physical_memory_mb),
            cores: AtomicU8::new(memory.cores),
        }
    }

    fn store(&self, sample: PingTelemetry) {
        self.moment_cpu_percent
            .store(sample.moment_cpu_percent, Ordering::Relaxed);
        self.total_cpu_percent
            .store(sample.total_cpu_percent, Ordering::Relaxed);
        if let Some(memory) = sample.memory {
            self.used_memory_mb
                .store(memory.used_memory_mb, Ordering::Relaxed);
            self.free_physical_memory_mb
                .store(memory.free_physical_memory_mb, Ordering::Relaxed);
            self.cores.store(memory.cores, Ordering::Relaxed);
        }
    }

    #[inline]
    fn load(&self, include_memory: bool) -> PingTelemetry {
        let memory = include_memory.then(|| PingMemoryInfo {
            used_memory_mb: self.used_memory_mb.load(Ordering::Relaxed),
            free_physical_memory_mb: self.free_physical_memory_mb.load(Ordering::Relaxed),
            cores: self.cores.load(Ordering::Relaxed),
        });
        PingTelemetry {
            moment_cpu_percent: self.moment_cpu_percent.load(Ordering::Relaxed),
            total_cpu_percent: self.total_cpu_percent.load(Ordering::Relaxed),
            memory,
        }
    }
}

fn start_sampler() -> Arc<LocalNodeTelemetryCache> {
    let mut system = System::new();
    let pid = get_current_pid().ok();
    let initial = refresh_system(&mut system, pid, true);
    let cache = Arc::new(LocalNodeTelemetryCache::new(initial));
    let worker_cache = Arc::clone(&cache);

    if let Err(error) = std::thread::Builder::new()
        .name("moonproto-node-telemetry".to_owned())
        .spawn(move || {
            let mut sample_index = 0u8;
            loop {
                std::thread::sleep(CPU_SAMPLE_INTERVAL);
                sample_index = sample_index.wrapping_add(1);
                let include_memory = sample_index % MEMORY_SAMPLE_EVERY == 0;
                worker_cache.store(refresh_system(&mut system, pid, include_memory));
            }
        })
    {
        log::warn!(target: "moonproto::telemetry", "failed to start node telemetry sampler: {error}");
    }

    cache
}

fn refresh_system(
    system: &mut System,
    pid: Option<sysinfo::Pid>,
    include_memory: bool,
) -> PingTelemetry {
    system.refresh_cpu_usage();
    if let Some(pid) = pid {
        let process_refresh = if include_memory {
            ProcessRefreshKind::nothing().with_cpu().with_memory()
        } else {
            ProcessRefreshKind::nothing().with_cpu()
        };
        system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, process_refresh);
    }

    let cpu_count = system.cpus().len().max(1) as f32;
    let moment_cpu = pid
        .and_then(|pid| system.process(pid))
        .map_or(0.0, |process| process.cpu_usage() / cpu_count);
    let total_cpu = system.global_cpu_usage();

    let memory = include_memory.then(|| {
        system.refresh_memory();
        let used_bytes = pid
            .and_then(|pid| system.process(pid))
            .map_or(0, |process| process.memory());
        PingMemoryInfo {
            used_memory_mb: bytes_to_wire_mb(used_bytes),
            free_physical_memory_mb: bytes_to_wire_mb(system.available_memory()),
            cores: system.cpus().len().min(u8::MAX as usize) as u8,
        }
    });

    PingTelemetry {
        moment_cpu_percent: percent_to_wire(moment_cpu),
        total_cpu_percent: percent_to_wire(total_cpu),
        memory,
    }
}

fn percent_to_wire(value: f32) -> u8 {
    value.round().clamp(0.0, 100.0) as u8
}

fn bytes_to_wire_mb(bytes: u64) -> u16 {
    (bytes / 1_000_000).min(u64::from(u16::MAX)) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clients_share_one_process_telemetry_cache() {
        let first = LocalNodeTelemetryReader::new();
        let second = LocalNodeTelemetryReader::new();
        assert!(Arc::ptr_eq(&first.cache, &second.cache));
    }

    #[test]
    fn ping_memory_tail_is_selected_without_resampling() {
        let cache = LocalNodeTelemetryCache::new(PingTelemetry {
            moment_cpu_percent: 12,
            total_cpu_percent: 34,
            memory: Some(PingMemoryInfo {
                used_memory_mb: 56,
                free_physical_memory_mb: 78,
                cores: 9,
            }),
        });

        assert_eq!(cache.load(false).memory, None);
        assert_eq!(
            cache.load(true),
            PingTelemetry {
                moment_cpu_percent: 12,
                total_cpu_percent: 34,
                memory: Some(PingMemoryInfo {
                    used_memory_mb: 56,
                    free_physical_memory_mb: 78,
                    cores: 9,
                }),
            }
        );
    }
}
