//! Diagnostics-only per-thread CPU clock.
//!
//! `Instant` measures elapsed wall time inside a protocol segment, so it also
//! includes scheduler preemption. FireTest uses this helper beside wall timings
//! to tell "the code spent CPU" from "the OS paused this thread".

use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ThreadCpuTimer {
    start_time: Option<Duration>,
    start_cycles: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ThreadCpuElapsed {
    pub(crate) time: Option<Duration>,
    pub(crate) cycles: Option<u64>,
}

impl ThreadCpuTimer {
    pub(crate) fn start() -> Self {
        Self {
            start_time: thread_cpu_time(),
            start_cycles: thread_cpu_cycles(),
        }
    }

    pub(crate) fn elapsed(self) -> ThreadCpuElapsed {
        ThreadCpuElapsed {
            time: self
                .start_time
                .and_then(|start| Some(thread_cpu_time()?.saturating_sub(start))),
            cycles: self
                .start_cycles
                .and_then(|start| Some(thread_cpu_cycles()?.saturating_sub(start))),
        }
    }
}

#[cfg(windows)]
fn thread_cpu_time() -> Option<Duration> {
    // `GetThreadTimes` has coarse tick granularity on Windows (commonly
    // 15.625ms), which is worse than useless for sub-millisecond protocol
    // segments. Use QueryThreadCycleTime instead and leave duration empty.
    None
}

#[cfg(windows)]
fn thread_cpu_cycles() -> Option<u64> {
    use windows_sys::Win32::System::Threading::GetCurrentThread;
    use windows_sys::Win32::System::WindowsProgramming::QueryThreadCycleTime;

    let mut cycles = 0u64;
    let ok = unsafe { QueryThreadCycleTime(GetCurrentThread(), &mut cycles) };
    (ok != 0).then_some(cycles)
}

#[cfg(unix)]
fn thread_cpu_time() -> Option<Duration> {
    let mut ts = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let ok = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, ts.as_mut_ptr()) };
    if ok != 0 {
        return None;
    }
    let ts = unsafe { ts.assume_init() };
    let secs = u64::try_from(ts.tv_sec).ok()?;
    let nanos = u32::try_from(ts.tv_nsec).ok()?;
    Some(Duration::new(secs, nanos))
}

#[cfg(unix)]
fn thread_cpu_cycles() -> Option<u64> {
    None
}

#[cfg(not(any(windows, unix)))]
fn thread_cpu_time() -> Option<Duration> {
    None
}

#[cfg(not(any(windows, unix)))]
fn thread_cpu_cycles() -> Option<u64> {
    None
}
