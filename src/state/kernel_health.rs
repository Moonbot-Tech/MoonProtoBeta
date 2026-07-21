/// Resource telemetry reported by the connected MoonBot core in protocol v4 Ping.
///
/// CPU values are refreshed on every Ping. Memory is a lower-rate tail and is
/// therefore `None` until the first memory-bearing Ping arrives; afterwards the
/// last reported values remain available between memory samples.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KernelHealth {
    /// MoonBot process CPU usage as a percentage of the whole machine.
    pub process_cpu_percent: u8,
    /// Total CPU usage of the machine running MoonBot.
    pub system_cpu_percent: u8,
    /// MoonBot process memory in decimal megabytes.
    pub used_memory_mb: Option<u16>,
    /// Available physical memory on the machine in decimal megabytes.
    pub free_physical_memory_mb: Option<u16>,
    /// Logical CPU count reported with the same periodic memory profile.
    pub logical_cpu_count: Option<u8>,
}
