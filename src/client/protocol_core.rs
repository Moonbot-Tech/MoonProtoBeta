use super::*;

pub(crate) struct ProtocolCore<'client> {
    pub(crate) client: &'client mut Client,
}

impl ProtocolCore<'_> {
    pub(crate) fn run_step(&mut self, mode: &mut RunMode<'_>) -> bool {
        #[cfg(any(test, feature = "diagnostics"))]
        let protocol_metrics = Arc::clone(&self.client.metrics.protocol_metrics);
        #[cfg(any(test, feature = "diagnostics"))]
        let _tick_timer = protocol_metrics.writer_tick_timer();
        if self.client.shutdown_requested() {
            self.client.disconnect();
            return false;
        }

        let cur_tm = self.client.now_ms();
        self.writer_tick_prologue(cur_tm);

        if self.ensure_socket_bound(cur_tm) {
            let drained_any = self.recv_one_phase(cur_tm, mode);
            #[cfg(any(test, feature = "diagnostics"))]
            let cpu_start = Instant::now();
            self.drain_app_commands(cur_tm, mode);
            #[cfg(any(test, feature = "diagnostics"))]
            self.send_maintenance_phase(cur_tm, mode, &protocol_metrics);
            #[cfg(not(any(test, feature = "diagnostics")))]
            self.send_maintenance_phase(cur_tm, mode);
            #[cfg(any(test, feature = "diagnostics"))]
            protocol_metrics.record_writer_cpu(cpu_start.elapsed());
            if !drained_any {
                self.wait_5ms();
            }
        } else {
            #[cfg(any(test, feature = "diagnostics"))]
            let cpu_start = Instant::now();
            #[cfg(any(test, feature = "diagnostics"))]
            protocol_metrics.record_writer_cpu(cpu_start.elapsed());
            thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
        }

        !self.client.shutdown_requested()
    }
}
