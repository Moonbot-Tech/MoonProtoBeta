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

    #[cfg(test)]
    pub(crate) fn run(&mut self, duration: Duration, mode: &mut RunMode<'_>) {
        let run_start = Instant::now();
        let run_deadline = run_start + duration;
        let protocol_metrics = Arc::clone(&self.client.metrics.protocol_metrics);

        loop {
            let _tick_timer = protocol_metrics.writer_tick_timer();
            if Instant::now() >= run_deadline || self.client.shutdown_requested() {
                if self.client.shutdown_requested() {
                    self.client.disconnect();
                }
                break;
            }
            let cur_tm = self.client.now_ms();

            self.writer_tick_prologue(cur_tm);

            if self.ensure_socket_bound(cur_tm) {
                let recv_deadline_reached = self.recv_drain_phase(cur_tm, run_deadline, mode);

                let cpu_start = Instant::now();
                self.drain_app_commands(cur_tm, mode);
                self.send_maintenance_phase(cur_tm, mode, &protocol_metrics);
                protocol_metrics.record_writer_cpu(cpu_start.elapsed());
                if recv_deadline_reached || Instant::now() >= run_deadline {
                    break;
                }
                self.wait_5ms();
            } else {
                let cpu_start = Instant::now();
                protocol_metrics.record_writer_cpu(cpu_start.elapsed());
                // Socket is not bound yet: short pause before the next bind try.
                thread::sleep(Duration::from_millis(DEFAULT_SLEEP_MS));
            }
        }
    }
}
