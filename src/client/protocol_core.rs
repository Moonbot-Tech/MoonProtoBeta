use super::*;

pub(crate) struct ProtocolCore<'client> {
    pub(crate) client: &'client mut Client,
}

impl ProtocolCore<'_> {
    pub(crate) fn run(&mut self, duration: Duration, mode: &mut RunMode<'_>) {
        let run_start = Instant::now();
        let protocol_metrics = Arc::clone(&self.client.protocol_metrics);

        loop {
            let _tick_timer = protocol_metrics.writer_tick_timer();
            if run_start.elapsed() >= duration {
                break;
            }
            let cur_tm = self.client.now_ms();

            self.writer_tick_prologue(cur_tm);

            if self.ensure_socket_bound(cur_tm) {
                self.recv_drain_phase(cur_tm, mode);

                let cpu_start = Instant::now();
                self.drain_app_commands(cur_tm, mode);
                self.send_maintenance_phase(cur_tm, mode, &protocol_metrics);
                protocol_metrics.record_writer_cpu(cpu_start.elapsed());
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
