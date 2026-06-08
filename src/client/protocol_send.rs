use super::protocol_core::ProtocolCore;
use super::*;

impl ProtocolCore<'_> {
    pub(crate) fn send_command(&mut self, cmd: Command, payload: &[u8]) {
        Self::send_command_on_client(self.client, cmd, payload);
    }

    pub(crate) fn send_command_raw(&mut self, cmd: u8, payload: &[u8]) {
        Self::send_command_raw_on_client(self.client, cmd, payload);
    }

    pub(crate) fn send_command_on_client(client: &mut Client, cmd: Command, payload: &[u8]) {
        client.send_raw_packet(cmd, payload);
    }

    pub(crate) fn send_command_raw_on_client(client: &mut Client, cmd: u8, payload: &[u8]) {
        client.send_raw_packet_cmd(cmd, payload);
    }

    pub(crate) fn copy_send_ack_and_check_sening_data(&mut self, cur_tm: i64) {
        let mut copy_send_list = std::mem::take(&mut self.client.copy_send_sliced);
        let mut copy_send_list_h = std::mem::take(&mut self.client.copy_send_high);
        let mut copy_send_list_l = std::mem::take(&mut self.client.copy_send_low);
        let mut copy_acks = std::mem::take(&mut self.client.copy_sliced_acks);

        // Delphi `Execute` under `SendLock`:
        // GetCopySendList; GetCopyAcks; FClient.CopyRecvdData.
        #[cfg(any(test, feature = "diagnostics"))]
        let send_lock_snapshot_start = Instant::now();
        self.get_copy_send_lock_snapshot(
            &mut copy_send_list,
            &mut copy_send_list_h,
            &mut copy_send_list_l,
            &mut copy_acks,
        );
        #[cfg(any(test, feature = "diagnostics"))]
        self.client
            .metrics
            .protocol_metrics
            .record_profile_phase_labeled(
                ProfilePhase::SendLockSnapshot,
                send_lock_snapshot_start.elapsed(),
                u8::MAX,
                u8::MAX,
                copy_send_list.len() + copy_send_list_h.len() + copy_send_list_l.len(),
            );

        #[cfg(any(test, feature = "diagnostics"))]
        let check_sening_start = Instant::now();
        self.check_sening_data(
            &copy_send_list,
            &mut copy_send_list_h,
            &copy_send_list_l,
            &mut copy_acks,
            cur_tm,
        );
        #[cfg(any(test, feature = "diagnostics"))]
        self.client
            .metrics
            .protocol_metrics
            .record_profile_phase_labeled(
                ProfilePhase::CheckSeningData,
                check_sening_start.elapsed(),
                u8::MAX,
                u8::MAX,
                copy_send_list.len() + copy_send_list_h.len() + copy_send_list_l.len(),
            );
        copy_send_list.clear();
        copy_send_list_h.clear();
        copy_send_list_l.clear();
        copy_acks.clear();
        self.client.copy_send_sliced = copy_send_list;
        self.client.copy_send_high = copy_send_list_h;
        self.client.copy_send_low = copy_send_list_l;
        self.client.copy_sliced_acks = copy_acks;
    }

    pub(crate) fn check_sening_data(
        &mut self,
        copy_send_list: &[SendItem],
        copy_send_list_h: &mut [SendItem],
        copy_send_list_l: &[SendItem],
        copy_acks: &mut Vec<SlicedAck>,
        cur_tm: i64,
    ) {
        // Delphi `CheckSeningData`: Sliced CopySendList first, then SlicedACK,
        // then regular H ACK bitmap, High send/retry, first Low flush, Sliced
        // retry, remaining Low flush. Keep this exact protocol order.
        self.apply_sliced_send_u_key_cleanup(copy_send_list);
        for item in copy_send_list {
            self.create_sliced_and_send(item);
        }
        self.apply_copy_acks(copy_acks, cur_tm);
        self.apply_regular_hl_ack();
        self.apply_high_send_u_key_cleanup(copy_send_list_h);
        for item in copy_send_list_h {
            self.send_h_item(item, cur_tm);
        }
        self.retry_pending_h(cur_tm);
        self.send_low_items_around_sliced_retry(copy_send_list_l, cur_tm);
    }

    pub(crate) fn get_copy_send_lock_snapshot(
        &mut self,
        sliced: &mut Vec<SendItem>,
        h_items: &mut Vec<SendItem>,
        l_items: &mut Vec<SendItem>,
        acks: &mut Vec<SlicedAck>,
    ) {
        let tmp_slider = self
            .client
            .send_lock
            .lock()
            .take_send_snapshot(sliced, h_items, l_items, acks);
        if let Some(tmp_slider) = tmp_slider {
            self.client.recv.recvd_slider = tmp_slider;
        }
    }

    #[cfg(test)]
    pub(crate) fn get_copy_acks(&mut self) -> Vec<SlicedAck> {
        let mut sliced = Vec::new();
        let mut high = Vec::new();
        let mut low = Vec::new();
        let mut acks = Vec::new();
        self.get_copy_send_lock_snapshot(&mut sliced, &mut high, &mut low, &mut acks);
        acks
    }

    #[cfg(test)]
    pub(crate) fn copy_recvd_data(&mut self) {
        if let Some(tmp_slider) = self.client.send_lock.lock().copy_tmp_slider() {
            self.client.recv.recvd_slider = tmp_slider;
        }
    }
}
