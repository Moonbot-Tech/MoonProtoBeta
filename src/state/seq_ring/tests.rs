use super::*;
use std::thread;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TimedRow {
    time_ms: u64,
    value: u64,
}

impl SeqRingTimedRow for TimedRow {
    fn seq_ring_time_ms(&self) -> i64 {
        self.time_ms as i64
    }
}

#[test]
fn rejects_zero_capacity() {
    assert_eq!(
        SeqRingWriter::<u64>::new(0).err(),
        Some(SeqRingError::ZeroCapacity)
    );
}

#[test]
fn copies_last_rows_in_sequence_order() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(4).unwrap();
    writer.push_batch(&[10, 11, 12]);

    let mut out = Vec::new();
    let meta = reader.copy_last(10, &mut out);

    assert_eq!(out, vec![10, 11, 12]);
    assert_eq!(
        meta,
        SeqRingReadMeta {
            requested_start_seq: 0,
            actual_start_seq: 0,
            next_seq: 3,
            copied: 3,
            clipped: false,
            concurrent_miss: false,
        }
    );
}

#[test]
fn replace_seq_updates_retained_slot_without_advancing_sequence() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(4).unwrap();
    writer.push_batch(&[10, 11, 12]);

    assert!(writer.replace_seq(1, 99));
    assert_eq!(reader.bounds().next_seq, 3);

    let mut out = Vec::new();
    reader.copy_last(4, &mut out);
    assert_eq!(out, vec![10, 99, 12]);
    assert!(!writer.replace_seq(10, 77));
}

#[test]
fn wrap_retains_only_capacity_tail() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(4).unwrap();
    writer.push_batch(&[0, 1, 2, 3, 4, 5]);

    assert_eq!(
        reader.bounds(),
        SeqRingBounds {
            oldest_seq: 2,
            next_seq: 6,
            len: 4,
            capacity: 4,
        }
    );
    assert_eq!(reader.read_at_seq(1), None);
    assert_eq!(reader.read_at_seq(2), Some(2));

    let mut out = Vec::new();
    let meta = reader.copy_from_seq(0, 10, &mut out);
    assert_eq!(out, vec![2, 3, 4, 5]);
    assert!(meta.clipped);
    assert_eq!(meta.actual_start_seq, 2);
}

#[test]
fn push_with_evicted_returns_overwritten_row() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(2).unwrap();
    assert_eq!(writer.push_with_evicted(10), (0, None));
    assert_eq!(writer.push_with_evicted(11), (1, None));
    assert_eq!(writer.push_with_evicted(12), (2, Some(10)));
    assert_eq!(writer.push_with_evicted(13), (3, Some(11)));

    let mut out = Vec::new();
    reader.copy_last(2, &mut out);
    assert_eq!(out, vec![12, 13]);
}

#[test]
fn push_batch_with_evicted_returns_all_overwritten_rows() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(3).unwrap();
    writer.push_batch(&[10, 11, 12]);

    let mut evicted = Vec::new();
    writer.push_batch_with_evicted(&[13, 14, 15, 16], &mut evicted);

    assert_eq!(evicted, vec![10, 11, 12, 13]);
    let mut out = Vec::new();
    reader.copy_last(3, &mut out);
    assert_eq!(out, vec![14, 15, 16]);
}

#[test]
fn zero_copy_view_handles_wrapped_tail() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(4).unwrap();
    writer.push_batch(&[0, 1, 2, 3, 4, 5]);

    let sum = reader.with_from_seq(2, 4, |view| {
        let (first, second) = view.as_slices();
        assert_eq!(first, &[2, 3]);
        assert_eq!(second, &[4, 5]);
        let mut sum = 0;
        view.for_each(|value| sum += *value);
        sum
    });

    assert_eq!(sum, 14);
}

#[test]
fn copy_new_since_uses_per_consumer_cursor() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(8).unwrap();
    writer.push_batch(&[10, 11, 12]);

    let mut a = reader.cursor_from_oldest();
    let mut b = reader.cursor_from_now();
    let mut out = Vec::new();

    reader.copy_new_since(&mut a, 2, &mut out);
    assert_eq!(out, vec![10, 11]);
    assert_eq!(a.next_seq(), 2);

    writer.push_batch(&[13, 14]);
    reader.copy_new_since(&mut a, 10, &mut out);
    assert_eq!(out, vec![12, 13, 14]);
    assert_eq!(a.next_seq(), 5);

    reader.copy_new_since(&mut b, 10, &mut out);
    assert_eq!(out, vec![13, 14]);
    assert_eq!(b.next_seq(), 5);
}

#[test]
fn bounded_drain_reports_not_caught_up_when_limit_is_smaller_than_backlog() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(8).unwrap();
    writer.push_batch(&[10, 11, 12, 13, 14]);

    let mut cursor = reader.cursor_from_oldest();
    let mut out = Vec::new();
    let meta = reader.copy_new_since_bounded_all(&mut cursor, 2, &mut out);

    assert_eq!(out, vec![10, 11]);
    assert_eq!(
        meta,
        SeqRingDrainMeta {
            copied: 2,
            clipped: false,
            caught_up: false,
            concurrent_miss: false,
        }
    );
    assert_eq!(cursor.next_seq(), 2);

    let meta = reader.copy_new_since_bounded_all(&mut cursor, 10, &mut out);
    assert_eq!(out, vec![12, 13, 14]);
    assert_eq!(
        meta,
        SeqRingDrainMeta {
            copied: 3,
            clipped: false,
            caught_up: true,
            concurrent_miss: false,
        }
    );
    assert_eq!(cursor.next_seq(), 5);
}

#[test]
fn bounded_drain_reports_clipped_when_cursor_fell_behind_retention() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(3).unwrap();
    writer.push_batch(&[10, 11, 12, 13, 14]);

    let mut cursor = SeqRingCursor::from_next_seq(0);
    let mut out = Vec::new();
    let meta = reader.copy_new_since_bounded_all(&mut cursor, 10, &mut out);

    assert_eq!(out, vec![12, 13, 14]);
    assert_eq!(
        meta,
        SeqRingDrainMeta {
            copied: 3,
            clipped: true,
            caught_up: true,
            concurrent_miss: false,
        }
    );
    assert_eq!(cursor.next_seq(), 5);
}

#[test]
fn scan_from_cursor_visits_retained_range_without_copying_rows() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(8).unwrap();
    writer.push_batch(&[10, 40, 20, 30]);

    let cursor = reader.cursor_from_oldest();
    let ((min, max), meta) =
        reader.scan_from_cursor(cursor, 3, (u64::MAX, 0), |(min, max), row| {
            (min.min(*row), max.max(*row))
        });

    assert_eq!((min, max), (10, 40));
    assert_eq!(meta.copied, 3);
    assert!(!meta.clipped);
}

#[test]
fn copy_from_time_hides_sequence_coordinates() {
    let (mut writer, reader) = SeqRingWriter::<TimedRow>::new(8).unwrap();
    for i in 0..6 {
        writer.push(TimedRow {
            time_ms: 1_000 + i * 250,
            value: i,
        });
    }

    let mut out = Vec::new();
    let meta = reader
        .copy_from_time(MoonTime::from_unix_millis(1_700), 3, &mut out)
        .unwrap();

    assert_eq!(meta.actual_start_seq, 3);
    assert_eq!(
        out,
        vec![
            TimedRow {
                time_ms: 1_750,
                value: 3,
            },
            TimedRow {
                time_ms: 2_000,
                value: 4,
            },
            TimedRow {
                time_ms: 2_250,
                value: 5,
            },
        ]
    );
}

#[test]
fn millisecond_time_range_helper_returns_rows_in_sequence_order() {
    let (mut writer, reader) = SeqRingWriter::<TimedRow>::new(8).unwrap();
    for i in 0..6 {
        writer.push(TimedRow {
            time_ms: 1_000 + i * 250,
            value: i,
        });
    }

    let mut out = Vec::new();
    reader
        .copy_time_range_ms(1_250, 2_000, 10, &mut out)
        .unwrap();

    assert_eq!(
        out,
        vec![
            TimedRow {
                time_ms: 1_250,
                value: 1,
            },
            TimedRow {
                time_ms: 1_500,
                value: 2,
            },
            TimedRow {
                time_ms: 1_750,
                value: 3,
            },
        ]
    );
}

#[test]
fn copy_time_range_stops_at_exclusive_end() {
    let (mut writer, reader) = SeqRingWriter::<TimedRow>::new(8).unwrap();
    for i in 0..6 {
        writer.push(TimedRow {
            time_ms: 1_000 + i * 250,
            value: i,
        });
    }

    let mut out = Vec::new();
    reader
        .copy_time_range(
            MoonTime::from_unix_millis(1_250),
            MoonTime::from_unix_millis(2_000),
            10,
            &mut out,
        )
        .unwrap();

    assert_eq!(
        out,
        vec![
            TimedRow {
                time_ms: 1_250,
                value: 1,
            },
            TimedRow {
                time_ms: 1_500,
                value: 2,
            },
            TimedRow {
                time_ms: 1_750,
                value: 3,
            },
        ]
    );
}

#[test]
fn timed_reads_scan_append_order_when_times_are_not_monotonic() {
    let (mut writer, reader) = SeqRingWriter::<TimedRow>::new(8).unwrap();
    writer.push(TimedRow {
        time_ms: 1_000,
        value: 0,
    });
    writer.push(TimedRow {
        time_ms: 2_000,
        value: 1,
    });
    writer.push(TimedRow {
        time_ms: 1_500,
        value: 2,
    });
    writer.push(TimedRow {
        time_ms: 2_250,
        value: 3,
    });

    assert_eq!(
        reader.first_seq_at_or_after_time(MoonTime::from_unix_millis(1_750)),
        Some(1)
    );

    let mut out = Vec::new();
    let meta = reader
        .copy_from_time(MoonTime::from_unix_millis(1_750), 10, &mut out)
        .unwrap();
    assert_eq!(meta.actual_start_seq, 1);
    assert_eq!(
        out,
        vec![
            TimedRow {
                time_ms: 2_000,
                value: 1,
            },
            TimedRow {
                time_ms: 1_500,
                value: 2,
            },
            TimedRow {
                time_ms: 2_250,
                value: 3,
            },
        ]
    );

    reader
        .copy_time_range(
            MoonTime::from_unix_millis(1_750),
            MoonTime::from_unix_millis(2_500),
            10,
            &mut out,
        )
        .unwrap();
    assert_eq!(
        out,
        vec![
            TimedRow {
                time_ms: 2_000,
                value: 1,
            },
            TimedRow {
                time_ms: 2_250,
                value: 3,
            },
        ]
    );
}

#[test]
fn copy_from_time_reports_retention_clip() {
    let (mut writer, reader) = SeqRingWriter::<TimedRow>::new(3).unwrap();
    for i in 0..5 {
        writer.push(TimedRow {
            time_ms: 1_000 + i * 250,
            value: i,
        });
    }

    let mut out = Vec::new();
    let meta = reader
        .copy_from_time(MoonTime::from_unix_millis(1_000), 10, &mut out)
        .unwrap();

    assert!(meta.clipped);
    assert_eq!(
        out,
        vec![
            TimedRow {
                time_ms: 1_500,
                value: 2,
            },
            TimedRow {
                time_ms: 1_750,
                value: 3,
            },
            TimedRow {
                time_ms: 2_000,
                value: 4,
            },
        ]
    );
}

#[test]
fn reader_clone_can_read_from_another_thread() {
    let (mut writer, reader) = SeqRingWriter::<u64>::new(128).unwrap();
    let reader2 = reader.clone();

    let handle = thread::spawn(move || {
        let mut out = Vec::new();
        loop {
            reader2.copy_last(16, &mut out);
            if out.last().copied() == Some(999) {
                return out;
            }
            thread::yield_now();
        }
    });

    for value in 0..1_000 {
        writer.push(value);
    }

    let out = handle.join().unwrap();
    assert_eq!(out.last().copied(), Some(999));
    assert!(out.len() <= 16);
}
