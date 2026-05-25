//! Lock-free retained ring for hot active-library histories.
//!
//! The ring is single-writer / multi-reader. The writer owns
//! [`SeqRingWriter`] and appends rows with monotonically increasing sequence
//! numbers. Readers clone [`SeqRingReader`] and copy retained rows by sequence
//! ranges. Public domain APIs should wrap this in time/N-row helpers; `seq` is
//! the low-level storage coordinate.
//!
//! This module intentionally does not store arbitrary `T: Copy` inside
//! `UnsafeCell`: a seqlock over non-atomic row bytes is easy to make unsound in
//! Rust because a reader may copy the same bytes while the writer overwrites
//! them. Rows are represented by atomic slots instead. The version word gives a
//! consistent multi-field snapshot; the row fields themselves avoid data races.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A copyable row that can be stored in [`SeqRing`].
///
/// Implement this for fixed hot-path records by providing an atomic slot type
/// with one atomic field per stored scalar. For floats, store IEEE bits in
/// `AtomicU32`/`AtomicU64`.
pub trait SeqRingRow: Copy + Send + Sync + 'static {
    type Slot: SeqRingRowSlot<Row = Self>;
}

/// Atomic storage for one [`SeqRingRow`].
pub trait SeqRingRowSlot: Default + Send + Sync + 'static {
    type Row: Copy + Send + Sync + 'static;

    fn store_row(&self, row: Self::Row);
    fn load_row(&self) -> Self::Row;
}

/// Row with a monotonic time coordinate.
///
/// Domain APIs use this to expose "from time T" and "time range" reads without
/// exposing internal sequence numbers to application code.
pub trait SeqRingTimedRow: SeqRingRow {
    fn seq_ring_time(&self) -> f64;
}

impl SeqRingRow for u64 {
    type Slot = AtomicU64;
}

impl SeqRingRowSlot for AtomicU64 {
    type Row = u64;

    fn store_row(&self, row: Self::Row) {
        AtomicU64::store(self, row, Ordering::Relaxed);
    }

    fn load_row(&self) -> Self::Row {
        AtomicU64::load(self, Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqRingError {
    ZeroCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeqRingBounds {
    pub oldest_seq: u64,
    /// One past the newest published sequence.
    pub next_seq: u64,
    pub len: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeqRingReadMeta {
    pub requested_start_seq: u64,
    pub actual_start_seq: u64,
    pub next_seq: u64,
    pub copied: usize,
    /// Requested start was older than retention.
    pub clipped: bool,
    /// A slot was overwritten while copying. The caller can retry if it needs a
    /// fully contiguous view.
    pub concurrent_miss: bool,
}

pub struct SeqRingWriter<T: SeqRingRow> {
    inner: Arc<SeqRingInner<T>>,
    next_seq: u64,
}

#[derive(Clone)]
pub struct SeqRingReader<T: SeqRingRow> {
    inner: Arc<SeqRingInner<T>>,
}

struct SeqRingInner<T: SeqRingRow> {
    slots: Box<[SeqRingSlotState<T>]>,
    capacity: u64,
    next_seq: AtomicU64,
}

struct SeqRingSlotState<T: SeqRingRow> {
    version: AtomicU64,
    row: T::Slot,
}

impl<T: SeqRingRow> SeqRingWriter<T> {
    pub fn new(capacity: usize) -> Result<(Self, SeqRingReader<T>), SeqRingError> {
        if capacity == 0 {
            return Err(SeqRingError::ZeroCapacity);
        }
        let slots = (0..capacity)
            .map(|_| SeqRingSlotState {
                version: AtomicU64::new(0),
                row: T::Slot::default(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let inner = Arc::new(SeqRingInner {
            slots,
            capacity: capacity as u64,
            next_seq: AtomicU64::new(0),
        });
        Ok((
            Self {
                inner: Arc::clone(&inner),
                next_seq: 0,
            },
            SeqRingReader { inner },
        ))
    }

    pub fn push(&mut self, row: T) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);

        let slot = self.inner.slot(seq);
        slot.version.store(writing_version(seq), Ordering::Release);
        slot.row.store_row(row);
        slot.version
            .store(published_version(seq), Ordering::Release);
        self.inner
            .next_seq
            .store(seq.wrapping_add(1), Ordering::Release);
        seq
    }

    pub fn push_batch(&mut self, rows: &[T]) {
        for &row in rows {
            self.push(row);
        }
    }

    pub fn reader(&self) -> SeqRingReader<T> {
        SeqRingReader {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn bounds(&self) -> SeqRingBounds {
        self.reader().bounds()
    }
}

impl<T: SeqRingRow> SeqRingReader<T> {
    pub fn capacity(&self) -> usize {
        self.inner.capacity as usize
    }

    pub fn bounds(&self) -> SeqRingBounds {
        let next_seq = self.inner.next_seq.load(Ordering::Acquire);
        let oldest_seq = next_seq.saturating_sub(self.inner.capacity);
        SeqRingBounds {
            oldest_seq,
            next_seq,
            len: (next_seq - oldest_seq) as usize,
            capacity: self.capacity(),
        }
    }

    pub fn read_at_seq(&self, seq: u64) -> Option<T> {
        let bounds = self.bounds();
        if seq < bounds.oldest_seq || seq >= bounds.next_seq {
            return None;
        }
        self.read_published_seq(seq)
    }

    /// Clears `out` and copies up to `limit` newest rows in sequence order.
    pub fn copy_last(&self, limit: usize, out: &mut Vec<T>) -> SeqRingReadMeta {
        let bounds = self.bounds();
        let limit = limit.min(bounds.len);
        let start_seq = bounds.next_seq - limit as u64;
        self.copy_from_seq(start_seq, limit, out)
    }

    /// Clears `out` and copies up to `limit` rows starting at `start_seq`.
    pub fn copy_from_seq(&self, start_seq: u64, limit: usize, out: &mut Vec<T>) -> SeqRingReadMeta {
        out.clear();

        let bounds = self.bounds();
        let actual_start_seq = start_seq.max(bounds.oldest_seq).min(bounds.next_seq);
        let end_seq = actual_start_seq
            .saturating_add(limit as u64)
            .min(bounds.next_seq);
        out.reserve((end_seq - actual_start_seq) as usize);

        let mut concurrent_miss = false;
        let mut seq = actual_start_seq;
        while seq < end_seq {
            if let Some(row) = self.read_published_seq(seq) {
                out.push(row);
                seq += 1;
            } else {
                concurrent_miss = true;
                break;
            }
        }

        SeqRingReadMeta {
            requested_start_seq: start_seq,
            actual_start_seq,
            next_seq: bounds.next_seq,
            copied: out.len(),
            clipped: actual_start_seq != start_seq,
            concurrent_miss,
        }
    }

    /// Binary-search a retained monotonic range. `is_before(row)` must return
    /// true for rows before the target and false for target/after rows.
    pub fn lower_bound_seq_by<F>(&self, mut is_before: F) -> Option<u64>
    where
        F: FnMut(T) -> bool,
    {
        let bounds = self.bounds();
        let mut lo = bounds.oldest_seq;
        let mut hi = bounds.next_seq;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let row = self.read_published_seq(mid)?;
            if is_before(row) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        Some(lo)
    }

    fn read_published_seq(&self, seq: u64) -> Option<T> {
        let slot = self.inner.slot(seq);
        for _ in 0..8 {
            let v1 = slot.version.load(Ordering::Acquire);
            if published_seq(v1) != Some(seq) {
                return None;
            }
            let row = slot.row.load_row();
            let v2 = slot.version.load(Ordering::Acquire);
            if v1 == v2 {
                return Some(row);
            }
            std::hint::spin_loop();
        }
        None
    }
}

impl<T: SeqRingTimedRow> SeqRingReader<T> {
    pub fn first_seq_at_or_after_time(&self, time: f64) -> Option<u64> {
        self.lower_bound_seq_by(|row| row.seq_ring_time() < time)
    }

    /// Clears `out` and copies up to `limit` rows starting at the first row
    /// whose time is `>= time`.
    pub fn copy_from_time(
        &self,
        time: f64,
        limit: usize,
        out: &mut Vec<T>,
    ) -> Option<SeqRingReadMeta> {
        let time_clipped = self.is_time_before_oldest(time);
        let start_seq = self.first_seq_at_or_after_time(time)?;
        let mut meta = self.copy_from_seq(start_seq, limit, out);
        meta.clipped |= time_clipped;
        Some(meta)
    }

    /// Clears `out` and copies rows with `from_time <= row.time < to_time`.
    pub fn copy_time_range(
        &self,
        from_time: f64,
        to_time: f64,
        limit: usize,
        out: &mut Vec<T>,
    ) -> Option<SeqRingReadMeta> {
        out.clear();
        let time_clipped = self.is_time_before_oldest(from_time);
        let start_seq = self.first_seq_at_or_after_time(from_time)?;
        let bounds = self.bounds();
        let actual_start_seq = start_seq.max(bounds.oldest_seq).min(bounds.next_seq);
        let mut seq = actual_start_seq;
        let mut concurrent_miss = false;

        while seq < bounds.next_seq && out.len() < limit {
            match self.read_published_seq(seq) {
                Some(row) => {
                    if row.seq_ring_time() >= to_time {
                        break;
                    }
                    out.push(row);
                    seq += 1;
                }
                None => {
                    concurrent_miss = true;
                    break;
                }
            }
        }

        Some(SeqRingReadMeta {
            requested_start_seq: start_seq,
            actual_start_seq,
            next_seq: bounds.next_seq,
            copied: out.len(),
            clipped: actual_start_seq != start_seq || time_clipped,
            concurrent_miss,
        })
    }

    fn is_time_before_oldest(&self, time: f64) -> bool {
        let bounds = self.bounds();
        if bounds.len == 0 {
            return false;
        }
        self.read_published_seq(bounds.oldest_seq)
            .is_some_and(|row| time < row.seq_ring_time())
    }
}

impl<T: SeqRingRow> SeqRingInner<T> {
    fn slot(&self, seq: u64) -> &SeqRingSlotState<T> {
        &self.slots[(seq % self.capacity) as usize]
    }
}

fn writing_version(seq: u64) -> u64 {
    published_version(seq) | 1
}

fn published_version(seq: u64) -> u64 {
    seq.wrapping_add(1).wrapping_shl(1)
}

fn published_seq(version: u64) -> Option<u64> {
    if version >= 2 && version & 1 == 0 {
        Some((version >> 1) - 1)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::thread;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TimedRow {
        time_ms: u64,
        value: u64,
    }

    #[derive(Default)]
    struct TimedRowSlot {
        time_ms: AtomicU64,
        value: AtomicU64,
    }

    impl SeqRingRow for TimedRow {
        type Slot = TimedRowSlot;
    }

    impl SeqRingTimedRow for TimedRow {
        fn seq_ring_time(&self) -> f64 {
            self.time_ms as f64
        }
    }

    impl SeqRingRowSlot for TimedRowSlot {
        type Row = TimedRow;

        fn store_row(&self, row: Self::Row) {
            self.time_ms.store(row.time_ms, Ordering::Relaxed);
            self.value.store(row.value, Ordering::Relaxed);
        }

        fn load_row(&self) -> Self::Row {
            TimedRow {
                time_ms: self.time_ms.load(Ordering::Relaxed),
                value: self.value.load(Ordering::Relaxed),
            }
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
    fn lower_bound_finds_time_position() {
        let (mut writer, reader) = SeqRingWriter::<TimedRow>::new(8).unwrap();
        for i in 0..6 {
            writer.push(TimedRow {
                time_ms: 1_000 + i * 250,
                value: i,
            });
        }

        let seq = reader
            .lower_bound_seq_by(|row| row.time_ms < 1_700)
            .unwrap();
        assert_eq!(seq, 3);

        let mut out = Vec::new();
        reader.copy_from_seq(seq, 2, &mut out);
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
            ]
        );
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
        let meta = reader.copy_from_time(1_700.0, 3, &mut out).unwrap();

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
            .copy_time_range(1_250.0, 2_000.0, 10, &mut out)
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
    fn copy_from_time_reports_retention_clip() {
        let (mut writer, reader) = SeqRingWriter::<TimedRow>::new(3).unwrap();
        for i in 0..5 {
            writer.push(TimedRow {
                time_ms: 1_000 + i * 250,
                value: i,
            });
        }

        let mut out = Vec::new();
        let meta = reader.copy_from_time(1_000.0, 10, &mut out).unwrap();

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
}
