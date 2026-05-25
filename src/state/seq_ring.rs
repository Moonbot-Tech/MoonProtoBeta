//! Dense retained ring for hot active-library histories.
//!
//! The ring is single-writer / multi-reader. The writer side is intended for
//! `StoreWorker`, not for the UDP protocol reader. Rows are stored as a dense
//! `Vec<T>` behind a short `parking_lot::RwLock`: appends take the write lock,
//! readers either copy a simple range or scan a zero-copy read view inside a
//! closure. This keeps the hot history memory layout close to Delphi's dense
//! arrays without unsafe aliasing around overwrite-ring slots.

use std::sync::Arc;

use parking_lot::RwLock;

/// A copyable row that can be stored in [`SeqRing`].
pub trait SeqRingRow: Copy + Default + Send + Sync + 'static {}

impl<T> SeqRingRow for T where T: Copy + Default + Send + Sync + 'static {}

/// Row with a domain time coordinate.
///
/// Domain APIs use this to expose "from time T" and "time range" reads without
/// exposing internal sequence numbers to application code. The retained futures
/// history preserves Delphi append order and can contain late resend rows, so
/// timed reads scan the retained sequence instead of assuming monotonic time.
pub trait SeqRingTimedRow: SeqRingRow {
    fn seq_ring_time(&self) -> f64;
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
    /// One past the newest sequence that existed when the read lock was taken.
    pub next_seq: u64,
    pub copied: usize,
    /// Requested start was older than retention.
    pub clipped: bool,
    /// Always false for the dense locked backend: the read lock prevents slot
    /// overwrite during the copy/view.
    pub concurrent_miss: bool,
}

/// Per-consumer "read only new rows" cursor.
///
/// The cursor deliberately belongs to the caller. Each UI/user/strategy thread
/// keeps its own cursor, so one consumer reading rows never marks them consumed
/// for another consumer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SeqRingCursor {
    next_seq: u64,
}

impl SeqRingCursor {
    pub fn from_next_seq(next_seq: u64) -> Self {
        Self { next_seq }
    }

    pub fn next_seq(self) -> u64 {
        self.next_seq
    }

    pub fn set_next_seq(&mut self, next_seq: u64) {
        self.next_seq = next_seq;
    }
}

pub struct SeqRingWriter<T: SeqRingRow> {
    inner: Arc<SeqRingInner<T>>,
}

#[derive(Clone)]
pub struct SeqRingReader<T: SeqRingRow> {
    inner: Arc<SeqRingInner<T>>,
}

pub struct SeqRingReadView<'a, T: SeqRingRow> {
    first: &'a [T],
    second: &'a [T],
    meta: SeqRingReadMeta,
}

impl<'a, T: SeqRingRow> SeqRingReadView<'a, T> {
    pub fn meta(&self) -> SeqRingReadMeta {
        self.meta
    }

    pub fn len(&self) -> usize {
        self.first.len() + self.second.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the physical slices in sequence order. Wrapped ranges produce two
    /// slices; callers that do not care about zero-copy should use `copy_to`.
    pub fn as_slices(&self) -> (&'a [T], &'a [T]) {
        (self.first, self.second)
    }

    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&T),
    {
        for row in self.first {
            f(row);
        }
        for row in self.second {
            f(row);
        }
    }

    pub fn copy_to(&self, out: &mut Vec<T>) {
        out.clear();
        out.reserve(self.len());
        out.extend_from_slice(self.first);
        out.extend_from_slice(self.second);
    }
}

struct SeqRingInner<T: SeqRingRow> {
    state: RwLock<SeqRingState<T>>,
}

struct SeqRingState<T: SeqRingRow> {
    rows: Box<[T]>,
    capacity: usize,
    next_seq: u64,
    len: usize,
}

impl<T: SeqRingRow> SeqRingWriter<T> {
    pub fn new(capacity: usize) -> Result<(Self, SeqRingReader<T>), SeqRingError> {
        if capacity == 0 {
            return Err(SeqRingError::ZeroCapacity);
        }
        let rows = vec![T::default(); capacity].into_boxed_slice();
        let inner = Arc::new(SeqRingInner {
            state: RwLock::new(SeqRingState {
                rows,
                capacity,
                next_seq: 0,
                len: 0,
            }),
        });
        Ok((
            Self {
                inner: Arc::clone(&inner),
            },
            SeqRingReader { inner },
        ))
    }

    pub fn push(&mut self, row: T) -> u64 {
        self.push_with_evicted(row).0
    }

    /// Append a row and return the overwritten row when the ring was full.
    ///
    /// `StoreWorker` uses this to preserve Delphi's old-trade compaction
    /// meaning: detailed rows that leave retained history can be folded into
    /// `TMiniCandle`-like aggregates instead of disappearing silently.
    pub fn push_with_evicted(&mut self, row: T) -> (u64, Option<T>) {
        let mut state = self.inner.state.write();
        let seq = state.next_seq;
        let idx = state.slot_index(seq);
        let evicted = if state.len == state.capacity {
            Some(state.rows[idx])
        } else {
            state.len += 1;
            None
        };
        state.rows[idx] = row;
        state.next_seq = state.next_seq.wrapping_add(1);
        (seq, evicted)
    }

    pub fn push_batch(&mut self, rows: &[T]) {
        let mut state = self.inner.state.write();
        for &row in rows {
            let seq = state.next_seq;
            let idx = state.slot_index(seq);
            if state.len < state.capacity {
                state.len += 1;
            }
            state.rows[idx] = row;
            state.next_seq = state.next_seq.wrapping_add(1);
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
        self.inner.state.read().capacity
    }

    pub fn bounds(&self) -> SeqRingBounds {
        self.inner.state.read().bounds()
    }

    pub fn cursor_from_oldest(&self) -> SeqRingCursor {
        SeqRingCursor::from_next_seq(self.bounds().oldest_seq)
    }

    pub fn cursor_from_now(&self) -> SeqRingCursor {
        SeqRingCursor::from_next_seq(self.bounds().next_seq)
    }

    pub fn read_at_seq(&self, seq: u64) -> Option<T> {
        let state = self.inner.state.read();
        if !state.contains_seq(seq) {
            return None;
        }
        Some(state.row_at_seq(seq))
    }

    /// Clears `out` and copies up to `limit` newest rows in sequence order.
    pub fn copy_last(&self, limit: usize, out: &mut Vec<T>) -> SeqRingReadMeta {
        self.with_last(limit, |view| {
            view.copy_to(out);
            view.meta()
        })
    }

    /// Clears `out` and copies up to `limit` rows starting at `start_seq`.
    pub fn copy_from_seq(&self, start_seq: u64, limit: usize, out: &mut Vec<T>) -> SeqRingReadMeta {
        self.with_from_seq(start_seq, limit, |view| {
            view.copy_to(out);
            view.meta()
        })
    }

    /// Copy rows after a per-consumer cursor and advance the cursor by exactly
    /// the copied rows. If the cursor fell behind retention, the read clips to
    /// the oldest retained row and reports `clipped`.
    pub fn copy_new_since(
        &self,
        cursor: &mut SeqRingCursor,
        limit: usize,
        out: &mut Vec<T>,
    ) -> SeqRingReadMeta {
        let meta = self.copy_from_seq(cursor.next_seq, limit, out);
        cursor.next_seq = meta.actual_start_seq + meta.copied as u64;
        meta
    }

    /// Lock the dense ring and expose the retained range as zero-copy slices
    /// for the duration of `f`.
    pub fn with_from_seq<R, F>(&self, start_seq: u64, limit: usize, f: F) -> R
    where
        F: FnOnce(SeqRingReadView<'_, T>) -> R,
    {
        let state = self.inner.state.read();
        let (meta, end_seq) = state.read_meta(start_seq, limit);
        let (first, second) = state.slices(meta.actual_start_seq, end_seq);
        f(SeqRingReadView {
            first,
            second,
            meta,
        })
    }

    pub fn with_last<R, F>(&self, limit: usize, f: F) -> R
    where
        F: FnOnce(SeqRingReadView<'_, T>) -> R,
    {
        let state = self.inner.state.read();
        let bounds = state.bounds();
        let limit = limit.min(bounds.len);
        let start_seq = bounds.next_seq - limit as u64;
        let (meta, end_seq) = state.read_meta(start_seq, limit);
        let (first, second) = state.slices(meta.actual_start_seq, end_seq);
        f(SeqRingReadView {
            first,
            second,
            meta,
        })
    }

    /// Binary-search a retained monotonic range. `is_before(row)` must return
    /// true for rows before the target and false for target/after rows.
    pub fn lower_bound_seq_by<F>(&self, mut is_before: F) -> Option<u64>
    where
        F: FnMut(T) -> bool,
    {
        let state = self.inner.state.read();
        let bounds = state.bounds();
        let mut lo = bounds.oldest_seq;
        let mut hi = bounds.next_seq;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let row = state.row_at_seq(mid);
            if is_before(row) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        Some(lo)
    }
}

impl<T: SeqRingTimedRow> SeqRingReader<T> {
    pub fn first_seq_at_or_after_time(&self, time: f64) -> Option<u64> {
        Some(self.inner.state.read().first_seq_at_or_after_time(time))
    }

    /// Clears `out` and copies up to `limit` rows starting at the first retained
    /// sequence whose time is `>= time`.
    ///
    /// Rows are returned in retained append order. If later rows have older
    /// timestamps because a resend arrived late, they are still returned; use
    /// [`Self::copy_time_range`] when every returned row must satisfy a time
    /// predicate.
    pub fn copy_from_time(
        &self,
        time: f64,
        limit: usize,
        out: &mut Vec<T>,
    ) -> Option<SeqRingReadMeta> {
        let state = self.inner.state.read();
        let time_clipped = state.is_time_before_retained_min(time);
        let start_seq = state.first_seq_at_or_after_time(time);
        let (mut meta, end_seq) = state.read_meta(start_seq, limit);
        meta.clipped |= time_clipped;
        let (first, second) = state.slices(meta.actual_start_seq, end_seq);
        out.clear();
        out.reserve(meta.copied);
        out.extend_from_slice(first);
        out.extend_from_slice(second);
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
        let state = self.inner.state.read();
        let bounds = state.bounds();
        let time_clipped = state.is_time_before_retained_min(from_time);
        let start_seq = state
            .first_seq_at_or_after_time(from_time)
            .max(bounds.oldest_seq)
            .min(bounds.next_seq);

        out.clear();
        out.reserve(limit.min(bounds.len));
        let mut end_seq = start_seq;
        while end_seq < bounds.next_seq && out.len() < limit {
            let row = state.row_at_seq(end_seq);
            if row.seq_ring_time() >= from_time && row.seq_ring_time() < to_time {
                out.push(row);
            }
            end_seq += 1;
        }

        Some(SeqRingReadMeta {
            requested_start_seq: start_seq,
            actual_start_seq: start_seq,
            next_seq: bounds.next_seq,
            copied: out.len(),
            clipped: time_clipped,
            concurrent_miss: false,
        })
    }
}

impl<T: SeqRingRow> SeqRingState<T> {
    fn bounds(&self) -> SeqRingBounds {
        let oldest_seq = self.next_seq.saturating_sub(self.len as u64);
        SeqRingBounds {
            oldest_seq,
            next_seq: self.next_seq,
            len: self.len,
            capacity: self.capacity,
        }
    }

    fn read_meta(&self, start_seq: u64, limit: usize) -> (SeqRingReadMeta, u64) {
        let bounds = self.bounds();
        let actual_start_seq = start_seq.max(bounds.oldest_seq).min(bounds.next_seq);
        let end_seq = actual_start_seq
            .saturating_add(limit as u64)
            .min(bounds.next_seq);
        let copied = (end_seq - actual_start_seq) as usize;
        (
            SeqRingReadMeta {
                requested_start_seq: start_seq,
                actual_start_seq,
                next_seq: bounds.next_seq,
                copied,
                clipped: actual_start_seq != start_seq,
                concurrent_miss: false,
            },
            end_seq,
        )
    }

    fn contains_seq(&self, seq: u64) -> bool {
        let bounds = self.bounds();
        seq >= bounds.oldest_seq && seq < bounds.next_seq
    }

    fn slot_index(&self, seq: u64) -> usize {
        (seq % self.capacity as u64) as usize
    }

    fn row_at_seq(&self, seq: u64) -> T {
        self.rows[self.slot_index(seq)]
    }

    fn slices(&self, start_seq: u64, end_seq: u64) -> (&[T], &[T]) {
        let len = (end_seq - start_seq) as usize;
        if len == 0 {
            return (&[], &[]);
        }
        let start = self.slot_index(start_seq);
        let first_len = len.min(self.capacity - start);
        let second_len = len - first_len;
        (
            &self.rows[start..start + first_len],
            &self.rows[0..second_len],
        )
    }
}

impl<T: SeqRingTimedRow> SeqRingState<T> {
    fn first_seq_at_or_after_time(&self, time: f64) -> u64 {
        let bounds = self.bounds();
        for seq in bounds.oldest_seq..bounds.next_seq {
            if self.row_at_seq(seq).seq_ring_time() >= time {
                return seq;
            }
        }
        bounds.next_seq
    }

    fn is_time_before_retained_min(&self, time: f64) -> bool {
        let bounds = self.bounds();
        if bounds.len == 0 {
            return false;
        }
        let mut min_time = self.row_at_seq(bounds.oldest_seq).seq_ring_time();
        for seq in (bounds.oldest_seq + 1)..bounds.next_seq {
            min_time = min_time.min(self.row_at_seq(seq).seq_ring_time());
        }
        time < min_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    struct TimedRow {
        time_ms: u64,
        value: u64,
    }

    impl SeqRingTimedRow for TimedRow {
        fn seq_ring_time(&self) -> f64 {
            self.time_ms as f64
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

        assert_eq!(reader.first_seq_at_or_after_time(1_750.0), Some(1));

        let mut out = Vec::new();
        let meta = reader.copy_from_time(1_750.0, 10, &mut out).unwrap();
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
            .copy_time_range(1_750.0, 2_500.0, 10, &mut out)
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
