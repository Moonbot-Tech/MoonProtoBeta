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

/// A copyable row that can be stored in the retained dense ring.
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
        let mut ignored_evicted = Vec::new();
        self.push_batch_with_evicted(rows, &mut ignored_evicted);
    }

    /// Append a batch and collect overwritten rows when the ring was full.
    ///
    /// This is the batch counterpart of [`Self::push_with_evicted`]. History
    /// writers that compact old detailed rows into coarse rows must use this
    /// instead of `push_batch` so eviction side effects are not silently lost.
    pub fn push_batch_with_evicted(&mut self, rows: &[T], evicted: &mut Vec<T>) {
        let mut state = self.inner.state.write();
        for &row in rows {
            let seq = state.next_seq;
            let idx = state.slot_index(seq);
            if state.len < state.capacity {
                state.len += 1;
            } else {
                evicted.push(state.rows[idx]);
            }
            state.rows[idx] = row;
            state.next_seq = state.next_seq.wrapping_add(1);
        }
    }

    pub fn replace_seq(&mut self, seq: u64, row: T) -> bool {
        let mut state = self.inner.state.write();
        if !state.contains_seq(seq) {
            return false;
        }
        let idx = state.slot_index(seq);
        state.rows[idx] = row;
        true
    }

    pub fn clear(&mut self) {
        let mut state = self.inner.state.write();
        state.next_seq = 0;
        state.len = 0;
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
        let (start_seq, time_clipped) = state.first_seq_at_or_after_time_with_clip(time);
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
        let (start_seq, time_clipped) = state.first_seq_at_or_after_time_with_clip(from_time);
        let start_seq = start_seq.max(bounds.oldest_seq).min(bounds.next_seq);

        out.clear();
        out.reserve(limit.min(bounds.len));
        let mut end_seq = start_seq;
        let mut slot = state.slot_index(start_seq);
        while end_seq < bounds.next_seq && out.len() < limit {
            let row = state.rows[slot];
            if row.seq_ring_time() >= from_time && row.seq_ring_time() < to_time {
                out.push(row);
            }
            end_seq += 1;
            slot += 1;
            if slot == state.capacity {
                slot = 0;
            }
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
        // Consecutive seqs map to consecutive slots (`seq % capacity`). Hoist the
        // single modulo out of the loop and advance the slot with a wrap instead
        // of a per-element int64 DIV (audit #12 opt #1, hot chart scan).
        let mut slot = self.slot_index(bounds.oldest_seq);
        for seq in bounds.oldest_seq..bounds.next_seq {
            if self.rows[slot].seq_ring_time() >= time {
                return seq;
            }
            slot += 1;
            if slot == self.capacity {
                slot = 0;
            }
        }
        bounds.next_seq
    }

    fn first_seq_at_or_after_time_with_clip(&self, time: f64) -> (u64, bool) {
        let bounds = self.bounds();
        if bounds.len == 0 {
            return (bounds.next_seq, false);
        }
        let mut first = bounds.next_seq;
        let mut min_time = f64::INFINITY;
        let mut slot = self.slot_index(bounds.oldest_seq);
        for seq in bounds.oldest_seq..bounds.next_seq {
            let row_time = self.rows[slot].seq_ring_time();
            if first == bounds.next_seq && row_time >= time {
                first = seq;
            }
            min_time = min_time.min(row_time);
            slot += 1;
            if slot == self.capacity {
                slot = 0;
            }
        }
        (first, time < min_time)
    }
}

#[cfg(test)]
mod tests;
