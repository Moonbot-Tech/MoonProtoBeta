//! Dense retained ring for hot active-library histories.
//!
//! The UDP protocol path does not take this history lock; it queues typed
//! batches to `StoreWorker`, the single writer. The dense backing array is
//! allocated on the first retained row, then stays behind a short
//! `parking_lot::RwLock`, so unused markets consume only ring metadata while
//! active histories still scan contiguous memory.
//! That cache-friendly layout is what charts and rolling analytics need:
//! rendering bulk-copies ranges, derived calculations scan borrowed slices, and
//! neither path pays per-row atomics.

use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard};

use crate::MoonTime;

/// A copyable row that can be stored in the retained dense ring.
pub trait SeqRingRow: Copy + Default + Send + Sync + 'static {}

impl<T> SeqRingRow for T where T: Copy + Default + Send + Sync + 'static {}

/// Row with a domain time coordinate.
///
/// Domain APIs use this to expose "from time T" and "time range" reads without
/// exposing internal sequence numbers to application code. The retained futures
/// history preserves append order and can contain late resend rows, so timed
/// reads scan the retained sequence instead of assuming monotonic time.
pub trait SeqRingTimedRow: SeqRingRow {
    fn seq_ring_time_ms(&self) -> i64;
}

/// Price range over retained history rows.
///
/// `count` is the number of rows that contributed at least one finite price to
/// the range. Empty scans return `None` from the helper methods instead of a
/// zero-count `PriceRange`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceRange {
    pub min: f32,
    pub max: f32,
    pub count: usize,
}

impl PriceRange {
    fn empty() -> Self {
        Self {
            min: f32::INFINITY,
            max: f32::NEG_INFINITY,
            count: 0,
        }
    }

    fn push_range(&mut self, low: f32, high: f32) {
        if !low.is_finite() || !high.is_finite() {
            return;
        }
        self.min = self.min.min(low.min(high));
        self.max = self.max.max(low.max(high));
        self.count += 1;
    }

    fn into_option(self) -> Option<Self> {
        if self.count == 0 {
            None
        } else {
            Some(self)
        }
    }
}

/// Row that can contribute a price range to retained-history aggregate queries.
pub trait SeqRingPriceRow: SeqRingTimedRow {
    fn seq_ring_price_range(&self) -> Option<(f32, f32)>;
}

/// Quantity/volume sum over retained history rows.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct QtySum {
    pub sum: f64,
    pub count: usize,
}

impl QtySum {
    fn push(&mut self, qty: f64) {
        if qty.is_finite() {
            self.sum += qty;
            self.count += 1;
        }
    }
}

/// Row that can contribute a quantity/volume value to retained-history queries.
pub trait SeqRingQtyRow: SeqRingTimedRow {
    fn seq_ring_qty(&self) -> Option<f64>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeqRingError {
    ZeroCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeqRingBounds {
    #[cfg(any(test, feature = "diagnostics"))]
    pub oldest_seq: u64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    oldest_seq: u64,
    /// One past the newest published sequence.
    #[cfg(any(test, feature = "diagnostics"))]
    pub next_seq: u64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    next_seq: u64,
    #[cfg(any(test, feature = "diagnostics"))]
    pub revision: u64,
    pub len: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeqRingReadMeta {
    #[cfg(any(test, feature = "diagnostics"))]
    pub requested_start_seq: u64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    requested_start_seq: u64,
    #[cfg(any(test, feature = "diagnostics"))]
    pub actual_start_seq: u64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    actual_start_seq: u64,
    /// One past the newest sequence that existed when the read lock was taken.
    #[cfg(any(test, feature = "diagnostics"))]
    pub next_seq: u64,
    #[cfg(not(any(test, feature = "diagnostics")))]
    next_seq: u64,
    #[cfg(any(test, feature = "diagnostics"))]
    pub revision: u64,
    pub copied: usize,
    /// Requested start was older than retention.
    pub clipped: bool,
    /// Always false for the dense locked backend: the read lock prevents slot
    /// overwrite during the copy/view.
    pub concurrent_miss: bool,
}

/// Result of draining new retained rows through a per-consumer cursor.
///
/// This intentionally omits raw sequence numbers from the public contract.
/// Consumers only need to know how many rows were copied, whether their cursor
/// fell behind retention, and whether another drain call is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeqRingDrainMeta {
    pub copied: usize,
    pub clipped: bool,
    pub caught_up: bool,
    /// Always false for the dense locked backend: the read lock prevents slot
    /// overwrite during the copy/view. Reserved for future lock-free backends.
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
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn from_next_seq(next_seq: u64) -> Self {
        Self { next_seq }
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    pub(crate) fn from_next_seq(next_seq: u64) -> Self {
        Self { next_seq }
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn next_seq(self) -> u64 {
        self.next_seq
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn set_next_seq(&mut self, next_seq: u64) {
        self.next_seq = next_seq;
    }
}

pub(crate) struct SeqRingWriter<T: SeqRingRow> {
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
    /// Empty until the first materialized row. `capacity` remains the public
    /// retention limit even while no backing array is needed.
    rows: Box<[T]>,
    capacity: usize,
    next_seq: u64,
    #[cfg(any(test, feature = "diagnostics"))]
    revision: u64,
    len: usize,
}

impl<T: SeqRingRow> SeqRingWriter<T> {
    pub(crate) fn new(capacity: usize) -> Result<(Self, SeqRingReader<T>), SeqRingError> {
        if capacity == 0 {
            return Err(SeqRingError::ZeroCapacity);
        }
        let inner = Arc::new(SeqRingInner {
            state: RwLock::new(SeqRingState {
                rows: Box::new([]),
                capacity,
                next_seq: 0,
                #[cfg(any(test, feature = "diagnostics"))]
                revision: 0,
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

    pub(crate) fn push(&mut self, row: T) -> u64 {
        self.push_with_evicted(row).0
    }

    /// Append a row and return the overwritten row when the ring was full.
    ///
    /// `StoreWorker` uses this to preserve old-trade compaction semantics:
    /// detailed rows that leave retained history can be folded into mini-candle
    /// aggregates instead of disappearing silently.
    pub(crate) fn push_with_evicted(&mut self, row: T) -> (u64, Option<T>) {
        let mut state = self.inner.state.write();
        state.ensure_rows();
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
        state.bump_revision();
        (seq, evicted)
    }

    /// Advance the ring with a default-valued slot without materializing the
    /// backing array. Used by slot-aligned optional companion histories.
    pub(crate) fn push_default_lazy(&mut self) -> u64 {
        let mut state = self.inner.state.write();
        let seq = state.next_seq;
        let idx = state.slot_index(seq);
        if !state.rows.is_empty() {
            state.rows[idx] = T::default();
        }
        if state.len < state.capacity {
            state.len += 1;
        }
        state.next_seq = state.next_seq.wrapping_add(1);
        state.bump_revision();
        seq
    }

    pub(crate) fn push_batch(&mut self, rows: &[T]) {
        let mut ignored_evicted = Vec::new();
        self.push_batch_with_evicted(rows, &mut ignored_evicted);
    }

    /// Append a batch and collect overwritten rows when the ring was full.
    ///
    /// This is the batch counterpart of [`Self::push_with_evicted`]. History
    /// writers that compact old detailed rows into coarse rows must use this
    /// instead of `push_batch` so eviction side effects are not silently lost.
    pub(crate) fn push_batch_with_evicted(&mut self, rows: &[T], evicted: &mut Vec<T>) {
        if rows.is_empty() {
            return;
        }
        let mut state = self.inner.state.write();
        state.ensure_rows();
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
        state.bump_revision();
    }

    #[cfg(test)]
    pub(crate) fn replace_seq(&mut self, seq: u64, row: T) -> bool {
        let mut state = self.inner.state.write();
        if !state.contains_seq(seq) {
            return false;
        }
        state.ensure_rows();
        let idx = state.slot_index(seq);
        state.rows[idx] = row;
        state.bump_revision();
        true
    }

    pub(crate) fn clear(&mut self) {
        let mut state = self.inner.state.write();
        if state.len != 0 || state.next_seq != 0 {
            state.bump_revision();
        }
        state.next_seq = 0;
        state.len = 0;
    }
}

impl<T: SeqRingRow> SeqRingReader<T> {
    pub fn capacity(&self) -> usize {
        self.inner.state.read().capacity
    }

    pub fn bounds(&self) -> SeqRingBounds {
        self.inner.state.read().bounds()
    }

    #[cfg(test)]
    pub(crate) fn is_allocated(&self) -> bool {
        !self.inner.state.read().rows.is_empty()
    }

    pub fn cursor_from_oldest(&self) -> SeqRingCursor {
        SeqRingCursor::from_next_seq(self.bounds().oldest_seq)
    }

    pub fn cursor_from_now(&self) -> SeqRingCursor {
        SeqRingCursor::from_next_seq(self.bounds().next_seq)
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn read_at_seq(&self, seq: u64) -> Option<T> {
        let state = self.inner.read_materialized();
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

    fn copy_from_seq_internal(
        &self,
        start_seq: u64,
        limit: usize,
        out: &mut Vec<T>,
    ) -> SeqRingReadMeta {
        self.with_from_seq_internal(start_seq, limit, |view| {
            view.copy_to(out);
            view.meta()
        })
    }

    /// Clears `out` and copies up to `limit` rows starting at a cursor.
    ///
    /// This is the user-facing "read from saved index" path. The cursor is a
    /// retained-history position, not a global consumed marker: passing it here
    /// does not advance it. Use [`Self::copy_new_since`] for "only new rows".
    pub fn copy_from_cursor(
        &self,
        cursor: SeqRingCursor,
        limit: usize,
        out: &mut Vec<T>,
    ) -> SeqRingReadMeta {
        self.copy_from_seq_internal(cursor.next_seq, limit, out)
    }

    /// Diagnostic/test helper for raw sequence-number reads.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn copy_from_seq(&self, start_seq: u64, limit: usize, out: &mut Vec<T>) -> SeqRingReadMeta {
        self.copy_from_seq_internal(start_seq, limit, out)
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
        let meta = self.copy_from_seq_internal(cursor.next_seq, limit, out);
        cursor.next_seq = meta.actual_start_seq + meta.copied as u64;
        meta
    }

    /// Copy new rows after a per-consumer cursor and report whether the caller
    /// caught up with the retained stream.
    ///
    /// This is the UI/tool-friendly drain contract: callers do not need raw
    /// sequence numbers to distinguish "copied a bounded batch, call again" from
    /// "caught up". The cursor advances to the actual retained start plus the
    /// rows copied, so a clipped stale cursor resumes from the oldest retained
    /// row and then progresses by the copied amount.
    pub fn drain_new_bounded(
        &self,
        cursor: &mut SeqRingCursor,
        limit: usize,
        out: &mut Vec<T>,
    ) -> SeqRingDrainMeta {
        let meta = self.copy_from_seq_internal(cursor.next_seq, limit, out);
        cursor.next_seq = meta.actual_start_seq + meta.copied as u64;
        SeqRingDrainMeta {
            copied: meta.copied,
            clipped: meta.clipped,
            caught_up: cursor.next_seq >= meta.next_seq,
            concurrent_miss: meta.concurrent_miss,
        }
    }

    /// Scan retained rows from a cursor without building a second history.
    ///
    /// The scan runs under the ring read lock and visits rows in retained
    /// sequence order. Use it for aggregate queries such as min/max over a
    /// retained range; use copy methods when the caller needs owned rows.
    ///
    /// Keep `f` short and non-blocking. It runs while this retained-history
    /// ring is read-locked, so it should do simple CPU work over the row, not
    /// UI rendering, logging, I/O, sleeps, or calls back into client code.
    pub fn scan_from_cursor<R, F>(
        &self,
        cursor: SeqRingCursor,
        limit: usize,
        init: R,
        mut f: F,
    ) -> (R, SeqRingReadMeta)
    where
        F: FnMut(R, &T) -> R,
    {
        self.with_from_cursor(cursor, limit, |view| {
            let mut acc = Some(init);
            view.for_each(|row| {
                let current = acc.take().expect("scan accumulator must be present");
                acc = Some(f(current, row));
            });
            (
                acc.expect("scan accumulator must be present after scan"),
                view.meta(),
            )
        })
    }

    fn with_from_seq_internal<R, F>(&self, start_seq: u64, limit: usize, f: F) -> R
    where
        F: FnOnce(SeqRingReadView<'_, T>) -> R,
    {
        let state = self.inner.read_materialized();
        let (meta, end_seq) = state.read_meta(start_seq, limit);
        let (first, second) = state.slices(meta.actual_start_seq, end_seq);
        f(SeqRingReadView {
            first,
            second,
            meta,
        })
    }

    /// Lock the dense ring and expose a retained range from a cursor as
    /// zero-copy slices for the duration of `f`.
    ///
    /// Keep `f` short and non-blocking. This is the low-level borrowed-slice
    /// path: the read lock is held until `f` returns.
    pub fn with_from_cursor<R, F>(&self, cursor: SeqRingCursor, limit: usize, f: F) -> R
    where
        F: FnOnce(SeqRingReadView<'_, T>) -> R,
    {
        self.with_from_seq_internal(cursor.next_seq, limit, f)
    }

    /// Diagnostic/test helper for raw sequence-number zero-copy reads.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn with_from_seq<R, F>(&self, start_seq: u64, limit: usize, f: F) -> R
    where
        F: FnOnce(SeqRingReadView<'_, T>) -> R,
    {
        self.with_from_seq_internal(start_seq, limit, f)
    }

    pub fn with_last<R, F>(&self, limit: usize, f: F) -> R
    where
        F: FnOnce(SeqRingReadView<'_, T>) -> R,
    {
        let state = self.inner.read_materialized();
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
    /// Return a cursor positioned at the first retained row whose domain time is
    /// greater than or equal to `time`.
    ///
    /// If no retained row is new enough, the cursor points at `next_seq`, so
    /// copying from it returns zero rows until new data arrives.
    pub fn cursor_at_or_after_time(&self, time: MoonTime) -> SeqRingCursor {
        SeqRingCursor::from_next_seq(
            self.inner
                .read_materialized()
                .first_seq_at_or_after_time_ms(time.unix_millis()),
        )
    }

    /// Diagnostic/test helper for raw sequence-number lookup by time.
    #[cfg(any(test, feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn first_seq_at_or_after_time(&self, time: MoonTime) -> Option<u64> {
        Some(self.cursor_at_or_after_time(time).next_seq())
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
        time: MoonTime,
        limit: usize,
        out: &mut Vec<T>,
    ) -> SeqRingReadMeta {
        let state = self.inner.read_materialized();
        let (start_seq, time_clipped) =
            state.first_seq_at_or_after_time_with_clip_ms(time.unix_millis());
        let (mut meta, end_seq) = state.read_meta(start_seq, limit);
        meta.clipped |= time_clipped;
        let (first, second) = state.slices(meta.actual_start_seq, end_seq);
        out.clear();
        out.reserve(meta.copied);
        out.extend_from_slice(first);
        out.extend_from_slice(second);
        meta
    }

    /// Millisecond-domain variant of [`Self::copy_from_time`].
    pub fn copy_from_time_ms(
        &self,
        time_ms: i64,
        limit: usize,
        out: &mut Vec<T>,
    ) -> SeqRingReadMeta {
        self.copy_from_time(MoonTime::from_unix_millis(time_ms), limit, out)
    }

    /// Clears `out` and copies rows with `from_time <= row.time < to_time`.
    pub fn copy_time_range(
        &self,
        from_time: MoonTime,
        to_time: MoonTime,
        limit: usize,
        out: &mut Vec<T>,
    ) -> SeqRingReadMeta {
        out.clear();
        out.reserve(limit.min(self.capacity()));
        let ((), meta) = self.fold_time_range(
            from_time.unix_millis(),
            to_time.unix_millis(),
            limit,
            (),
            |(), row| {
                out.push(*row);
            },
        );
        meta
    }

    /// Millisecond-domain variant of [`Self::copy_time_range`].
    pub fn copy_time_range_ms(
        &self,
        from_ms: i64,
        to_ms: i64,
        limit: usize,
        out: &mut Vec<T>,
    ) -> SeqRingReadMeta {
        self.copy_time_range(
            MoonTime::from_unix_millis(from_ms),
            MoonTime::from_unix_millis(to_ms),
            limit,
            out,
        )
    }

    fn fold_time_range<R, F>(
        &self,
        from_ms: i64,
        to_ms: i64,
        limit: usize,
        init: R,
        mut f: F,
    ) -> (R, SeqRingReadMeta)
    where
        F: FnMut(R, &T) -> R,
    {
        let state = self.inner.read_materialized();
        let bounds = state.bounds();
        let (start_seq, time_clipped) = state.first_seq_at_or_after_time_with_clip_ms(from_ms);
        let start_seq = start_seq.max(bounds.oldest_seq).min(bounds.next_seq);
        let mut acc = Some(init);
        let mut copied = 0usize;

        if limit > 0 && from_ms < to_ms {
            let mut seq = start_seq;
            let mut slot = state.slot_index(start_seq);
            while seq < bounds.next_seq && copied < limit {
                let row = state.rows[slot];
                let row_time_ms = row.seq_ring_time_ms();
                if row_time_ms >= from_ms && row_time_ms < to_ms {
                    let current = acc.take().expect("time-range accumulator must be present");
                    acc = Some(f(current, &row));
                    copied += 1;
                }
                seq += 1;
                slot += 1;
                if slot == state.capacity {
                    slot = 0;
                }
            }
        }

        (
            acc.expect("time-range accumulator must be present after scan"),
            SeqRingReadMeta {
                requested_start_seq: start_seq,
                actual_start_seq: start_seq,
                next_seq: bounds.next_seq,
                #[cfg(any(test, feature = "diagnostics"))]
                revision: bounds.revision,
                copied,
                clipped: time_clipped,
                concurrent_miss: false,
            },
        )
    }
}

impl<T: SeqRingPriceRow> SeqRingReader<T> {
    /// Return the finite price range for a retained cursor window.
    ///
    /// The aggregate is computed inside MoonProto with a tight bounded scan, so
    /// callers do not need to run arbitrary user code under the ring read lock
    /// for common min/max queries.
    pub fn price_range_from_cursor(
        &self,
        cursor: SeqRingCursor,
        limit: usize,
    ) -> (Option<PriceRange>, SeqRingReadMeta) {
        self.with_from_cursor(cursor, limit, |view| {
            let mut range = PriceRange::empty();
            view.for_each(|row| {
                if let Some((low, high)) = row.seq_ring_price_range() {
                    range.push_range(low, high);
                }
            });
            (range.into_option(), view.meta())
        })
    }

    /// Return the finite price range for `from_time <= row.time < to_time`.
    pub fn price_range_time(
        &self,
        from_time: MoonTime,
        to_time: MoonTime,
        limit: usize,
    ) -> (Option<PriceRange>, SeqRingReadMeta) {
        self.price_range_time_ms(from_time.unix_millis(), to_time.unix_millis(), limit)
    }

    /// Millisecond-domain variant of [`Self::price_range_time`].
    pub fn price_range_time_ms(
        &self,
        from_ms: i64,
        to_ms: i64,
        limit: usize,
    ) -> (Option<PriceRange>, SeqRingReadMeta) {
        let (range, meta) = self.fold_time_range(
            from_ms,
            to_ms,
            limit,
            PriceRange::empty(),
            |mut range, row| {
                if let Some((low, high)) = row.seq_ring_price_range() {
                    range.push_range(low, high);
                }
                range
            },
        );
        (range.into_option(), meta)
    }
}

impl<T: SeqRingQtyRow> SeqRingReader<T> {
    /// Sum finite quantity/volume values for a retained cursor window.
    pub fn qty_sum_from_cursor(
        &self,
        cursor: SeqRingCursor,
        limit: usize,
    ) -> (QtySum, SeqRingReadMeta) {
        self.with_from_cursor(cursor, limit, |view| {
            let mut sum = QtySum::default();
            view.for_each(|row| {
                if let Some(qty) = row.seq_ring_qty() {
                    sum.push(qty);
                }
            });
            (sum, view.meta())
        })
    }

    /// Sum finite quantity/volume values for `from_time <= row.time < to_time`.
    pub fn qty_sum_time(
        &self,
        from_time: MoonTime,
        to_time: MoonTime,
        limit: usize,
    ) -> (QtySum, SeqRingReadMeta) {
        self.qty_sum_time_ms(from_time.unix_millis(), to_time.unix_millis(), limit)
    }

    /// Millisecond-domain variant of [`Self::qty_sum_time`].
    pub fn qty_sum_time_ms(
        &self,
        from_ms: i64,
        to_ms: i64,
        limit: usize,
    ) -> (QtySum, SeqRingReadMeta) {
        self.fold_time_range(from_ms, to_ms, limit, QtySum::default(), |mut sum, row| {
            if let Some(qty) = row.seq_ring_qty() {
                sum.push(qty);
            }
            sum
        })
    }
}

impl<T: SeqRingRow> SeqRingState<T> {
    fn ensure_rows(&mut self) {
        if self.rows.is_empty() {
            self.rows = vec![T::default(); self.capacity].into_boxed_slice();
        }
    }

    fn bounds(&self) -> SeqRingBounds {
        let oldest_seq = self.next_seq.saturating_sub(self.len as u64);
        SeqRingBounds {
            oldest_seq,
            next_seq: self.next_seq,
            #[cfg(any(test, feature = "diagnostics"))]
            revision: self.revision,
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
                #[cfg(any(test, feature = "diagnostics"))]
                revision: bounds.revision,
                copied,
                clipped: actual_start_seq != start_seq,
                concurrent_miss: false,
            },
            end_seq,
        )
    }

    #[cfg(any(test, feature = "diagnostics"))]
    fn contains_seq(&self, seq: u64) -> bool {
        let bounds = self.bounds();
        seq >= bounds.oldest_seq && seq < bounds.next_seq
    }

    fn slot_index(&self, seq: u64) -> usize {
        (seq % self.capacity as u64) as usize
    }

    #[cfg(any(test, feature = "diagnostics"))]
    fn bump_revision(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }

    #[cfg(not(any(test, feature = "diagnostics")))]
    fn bump_revision(&mut self) {}

    #[cfg(any(test, feature = "diagnostics"))]
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

impl<T: SeqRingRow> SeqRingInner<T> {
    fn read_materialized(&self) -> RwLockReadGuard<'_, SeqRingState<T>> {
        let state = self.state.read();
        if state.len == 0 || !state.rows.is_empty() {
            return state;
        }
        drop(state);

        let mut state = self.state.write();
        state.ensure_rows();
        drop(state);
        self.state.read()
    }
}

impl<T: SeqRingTimedRow> SeqRingState<T> {
    fn first_seq_at_or_after_time_ms(&self, time_ms: i64) -> u64 {
        let bounds = self.bounds();
        // Consecutive seqs map to consecutive slots (`seq % capacity`). Hoist
        // the single modulo out of the loop and advance the slot with a cheap
        // wrap, so chart/history scans do not spend work on per-row int64 DIV.
        let mut slot = self.slot_index(bounds.oldest_seq);
        for seq in bounds.oldest_seq..bounds.next_seq {
            if self.rows[slot].seq_ring_time_ms() >= time_ms {
                return seq;
            }
            slot += 1;
            if slot == self.capacity {
                slot = 0;
            }
        }
        bounds.next_seq
    }

    fn first_seq_at_or_after_time_with_clip_ms(&self, time_ms: i64) -> (u64, bool) {
        let bounds = self.bounds();
        if bounds.len == 0 {
            return (bounds.next_seq, false);
        }
        let mut first = bounds.next_seq;
        let mut min_time_ms = i64::MAX;
        let mut slot = self.slot_index(bounds.oldest_seq);
        for seq in bounds.oldest_seq..bounds.next_seq {
            let row_time_ms = self.rows[slot].seq_ring_time_ms();
            if first == bounds.next_seq && row_time_ms >= time_ms {
                first = seq;
            }
            min_time_ms = min_time_ms.min(row_time_ms);
            slot += 1;
            if slot == self.capacity {
                slot = 0;
            }
        }
        (first, time_ms < min_time_ms)
    }
}

#[cfg(test)]
mod tests;
