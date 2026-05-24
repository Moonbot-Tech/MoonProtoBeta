//! No-cap application event queue.
//!
//! This is the Rust analogue of Delphi's "queue work for the app/UI side"
//! boundary: correctness never depends on a fixed capacity. Diagnostics may
//! observe queue length, but the queue itself must not drop or reject events.

#[derive(Debug, Clone)]
pub(crate) struct AppQueue<T> {
    items: Vec<T>,
    max_len: usize,
}

impl<T> Default for AppQueue<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            max_len: 0,
        }
    }
}

impl<T> AppQueue<T> {
    pub(crate) fn as_slice(&self) -> &[T] {
        &self.items
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn max_len(&self) -> usize {
        self.max_len
    }

    pub(crate) fn clear(&mut self) {
        self.items.clear();
    }

    pub(crate) fn take(&mut self) -> Vec<T> {
        std::mem::take(&mut self.items)
    }

    pub(crate) fn extend<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = T>,
    {
        self.items.extend(items);
        self.max_len = self.max_len.max(self.items.len());
    }
}
