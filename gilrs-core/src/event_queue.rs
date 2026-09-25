use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub(crate) const DEFAULT_EVENT_QUEUE_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueResult {
    Enqueued,
    Coalesced,
    Overflowed,
    Stale,
    Closed,
}

#[derive(Debug)]
pub(crate) enum QueueItem<T> {
    Event(T),
    Overflow { dropped: u64 },
}

struct Entry<T> {
    key: Option<u64>,
    epoch: u64,
    value: T,
}

struct State<T> {
    entries: VecDeque<Entry<T>>,
    sender_count: usize,
    receiver_alive: bool,
    closed: bool,
    overflow_pending: bool,
    pending_dropped: u64,
    total_dropped: u64,
    current_epoch: u64,
}

struct Inner<T> {
    state: Mutex<State<T>>,
    ready: Condvar,
    capacity: usize,
}

pub(crate) struct EventSender<T> {
    inner: Arc<Inner<T>>,
}

pub(crate) struct EventReceiver<T> {
    inner: Arc<Inner<T>>,
}

pub(crate) fn bounded<T>(capacity: usize) -> (EventSender<T>, EventReceiver<T>) {
    assert!(capacity > 0, "event queue capacity must be non-zero");

    let inner = Arc::new(Inner {
        state: Mutex::new(State {
            entries: VecDeque::with_capacity(capacity),
            sender_count: 1,
            receiver_alive: true,
            closed: false,
            overflow_pending: false,
            pending_dropped: 0,
            total_dropped: 0,
            current_epoch: 0,
        }),
        ready: Condvar::new(),
        capacity,
    });

    (
        EventSender {
            inner: Arc::clone(&inner),
        },
        EventReceiver { inner },
    )
}

impl<T> EventSender<T> {
    fn lock_state(&self) -> MutexGuard<'_, State<T>> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.lock_state().current_epoch
    }

    pub(crate) fn push_with_epoch(&self, epoch: u64, value: T) -> EnqueueResult {
        self.push_key(epoch, None, value)
    }

    pub(crate) fn push_latest_with_epoch(&self, epoch: u64, key: u64, value: T) -> EnqueueResult {
        self.push_key(epoch, Some(key), value)
    }

    fn push_key(&self, epoch: u64, key: Option<u64>, value: T) -> EnqueueResult {
        let mut state = self.lock_state();
        if state.closed || !state.receiver_alive {
            return EnqueueResult::Closed;
        }
        if epoch != state.current_epoch {
            return EnqueueResult::Stale;
        }

        if state.overflow_pending {
            state.pending_dropped = state.pending_dropped.saturating_add(1);
            state.total_dropped = state.total_dropped.saturating_add(1);
            return EnqueueResult::Overflowed;
        }

        if let Some(key) = key {
            if let Some(entry) = state
                .entries
                .iter_mut()
                .find(|entry| entry.epoch == epoch && entry.key == Some(key))
            {
                entry.value = value;
                return EnqueueResult::Coalesced;
            }
        }

        if state.entries.len() >= self.inner.capacity {
            let dropped = state.entries.len() as u64 + 1;
            state.entries.clear();
            state.overflow_pending = true;
            state.pending_dropped = dropped;
            state.total_dropped = state.total_dropped.saturating_add(dropped);
            self.inner.ready.notify_all();
            return EnqueueResult::Overflowed;
        }

        state.entries.push_back(Entry { key, epoch, value });
        self.inner.ready.notify_one();
        EnqueueResult::Enqueued
    }
}

impl<T> Clone for EventSender<T> {
    fn clone(&self) -> Self {
        let mut state = self.lock_state();
        state.sender_count = state.sender_count.saturating_add(1);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for EventSender<T> {
    fn drop(&mut self) {
        let mut state = self.lock_state();
        state.sender_count = state.sender_count.saturating_sub(1);
        if state.sender_count == 0 {
            state.closed = true;
            self.inner.ready.notify_all();
        }
    }
}

impl<T> EventReceiver<T> {
    fn lock_state(&self) -> MutexGuard<'_, State<T>> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn try_pop(&self) -> Option<QueueItem<T>> {
        let mut state = self.lock_state();
        pop_locked(&mut state)
    }

    pub(crate) fn pop_timeout(&self, timeout: Option<Duration>) -> Option<QueueItem<T>> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let mut state = self.lock_state();

        loop {
            if let Some(item) = pop_locked(&mut state) {
                return Some(item);
            }
            if state.closed {
                return None;
            }

            let Some(deadline) = deadline else {
                state = self
                    .inner
                    .ready
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                continue;
            };

            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let (next_state, wait_result) = self
                .inner
                .ready
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
            if wait_result.timed_out()
                && !state.overflow_pending
                && state.entries.is_empty()
                && !state.closed
            {
                return None;
            }
        }
    }

    pub(crate) fn advance_epoch(&self) -> u64 {
        let mut state = self.lock_state();
        state.current_epoch = state.current_epoch.saturating_add(1);
        state.entries.clear();
        state.overflow_pending = false;
        state.pending_dropped = 0;
        self.inner.ready.notify_all();
        state.current_epoch
    }

    pub(crate) fn dropped_count(&self) -> u64 {
        self.lock_state().total_dropped
    }
}

impl<T> Drop for EventReceiver<T> {
    fn drop(&mut self) {
        let mut state = self.lock_state();
        state.receiver_alive = false;
        state.closed = true;
        state.entries.clear();
        state.overflow_pending = false;
        self.inner.ready.notify_all();
    }
}

fn pop_locked<T>(state: &mut State<T>) -> Option<QueueItem<T>> {
    if state.overflow_pending {
        let dropped = state.pending_dropped;
        state.overflow_pending = false;
        state.pending_dropped = 0;
        return Some(QueueItem::Overflow { dropped });
    }

    while let Some(entry) = state.entries.pop_front() {
        if entry.epoch == state.current_epoch {
            return Some(QueueItem::Event(entry.value));
        }
    }

    None
}

impl<T> fmt::Debug for EventSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventSender").finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for EventReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventReceiver").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn overflow_discards_stale_entries_and_reports_the_count() {
        let (sender, receiver) = bounded(2);
        assert_eq!(
            sender.push_with_epoch(sender.epoch(), 1),
            EnqueueResult::Enqueued
        );
        assert_eq!(
            sender.push_with_epoch(sender.epoch(), 2),
            EnqueueResult::Enqueued
        );
        assert_eq!(
            sender.push_with_epoch(sender.epoch(), 3),
            EnqueueResult::Overflowed
        );
        assert_eq!(
            sender.push_with_epoch(sender.epoch(), 4),
            EnqueueResult::Overflowed
        );

        assert!(matches!(
            receiver.try_pop(),
            Some(QueueItem::Overflow { dropped: 4 })
        ));
        assert!(receiver.try_pop().is_none());
        assert_eq!(receiver.dropped_count(), 4);
    }

    #[test]
    fn latest_values_coalesce_without_changing_queue_order() {
        let (sender, receiver) = bounded(2);
        assert_eq!(
            sender.push_with_epoch(sender.epoch(), "edge"),
            EnqueueResult::Enqueued
        );
        assert_eq!(
            sender.push_latest_with_epoch(sender.epoch(), 7, "axis-old"),
            EnqueueResult::Enqueued
        );
        assert_eq!(
            sender.push_latest_with_epoch(sender.epoch(), 7, "axis-new"),
            EnqueueResult::Coalesced
        );

        assert!(matches!(receiver.try_pop(), Some(QueueItem::Event("edge"))));
        assert!(matches!(
            receiver.try_pop(),
            Some(QueueItem::Event("axis-new"))
        ));
        assert!(receiver.try_pop().is_none());
    }

    #[test]
    fn advancing_epoch_purges_old_events_and_rejects_late_producers() {
        let (sender, receiver) = bounded(2);
        let old_epoch = sender.epoch();
        assert_eq!(
            sender.push_with_epoch(old_epoch, "old"),
            EnqueueResult::Enqueued
        );
        let new_epoch = receiver.advance_epoch();
        assert_ne!(old_epoch, new_epoch);
        assert_eq!(
            sender.push_with_epoch(old_epoch, "late"),
            EnqueueResult::Stale
        );
        assert!(receiver.try_pop().is_none());
        assert_eq!(
            sender.push_with_epoch(new_epoch, "new"),
            EnqueueResult::Enqueued
        );
        assert!(matches!(receiver.try_pop(), Some(QueueItem::Event("new"))));
    }

    #[test]
    fn blocking_pop_wakes_for_a_new_event() {
        let (sender, receiver) = bounded(1);
        let (ready_tx, ready_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            receiver.pop_timeout(None)
        });

        ready_rx.recv().unwrap();
        sender.push_with_epoch(sender.epoch(), 11);
        assert!(matches!(worker.join().unwrap(), Some(QueueItem::Event(11))));
    }

    #[test]
    fn dropping_senders_closes_a_blocking_receiver() {
        let (sender, receiver) = bounded::<i32>(1);
        let (ready_tx, ready_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            receiver.pop_timeout(None)
        });

        ready_rx.recv().unwrap();
        drop(sender);
        assert!(worker.join().unwrap().is_none());
    }
}
