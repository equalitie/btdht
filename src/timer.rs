use futures_util::Stream;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::time::{self, Sleep};

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct Timeout {
    deadline: Instant,
    id: u64,
}

pub(crate) struct Timer<T> {
    next_id: u64,
    current: Option<CurrentTimerEntry<T>>,
    queue: BTreeMap<Timeout, T>,
}

impl<T> Timer<T> {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            current: None,
            queue: BTreeMap::new(),
        }
    }

    /// Has the timer no scheduled timeouts?
    pub fn is_empty(&self) -> bool {
        self.current.is_none() && self.queue.is_empty()
    }

    pub fn schedule_in(&mut self, deadline: Duration, value: T) -> Timeout {
        self.schedule_at(Instant::now() + deadline, value)
    }

    pub fn schedule_at(&mut self, deadline: Instant, value: T) -> Timeout {
        // If the current timeout is later than the new one, push it back into the queue.
        if let Some(current) = &self.current {
            let key = current.key();

            if deadline < key.deadline {
                let CurrentTimerEntry { value, .. } = self.current.take().unwrap();
                self.queue.insert(key, value);
            }
        }

        let id = self.next_id();
        let key = Timeout { deadline, id };
        self.queue.insert(key, value);

        key
    }

    pub fn cancel(&mut self, timeout: Timeout) -> bool {
        if let Some(current) = &self.current {
            if current.key() == timeout {
                self.current = None;
                return true;
            }
        }

        self.queue.remove(&timeout).is_some()
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }
}

impl<T: Unpin> Stream for Timer<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(current) = &mut self.current {
                match current.sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        let CurrentTimerEntry { value, .. } = self.current.take().unwrap();
                        return Poll::Ready(Some(value));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // TODO: use BTreeMap::pop_first when it becomes stable.
            let (key, value) = if let Some(key) = self.queue.keys().next().copied() {
                self.queue.remove_entry(&key).unwrap()
            } else {
                return Poll::Ready(None);
            };

            self.current = Some(CurrentTimerEntry {
                sleep: Box::pin(time::sleep_until(key.deadline.into())),
                value,
                id: key.id,
            });
        }
    }
}

struct CurrentTimerEntry<T> {
    sleep: Pin<Box<Sleep>>,
    value: T,
    id: u64,
}

impl<T> CurrentTimerEntry<T> {
    fn key(&self) -> Timeout {
        Timeout {
            deadline: self.sleep.deadline().into_std(),
            id: self.id,
        }
    }
}
