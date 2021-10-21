//! Shim for mio v0.5.0 that is backed by tokio. Useful to simplify porting this library to tokio.

use futures_util::{
    future::{self, Either},
    pin_mut, Stream, StreamExt,
};
use std::{
    collections::BTreeMap,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc,
    task,
    time::{self, Sleep},
};

pub struct EventLoop<H: Handler> {
    running: bool,
    notify_tx: mpsc::UnboundedSender<H::Message>,
    notify_rx: mpsc::UnboundedReceiver<H::Message>,
    timer: Timer<H::Timeout>,
}

impl<H: Handler> EventLoop<H> {
    pub fn new() -> io::Result<Self> {
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();

        Ok(Self {
            running: false,
            notify_rx,
            notify_tx,
            timer: Timer::new(),
        })
    }

    pub fn channel(&self) -> Sender<H::Message> {
        self.notify_tx.clone()
    }

    pub fn timeout_ms(&mut self, token: H::Timeout, delay: u64) -> Result<Timeout, TimerError> {
        Ok(self
            .timer
            .schedule(Instant::now() + Duration::from_millis(delay), token))
    }

    pub fn clear_timeout(&mut self, timeout: Timeout) -> bool {
        self.timer.cancel(timeout)
    }

    pub fn shutdown(&mut self) {
        self.running = false;
    }

    pub async fn run(&mut self, handler: &mut H) -> io::Result<()> {
        self.running = true;

        while self.running {
            self.run_once(handler).await?
        }

        Ok(())
    }

    async fn run_once(&mut self, handler: &mut H) -> io::Result<()> {
        let event = {
            let notify = self.notify_rx.recv();
            pin_mut!(notify);

            let timeout = self.timer.next();
            pin_mut!(timeout);

            match future::select(notify, timeout).await {
                Either::Left((Some(message), _)) => Either::Left(message),
                Either::Right((Some(token), _)) => Either::Right(token),
                Either::Left((None, _)) | Either::Right((None, _)) => return Ok(()),
            }
        };

        match event {
            Either::Left(message) => task::block_in_place(|| handler.notify(self, message)),
            Either::Right(token) => task::block_in_place(|| handler.timeout(self, token)),
        }

        Ok(())
    }
}

pub trait Handler: Sized {
    type Timeout: Unpin;
    type Message: Send;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, message: Self::Message);
    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Self::Timeout);

    // NOTE: we don't need the other methods that the original mio `Handler` has.
}

pub type Sender<T> = mpsc::UnboundedSender<T>;

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub struct Timeout {
    deadline: Instant,
    id: u64,
}

#[derive(Debug)]
pub struct TimerError;

struct Timer<T> {
    next_id: u64,
    current: Option<CurrentTimerEntry<T>>,
    queue: BTreeMap<Timeout, T>,
}

impl<T> Timer<T> {
    fn new() -> Self {
        Self {
            next_id: 0,
            current: None,
            queue: BTreeMap::new(),
        }
    }

    fn schedule(&mut self, deadline: Instant, value: T) -> Timeout {
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

    fn cancel(&mut self, timeout: Timeout) -> bool {
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
