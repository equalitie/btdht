//! Shim for mio v0.5.0 that is backed by tokio. Useful to simplify porting this library to tokio.

use crate::worker::timer::Timer;
use futures_util::{
    future::{self, Either},
    pin_mut, StreamExt,
};
use std::{
    io,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task};

pub(crate) use crate::worker::timer::Timeout;

pub(crate) struct EventLoop<H: Handler> {
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

    pub fn channel(&self) -> mpsc::UnboundedSender<H::Message> {
        self.notify_tx.clone()
    }

    pub fn timeout(&mut self, token: H::Timeout, delay: Duration) -> Timeout {
        self.timer.schedule(Instant::now() + delay, token)
    }

    pub fn clear_timeout(&mut self, timeout: Timeout) -> bool {
        self.timer.cancel(timeout)
    }

    pub fn shutdown(&mut self) {
        self.running = false;
    }

    pub async fn run(&mut self, handler: &mut H) {
        self.running = true;

        while self.running {
            self.run_once(handler).await
        }
    }

    async fn run_once(&mut self, handler: &mut H) {
        let event = {
            let notify = self.notify_rx.recv();
            pin_mut!(notify);

            let timeout = self.timer.next();
            pin_mut!(timeout);

            match future::select(notify, timeout).await {
                Either::Left((Some(message), _)) => Either::Left(message),
                Either::Right((Some(token), _)) => Either::Right(token),
                Either::Left((None, _)) | Either::Right((None, _)) => return,
            }
        };

        match event {
            Either::Left(message) => task::block_in_place(|| handler.notify(self, message)),
            Either::Right(token) => task::block_in_place(|| handler.timeout(self, token)),
        }
    }
}

pub(crate) trait Handler: Sized {
    type Timeout: Unpin;
    type Message: Send;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, message: Self::Message);
    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Self::Timeout);

    // NOTE: we don't need the other methods that the original mio `Handler` has.
}
