//! Shim for mio v0.5.0 that is backed by tokio. Useful to simplify porting this library to tokio.

use std::io;
use tokio::sync::mpsc;

pub struct EventLoop<H: Handler> {
    notify_tx: mpsc::UnboundedSender<H::Message>,
    notify_rx: mpsc::UnboundedReceiver<H::Message>,
}

impl<H: Handler> EventLoop<H> {
    pub fn new() -> io::Result<Self> {
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();

        Ok(Self {
            notify_rx,
            notify_tx,
        })
    }

    pub fn channel(&self) -> Sender<H::Message> {
        self.notify_tx.clone()
    }

    pub fn timeout_ms(&mut self, token: H::Timeout, delay: u64) -> Result<Timeout, TimerError> {
        todo!()
    }

    pub fn clear_timeout(&mut self, timeout: Timeout) -> bool {
        todo!()
    }

    pub fn shutdown(&mut self) {
        todo!()
    }

    pub async fn run(&mut self, handler: &mut H) -> io::Result<()> {
        todo!()
    }
}

pub trait Handler: Sized {
    type Timeout;
    type Message: Send;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, message: Self::Message);
    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Self::Timeout);

    // NOTE: we don't need the other methods that the original mio `Handler` has here.
}

pub type Sender<T> = mpsc::UnboundedSender<T>;

#[derive(Clone, Copy)]
pub struct Timeout;

#[derive(Debug)]
pub struct TimerError;
