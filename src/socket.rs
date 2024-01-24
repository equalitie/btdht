//! Helpers to simplify work with UdpSocket.

use super::IpVersion;
use crate::{
    message::{Message, TransactionId},
    SocketTrait,
};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::{net::UdpSocket, time::Sleep};

type Transactions = HashMap<(SocketAddr, TransactionId), Arc<Mutex<RespondedInner>>>;

pub struct Socket {
    inner_socket: Box<dyn SocketTrait + Send + Sync + 'static>,
    local_addr: SocketAddr,
    transactions: Arc<Mutex<Transactions>>,
}

impl Socket {
    pub fn new<S: SocketTrait + Send + Sync + 'static>(inner: S) -> io::Result<Self> {
        let local_addr = inner.local_addr()?;
        let inner_socket = Box::new(inner);
        Ok(Self {
            inner_socket,
            local_addr,
            transactions: Arc::new(Mutex::new(Default::default())),
        })
    }

    pub(crate) async fn send(&self, message: &Message, addr: SocketAddr) -> io::Result<()> {
        log::trace!("Sending to {addr:?} {message:?}");
        // Note: if the socket fails to send the entire buffer, then there is no point in trying to
        // send the rest (no node will attempt to reassemble two or more datagrams into a
        // meaningful message).
        self.inner_socket.send_to(&message.encode(), &addr).await?;
        Ok(())
    }

    /// Send the message and return a future on which we can await the response.
    pub(crate) async fn send_request(
        &self,
        message: &Message,
        addr: SocketAddr,
        timeout: Duration,
    ) -> io::Result<Responded> {
        let responded = self.responded(addr, message.transaction_id.clone(), timeout);
        self.send(message, addr).await?;
        Ok(responded)
    }

    /// This function is cancel safe: https://docs.rs/tokio/1.12.0/tokio/net/struct.UdpSocket.html#cancel-safety-6
    ///
    /// NOTE: This function is in a limbo state right now. Originally, it just received a message
    /// and returned the (Message, SocketAddr) pair, then the caller would decide what handler
    /// should handle it. Using it that way created a state machine that worked, but was hard to
    /// modify when we wanted more features (e.g. rebootstra, IP reuse,...). Later the
    /// `send_request` function was added which should allow us to receive responses directly in
    /// the code where requests are being sent. To avoid a complete and sudden rewrite, both
    /// approaches are now supported but would be good if we gradually switch to the latter.
    pub(crate) async fn recv(&self) -> io::Result<(Message, SocketAddr)> {
        let mut buffer = vec![0u8; 1500];
        loop {
            let r = self.inner_socket.recv_from(&mut buffer).await;
            let (size, addr) = r?;
            match Message::decode(&buffer[0..size]) {
                Ok(message) => {
                    if let Some(responded) = self
                        .transactions
                        .lock()
                        .unwrap()
                        .remove(&(addr, message.transaction_id.clone()))
                    {
                        responded.lock().unwrap().make_ready(message);
                    } else {
                        return Ok((message, addr));
                    }
                }
                Err(_) => {
                    log::warn!(
                        "{}: Failed decode incoming message from {addr:?}",
                        self.ip_version()
                    );
                }
            }
        }
    }

    fn responded(
        &self,
        from: SocketAddr,
        transaction_id: TransactionId,
        timeout: Duration,
    ) -> Responded {
        let inner = Arc::new(Mutex::new(RespondedInner::new(timeout)));
        assert!(self
            .transactions
            .lock()
            .unwrap()
            .insert((from, transaction_id.clone()), inner.clone())
            .is_none());
        Responded {
            from,
            transaction_id,
            inner,
            transactions: self.transactions.clone(),
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn ip_version(&self) -> IpVersion {
        match self.local_addr {
            SocketAddr::V4(_) => IpVersion::V4,
            SocketAddr::V6(_) => IpVersion::V6,
        }
    }
}

#[async_trait]
impl SocketTrait for UdpSocket {
    async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<()> {
        UdpSocket::send_to(self, buf, target).await.map(|_| ())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        UdpSocket::recv_from(self, buf).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        UdpSocket::local_addr(self)
    }
}

/// Future awaiting a response
pub(crate) struct Responded {
    from: SocketAddr,
    transaction_id: TransactionId,
    inner: Arc<Mutex<RespondedInner>>,
    transactions: Arc<Mutex<Transactions>>,
}

impl Drop for Responded {
    fn drop(&mut self) {
        self.transactions
            .lock()
            .unwrap()
            .remove(&(self.from, self.transaction_id.clone()));
    }
}

struct RespondedInner {
    sleep: Pin<Box<Sleep>>,
    message: Option<Message>,
    waker: Option<Waker>,
}

impl RespondedInner {
    fn new(timeout: Duration) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(timeout)),
            message: None,
            waker: None,
        }
    }

    fn make_ready(&mut self, message: Message) {
        // Should not happen because we remove `self` from `transactions` when first response
        // arrives.
        assert!(self.message.is_none());

        self.message = Some(message);

        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl Future for Responded {
    type Output = Option<(Message, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut this = self.inner.lock().unwrap();

        if let Some(message) = this.message.take() {
            return Poll::Ready(Some((message, self.from)));
        }

        match this.sleep.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(None),
            Poll::Pending => {
                if this.waker.is_none() {
                    this.waker = Some(cx.waker().clone());
                }

                Poll::Pending
            }
        }
    }
}
