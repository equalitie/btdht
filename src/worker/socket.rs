//! Helpers to simplify work with UdpSocket.

use super::IpVersion;
use crate::SocketTrait;
use async_trait::async_trait;
use std::{io, net::SocketAddr};
use tokio::net::UdpSocket;

pub struct Socket(Box<dyn SocketTrait + Send + Sync + 'static>, SocketAddr);

impl Socket {
    pub fn new<S: SocketTrait + Send + Sync + 'static>(inner: S) -> io::Result<Self> {
        let inner = Box::new(inner);
        let local_addr = inner.local_addr()?;
        Ok(Self(inner, local_addr))
    }

    pub(crate) async fn send(&self, bytes: &[u8], addr: SocketAddr) -> io::Result<()> {
        // Note: if the socket fails to send the entire buffer, then there is no point in trying to
        // send the rest (no node will attempt to reassemble two or more datagrams into a
        // meaningful message).
        self.0.send_to(&bytes, &addr).await?;
        Ok(())
    }

    /// This function is cancel safe: https://docs.rs/tokio/1.12.0/tokio/net/struct.UdpSocket.html#cancel-safety-6
    pub(crate) async fn recv(&mut self) -> io::Result<(Vec<u8>, SocketAddr)> {
        let mut buffer = vec![0u8; 1500];
        let (size, addr) = self.0.recv_from(&mut buffer).await?;
        buffer.truncate(size);
        Ok((buffer, addr))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.1
    }

    pub fn ip_version(&self) -> IpVersion {
        match self.1 {
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

    async fn recv_from(&mut self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        UdpSocket::recv_from(self, buf).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        UdpSocket::local_addr(self)
    }
}
