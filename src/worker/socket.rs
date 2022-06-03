//! Helpers to simplify work with UdpSocket.

use super::IpVersion;
use std::{io, net::SocketAddr};
use tokio::net::UdpSocket;

pub struct Socket(UdpSocket, SocketAddr);

impl Socket {
    pub fn new(inner: UdpSocket) -> io::Result<Self> {
        let local_addr = inner.local_addr()?;
        Ok(Self(inner, local_addr))
    }

    pub(crate) async fn send(&self, bytes: &[u8], addr: SocketAddr) -> io::Result<()> {
        log::debug!("sending to {:?}", addr);
        let mut bytes_sent = 0;

        while bytes_sent < bytes.len() {
            let num_sent = self.0.send_to(&bytes[bytes_sent..], addr).await?;
            bytes_sent += num_sent;
        }

        Ok(())
    }

    /// This function is cancel safe: https://docs.rs/tokio/1.12.0/tokio/net/struct.UdpSocket.html#cancel-safety-6
    pub(crate) async fn recv(&self) -> io::Result<(Vec<u8>, SocketAddr)> {
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
