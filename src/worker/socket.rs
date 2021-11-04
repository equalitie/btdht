//! Helpers to simplify work with UdpSocket.

use std::{io, net::SocketAddr};
use tokio::net::UdpSocket;

pub(crate) struct Socket(UdpSocket);

impl Socket {
    pub fn new(inner: UdpSocket) -> Self {
        Self(inner)
    }

    pub(crate) async fn send(&self, bytes: &[u8], addr: SocketAddr) -> io::Result<()> {
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

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
}
