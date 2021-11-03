//! Helpers to simplify work with UdpSocket.

use std::{io, net::SocketAddr};
use tokio::{net::UdpSocket, select};

/// UDP socket that can send/recevie to/from both ipv4 and ipv6 addresses. Implemented as a pair of
/// normal UDP sockets.
pub enum MultiSocket {
    /// `MultiSocket` that wraps only a single socket (v4 or v6). Useful on single-stack devices.
    SingleStack(UdpSocket),
    /// `MultiSocket` that wraps both v4 and v6 sockets.
    DualStack { v4: UdpSocket, v6: UdpSocket },
}

impl MultiSocket {
    /// Send a message to the specified address on the underlying socket depending on the address
    /// family.
    pub(crate) async fn send(&self, bytes: &[u8], addr: SocketAddr) -> io::Result<()> {
        match (self, addr) {
            (Self::SingleStack(socket), _)
            | (Self::DualStack { v4: socket, .. }, SocketAddr::V4(_))
            | (Self::DualStack { v6: socket, .. }, SocketAddr::V6(_)) => {
                send(socket, bytes, addr).await
            }
        }
    }

    /// Receive a message on both sockets and return whichever finishes first.
    /// This function is cancel safe.
    pub(crate) async fn recv(&self) -> io::Result<(Vec<u8>, SocketAddr)> {
        match self {
            Self::SingleStack(socket) => recv(socket).await,
            Self::DualStack { v4, v6 } => select! {
                result = recv(v4) => {
                    // if one failed, the other one might still succeed
                    match result {
                        Ok(output) => Ok(output),
                        Err(_) => recv(v6).await,
                    }
                }
                result = recv(v6) => {
                    match result {
                        Ok(output) => Ok(output),
                        Err(_) => recv(v4).await,
                    }
                }
            },
        }
    }
}

async fn send(socket: &UdpSocket, bytes: &[u8], addr: SocketAddr) -> io::Result<()> {
    let mut bytes_sent = 0;

    while bytes_sent < bytes.len() {
        let num_sent = socket.send_to(&bytes[bytes_sent..], addr).await?;
        bytes_sent += num_sent;
    }

    Ok(())
}

// This function is cancel safe: https://docs.rs/tokio/1.12.0/tokio/net/struct.UdpSocket.html#cancel-safety-6
async fn recv(socket: &UdpSocket) -> io::Result<(Vec<u8>, SocketAddr)> {
    let mut buffer = vec![0u8; 1500];
    let (size, addr) = socket.recv_from(&mut buffer).await?;
    buffer.truncate(size);
    Ok((buffer, addr))
}
