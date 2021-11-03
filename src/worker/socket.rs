//! Helpers to simplify work with UdpSocket.

use std::{io, net::SocketAddr};
use tokio::{net::UdpSocket, select};

/// UDP socket that can send/recevie to/from both ipv4 and ipv6 addresses. Implemented as a pair of
/// normal UDP sockets.
pub(crate) enum MultiSocket {
    V4(UdpSocket),
    V6(UdpSocket),
    Both { v4: UdpSocket, v6: UdpSocket },
}

impl MultiSocket {
    /// Create a `MultiSocket` that wraps only a single socket bound to a ipv4 address.
    /// Returns error if the socket is not bound to an ipv4 address.
    pub fn new_v4(socket: UdpSocket) -> io::Result<Self> {
        check_v4(&socket)?;
        Ok(Self::V4(socket))
    }

    /// Create a `MultiSocket` that wraps only a single socket bound to a ipv6 address.
    /// Returns error if the socket is not bound to an ipv6 address.
    pub fn new_v6(socket: UdpSocket) -> io::Result<Self> {
        check_v6(&socket)?;
        Ok(Self::V6(socket))
    }

    /// Create a `MultiSocket` that wraps two sockets - one bound to a ipv4 and the other to a ipv6
    /// addresses.
    /// Returns error if the first socket is not bound to an ipv4 address or if the second socket
    /// is not bound to a ipv6 address.
    pub fn new(socket_v4: UdpSocket, socket_v6: UdpSocket) -> io::Result<Self> {
        check_v4(&socket_v4)?;
        check_v6(&socket_v6)?;
        Ok(Self::Both {
            v4: socket_v4,
            v6: socket_v6,
        })
    }

    /// Send a message to the specified address on the underlying socket depending on the address
    /// family.
    pub async fn send(&self, bytes: &[u8], addr: SocketAddr) -> io::Result<()> {
        // Try to send on the socket with the matching family, if available. If not, send on the
        // other socket which is expected to fail.
        match (self, addr) {
            (Self::V4(socket) | Self::Both { v4: socket, .. }, SocketAddr::V4(_))
            | (Self::V6(socket) | Self::Both { v6: socket, .. }, SocketAddr::V6(_))
            | (Self::V4(socket), SocketAddr::V6(_))
            | (Self::V6(socket), SocketAddr::V4(_)) => send(socket, bytes, addr).await,
        }
    }

    /// Receive a message on both sockets and return whichever finishes first.
    /// This function is cancel safe.
    pub async fn recv(&self) -> io::Result<(Vec<u8>, SocketAddr)> {
        match self {
            Self::V4(socket) => recv(socket).await,
            Self::V6(socket) => recv(socket).await,
            Self::Both { v4, v6 } => select! {
                result = recv(v4) => result,
                result = recv(v6) => result,
            },
        }
    }
}

fn check_v4(socket: &UdpSocket) -> io::Result<()> {
    if socket.local_addr()?.is_ipv4() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "socket not bound to an ipv4 address",
        ))
    }
}

fn check_v6(socket: &UdpSocket) -> io::Result<()> {
    if socket.local_addr()?.is_ipv6() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "socket not bound to an ipv6 address",
        ))
    }
}

pub(crate) async fn send(socket: &UdpSocket, bytes: &[u8], addr: SocketAddr) -> io::Result<()> {
    let mut bytes_sent = 0;

    while bytes_sent < bytes.len() {
        let num_sent = socket.send_to(&bytes[bytes_sent..], addr).await?;
        bytes_sent += num_sent;
    }

    Ok(())
}

/// This function is cancel safe: https://docs.rs/tokio/1.12.0/tokio/net/struct.UdpSocket.html#cancel-safety-6
pub(crate) async fn recv(socket: &UdpSocket) -> io::Result<(Vec<u8>, SocketAddr)> {
    let mut buffer = vec![0u8; 1500];
    let (size, addr) = socket.recv_from(&mut buffer).await?;
    buffer.truncate(size);
    Ok((buffer, addr))
}
