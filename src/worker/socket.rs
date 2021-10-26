//! Helpers to simplify work with UdpSocket.

use std::{io, net::SocketAddr};
use tokio::{net::UdpSocket, runtime::Handle};

pub(crate) fn blocking_send(
    socket: &UdpSocket,
    message: &[u8],
    addr: SocketAddr,
) -> io::Result<()> {
    Handle::current().block_on(send(socket, message, addr))
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
