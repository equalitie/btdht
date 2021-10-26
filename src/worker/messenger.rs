use crate::worker::OneshotTask;
use std::net::SocketAddr;
use tokio::{net::UdpSocket, select, sync::mpsc};

pub(crate) async fn create(
    socket: UdpSocket,
    incoming_tx: mpsc::UnboundedSender<OneshotTask>,
    outgoing_rx: mpsc::Receiver<(Vec<u8>, SocketAddr)>,
) {
    select! {
        _ = handle_incoming_messages(&socket, incoming_tx) => (),
        _ = handle_outgoing_messages(&socket, outgoing_rx) => (),
    }
}

async fn handle_incoming_messages(
    socket: &UdpSocket,
    incoming_tx: mpsc::UnboundedSender<OneshotTask>,
) {
    let mut channel_is_open = true;

    while channel_is_open {
        let mut buffer = vec![0u8; 1500];

        select! {
            result = socket.recv_from(&mut buffer) => {
                match result {
                    Ok((size, addr)) => {
                        buffer.truncate(size);
                        channel_is_open = send_message(&incoming_tx, buffer, addr);
                    }
                    Err(_) =>
                        warn!("bip_dht: Incoming messenger failed to receive bytes..."),
                }
            }
            _ = incoming_tx.closed() => {
                channel_is_open = false;
            }
        }
    }

    info!("bip_dht: Incoming messenger received a channel hangup, exiting thread...");
}

fn send_message(
    send: &mpsc::UnboundedSender<OneshotTask>,
    bytes: Vec<u8>,
    addr: SocketAddr,
) -> bool {
    send.send(OneshotTask::Incoming(bytes, addr)).is_ok()
}

async fn handle_outgoing_messages(
    socket: &UdpSocket,
    mut outgoing_rx: mpsc::Receiver<(Vec<u8>, SocketAddr)>,
) {
    while let Some((message, addr)) = outgoing_rx.recv().await {
        send_bytes(socket, &message[..], addr).await;
    }

    info!("bip_dht: Outgoing messenger received a channel hangup, exiting thread...");
}

async fn send_bytes(socket: &UdpSocket, bytes: &[u8], addr: SocketAddr) {
    let mut bytes_sent = 0;

    while bytes_sent != bytes.len() {
        if let Ok(num_sent) = socket.send_to(&bytes[bytes_sent..], addr).await {
            bytes_sent += num_sent;
        } else {
            // TODO: Maybe shut down in this case, will fail on every write...
            warn!(
                "bip_dht: Outgoing messenger failed to write {} bytes to {}; {} bytes written \
                   before error...",
                bytes.len(),
                addr,
                bytes_sent
            );
            break;
        }
    }
}
