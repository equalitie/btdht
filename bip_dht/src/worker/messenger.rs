use futures_util::{
    future::{self, Either},
    pin_mut,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::UdpSocket, sync::mpsc, task};

use crate::mio::Sender;
use crate::worker::OneshotTask;

const OUTGOING_MESSAGE_CAPACITY: usize = 4096;

pub fn create_outgoing_messenger(socket: Arc<UdpSocket>) -> mpsc::Sender<(Vec<u8>, SocketAddr)> {
    let (send, mut recv) = mpsc::channel::<(Vec<_>, _)>(OUTGOING_MESSAGE_CAPACITY);

    task::spawn(async move {
        while let Some((message, addr)) = recv.recv().await {
            send_bytes(&socket, &message[..], addr).await;
        }

        info!("bip_dht: Outgoing messenger received a channel hangup, exiting thread...");
    });

    send
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

pub fn create_incoming_messenger(socket: Arc<UdpSocket>, send: Sender<OneshotTask>) {
    task::spawn(async move {
        let mut channel_is_open = true;

        while channel_is_open {
            let mut buffer = vec![0u8; 1500];

            let result = {
                let recv = socket.recv_from(&mut buffer);
                pin_mut!(recv);

                let closed = send.closed();
                pin_mut!(closed);

                match future::select(recv, closed).await {
                    Either::Left((result, _)) => Some(result),
                    Either::Right(_) => None,
                }
            };

            match result {
                Some(Ok((size, addr))) => {
                    buffer.truncate(size);
                    channel_is_open = send_message(&send, buffer, addr);
                }
                Some(Err(_)) => {
                    warn!("bip_dht: Incoming messenger failed to receive bytes...")
                }
                None => {
                    channel_is_open = false;
                }
            }
        }

        info!("bip_dht: Incoming messenger received a channel hangup, exiting thread...");
    });
}

fn send_message(send: &Sender<OneshotTask>, bytes: Vec<u8>, addr: SocketAddr) -> bool {
    send.send(OneshotTask::Incoming(bytes, addr)).is_ok()
}
