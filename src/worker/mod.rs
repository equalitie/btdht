use std::{collections::HashSet, io, net::SocketAddr};
use tokio::{net::UdpSocket, sync::mpsc, task};

use crate::id::InfoHash;
use crate::mio;
use crate::routing::table::{self, RoutingTable};
use crate::transaction::TransactionID;

pub mod bootstrap;
pub mod handler;
pub mod lookup;
pub mod messenger;
pub mod refresh;

/// Task that our DHT will execute immediately.
#[derive(Clone)]
pub enum OneshotTask {
    /// Process an incoming message from a remote node.
    Incoming(Vec<u8>, SocketAddr),
    /// Register a sender to send DhtEvents to.
    RegisterSender(mpsc::UnboundedSender<DhtEvent>),
    /// Load a new bootstrap operation into worker storage.
    StartBootstrap(HashSet<SocketAddr>, HashSet<SocketAddr>),
    /// Start a lookup for the given InfoHash.
    StartLookup(InfoHash, bool),
    /// Gracefully shutdown the DHT and associated workers.
    Shutdown(ShutdownCause),
}

/// Task that our DHT will execute some time later.
#[derive(Copy, Clone, Debug)]
pub enum ScheduledTaskCheck {
    /// Check the progress of the bucket refresh.
    TableRefresh(TransactionID),
    /// Check the progress of the current bootstrap.
    BootstrapTimeout(TransactionID),
    /// Check the progress of a current lookup.
    LookupTimeout(TransactionID),
    /// Check the progress of the lookup endgame.
    LookupEndGame(TransactionID),
}

/// Event that occured within the DHT which clients may be interested in.
#[derive(Copy, Clone, Debug)]
pub enum DhtEvent {
    /// DHT completed the bootstrap.
    BootstrapCompleted,
    /// Lookup operation for the given InfoHash found a peer.
    PeerFound(InfoHash, SocketAddr),
    /// Lookup operation for the given InfoHash completed.
    LookupCompleted(InfoHash),
    /// DHT is shutting down for some reason.
    ShuttingDown(ShutdownCause),
}

/// Event that occured within the DHT which caused it to shutdown.
#[derive(Copy, Clone, Debug)]
pub enum ShutdownCause {
    /// DHT failed to bootstrap more than once.
    BootstrapFailed,
    /// Client controlling the DHT intentionally shut it down.
    ClientInitiated,
    /// Cause of shutdown is not specified.
    Unspecified,
}

const OUTGOING_MESSAGE_CAPACITY: usize = 4096;

/// Spawns the necessary workers that make up our local DHT node and connects them via channels
/// so that they can send and receive DHT messages.
pub fn start_mainline_dht(
    socket: UdpSocket,
    read_only: bool,
    announce_port: Option<u16>,
) -> io::Result<mio::Sender<OneshotTask>> {
    let (outgoing_tx, outgoing_rx) = mpsc::channel(OUTGOING_MESSAGE_CAPACITY);

    // TODO: Utilize the security extension.
    let routing_table = RoutingTable::new(table::random_node_id());
    let message_sender =
        handler::create_dht_handler(routing_table, outgoing_tx, read_only, announce_port)?;

    task::spawn(messenger::create(
        socket,
        message_sender.clone(),
        outgoing_rx,
    ));

    Ok(message_sender)
}
