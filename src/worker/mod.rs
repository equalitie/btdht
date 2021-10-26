use self::handler::DhtHandler;
use crate::id::InfoHash;
use crate::routing::table::{self, RoutingTable};
use crate::transaction::TransactionID;
use std::{collections::HashSet, net::SocketAddr};
use tokio::{net::UdpSocket, sync::mpsc, task};

mod bootstrap;
mod handler;
mod lookup;
mod refresh;
mod socket;
mod timer;

/// Task that our DHT will execute immediately.
#[derive(Clone)]
pub(crate) enum OneshotTask {
    /// Load a new bootstrap operation into worker storage.
    StartBootstrap(HashSet<SocketAddr>, HashSet<SocketAddr>),
    /// Start a lookup for the given InfoHash.
    StartLookup(InfoHash, bool),
}

/// Task that our DHT will execute some time later.
#[derive(Copy, Clone, Debug)]
pub(crate) enum ScheduledTaskCheck {
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
    /// The bootstrap failed.
    BootstrapFailed,
    /// Lookup operation for the given InfoHash found a peer.
    PeerFound(InfoHash, SocketAddr),
    /// Lookup operation for the given InfoHash completed.
    LookupCompleted(InfoHash),
}

/// Spawns the necessary workers that make up our local DHT node and connects them via channels
/// so that they can send and receive DHT messages.
pub(crate) fn start_mainline_dht(
    socket: UdpSocket,
    read_only: bool,
    announce_port: Option<u16>,
    command_rx: mpsc::UnboundedReceiver<OneshotTask>,
    event_tx: mpsc::UnboundedSender<DhtEvent>,
) {
    // TODO: Utilize the security extension.
    let routing_table = RoutingTable::new(table::random_node_id());
    let mut handler = DhtHandler::new(
        routing_table,
        socket,
        read_only,
        announce_port,
        command_rx,
        event_tx,
    );

    task::spawn(async move { handler.run().await });
}
