pub(crate) use self::{handler::DhtHandler, socket::Socket};
use crate::{id::InfoHash, transaction::TransactionID};
use std::{collections::HashSet, io, net::SocketAddr};
use thiserror::Error;

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
    TableRefresh,
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

#[derive(Error, Debug)]
pub(crate) enum WorkerError {
    #[error("invalid bencode data")]
    InvalidBencode(#[source] serde_bencode::Error),
    #[error("invalid transaction id")]
    InvalidTransactionId,
    #[error("received unsolicited response")]
    UnsolicitedResponse,
    #[error("socket error")]
    SocketError(#[from] io::Error),
}
