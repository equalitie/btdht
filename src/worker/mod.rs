pub(crate) use self::{handler::DhtHandler, socket::Socket};
use crate::{id::InfoHash, transaction::TransactionID};
use std::{collections::HashSet, io, net::SocketAddr};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

mod bootstrap;
mod handler;
mod lookup;
mod refresh;
mod socket;
mod timer;

/// Task that our DHT will execute immediately.
pub(crate) enum OneshotTask {
    /// Load a new bootstrap operation into worker storage.
    StartBootstrap(HashSet<SocketAddr>, HashSet<SocketAddr>),
    /// Check bootstrap status. The given sender will be notified when the bootstrap completed.
    CheckBootstrap(oneshot::Sender<bool>),
    /// Start a lookup for the given InfoHash.
    StartLookup(StartLookup),
    /// Get the local address the socket is bound to.
    GetLocalAddr(oneshot::Sender<io::Result<SocketAddr>>),
}

pub(crate) struct StartLookup {
    pub info_hash: InfoHash,
    pub announce: bool,
    pub tx: mpsc::UnboundedSender<SocketAddr>,
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

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ActionStatus {
    /// Action is in progress
    Ongoing,
    /// Action completed
    Completed,
}
