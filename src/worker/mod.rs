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

#[derive(Copy, Clone, Debug)]
pub struct DebugState {
    pub is_running: bool,
    pub bootstrapped: bool,
    pub good_node_count: usize,
    pub questionable_node_count: usize,
    pub bucket_count: usize,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum IpVersion {
    V4,
    V6,
}

/// Task that our DHT will execute immediately.
pub(crate) enum OneshotTask {
    /// Load a new bootstrap operation into worker storage.
    StartBootstrap(),
    /// Check bootstrap status. The given sender will be notified when the bootstrap completed.
    CheckBootstrap(oneshot::Sender<bool>),
    /// Start a lookup for the given InfoHash.
    StartLookup(StartLookup),
    /// Get the local address the socket is bound to.
    GetLocalAddr(oneshot::Sender<SocketAddr>),
    /// Retrieve debug information.
    GetDebugState(oneshot::Sender<DebugState>),
}

pub(crate) struct StartLookup {
    pub info_hash: InfoHash,
    pub announce: bool,
    pub tx: mpsc::UnboundedSender<SocketAddr>,
}

/// Signifies what has timed out in the TableBootstrap class.
#[derive(Copy, Clone, Debug)]
pub(crate) enum BootstrapTimeout {
    Transaction(TransactionID),
    IdleWakeUp,
}

/// Task that our DHT will execute some time later.
#[derive(Copy, Clone, Debug)]
pub(crate) enum ScheduledTaskCheck {
    /// Check the progress of the bucket refresh.
    TableRefresh,
    /// Check the progress of the current bootstrap.
    BootstrapTimeout(BootstrapTimeout),
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

pub(crate) async fn resolve(routers: &HashSet<String>, ip_v: IpVersion) -> HashSet<SocketAddr> {
    futures_util::future::join_all(routers.iter().map(tokio::net::lookup_host))
        .await
        .into_iter()
        .filter_map(|result| result.ok())
        .flatten()
        .filter(|addr| match ip_v {
            IpVersion::V4 => addr.is_ipv4(),
            IpVersion::V6 => addr.is_ipv6(),
        })
        .collect()
}
