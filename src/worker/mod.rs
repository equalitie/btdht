pub(crate) use self::handler::DhtHandler;
use crate::socket::Socket;
use crate::{id::InfoHash, transaction::TransactionID};
use std::{collections::HashSet, fmt, io, net::SocketAddr, time::Duration};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

mod bootstrap;
mod handler;
mod lookup;
mod refresh;

#[derive(Copy, Clone, Debug)]
pub struct State {
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

impl fmt::Display for IpVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::V4 => write!(f, "IPv4"),
            Self::V6 => write!(f, "IPv6"),
        }
    }
}

/// Task that our DHT will execute immediately.
pub(crate) enum OneshotTask {
    /// Load a new bootstrap operation into worker storage.
    StartBootstrap(),
    /// Check bootstrap status. The given sender will be notified when the bootstrap completed,
    /// with an optional timeout.
    CheckBootstrap(oneshot::Sender<bool>, Option<Duration>),
    /// Start a lookup for the given InfoHash.
    StartLookup(StartLookup),
    /// Get the local address the socket is bound to.
    GetLocalAddr(oneshot::Sender<SocketAddr>),
    /// Retrieve debug information.
    GetState(oneshot::Sender<State>),
    /// Retrieve IP:PORT pairs of "good" and "questionable" nodes in the routing table.
    LoadContacts(oneshot::Sender<(HashSet<SocketAddr>, HashSet<SocketAddr>)>),
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
    /// Timeout for user waiting to get bootstrapped.
    UserBootstrappedTimeout(u64),
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
