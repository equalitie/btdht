use crate::{
    action::{OneshotTask, StartLookup, State},
    handler::DhtHandler,
    info_hash::{InfoHash, NodeId},
    socket::Socket,
    table::RoutingTable,
    SocketTrait,
};
use futures_util::Stream;
use std::{
    collections::HashSet,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    task,
};

/// Maintains a Distributed Hash (Routing) Table.
///
/// This type is cheaply cloneable where each clone refers to the same underlying DHT instance. This
/// is useful to be able to issue DHT operations from multiple tasks/threads.
///
/// # IPv6
///
/// This implementation supports IPv6 as per [BEP32](https://www.bittorrent.org/beps/bep_0032.html).
/// To enable dual-stack DHT (use both IPv4 and IPv6), one needs to create two separate
/// `MainlineDht` instances, one bound to an IPv4 and the other to an IPv6 address. It is
/// recommended that both instances use the same node id ([`DhtBuilder::set_node_id`]). Any lookup
/// should then be performed on both instances and their results aggregated.
#[derive(Clone)]
pub struct MainlineDht {
    send: mpsc::UnboundedSender<OneshotTask>,
}

impl MainlineDht {
    /// Create a new DhtBuilder.
    pub fn builder() -> DhtBuilder {
        DhtBuilder {
            nodes: HashSet::new(),
            routers: HashSet::new(),
            read_only: true,
            announce_port: None,
            node_id: None,
        }
    }

    /// Start the MainlineDht with the given DhtBuilder.
    fn with_builder(builder: DhtBuilder, socket: Socket) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        // TODO: Utilize the security extension.
        let routing_table = RoutingTable::new(builder.node_id.unwrap_or_else(rand::random));
        let handler = DhtHandler::new(
            routing_table,
            socket,
            builder.read_only,
            builder.routers,
            builder.nodes,
            builder.announce_port,
            command_rx,
        );

        if command_tx.send(OneshotTask::StartBootstrap()).is_err() {
            // `unreachable` is OK here because the corresponding receiver definitely exists at
            // this point inside `handler`.
            unreachable!()
        }

        task::spawn(handler.run());

        Self { send: command_tx }
    }

    /// Get the state of the DHT state machine, can be used for debugging.
    pub async fn get_state(&self) -> Option<State> {
        let (tx, rx) = oneshot::channel();

        if self.send.send(OneshotTask::GetState(tx)).is_err() {
            None
        } else {
            rx.await.ok()
        }
    }

    /// Waits until the DHT bootstrap completes, or returns immediately if it already completed.
    /// Returns whether the bootstrap was successful.
    pub async fn bootstrapped(&self, timeout: Option<Duration>) -> bool {
        let (tx, rx) = oneshot::channel();

        if self
            .send
            .send(OneshotTask::CheckBootstrap(tx, timeout))
            .is_err()
        {
            // handler has shut down, consider this as bootstrap failure.
            false
        } else {
            rx.await.unwrap_or(false)
        }
    }

    /// Perform a search for the given InfoHash with an optional announce on the closest nodes.
    ///
    ///
    /// Announcing will place your contact information in the DHT so others performing lookups
    /// for the InfoHash will be able to find your contact information and initiate a handshake.
    ///
    /// If the initial bootstrap has not finished, the search will be queued and executed once
    /// the bootstrap has completed.
    pub fn search(&self, info_hash: InfoHash, announce: bool) -> SearchStream {
        let (tx, rx) = mpsc::unbounded_channel();

        if self
            .send
            .send(OneshotTask::StartLookup(StartLookup {
                info_hash,
                announce,
                tx,
            }))
            .is_err()
        {
            log::error!("failed to start search - DhtHandler has shut down");
        }

        SearchStream(rx)
    }

    /// Get the local address this DHT instance is bound to
    pub async fn local_addr(&self) -> io::Result<SocketAddr> {
        let (tx, rx) = oneshot::channel();

        fn error() -> io::Error {
            io::Error::new(io::ErrorKind::Other, "DhtHandler has shut down")
        }

        self.send
            .send(OneshotTask::GetLocalAddr(tx))
            .map_err(|_| error())?;

        rx.await.map_err(|_| error())
    }

    /// Return IP:PORT pairs of "good" and "questionable" nodes from the routing table.
    pub async fn load_contacts(&self) -> io::Result<(HashSet<SocketAddr>, HashSet<SocketAddr>)> {
        let (tx, rx) = oneshot::channel();

        fn error() -> io::Error {
            io::Error::new(io::ErrorKind::Other, "DhtHandler has shut down")
        }

        self.send
            .send(OneshotTask::LoadContacts(tx))
            .map_err(|_| error())?;

        rx.await.map_err(|_| error())
    }
}

/// Stream returned from [`MainlineDht::search()`]
#[must_use = "streams do nothing unless polled"]
pub struct SearchStream(mpsc::UnboundedReceiver<SocketAddr>);

impl Stream for SearchStream {
    type Item = SocketAddr;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_recv(cx)
    }
}

// ----------------------------------------------------------------------------//

/// Stores information for initializing a DHT.
#[derive(Debug)]
pub struct DhtBuilder {
    nodes: HashSet<SocketAddr>,
    routers: HashSet<String>,
    read_only: bool,
    announce_port: Option<u16>,
    node_id: Option<NodeId>,
}

impl DhtBuilder {
    /// Add nodes which will be distributed within our routing table.
    pub fn add_node(mut self, node_addr: SocketAddr) -> DhtBuilder {
        self.nodes.insert(node_addr);
        self
    }

    /// Add a router which will let us gather nodes if our routing table is ever empty.
    ///
    /// The difference between routers and nodes is that routers are not added to the routing table.
    pub fn add_router(mut self, router: String) -> DhtBuilder {
        self.routers.insert(router);
        self
    }

    /// Add routers. Same as calling `add_router` multiple times but more convenient in some cases.
    pub fn add_routers<I, T>(mut self, routers: I) -> DhtBuilder
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.routers.extend(routers.into_iter().map(|r| r.into()));
        self
    }

    /// Set the read only flag when communicating with other nodes. Indicates
    /// that remote nodes should not add us to their routing table.
    ///
    /// Used when we are behind a restrictive NAT and/or we want to decrease
    /// incoming network traffic. Defaults value is true.
    pub fn set_read_only(mut self, read_only: bool) -> DhtBuilder {
        self.read_only = read_only;

        self
    }

    /// Provide a port to include in the `announce_peer` requests we send.
    ///
    /// If this is not supplied, will use implied port.
    pub fn set_announce_port(mut self, port: u16) -> Self {
        self.announce_port = Some(port);
        self
    }

    /// Set the id of this node. If not provided, a random node id is generated.
    ///
    /// NOTE: when creating a double-stack DHT (ipv4 + ipv6), it's recommended that both DHTs use
    /// the same node id.
    pub fn set_node_id(mut self, id: NodeId) -> Self {
        self.node_id = Some(id);
        self
    }

    /// Start a mainline DHT with the current configuration and bind it to the provided socket.
    /// Fails only if `socket.local_addr()` fails.
    pub fn start<S: SocketTrait + Send + Sync + 'static>(
        self,
        socket: S,
    ) -> io::Result<MainlineDht> {
        let socket = Socket::new(socket)?;
        Ok(MainlineDht::with_builder(self, socket))
    }
}
