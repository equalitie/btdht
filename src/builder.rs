use crate::{
    id::{InfoHash, NodeId},
    routing::table::RoutingTable,
    worker::{DhtEvent, DhtHandler, OneshotTask, Socket},
};
use std::{collections::HashSet, net::SocketAddr};
use tokio::{net::UdpSocket, sync::mpsc, task};

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
    fn with_builder(
        builder: DhtBuilder,
        socket: UdpSocket,
    ) -> (Self, mpsc::UnboundedReceiver<DhtEvent>) {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // TODO: Utilize the security extension.
        let routing_table = RoutingTable::new(builder.node_id.unwrap_or_else(rand::random));
        let handler = DhtHandler::new(
            routing_table,
            Socket::new(socket),
            builder.read_only,
            builder.announce_port,
            command_rx,
            event_tx,
        );

        if command_tx
            .send(OneshotTask::StartBootstrap(builder.routers, builder.nodes))
            .is_err()
        {
            // `unreachable` is OK here because the corresponding receiver definitely exists at
            // this point inside `handler`.
            unreachable!()
        }

        task::spawn(handler.run());

        (Self { send: command_tx }, event_rx)
    }

    /// Perform a search for the given InfoHash with an optional announce on the closest nodes.
    ///
    ///
    /// Announcing will place your contact information in the DHT so others performing lookups
    /// for the InfoHash will be able to find your contact information and initiate a handshake.
    ///
    /// If the initial bootstrap has not finished, the search will be queued and executed once
    /// the bootstrap has completed.
    pub fn search(&self, hash: InfoHash, announce: bool) {
        if self
            .send
            .send(OneshotTask::StartLookup(hash, announce))
            .is_err()
        {
            error!("failed to start search - DhtHandler has shut down");
        }
    }
}

// ----------------------------------------------------------------------------//

/// Stores information for initializing a DHT.
#[derive(Debug)]
pub struct DhtBuilder {
    nodes: HashSet<SocketAddr>,
    routers: HashSet<SocketAddr>,
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
    pub fn add_router(mut self, router: SocketAddr) -> DhtBuilder {
        self.routers.insert(router);
        self
    }

    /// Add routers. Same as calling `add_router` multiple times but more convenient in some cases.
    pub fn add_routers<I>(mut self, routers: I) -> DhtBuilder
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        self.routers.extend(routers);
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
    pub fn start(self, socket: UdpSocket) -> (MainlineDht, mpsc::UnboundedReceiver<DhtEvent>) {
        MainlineDht::with_builder(self, socket)
    }
}
