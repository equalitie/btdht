use crate::id::InfoHash;
use crate::routing::table::{self, RoutingTable};
use crate::worker::{
    socket::{self, MultiSocket},
    DhtEvent, DhtHandler, OneshotTask,
};
use std::{collections::HashSet, io, net::SocketAddr};
use tokio::{net::UdpSocket, sync::mpsc, task};

/// Maintains a Distributed Hash (Routing) Table.
///
/// This type is cheaply cloneable where each clone refers to the same underlying DHT instance. This
/// is useful to be able to issue DHT operations from multiple tasks/threads.
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
            socket_v4: None,
            socket_v6: None,
        }
    }

    /// Start the MainlineDht with the given DhtBuilder.
    fn with_builder(builder: DhtBuilder) -> (Self, mpsc::UnboundedReceiver<DhtEvent>) {
        let socket = match (builder.socket_v4, builder.socket_v6) {
            (Some(v4), Some(v6)) => MultiSocket::Both { v4, v6 },
            (Some(v4), None) => MultiSocket::V4(v4),
            (None, Some(v6)) => MultiSocket::V6(v6),
            (None, None) => panic!("no socket bound"),
        };

        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // TODO: Utilize the security extension.
        let routing_table = RoutingTable::new(table::random_node_id());
        let handler = DhtHandler::new(
            routing_table,
            socket,
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
    socket_v4: Option<UdpSocket>,
    socket_v6: Option<UdpSocket>,
}

impl DhtBuilder {
    /// Add nodes which will be distributed within our routing table.
    pub fn add_node(mut self, node_addr: SocketAddr) -> DhtBuilder {
        self.nodes.insert(node_addr);
        self
    }

    /// Add a router which will let us gather nodes if our routing table is ever empty.
    ///
    /// See [Self::with_router] for difference between a router and a node.
    pub fn add_router(mut self, router: SocketAddr) -> DhtBuilder {
        self.routers.insert(router);
        self
    }

    /// Add routers
    ///
    /// See [Self::with_router] for difference between a router and a node.
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

    /// Set the UDP socket to use for communication with ipv4 peers.
    /// Returns error if the socket is not bound to an ipv4 address.
    pub fn set_socket_v4(mut self, socket: UdpSocket) -> io::Result<Self> {
        socket::check_v4(&socket)?;
        self.socket_v4 = Some(socket);
        Ok(self)
    }

    /// Set the UDP socket to use for communication with ipv6 peers.
    /// Returns error if the socket is not bound to an ipv6 address.
    pub fn set_socket_v6(mut self, socket: UdpSocket) -> io::Result<Self> {
        socket::check_v6(&socket)?;
        self.socket_v6 = Some(socket);
        Ok(self)
    }

    /// Start a mainline DHT with the current configuration.
    pub fn start(self) -> (MainlineDht, mpsc::UnboundedReceiver<DhtEvent>) {
        MainlineDht::with_builder(self)
    }
}
