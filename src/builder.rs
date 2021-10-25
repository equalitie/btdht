use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;

use tokio::{net::UdpSocket, sync::mpsc};

use crate::id::InfoHash;
use crate::mio::Sender;
use crate::worker::{self, DhtEvent, OneshotTask, ShutdownCause};

/// Maintains a Distributed Hash (Routing) Table.
pub struct MainlineDht {
    send: Sender<OneshotTask>,
}

impl MainlineDht {
    /// Create a new DhtBuilder.
    pub fn builder() -> DhtBuilder {
        DhtBuilder {
            nodes: HashSet::new(),
            routers: HashSet::new(),
            read_only: true,
            announce_port: None,
        }
    }

    /// Start the MainlineDht with the given DhtBuilder.
    fn with_builder(builder: DhtBuilder, socket: UdpSocket) -> io::Result<Self> {
        let send = worker::start_mainline_dht(socket, builder.read_only, builder.announce_port)?;

        if send
            .send(OneshotTask::StartBootstrap(builder.routers, builder.nodes))
            .is_err()
        {
            warn!("bip_dt: MainlineDht failed to send a start bootstrap message...");
        }

        Ok(MainlineDht { send })
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
            warn!("bip_dht: MainlineDht failed to send a start lookup message...");
        }
    }

    /// An event Receiver which will receive events occuring within the DHT.
    ///
    /// It is important to at least monitor the DHT for shutdown events as any calls
    /// after that event occurs will not be processed but no indication will be given.
    pub fn events(&self) -> mpsc::UnboundedReceiver<DhtEvent> {
        let (send, recv) = mpsc::unbounded_channel();

        if self.send.send(OneshotTask::RegisterSender(send)).is_err() {
            warn!("bip_dht: MainlineDht failed to send a register sender message...");
            // TODO: Should we push a Shutdown event through the sender here? We would need
            // to know the cause or create a new cause for this specific scenario since the
            // client could have been lazy and wasnt monitoring this until after it shutdown!
        }

        recv
    }
}

impl Drop for MainlineDht {
    fn drop(&mut self) {
        if self
            .send
            .send(OneshotTask::Shutdown(ShutdownCause::ClientInitiated))
            .is_err()
        {
            warn!(
                "bip_dht: MainlineDht failed to send a shutdown message (may have already been \
                   shutdown)..."
            );
        }
    }
}

// ----------------------------------------------------------------------------//

/// Stores information for initializing a DHT.
#[derive(Clone, Debug)]
pub struct DhtBuilder {
    nodes: HashSet<SocketAddr>,
    routers: HashSet<SocketAddr>,
    read_only: bool,
    announce_port: Option<u16>,
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

    /// Start a mainline DHT with the current configuration and bind it to the provided socket.
    pub fn start(self, socket: UdpSocket) -> io::Result<MainlineDht> {
        MainlineDht::with_builder(self, socket)
    }
}
