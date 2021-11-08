use super::{
    bootstrap::{BootstrapStatus, TableBootstrap},
    lookup::{LookupStatus, TableLookup},
    refresh::TableRefresh,
    socket::Socket,
    timer::Timer,
    DhtEvent, OneshotTask, ScheduledTaskCheck, WorkerError,
};
use crate::{
    id::InfoHash,
    message::{
        error_code, Error, GetPeersResponse, Message, MessageBody, OtherResponse, Request,
        Response, Want,
    },
    routing::{
        node::{Node, NodeHandle, NodeStatus},
        table::RoutingTable,
    },
    storage::AnnounceStorage,
    token::{Token, TokenStore},
    transaction::{AIDGenerator, ActionID, TransactionID},
};
use futures_util::StreamExt;
use std::convert::AsRef;
use std::net::SocketAddr;
use std::{
    collections::{HashMap, HashSet},
    mem,
};
use tokio::{select, sync::mpsc};

const MAX_BOOTSTRAP_ATTEMPTS: usize = 3;
const BOOTSTRAP_GOOD_NODE_THRESHOLD: usize = 10;

/// Storage for our EventLoop to invoke actions upon.
pub(crate) struct DhtHandler {
    running: bool,
    command_rx: mpsc::UnboundedReceiver<OneshotTask>,
    timer: Timer<ScheduledTaskCheck>,
    read_only: bool,
    announce_port: Option<u16>,
    socket: Socket,
    token_store: TokenStore,
    aid_generator: AIDGenerator,
    routing_table: RoutingTable,
    active_stores: AnnounceStorage,
    event_tx: mpsc::UnboundedSender<DhtEvent>,
    // TableBootstrap action (if bootstrap is ongoing) and the number of bootstrap attempts.
    bootstrap: Option<(TableBootstrap, usize)>,
    // TableRefresh action.
    refresh: TableRefresh,
    // Ongoing TableLookups.
    lookups: HashMap<ActionID, TableLookup>,
    // Lookups initiated while we were still bootstrapping, to be executed when the bootstrap is
    // complete.
    future_lookups: Vec<(InfoHash, bool)>,
}

impl DhtHandler {
    pub fn new(
        table: RoutingTable,
        socket: Socket,
        read_only: bool,
        announce_port: Option<u16>,
        command_rx: mpsc::UnboundedReceiver<OneshotTask>,
        event_tx: mpsc::UnboundedSender<DhtEvent>,
    ) -> Self {
        let mut aid_generator = AIDGenerator::new();

        // The refresh task to execute after the bootstrap
        let mid_generator = aid_generator.generate();
        let table_refresh = TableRefresh::new(mid_generator);

        Self {
            running: true,
            command_rx,
            timer: Timer::new(),
            read_only,
            announce_port,
            socket,
            token_store: TokenStore::new(),
            aid_generator,
            routing_table: table,
            active_stores: AnnounceStorage::new(),
            event_tx,
            bootstrap: None,
            refresh: table_refresh,
            lookups: HashMap::new(),
            future_lookups: vec![],
        }
    }

    pub async fn run(mut self) {
        while self.running {
            self.run_once().await
        }
    }

    async fn run_once(&mut self) {
        select! {
            token = self.timer.next(), if !self.timer.is_empty() => {
                // `unwrap` is OK because we checked the timer is non-empty, so it should never
                // return `None`.
                let token = token.unwrap();
                self.handle_timeout(token).await
            }
            command = self.command_rx.recv() => {
                if let Some(command) = command {
                    self.handle_command(command).await
                } else {
                    self.shutdown()
                }
            }
            message = self.socket.recv() => {
                match message {
                    Ok((buffer, addr)) => if let Err(error) = self.handle_incoming(&buffer, addr).await {
                        error!("Failed to handle incoming message: {}", error);
                    }
                    Err(error) => error!("Failed to receive incoming message: {}", error),
                }
            }
        }
    }

    async fn handle_command(&mut self, task: OneshotTask) {
        match task {
            OneshotTask::StartBootstrap(routers, nodes) => {
                self.handle_start_bootstrap(routers, nodes).await;
            }
            OneshotTask::StartLookup(info_hash, should_announce) => {
                self.handle_start_lookup(info_hash, should_announce).await;
            }
        }
    }

    async fn handle_timeout(&mut self, token: ScheduledTaskCheck) {
        match token {
            ScheduledTaskCheck::TableRefresh => {
                self.handle_check_table_refresh().await;
            }
            ScheduledTaskCheck::BootstrapTimeout(trans_id) => {
                self.handle_check_bootstrap_timeout(trans_id).await;
            }
            ScheduledTaskCheck::LookupTimeout(trans_id) => {
                self.handle_check_lookup_timeout(trans_id).await;
            }
            ScheduledTaskCheck::LookupEndGame(trans_id) => {
                self.handle_check_lookup_endgame(trans_id).await;
            }
        }
    }

    async fn handle_incoming(
        &mut self,
        buffer: &[u8],
        addr: SocketAddr,
    ) -> Result<(), WorkerError> {
        let message = Message::decode(buffer).map_err(WorkerError::InvalidBencode)?;

        // Do not process requests if we are read only
        // TODO: Add read only flags to messages we send it we are read only!
        // Also, check for read only flags on responses we get before adding nodes
        // to our RoutingTable.
        if self.read_only && matches!(message.body, MessageBody::Request(_)) {
            return Ok(());
        }

        // Process the given message
        match message.body {
            MessageBody::Request(Request::Ping(p)) => {
                info!("Received a PingRequest");
                let node = NodeHandle::new(p.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.find_node_mut(&node) {
                    n.remote_request()
                }

                let ping_rsp = OtherResponse {
                    id: self.routing_table.node_id(),
                    nodes_v4: vec![],
                    nodes_v6: vec![],
                };
                let ping_msg = Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(Response::Other(ping_rsp)),
                };
                let ping_msg = ping_msg.encode();

                self.socket.send(&ping_msg, addr).await?
            }
            MessageBody::Request(Request::FindNode(f)) => {
                info!("Received a FindNodeRequest");
                let node = NodeHandle::new(f.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.find_node_mut(&node) {
                    n.remote_request()
                }

                let (nodes_v4, nodes_v6) = self.find_closest_nodes(f.target, f.want)?;

                let find_node_rsp = OtherResponse {
                    id: self.routing_table.node_id(),
                    nodes_v4,
                    nodes_v6,
                };
                let find_node_msg = Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(Response::Other(find_node_rsp)),
                };
                let find_node_msg = find_node_msg.encode();

                self.socket.send(&find_node_msg, addr).await?
            }
            MessageBody::Request(Request::GetPeers(g)) => {
                info!("Received a GetPeersRequest");
                let node = NodeHandle::new(g.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.find_node_mut(&node) {
                    n.remote_request()
                }

                // TODO: Check what the maximum number of values we can give without overflowing a udp packet
                // Also, if we arent going to give all of the contacts, we may want to shuffle which ones we give
                let values: Vec<_> = self
                    .active_stores
                    .find_items(&g.info_hash)
                    .filter(|value_addr| {
                        // According to the spec (BEP32), `values` should contain only addresses of the
                        // same family as the address the request came from. The `want` field affects only
                        // the `nodes` and `nodes6` fields, not the `values` field.
                        match (addr, value_addr) {
                            (SocketAddr::V4(_), SocketAddr::V4(_)) => true,
                            (SocketAddr::V6(_), SocketAddr::V6(_)) => true,
                            (SocketAddr::V4(_), SocketAddr::V6(_)) => false,
                            (SocketAddr::V6(_), SocketAddr::V4(_)) => false,
                        }
                    })
                    .collect();

                // Grab the closest nodes
                let (nodes_v4, nodes_v6) = self.find_closest_nodes(g.info_hash, g.want)?;
                let token = self.token_store.checkout(addr.ip());

                let get_peers_rsp = GetPeersResponse {
                    id: self.routing_table.node_id(),
                    values,
                    nodes_v4,
                    nodes_v6,
                    token: token.as_ref().to_vec(),
                };
                let get_peers_msg = Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(Response::GetPeers(get_peers_rsp)),
                };
                let get_peers_msg = get_peers_msg.encode();

                self.socket.send(&get_peers_msg, addr).await?
            }
            MessageBody::Request(Request::AnnouncePeer(a)) => {
                info!("Received an AnnouncePeerRequest");
                let node = NodeHandle::new(a.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.find_node_mut(&node) {
                    n.remote_request()
                }

                // Validate the token
                let is_valid = match Token::new(&a.token) {
                    Ok(t) => self.token_store.checkin(addr.ip(), t),
                    Err(_) => false,
                };

                // Create a socket address based on the implied/explicit port number
                let connect_addr = match a.port {
                    None => addr,
                    Some(port) => {
                        let mut addr = addr;
                        addr.set_port(port);
                        addr
                    }
                };

                // Resolve type of response we are going to send
                let response_msg = if !is_valid {
                    // Node gave us an invalid token
                    warn!("Remote node sent us an invalid token for an AnnounceRequest");
                    Message {
                        transaction_id: message.transaction_id,
                        body: MessageBody::Error(Error {
                            code: error_code::PROTOCOL_ERROR,
                            message: "received an invalid token".to_owned(),
                        }),
                    }
                    .encode()
                } else if self.active_stores.add_item(a.info_hash, connect_addr) {
                    // Node successfully stored the value with us, send an announce response
                    Message {
                        transaction_id: message.transaction_id,
                        body: MessageBody::Response(Response::Other(OtherResponse {
                            id: self.routing_table.node_id(),
                            nodes_v4: vec![],
                            nodes_v6: vec![],
                        })),
                    }
                    .encode()
                } else {
                    // Node unsuccessfully stored the value with us, send them an error message
                    // TODO: Spec doesnt actually say what error message to send, or even if we should send one...
                    warn!("AnnounceStorage failed to store contact information because it is full");

                    Message {
                        transaction_id: message.transaction_id,
                        body: MessageBody::Error(Error {
                            code: error_code::SERVER_ERROR,
                            message: "announce storage is full".to_owned(),
                        }),
                    }
                    .encode()
                };

                self.socket.send(&response_msg, addr).await?
            }
            MessageBody::Response(Response::Other(f)) => {
                info!("Received a OtherResponse");
                let trans_id = TransactionID::from_bytes(&message.transaction_id)
                    .ok_or(WorkerError::InvalidTransactionId)?;
                let node = Node::as_good(f.id, addr);

                let nodes = match self.socket.local_addr()? {
                    SocketAddr::V4(_) => f.nodes_v4,
                    SocketAddr::V6(_) => f.nodes_v6,
                };

                // Add the payload nodes as questionable
                for node in nodes {
                    self.routing_table
                        .add_node(Node::as_questionable(node.id, node.addr));
                }

                if let Some((bootstrap, attempts)) = self
                    .bootstrap
                    .as_mut()
                    .filter(|(bootstrap, _)| bootstrap.action_id() == trans_id.action_id())
                {
                    if !bootstrap.is_router(&node.addr()) {
                        self.routing_table.add_node(node);
                    }

                    let bootstrap_complete = match bootstrap
                        .recv_response(
                            addr,
                            &trans_id,
                            &mut self.routing_table,
                            &self.socket,
                            &mut self.timer,
                        )
                        .await
                    {
                        BootstrapStatus::Idle => true,
                        BootstrapStatus::Bootstrapping => false,
                        BootstrapStatus::Failed => {
                            self.event_tx.send(DhtEvent::BootstrapFailed).unwrap_or(());
                            self.shutdown();
                            false
                        }
                        BootstrapStatus::Completed => {
                            if should_rebootstrap(&self.routing_table) {
                                match attempt_rebootstrap(
                                    bootstrap,
                                    attempts,
                                    &self.routing_table,
                                    &self.socket,
                                    &mut self.timer,
                                )
                                .await
                                {
                                    Some(bootstrap_started) => !bootstrap_started,
                                    None => {
                                        self.shutdown();
                                        false
                                    }
                                }
                            } else {
                                true
                            }
                        }
                    };

                    if bootstrap_complete {
                        self.broadcast_bootstrap_completed().await;
                    }
                } else if self.refresh.action_id() == trans_id.action_id() {
                    self.routing_table.add_node(node);
                } else {
                    return Err(WorkerError::UnsolicitedResponse);
                }

                if log_enabled!(log::Level::Info) {
                    let mut total = 0;

                    for (index, bucket) in self.routing_table.buckets().enumerate() {
                        let num_nodes = bucket
                            .iter()
                            .filter(|n| n.status() == NodeStatus::Good)
                            .count();
                        total += num_nodes;

                        if num_nodes != 0 {
                            print!("Bucket {}: {} | ", index, num_nodes);
                        }
                    }

                    print!("\nTotal: {}\n\n\n", total);
                }
            }
            MessageBody::Response(Response::GetPeers(g)) => {
                info!("Received a GetPeersResponse");

                let trans_id = TransactionID::from_bytes(&message.transaction_id)
                    .ok_or(WorkerError::InvalidTransactionId)?;
                let node = Node::as_good(g.id, addr);

                self.routing_table.add_node(node.clone());

                let lookup = self
                    .lookups
                    .get_mut(&trans_id.action_id())
                    .ok_or(WorkerError::UnsolicitedResponse)?;

                match lookup
                    .recv_response(
                        node,
                        &trans_id,
                        g,
                        &mut self.routing_table,
                        &self.socket,
                        &mut self.timer,
                    )
                    .await
                {
                    LookupStatus::Searching => (),
                    LookupStatus::Completed => self
                        .event_tx
                        .send(DhtEvent::LookupCompleted(lookup.info_hash()))
                        .unwrap_or(()),
                    LookupStatus::Values(values) => {
                        for addr in values {
                            self.event_tx
                                .send(DhtEvent::PeerFound(lookup.info_hash(), addr))
                                .unwrap_or(());
                        }
                    }
                }
            }
            MessageBody::Error(e) => {
                warn!("Received an ErrorMessage from {}: {:?}", addr, e);
            }
        }

        Ok(())
    }

    async fn handle_start_bootstrap(
        &mut self,
        routers: HashSet<SocketAddr>,
        nodes: HashSet<SocketAddr>,
    ) {
        let mid_generator = self.aid_generator.generate();
        let mut table_bootstrap = TableBootstrap::new(mid_generator, nodes, routers);

        // Begin the bootstrap operation
        let bootstrap_status = table_bootstrap
            .start_bootstrap(self.routing_table.node_id(), &self.socket, &mut self.timer)
            .await;

        let (table_bootstrap, attempts) = self.bootstrap.insert((table_bootstrap, 0));

        let bootstrap_complete = match bootstrap_status {
            BootstrapStatus::Idle => true,
            BootstrapStatus::Bootstrapping => false,
            BootstrapStatus::Failed => {
                self.event_tx.send(DhtEvent::BootstrapFailed).unwrap_or(());
                self.shutdown();
                false
            }
            BootstrapStatus::Completed => {
                // Check if our bootstrap was actually good
                if should_rebootstrap(&self.routing_table) {
                    match attempt_rebootstrap(
                        table_bootstrap,
                        attempts,
                        &self.routing_table,
                        &self.socket,
                        &mut self.timer,
                    )
                    .await
                    {
                        Some(bootstrap_started) => !bootstrap_started,
                        None => {
                            self.shutdown();
                            false
                        }
                    }
                } else {
                    true
                }
            }
        };

        if bootstrap_complete {
            self.broadcast_bootstrap_completed().await;
        }
    }

    async fn handle_check_bootstrap_timeout(&mut self, trans_id: TransactionID) {
        let (bootstrap, attempts) = if let Some(bootstrap) = self
            .bootstrap
            .as_mut()
            .filter(|(bootstrap, _)| bootstrap.action_id() == trans_id.action_id())
        {
            bootstrap
        } else {
            error!("Bootstrap timeout expired but no bootstrap is in progress");
            return;
        };

        let bootstrap_status = bootstrap
            .recv_timeout(
                &trans_id,
                &mut self.routing_table,
                &self.socket,
                &mut self.timer,
            )
            .await;

        let bootstrap_complete = match bootstrap_status {
            BootstrapStatus::Idle => true,
            BootstrapStatus::Bootstrapping => false,
            BootstrapStatus::Failed => {
                self.event_tx.send(DhtEvent::BootstrapFailed).unwrap_or(());
                self.shutdown();
                false
            }
            BootstrapStatus::Completed => {
                // Check if our bootstrap was actually good
                if should_rebootstrap(&self.routing_table) {
                    match attempt_rebootstrap(
                        bootstrap,
                        attempts,
                        &self.routing_table,
                        &self.socket,
                        &mut self.timer,
                    )
                    .await
                    {
                        Some(bootstrap_started) => !bootstrap_started,
                        None => {
                            self.shutdown();
                            false
                        }
                    }
                } else {
                    true
                }
            }
        };

        if bootstrap_complete {
            self.broadcast_bootstrap_completed().await;
        }
    }

    /// Broadcast that the bootstrap has completed.
    /// IMPORTANT: Should call this instead of just sending the event!
    async fn broadcast_bootstrap_completed(&mut self) {
        // Send notification that the bootstrap has completed.
        self.event_tx
            .send(DhtEvent::BootstrapCompleted)
            .unwrap_or(());

        // Indicates we are out of the bootstrapping phase
        self.bootstrap = None;

        // Start the refresh action.
        self.handle_check_table_refresh().await;

        // Start the post bootstrap actions.
        for (info_hash, should_announce) in mem::take(&mut self.future_lookups) {
            self.handle_start_lookup(info_hash, should_announce).await;
        }
    }

    async fn handle_start_lookup(&mut self, info_hash: InfoHash, should_announce: bool) {
        let mid_generator = self.aid_generator.generate();
        let action_id = mid_generator.action_id();

        if self.bootstrap.is_some() {
            // Queue it up if we are currently bootstrapping
            self.future_lookups.push((info_hash, should_announce));
        } else {
            // Start the lookup right now if not bootstrapping
            let lookup = TableLookup::new(
                info_hash,
                mid_generator,
                should_announce,
                &mut self.routing_table,
                &self.socket,
                &mut self.timer,
            )
            .await;
            self.lookups.insert(action_id, lookup);
        }
    }

    async fn handle_check_lookup_timeout(&mut self, trans_id: TransactionID) {
        let lookup = if let Some(lookup) = self.lookups.get_mut(&trans_id.action_id()) {
            lookup
        } else {
            error!("Resolved a TransactionID to a check table lookup but no action found");
            return;
        };

        let lookup_status = lookup
            .recv_timeout(
                &trans_id,
                &mut self.routing_table,
                &self.socket,
                &mut self.timer,
            )
            .await;
        let info_hash = lookup.info_hash();

        match lookup_status {
            LookupStatus::Searching => (),
            LookupStatus::Completed => self
                .event_tx
                .send(DhtEvent::LookupCompleted(info_hash))
                .unwrap_or(()),
            LookupStatus::Values(v) => {
                for addr in v {
                    self.event_tx
                        .send(DhtEvent::PeerFound(info_hash, addr))
                        .unwrap_or(());
                }
            }
        }
    }

    async fn handle_check_lookup_endgame(&mut self, trans_id: TransactionID) {
        let mut lookup = if let Some(lookup) = self.lookups.remove(&trans_id.action_id()) {
            lookup
        } else {
            error!("Resolved a TransactionID to a check table lookup but no action found");
            return;
        };

        let lookup_status = lookup
            .recv_finished(self.announce_port, &mut self.routing_table, &self.socket)
            .await;
        let info_hash = lookup.info_hash();

        match lookup_status {
            LookupStatus::Searching => (),
            LookupStatus::Completed => self
                .event_tx
                .send(DhtEvent::LookupCompleted(info_hash))
                .unwrap_or(()),
            LookupStatus::Values(v) => {
                // Add values to handshaker
                for addr in v {
                    self.event_tx
                        .send(DhtEvent::PeerFound(info_hash, addr))
                        .unwrap_or(());
                }
            }
        }
    }

    async fn handle_check_table_refresh(&mut self) {
        self.refresh
            .continue_refresh(&mut self.routing_table, &self.socket, &mut self.timer)
            .await
    }

    fn shutdown(&mut self) {
        self.running = false;
    }

    fn find_closest_nodes(
        &self,
        target: InfoHash,
        want: Option<Want>,
    ) -> Result<(Vec<NodeHandle>, Vec<NodeHandle>), WorkerError> {
        let want = match want {
            Some(want) => want,
            None => match self.socket.local_addr()? {
                SocketAddr::V4(_) => Want::V4,
                SocketAddr::V6(_) => Want::V6,
            },
        };

        let nodes_v4 = if matches!(want, Want::V4 | Want::Both) {
            self.routing_table
                .closest_nodes(target)
                .filter(|node| node.addr().is_ipv4())
                .take(8)
                .map(|node| *node.handle())
                .collect()
        } else {
            vec![]
        };

        let nodes_v6 = if matches!(want, Want::V6 | Want::Both) {
            self.routing_table
                .closest_nodes(target)
                .filter(|node| node.addr().is_ipv6())
                .take(8)
                .map(|node| *node.handle())
                .collect()
        } else {
            vec![]
        };

        Ok((nodes_v4, nodes_v6))
    }
}

// ----------------------------------------------------------------------------//

/// We should rebootstrap if we have a low number of nodes.
fn should_rebootstrap(table: &RoutingTable) -> bool {
    table.num_good_nodes() <= BOOTSTRAP_GOOD_NODE_THRESHOLD
}

/// Attempt to rebootstrap or shutdown the dht if we have no nodes after rebootstrapping multiple time.
/// Returns None if the DHT is shutting down, Some(true) if the rebootstrap process started,
/// Some(false) if a rebootstrap is not necessary.
async fn attempt_rebootstrap(
    bootstrap: &mut TableBootstrap,
    attempts: &mut usize,
    routing_table: &RoutingTable,
    socket: &Socket,
    timer: &mut Timer<ScheduledTaskCheck>,
) -> Option<bool> {
    loop {
        // Increment the bootstrap counter
        *attempts += 1;

        warn!(
            "Bootstrap attempt {} failed, attempting a rebootstrap",
            *attempts
        );

        // Check if we reached the maximum bootstrap attempts
        if *attempts >= MAX_BOOTSTRAP_ATTEMPTS {
            if routing_table.num_good_nodes() == 0 {
                // Failed to get any nodes in the rebootstrap attempts, shut down
                return None;
            } else {
                return Some(false);
            }
        } else {
            let bootstrap_status = bootstrap
                .start_bootstrap(routing_table.node_id(), socket, timer)
                .await;

            match bootstrap_status {
                BootstrapStatus::Idle => return Some(false),
                BootstrapStatus::Bootstrapping => return Some(true),
                BootstrapStatus::Failed => {
                    return None;
                }
                BootstrapStatus::Completed => {
                    if !should_rebootstrap(routing_table) {
                        return Some(false);
                    }
                }
            }
        }
    }
}
