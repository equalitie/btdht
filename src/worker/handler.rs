use super::bootstrap::{BootstrapStatus, TableBootstrap};
use super::lookup::{LookupStatus, TableLookup};
use super::refresh::{RefreshStatus, TableRefresh};
use super::timer::Timer;
use super::{DhtEvent, OneshotTask, ScheduledTaskCheck, ShutdownCause};
use crate::id::InfoHash;
use crate::message::{
    error_code, AckResponse, Error, FindNodeResponse, GetPeersResponse, Message, MessageBody,
    Request, Response,
};
use crate::routing::node::Node;
use crate::routing::node::NodeStatus;
use crate::routing::table::BucketContents;
use crate::routing::table::RoutingTable;
use crate::storage::AnnounceStorage;
use crate::token::{Token, TokenStore};
use crate::transaction::{AIDGenerator, ActionID, TransactionID};
use futures_util::{
    future::{self, Either},
    pin_mut, StreamExt,
};
use std::collections::{HashMap, HashSet};
use std::convert::AsRef;
use std::io;
use std::net::SocketAddr;
use tokio::{sync::mpsc, task};

const MAX_BOOTSTRAP_ATTEMPTS: usize = 3;
const BOOTSTRAP_GOOD_NODE_THRESHOLD: usize = 10;

/// Spawns a DHT handler that maintains our routing table and executes our actions on the DHT.
pub(crate) fn create_dht_handler(
    table: RoutingTable,
    out: mpsc::Sender<(Vec<u8>, SocketAddr)>,
    read_only: bool,
    announce_port: Option<u16>,
    event_tx: mpsc::UnboundedSender<DhtEvent>,
) -> io::Result<mpsc::UnboundedSender<OneshotTask>> {
    let (command_tx, command_rx) = mpsc::unbounded_channel();

    let mut aid_generator = AIDGenerator::new();

    // Insert the refresh task to execute after the bootstrap
    let mut mid_generator = aid_generator.generate();
    let refresh_trans_id = mid_generator.generate();
    let table_refresh = TableRefresh::new(mid_generator);
    let future_actions = vec![PostBootstrapAction::Refresh(
        table_refresh,
        refresh_trans_id,
    )];

    let mut handler = DhtHandler {
        running: false,
        command_rx,
        timer: Timer::new(),
        read_only,
        announce_port,
        out_channel: out,
        token_store: TokenStore::new(),
        aid_generator,
        bootstrapping: false,
        routing_table: table,
        active_stores: AnnounceStorage::new(),
        future_actions,
        event_tx,
        table_actions: HashMap::new(),
    };

    task::spawn(async move {
        handler.run().await;
        info!("bip_dht: DhtHandler gracefully shut down, exiting thread...");
    });

    Ok(command_tx)
}

// ----------------------------------------------------------------------------//

/// Actions that we can perform on our RoutingTable.
enum TableAction {
    /// Lookup action.
    Lookup(TableLookup),
    /// Refresh action.
    Refresh(TableRefresh),
    /// Bootstrap action.
    ///
    /// Includes number of bootstrap attempts.
    Bootstrap(TableBootstrap, usize),
}

/// Actions that we want to perform on our RoutingTable after bootstrapping finishes.
#[allow(clippy::large_enum_variant)]
enum PostBootstrapAction {
    /// Future lookup action.
    Lookup(InfoHash, bool),
    /// Future refresh action.
    Refresh(TableRefresh, TransactionID),
}

/// Storage for our EventLoop to invoke actions upon.
pub(crate) struct DhtHandler {
    running: bool,
    command_rx: mpsc::UnboundedReceiver<OneshotTask>,
    timer: Timer<ScheduledTaskCheck>,
    read_only: bool,
    announce_port: Option<u16>,
    out_channel: mpsc::Sender<(Vec<u8>, SocketAddr)>,
    token_store: TokenStore,
    aid_generator: AIDGenerator,
    bootstrapping: bool,
    routing_table: RoutingTable,
    active_stores: AnnounceStorage,
    // If future actions is not empty, that means we are still bootstrapping
    // since we will always spin up a table refresh action after bootstrapping.
    future_actions: Vec<PostBootstrapAction>,
    event_tx: mpsc::UnboundedSender<DhtEvent>,
    table_actions: HashMap<ActionID, TableAction>,
}

impl DhtHandler {
    pub async fn run(&mut self) {
        self.running = true;

        while self.running {
            self.run_once().await
        }
    }

    async fn run_once(&mut self) {
        let event = {
            let command = self.command_rx.recv();
            pin_mut!(command);

            let timeout = self.timer.next();
            pin_mut!(timeout);

            match future::select(command, timeout).await {
                Either::Left((Some(message), _)) => Either::Left(message),
                Either::Right((Some(token), _)) => Either::Right(token),
                Either::Left((None, _)) | Either::Right((None, _)) => return,
            }
        };

        // TODO: change the `handle_command` and `handle_timeout` functions to `async` and remove
        //       the `block_in_place`.
        match event {
            Either::Left(message) => task::block_in_place(|| self.handle_command(message)),
            Either::Right(token) => task::block_in_place(|| self.handle_timeout(token)),
        }
    }

    fn handle_command(&mut self, task: OneshotTask) {
        match task {
            OneshotTask::Incoming(buffer, addr) => {
                self.handle_incoming(&buffer[..], addr);
            }
            OneshotTask::StartBootstrap(routers, nodes) => {
                self.handle_start_bootstrap(routers, nodes);
            }
            OneshotTask::StartLookup(info_hash, should_announce) => {
                self.handle_start_lookup(info_hash, should_announce);
            }
            OneshotTask::Shutdown(cause) => {
                self.shutdown(cause);
            }
        }
    }

    fn handle_timeout(&mut self, token: ScheduledTaskCheck) {
        match token {
            ScheduledTaskCheck::TableRefresh(trans_id) => {
                self.handle_check_table_refresh(trans_id);
            }
            ScheduledTaskCheck::BootstrapTimeout(trans_id) => {
                self.handle_check_bootstrap_timeout(trans_id);
            }
            ScheduledTaskCheck::LookupTimeout(trans_id) => {
                self.handle_check_lookup_timeout(trans_id);
            }
            ScheduledTaskCheck::LookupEndGame(trans_id) => {
                self.handle_check_lookup_endgame(trans_id);
            }
        }
    }

    fn handle_incoming(&mut self, buffer: &[u8], addr: SocketAddr) {
        let message = match Message::decode(buffer) {
            Ok(message) => message,
            Err(error) => {
                warn!("bip_dht: Received invalid bencode data: {}", error);
                return;
            }
        };

        // Validate response
        if let MessageBody::Response(response) = &message.body {
            // Check if we can interpret the response transaction id as one of ours.
            let trans_id =
                if let Some(trans_id) = TransactionID::from_bytes(&message.transaction_id) {
                    trans_id
                } else {
                    warn!("bip_dht: Received response with invalid transaction id");
                    return;
                };

            // Match the response action id with our current actions
            match (self.table_actions.get(&trans_id.action_id()), response) {
                (Some(TableAction::Lookup(_)), Response::GetPeers(_))
                | (Some(TableAction::Refresh(_)), Response::FindNode(_))
                | (Some(TableAction::Bootstrap(..)), Response::FindNode(_)) => (),
                _ => {
                    warn!("bip_dht: Received unsolicited response");
                    return;
                }
            }
        }

        // Do not process requests if we are read only
        // TODO: Add read only flags to messages we send it we are read only!
        // Also, check for read only flags on responses we get before adding nodes
        // to our RoutingTable.
        if self.read_only {
            if let MessageBody::Request(_) = message.body {
                return;
            }
        }

        // Process the given message
        match message.body {
            MessageBody::Request(Request::Ping(p)) => {
                info!("bip_dht: Received a PingRequest...");
                let node = Node::as_good(p.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.find_node(&node) {
                    n.remote_request()
                }

                let ping_rsp = AckResponse {
                    id: self.routing_table.node_id(),
                };
                let ping_msg = Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(Response::Ack(ping_rsp)),
                };
                let ping_msg = ping_msg.encode();

                if self.out_channel.blocking_send((ping_msg, addr)).is_err() {
                    error!("bip_dht: Failed to send a ping response on the out channel...");
                    self.shutdown(ShutdownCause::Unspecified);
                }
            }
            MessageBody::Request(Request::FindNode(f)) => {
                info!("bip_dht: Received a FindNodeRequest...");
                let node = Node::as_good(f.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.find_node(&node) {
                    n.remote_request()
                }

                // Grab the closest nodes
                let closest_nodes = self
                    .routing_table
                    .closest_nodes(f.target)
                    .take(8)
                    .map(|node| *node.info())
                    .collect();

                let find_node_rsp = FindNodeResponse {
                    id: self.routing_table.node_id(),
                    nodes: closest_nodes,
                };
                let find_node_msg = Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(Response::FindNode(find_node_rsp)),
                };
                let find_node_msg = find_node_msg.encode();

                if self
                    .out_channel
                    .blocking_send((find_node_msg, addr))
                    .is_err()
                {
                    error!("bip_dht: Failed to send a find node response on the out channel...");
                    self.shutdown(ShutdownCause::Unspecified);
                }
            }
            MessageBody::Request(Request::GetPeers(g)) => {
                info!("bip_dht: Received a GetPeersRequest...");
                let node = Node::as_good(g.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.find_node(&node) {
                    n.remote_request()
                }

                // TODO: Check what the maximum number of values we can give without overflowing a udp packet
                // Also, if we arent going to give all of the contacts, we may want to shuffle which ones we give
                let mut values = Vec::new();
                self.active_stores
                    .find_items(&g.info_hash, |addr| match addr {
                        SocketAddr::V4(v4_addr) => {
                            values.push(v4_addr);
                        }
                        SocketAddr::V6(_) => {
                            error!("AnnounceStorage contained an IPv6 Address...");
                        }
                    });

                // Grab the closest nodes
                let nodes = self
                    .routing_table
                    .closest_nodes(g.info_hash)
                    .take(8)
                    .map(|node| *node.info())
                    .collect();

                let token = self.token_store.checkout(addr.ip());

                let get_peers_rsp = GetPeersResponse {
                    id: self.routing_table.node_id(),
                    values,
                    nodes,
                    token: token.as_ref().to_vec(),
                };
                let get_peers_msg = Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(Response::GetPeers(get_peers_rsp)),
                };
                let get_peers_msg = get_peers_msg.encode();

                if self
                    .out_channel
                    .blocking_send((get_peers_msg, addr))
                    .is_err()
                {
                    error!("bip_dht: Failed to send a get peers response on the out channel...");
                    self.shutdown(ShutdownCause::Unspecified);
                }
            }
            MessageBody::Request(Request::AnnouncePeer(a)) => {
                info!("bip_dht: Received an AnnouncePeerRequest...");
                let node = Node::as_good(a.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.find_node(&node) {
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
                    warn!(
                        "bip_dht: Remote node sent us an invalid token for an AnnounceRequest..."
                    );
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
                        body: MessageBody::Response(Response::Ack(AckResponse {
                            id: self.routing_table.node_id(),
                        })),
                    }
                    .encode()
                } else {
                    // Node unsuccessfully stored the value with us, send them an error message
                    // TODO: Spec doesnt actually say what error message to send, or even if we should send one...
                    warn!("bip_dht: AnnounceStorage failed to store contact information because it is full...");

                    Message {
                        transaction_id: message.transaction_id,
                        body: MessageBody::Error(Error {
                            code: error_code::SERVER_ERROR,
                            message: "announce storage is full".to_owned(),
                        }),
                    }
                    .encode()
                };

                if self
                    .out_channel
                    .blocking_send((response_msg, addr))
                    .is_err()
                {
                    error!(
                        "bip_dht: Failed to send an announce peer response on the out channel..."
                    );
                    self.shutdown(ShutdownCause::Unspecified);
                }
            }
            MessageBody::Response(Response::FindNode(f)) => {
                info!("bip_dht: Received a FindNodeResponse...");
                let trans_id = TransactionID::from_bytes(&message.transaction_id).unwrap();
                let node = Node::as_good(f.id, addr);

                // Add the payload nodes as questionable
                for node in f.nodes {
                    self.routing_table
                        .add_node(Node::as_questionable(node.id, node.addr));
                }

                let bootstrap_complete = {
                    let opt_bootstrap = match self.table_actions.get_mut(&trans_id.action_id()) {
                        Some(&mut TableAction::Refresh(_)) => {
                            self.routing_table.add_node(node);
                            None
                        }
                        Some(&mut TableAction::Bootstrap(ref mut bootstrap, ref mut attempts)) => {
                            if !bootstrap.is_router(&node.addr()) {
                                self.routing_table.add_node(node);
                            }
                            Some((bootstrap, attempts))
                        }
                        Some(&mut TableAction::Lookup(_)) => {
                            error!(
                                "bip_dht: Resolved a FindNodeResponse ActionID to a TableLookup..."
                            );
                            None
                        }
                        None => {
                            error!(
                                "bip_dht: Resolved a TransactionID to a FindNodeResponse but no \
                                action found..."
                            );
                            None
                        }
                    };

                    if let Some((bootstrap, attempts)) = opt_bootstrap {
                        match bootstrap.recv_response(
                            &trans_id,
                            &self.routing_table,
                            &self.out_channel,
                            &mut self.timer,
                        ) {
                            BootstrapStatus::Idle => true,
                            BootstrapStatus::Bootstrapping => false,
                            BootstrapStatus::Failed => {
                                self.shutdown(ShutdownCause::Unspecified);
                                false
                            }
                            BootstrapStatus::Completed => {
                                if should_rebootstrap(&self.routing_table) {
                                    match attempt_rebootstrap(
                                        bootstrap,
                                        attempts,
                                        &self.routing_table,
                                        &self.out_channel,
                                        &mut self.timer,
                                    ) {
                                        Ok(bootstrap_started) => !bootstrap_started,
                                        Err(cause) => {
                                            self.shutdown(cause);
                                            false
                                        }
                                    }
                                } else {
                                    true
                                }
                            }
                        }
                    } else {
                        false
                    }
                };

                if bootstrap_complete {
                    self.broadcast_bootstrap_completed(trans_id.action_id());
                }

                if log_enabled!(log::Level::Info) {
                    let mut total = 0;

                    for (index, bucket) in self.routing_table.buckets().enumerate() {
                        let num_nodes = match bucket {
                            BucketContents::Empty => 0,
                            BucketContents::Sorted(b) => {
                                b.iter().filter(|n| n.status() == NodeStatus::Good).count()
                            }
                            BucketContents::Assorted(b) => {
                                b.iter().filter(|n| n.status() == NodeStatus::Good).count()
                            }
                        };
                        total += num_nodes;

                        if num_nodes != 0 {
                            print!("Bucket {}: {} | ", index, num_nodes);
                        }
                    }

                    print!("\nTotal: {}\n\n\n", total);
                }
            }
            MessageBody::Response(Response::GetPeers(g)) => {
                // info!("bip_dht: Received a GetPeersResponse...");
                let trans_id = TransactionID::from_bytes(&message.transaction_id).unwrap();
                let node = Node::as_good(g.id, addr);

                self.routing_table.add_node(node.clone());

                let opt_lookup = {
                    match self.table_actions.get_mut(&trans_id.action_id()) {
                        Some(TableAction::Lookup(lookup)) => Some(lookup),
                        Some(TableAction::Refresh(_)) => {
                            error!(
                                "bip_dht: Resolved a GetPeersResponse ActionID to a \
                                TableRefresh..."
                            );
                            None
                        }
                        Some(TableAction::Bootstrap(_, _)) => {
                            error!(
                                "bip_dht: Resolved a GetPeersResponse ActionID to a \
                                TableBootstrap..."
                            );
                            None
                        }
                        None => {
                            error!(
                                "bip_dht: Resolved a TransactionID to a GetPeersResponse but no \
                                action found..."
                            );
                            None
                        }
                    }
                };

                if let Some(lookup) = opt_lookup {
                    match lookup.recv_response(
                        node,
                        &trans_id,
                        g,
                        &self.routing_table,
                        &self.out_channel,
                        &mut self.timer,
                    ) {
                        LookupStatus::Searching => (),
                        LookupStatus::Completed => self
                            .event_tx
                            .send(DhtEvent::LookupCompleted(lookup.info_hash()))
                            .unwrap_or(()),
                        LookupStatus::Failed => self.shutdown(ShutdownCause::Unspecified),
                        LookupStatus::Values(values) => {
                            for v4_addr in values {
                                let sock_addr = SocketAddr::V4(v4_addr);
                                self.event_tx
                                    .send(DhtEvent::PeerFound(lookup.info_hash(), sock_addr))
                                    .unwrap_or(());
                            }
                        }
                    }
                }
            }
            MessageBody::Response(Response::Ack(_)) => {
                info!("bip_dht: Received a AckResponse...");
                // TODO: mark the node as good?
            }
            MessageBody::Error(e) => {
                info!("bip_dht: Received an ErrorMessage...");
                warn!("bip_dht: KRPC error message from {:?}: {:?}", addr, e);
            }
        }
    }

    fn handle_start_bootstrap(&mut self, routers: HashSet<SocketAddr>, nodes: HashSet<SocketAddr>) {
        let mid_generator = self.aid_generator.generate();
        let action_id = mid_generator.action_id();
        let mut table_bootstrap =
            TableBootstrap::new(self.routing_table.node_id(), mid_generator, nodes, routers);

        // Begin the bootstrap operation
        let bootstrap_status = table_bootstrap.start_bootstrap(&self.out_channel, &mut self.timer);

        self.bootstrapping = true;
        self.table_actions
            .insert(action_id, TableAction::Bootstrap(table_bootstrap, 0));

        let bootstrap_complete = match bootstrap_status {
            BootstrapStatus::Idle => true,
            BootstrapStatus::Bootstrapping => false,
            BootstrapStatus::Failed => {
                self.shutdown(ShutdownCause::Unspecified);
                false
            }
            BootstrapStatus::Completed => {
                // Check if our bootstrap was actually good
                if should_rebootstrap(&self.routing_table) {
                    let (bootstrap, attempts) = match self.table_actions.get_mut(&action_id) {
                        Some(&mut TableAction::Bootstrap(ref mut bootstrap, ref mut attempts)) => {
                            (bootstrap, attempts)
                        }
                        _ => panic!("bip_dht: Bug, in DhtHandler..."),
                    };

                    match attempt_rebootstrap(
                        bootstrap,
                        attempts,
                        &self.routing_table,
                        &self.out_channel,
                        &mut self.timer,
                    ) {
                        Ok(bootstrap_started) => !bootstrap_started,
                        Err(cause) => {
                            self.shutdown(cause);
                            false
                        }
                    }
                } else {
                    true
                }
            }
        };

        if bootstrap_complete {
            self.broadcast_bootstrap_completed(action_id);
        }
    }

    fn handle_check_bootstrap_timeout(&mut self, trans_id: TransactionID) {
        let bootstrap_complete = {
            let opt_bootstrap_info = match self.table_actions.get_mut(&trans_id.action_id()) {
                Some(&mut TableAction::Bootstrap(ref mut bootstrap, ref mut attempts)) => Some((
                    bootstrap.recv_timeout(
                        &trans_id,
                        &self.routing_table,
                        &self.out_channel,
                        &mut self.timer,
                    ),
                    bootstrap,
                    attempts,
                )),
                Some(&mut TableAction::Lookup(_)) => {
                    error!(
                        "bip_dht: Resolved a TransactionID to a check table bootstrap but \
                        TableLookup found..."
                    );
                    None
                }
                Some(&mut TableAction::Refresh(_)) => {
                    error!(
                        "bip_dht: Resolved a TransactionID to a check table bootstrap but \
                        TableRefresh found..."
                    );
                    None
                }
                None => {
                    error!(
                        "bip_dht: Resolved a TransactionID to a check table bootstrap but no \
                        action found..."
                    );
                    None
                }
            };

            match opt_bootstrap_info {
                None => false,
                Some((BootstrapStatus::Idle, _, _)) => true,
                Some((BootstrapStatus::Bootstrapping, _, _)) => false,
                Some((BootstrapStatus::Failed, _, _)) => {
                    self.shutdown(ShutdownCause::Unspecified);
                    false
                }
                Some((BootstrapStatus::Completed, bootstrap, attempts)) => {
                    // Check if our bootstrap was actually good
                    if should_rebootstrap(&self.routing_table) {
                        match attempt_rebootstrap(
                            bootstrap,
                            attempts,
                            &self.routing_table,
                            &self.out_channel,
                            &mut self.timer,
                        ) {
                            Ok(bootstrap_started) => !bootstrap_started,
                            Err(cause) => {
                                self.shutdown(cause);
                                false
                            }
                        }
                    } else {
                        true
                    }
                }
            }
        };

        if bootstrap_complete {
            self.broadcast_bootstrap_completed(trans_id.action_id());
        }
    }

    /// Broadcast that the bootstrap has completed.
    /// IMPORTANT: Should call this instead of broadcast_dht_event()!
    fn broadcast_bootstrap_completed(&mut self, action_id: ActionID) {
        // Send notification that the bootstrap has completed.
        self.event_tx
            .send(DhtEvent::BootstrapCompleted)
            .unwrap_or(());

        // Indicates we are out of the bootstrapping phase
        self.bootstrapping = false;

        // Remove the bootstrap action from our table actions
        self.table_actions.remove(&action_id);

        // Start the post bootstrap actions.
        let mut future_actions = self.future_actions.split_off(0);
        for table_action in future_actions.drain(..) {
            match table_action {
                PostBootstrapAction::Lookup(info_hash, should_announce) => {
                    self.handle_start_lookup(info_hash, should_announce);
                }
                PostBootstrapAction::Refresh(refresh, trans_id) => {
                    self.table_actions
                        .insert(trans_id.action_id(), TableAction::Refresh(refresh));

                    self.handle_check_table_refresh(trans_id);
                }
            }
        }
    }

    fn handle_start_lookup(&mut self, info_hash: InfoHash, should_announce: bool) {
        let mid_generator = self.aid_generator.generate();
        let action_id = mid_generator.action_id();

        if self.bootstrapping {
            // Queue it up if we are currently bootstrapping
            self.future_actions
                .push(PostBootstrapAction::Lookup(info_hash, should_announce));
        } else {
            // Start the lookup right now if not bootstrapping
            match TableLookup::new(
                self.routing_table.node_id(),
                info_hash,
                mid_generator,
                should_announce,
                &self.routing_table,
                &self.out_channel,
                &mut self.timer,
            ) {
                Some(lookup) => {
                    self.table_actions
                        .insert(action_id, TableAction::Lookup(lookup));
                }
                None => self.shutdown(ShutdownCause::Unspecified),
            }
        }
    }

    fn handle_check_lookup_timeout(&mut self, trans_id: TransactionID) {
        let opt_lookup_info = match self.table_actions.get_mut(&trans_id.action_id()) {
            Some(&mut TableAction::Lookup(ref mut lookup)) => Some((
                lookup.recv_timeout(
                    &trans_id,
                    &self.routing_table,
                    &self.out_channel,
                    &mut self.timer,
                ),
                lookup.info_hash(),
            )),
            Some(&mut TableAction::Bootstrap(_, _)) => {
                error!(
                    "bip_dht: Resolved a TransactionID to a check table lookup but TableBootstrap \
                    found..."
                );
                None
            }
            Some(&mut TableAction::Refresh(_)) => {
                error!(
                    "bip_dht: Resolved a TransactionID to a check table lookup but TableRefresh \
                    found..."
                );
                None
            }
            None => {
                error!(
                    "bip_dht: Resolved a TransactionID to a check table lookup but no action \
                    found..."
                );
                None
            }
        };

        match opt_lookup_info {
            None => (),
            Some((LookupStatus::Searching, _)) => (),
            Some((LookupStatus::Completed, info_hash)) => self
                .event_tx
                .send(DhtEvent::LookupCompleted(info_hash))
                .unwrap_or(()),
            Some((LookupStatus::Failed, _)) => self.shutdown(ShutdownCause::Unspecified),
            Some((LookupStatus::Values(v), info_hash)) => {
                // Add values to handshaker
                for v4_addr in v {
                    let sock_addr = SocketAddr::V4(v4_addr);
                    self.event_tx
                        .send(DhtEvent::PeerFound(info_hash, sock_addr))
                        .unwrap_or(());
                }
            }
        }
    }

    fn handle_check_lookup_endgame(&mut self, trans_id: TransactionID) {
        let opt_lookup_info = match self.table_actions.remove(&trans_id.action_id()) {
            Some(TableAction::Lookup(mut lookup)) => Some((
                lookup.recv_finished(self.announce_port, &self.routing_table, &self.out_channel),
                lookup.info_hash(),
            )),
            Some(TableAction::Bootstrap(_, _)) => {
                error!(
                    "bip_dht: Resolved a TransactionID to a check table lookup but TableBootstrap \
                    found..."
                );
                None
            }
            Some(TableAction::Refresh(_)) => {
                error!(
                    "bip_dht: Resolved a TransactionID to a check table lookup but TableRefresh \
                    found..."
                );
                None
            }
            None => {
                error!(
                    "bip_dht: Resolved a TransactionID to a check table lookup but no action \
                    found..."
                );
                None
            }
        };

        match opt_lookup_info {
            None => (),
            Some((LookupStatus::Searching, _)) => (),
            Some((LookupStatus::Completed, info_hash)) => self
                .event_tx
                .send(DhtEvent::LookupCompleted(info_hash))
                .unwrap_or(()),
            Some((LookupStatus::Failed, _)) => self.shutdown(ShutdownCause::Unspecified),
            Some((LookupStatus::Values(v), info_hash)) => {
                // Add values to handshaker
                for v4_addr in v {
                    let sock_addr = SocketAddr::V4(v4_addr);
                    self.event_tx
                        .send(DhtEvent::PeerFound(info_hash, sock_addr))
                        .unwrap_or(());
                }
            }
        }
    }

    fn handle_check_table_refresh(&mut self, trans_id: TransactionID) {
        let opt_refresh_status = match self.table_actions.get_mut(&trans_id.action_id()) {
            Some(&mut TableAction::Refresh(ref mut refresh)) => Some(refresh.continue_refresh(
                &self.routing_table,
                &self.out_channel,
                &mut self.timer,
            )),
            Some(&mut TableAction::Lookup(_)) => {
                error!(
                    "bip_dht: Resolved a TransactionID to a check table refresh but TableLookup \
                    found..."
                );
                None
            }
            Some(&mut TableAction::Bootstrap(_, _)) => {
                error!(
                    "bip_dht: Resolved a TransactionID to a check table refresh but \
                    TableBootstrap found..."
                );
                None
            }
            None => {
                error!(
                    "bip_dht: Resolved a TransactionID to a check table refresh but no action \
                    found..."
                );
                None
            }
        };

        match opt_refresh_status {
            None => (),
            Some(RefreshStatus::Refreshing) => (),
            Some(RefreshStatus::Failed) => self.shutdown(ShutdownCause::Unspecified),
        }
    }

    fn shutdown(&mut self, cause: ShutdownCause) {
        self.event_tx
            .send(DhtEvent::ShuttingDown(cause))
            .unwrap_or(());
        self.running = false;
    }
}

// ----------------------------------------------------------------------------//

/// Number of good nodes in the RoutingTable.
fn num_good_nodes(table: &RoutingTable) -> usize {
    table
        .closest_nodes(table.node_id())
        .filter(|n| n.status() == NodeStatus::Good)
        .count()
}

/// We should rebootstrap if we have a low number of nodes.
fn should_rebootstrap(table: &RoutingTable) -> bool {
    num_good_nodes(table) <= BOOTSTRAP_GOOD_NODE_THRESHOLD
}

/// Attempt to rebootstrap or shutdown the dht if we have no nodes after rebootstrapping multiple time.
/// Returns Err if the DHT is shutting down, Ok(true) if the rebootstrap process started, Ok(false) if a rebootstrap is not necessary.
fn attempt_rebootstrap(
    bootstrap: &mut TableBootstrap,
    attempts: &mut usize,
    routing_table: &RoutingTable,
    out_channel: &mpsc::Sender<(Vec<u8>, SocketAddr)>,
    timer: &mut Timer<ScheduledTaskCheck>,
) -> Result<bool, ShutdownCause> {
    // Increment the bootstrap counter
    *attempts += 1;

    warn!(
        "bip_dht: Bootstrap attempt {} failed, attempting a rebootstrap...",
        *attempts
    );

    // Check if we reached the maximum bootstrap attempts
    if *attempts >= MAX_BOOTSTRAP_ATTEMPTS {
        if num_good_nodes(routing_table) == 0 {
            // Failed to get any nodes in the rebootstrap attempts, shut down
            Err(ShutdownCause::BootstrapFailed)
        } else {
            Ok(false)
        }
    } else {
        let bootstrap_status = bootstrap.start_bootstrap(out_channel, timer);

        match bootstrap_status {
            BootstrapStatus::Idle => Ok(false),
            BootstrapStatus::Bootstrapping => Ok(true),
            BootstrapStatus::Failed => {
                // TODO: should we use `BootstrapFailed` here?
                Err(ShutdownCause::Unspecified)
            }
            BootstrapStatus::Completed => {
                if should_rebootstrap(routing_table) {
                    attempt_rebootstrap(bootstrap, attempts, routing_table, out_channel, timer)
                } else {
                    Ok(false)
                }
            }
        }
    }
}
