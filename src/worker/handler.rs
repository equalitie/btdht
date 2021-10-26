use std::collections::{HashMap, HashSet};
use std::convert::AsRef;
use std::io;
use std::net::SocketAddr;

use tokio::{sync::mpsc, task};

use crate::id::InfoHash;
use crate::message::{
    error_code, AckResponse, Error, FindNodeResponse, GetPeersResponse, Message, MessageBody,
    Request, Response,
};
use crate::mio::{self, EventLoop, Handler};
use crate::routing::node::Node;
use crate::routing::node::NodeStatus;
use crate::routing::table::BucketContents;
use crate::routing::table::RoutingTable;
use crate::storage::AnnounceStorage;
use crate::token::{Token, TokenStore};
use crate::transaction::{AIDGenerator, ActionID, TransactionID};
use crate::worker::bootstrap::{BootstrapStatus, TableBootstrap};
use crate::worker::lookup::{LookupStatus, TableLookup};
use crate::worker::refresh::{RefreshStatus, TableRefresh};
use crate::worker::{DhtEvent, OneshotTask, ScheduledTaskCheck, ShutdownCause};

// TODO: Update modules to use find_node on the routing table to update the status of a given node.

const MAX_BOOTSTRAP_ATTEMPTS: usize = 3;
const BOOTSTRAP_GOOD_NODE_THRESHOLD: usize = 10;

/// Spawns a DHT handler that maintains our routing table and executes our actions on the DHT.
pub(crate) fn create_dht_handler(
    table: RoutingTable,
    out: mpsc::Sender<(Vec<u8>, SocketAddr)>,
    read_only: bool,
    announce_port: Option<u16>,
    event_tx: mpsc::UnboundedSender<DhtEvent>,
) -> io::Result<mio::Sender<OneshotTask>> {
    let mut handler = DhtHandler::new(table, out, read_only, announce_port, event_tx);
    let mut event_loop = EventLoop::new()?;

    let loop_channel = event_loop.channel();

    task::spawn(async move {
        event_loop.run(&mut handler).await;
        info!("bip_dht: DhtHandler gracefully shut down, exiting thread...");
    });

    Ok(loop_channel)
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
    detached: DetachedDhtHandler,
    table_actions: HashMap<ActionID, TableAction>,
}

/// Storage separate from the table actions allowing us to hold mutable references
/// to table actions while still being able to pass around the bulky parameters.
struct DetachedDhtHandler {
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
}

impl DhtHandler {
    fn new(
        table: RoutingTable,
        out: mpsc::Sender<(Vec<u8>, SocketAddr)>,
        read_only: bool,
        announce_port: Option<u16>,
        event_tx: mpsc::UnboundedSender<DhtEvent>,
    ) -> Self {
        let mut aid_generator = AIDGenerator::new();

        // Insert the refresh task to execute after the bootstrap
        let mut mid_generator = aid_generator.generate();
        let refresh_trans_id = mid_generator.generate();
        let table_refresh = TableRefresh::new(mid_generator);
        let future_actions = vec![PostBootstrapAction::Refresh(
            table_refresh,
            refresh_trans_id,
        )];

        let detached = DetachedDhtHandler {
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
        };

        Self {
            detached,
            table_actions: HashMap::new(),
        }
    }
}

impl Handler for DhtHandler {
    type Timeout = ScheduledTaskCheck;
    type Message = OneshotTask;

    fn notify(&mut self, event_loop: &mut EventLoop<DhtHandler>, task: OneshotTask) {
        match task {
            OneshotTask::Incoming(buffer, addr) => {
                handle_incoming(self, event_loop, &buffer[..], addr);
            }
            OneshotTask::StartBootstrap(routers, nodes) => {
                handle_start_bootstrap(self, event_loop, routers, nodes);
            }
            OneshotTask::StartLookup(info_hash, should_announce) => {
                handle_start_lookup(
                    &mut self.table_actions,
                    &mut self.detached,
                    event_loop,
                    info_hash,
                    should_announce,
                );
            }
            OneshotTask::Shutdown(cause) => {
                handle_shutdown(self, event_loop, cause);
            }
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<DhtHandler>, task: Self::Timeout) {
        match task {
            ScheduledTaskCheck::TableRefresh(trans_id) => {
                handle_check_table_refresh(
                    &mut self.table_actions,
                    &mut self.detached,
                    event_loop,
                    trans_id,
                );
            }
            ScheduledTaskCheck::BootstrapTimeout(trans_id) => {
                handle_check_bootstrap_timeout(self, event_loop, trans_id);
            }
            ScheduledTaskCheck::LookupTimeout(trans_id) => {
                handle_check_lookup_timeout(self, event_loop, trans_id);
            }
            ScheduledTaskCheck::LookupEndGame(trans_id) => {
                handle_check_lookup_endgame(self, event_loop, trans_id);
            }
        }
    }
}

// ----------------------------------------------------------------------------//

/// Shut down the event loop by sending it a shutdown message with the given cause.
fn shutdown_event_loop(event_loop: &mut EventLoop<DhtHandler>, cause: ShutdownCause) {
    if event_loop
        .channel()
        .send(OneshotTask::Shutdown(cause))
        .is_err()
    {
        error!("bip_dht: Failed to sent a shutdown message to the EventLoop...");
    }
}

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

/// Broadcast that the bootstrap has completed.
/// IMPORTANT: Should call this instead of broadcast_dht_event()!
fn broadcast_bootstrap_completed(
    action_id: ActionID,
    table_actions: &mut HashMap<ActionID, TableAction>,
    work_storage: &mut DetachedDhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
) {
    // Send notification that the bootstrap has completed.
    work_storage
        .event_tx
        .send(DhtEvent::BootstrapCompleted)
        .unwrap_or(());

    // Indicates we are out of the bootstrapping phase
    work_storage.bootstrapping = false;

    // Remove the bootstrap action from our table actions
    table_actions.remove(&action_id);

    // Start the post bootstrap actions.
    let mut future_actions = work_storage.future_actions.split_off(0);
    for table_action in future_actions.drain(..) {
        match table_action {
            PostBootstrapAction::Lookup(info_hash, should_announce) => {
                handle_start_lookup(
                    table_actions,
                    work_storage,
                    event_loop,
                    info_hash,
                    should_announce,
                );
            }
            PostBootstrapAction::Refresh(refresh, trans_id) => {
                table_actions.insert(trans_id.action_id(), TableAction::Refresh(refresh));

                handle_check_table_refresh(table_actions, work_storage, event_loop, trans_id);
            }
        }
    }
}

/// Attempt to rebootstrap or shutdown the dht if we have no nodes after rebootstrapping multiple time.
/// Returns None if the DHT is shutting down, Some(true) if the rebootstrap process started, Some(false) if a rebootstrap is not necessary.
fn attempt_rebootstrap(
    bootstrap: &mut TableBootstrap,
    attempts: &mut usize,
    work_storage: &mut DetachedDhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
) -> Option<bool> {
    // Increment the bootstrap counter
    *attempts += 1;

    warn!(
        "bip_dht: Bootstrap attempt {} failed, attempting a rebootstrap...",
        *attempts
    );

    // Check if we reached the maximum bootstrap attempts
    if *attempts >= MAX_BOOTSTRAP_ATTEMPTS {
        if num_good_nodes(&work_storage.routing_table) == 0 {
            // Failed to get any nodes in the rebootstrap attempts, shut down
            shutdown_event_loop(event_loop, ShutdownCause::BootstrapFailed);
            None
        } else {
            Some(false)
        }
    } else {
        let bootstrap_status = bootstrap.start_bootstrap(&work_storage.out_channel, event_loop);

        match bootstrap_status {
            BootstrapStatus::Idle => Some(false),
            BootstrapStatus::Bootstrapping => Some(true),
            BootstrapStatus::Failed => {
                shutdown_event_loop(event_loop, ShutdownCause::Unspecified);
                None
            }
            BootstrapStatus::Completed => {
                if should_rebootstrap(&work_storage.routing_table) {
                    attempt_rebootstrap(bootstrap, attempts, work_storage, event_loop)
                } else {
                    Some(false)
                }
            }
        }
    }
}

// ----------------------------------------------------------------------------//

fn handle_incoming(
    handler: &mut DhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
    buffer: &[u8],
    addr: SocketAddr,
) {
    let (work_storage, table_actions) = (&mut handler.detached, &mut handler.table_actions);

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
        let trans_id = if let Some(trans_id) = TransactionID::from_bytes(&message.transaction_id) {
            trans_id
        } else {
            warn!("bip_dht: Received response with invalid transaction id");
            return;
        };

        // Match the response action id with our current actions
        match (table_actions.get(&trans_id.action_id()), response) {
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
    if work_storage.read_only {
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
            if let Some(n) = work_storage.routing_table.find_node(&node) {
                n.remote_request()
            }

            let ping_rsp = AckResponse {
                id: work_storage.routing_table.node_id(),
            };
            let ping_msg = Message {
                transaction_id: message.transaction_id,
                body: MessageBody::Response(Response::Ack(ping_rsp)),
            };
            let ping_msg = ping_msg.encode();

            if work_storage
                .out_channel
                .blocking_send((ping_msg, addr))
                .is_err()
            {
                error!("bip_dht: Failed to send a ping response on the out channel...");
                shutdown_event_loop(event_loop, ShutdownCause::Unspecified);
            }
        }
        MessageBody::Request(Request::FindNode(f)) => {
            info!("bip_dht: Received a FindNodeRequest...");
            let node = Node::as_good(f.id, addr);

            // Node requested from us, mark it in the Routingtable
            if let Some(n) = work_storage.routing_table.find_node(&node) {
                n.remote_request()
            }

            // Grab the closest nodes
            let closest_nodes = work_storage
                .routing_table
                .closest_nodes(f.target)
                .take(8)
                .map(|node| *node.info())
                .collect();

            let find_node_rsp = FindNodeResponse {
                id: work_storage.routing_table.node_id(),
                nodes: closest_nodes,
            };
            let find_node_msg = Message {
                transaction_id: message.transaction_id,
                body: MessageBody::Response(Response::FindNode(find_node_rsp)),
            };
            let find_node_msg = find_node_msg.encode();

            if work_storage
                .out_channel
                .blocking_send((find_node_msg, addr))
                .is_err()
            {
                error!("bip_dht: Failed to send a find node response on the out channel...");
                shutdown_event_loop(event_loop, ShutdownCause::Unspecified);
            }
        }
        MessageBody::Request(Request::GetPeers(g)) => {
            info!("bip_dht: Received a GetPeersRequest...");
            let node = Node::as_good(g.id, addr);

            // Node requested from us, mark it in the Routingtable
            if let Some(n) = work_storage.routing_table.find_node(&node) {
                n.remote_request()
            }

            // TODO: Check what the maximum number of values we can give without overflowing a udp packet
            // Also, if we arent going to give all of the contacts, we may want to shuffle which ones we give
            let mut values = Vec::new();
            work_storage
                .active_stores
                .find_items(&g.info_hash, |addr| match addr {
                    SocketAddr::V4(v4_addr) => {
                        values.push(v4_addr);
                    }
                    SocketAddr::V6(_) => {
                        error!("AnnounceStorage contained an IPv6 Address...");
                    }
                });

            // Grab the closest nodes
            let nodes = work_storage
                .routing_table
                .closest_nodes(g.info_hash)
                .take(8)
                .map(|node| *node.info())
                .collect();

            let token = work_storage.token_store.checkout(addr.ip());

            let get_peers_rsp = GetPeersResponse {
                id: work_storage.routing_table.node_id(),
                values,
                nodes,
                token: token.as_ref().to_vec(),
            };
            let get_peers_msg = Message {
                transaction_id: message.transaction_id,
                body: MessageBody::Response(Response::GetPeers(get_peers_rsp)),
            };
            let get_peers_msg = get_peers_msg.encode();

            if work_storage
                .out_channel
                .blocking_send((get_peers_msg, addr))
                .is_err()
            {
                error!("bip_dht: Failed to send a get peers response on the out channel...");
                shutdown_event_loop(event_loop, ShutdownCause::Unspecified);
            }
        }
        MessageBody::Request(Request::AnnouncePeer(a)) => {
            info!("bip_dht: Received an AnnouncePeerRequest...");
            let node = Node::as_good(a.id, addr);

            // Node requested from us, mark it in the Routingtable
            if let Some(n) = work_storage.routing_table.find_node(&node) {
                n.remote_request()
            }

            // Validate the token
            let is_valid = match Token::new(&a.token) {
                Ok(t) => work_storage.token_store.checkin(addr.ip(), t),
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
                warn!("bip_dht: Remote node sent us an invalid token for an AnnounceRequest...");
                Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Error(Error {
                        code: error_code::PROTOCOL_ERROR,
                        message: "received an invalid token".to_owned(),
                    }),
                }
                .encode()
            } else if work_storage
                .active_stores
                .add_item(a.info_hash, connect_addr)
            {
                // Node successfully stored the value with us, send an announce response
                Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(Response::Ack(AckResponse {
                        id: work_storage.routing_table.node_id(),
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

            if work_storage
                .out_channel
                .blocking_send((response_msg, addr))
                .is_err()
            {
                error!("bip_dht: Failed to send an announce peer response on the out channel...");
                shutdown_event_loop(event_loop, ShutdownCause::Unspecified);
            }
        }
        MessageBody::Response(Response::FindNode(f)) => {
            info!("bip_dht: Received a FindNodeResponse...");
            let trans_id = TransactionID::from_bytes(&message.transaction_id).unwrap();
            let node = Node::as_good(f.id, addr);

            // Add the payload nodes as questionable
            for node in f.nodes {
                work_storage
                    .routing_table
                    .add_node(Node::as_questionable(node.id, node.addr));
            }

            let bootstrap_complete = {
                let opt_bootstrap = match table_actions.get_mut(&trans_id.action_id()) {
                    Some(&mut TableAction::Refresh(_)) => {
                        work_storage.routing_table.add_node(node);
                        None
                    }
                    Some(&mut TableAction::Bootstrap(ref mut bootstrap, ref mut attempts)) => {
                        if !bootstrap.is_router(&node.addr()) {
                            work_storage.routing_table.add_node(node);
                        }
                        Some((bootstrap, attempts))
                    }
                    Some(&mut TableAction::Lookup(_)) => {
                        error!("bip_dht: Resolved a FindNodeResponse ActionID to a TableLookup...");
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
                        &work_storage.routing_table,
                        &work_storage.out_channel,
                        event_loop,
                    ) {
                        BootstrapStatus::Idle => true,
                        BootstrapStatus::Bootstrapping => false,
                        BootstrapStatus::Failed => {
                            shutdown_event_loop(event_loop, ShutdownCause::Unspecified);
                            false
                        }
                        BootstrapStatus::Completed => {
                            if should_rebootstrap(&work_storage.routing_table) {
                                attempt_rebootstrap(bootstrap, attempts, work_storage, event_loop)
                                    == Some(false)
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
                broadcast_bootstrap_completed(
                    trans_id.action_id(),
                    table_actions,
                    work_storage,
                    event_loop,
                );
            }

            if log_enabled!(log::Level::Info) {
                let mut total = 0;

                for (index, bucket) in work_storage.routing_table.buckets().enumerate() {
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

            work_storage.routing_table.add_node(node.clone());

            let opt_lookup = {
                match table_actions.get_mut(&trans_id.action_id()) {
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
                    &work_storage.routing_table,
                    &work_storage.out_channel,
                    event_loop,
                ) {
                    LookupStatus::Searching => (),
                    LookupStatus::Completed => work_storage
                        .event_tx
                        .send(DhtEvent::LookupCompleted(lookup.info_hash()))
                        .unwrap_or(()),
                    LookupStatus::Failed => {
                        shutdown_event_loop(event_loop, ShutdownCause::Unspecified)
                    }
                    LookupStatus::Values(values) => {
                        for v4_addr in values {
                            let sock_addr = SocketAddr::V4(v4_addr);
                            work_storage
                                .event_tx
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

fn handle_start_bootstrap(
    handler: &mut DhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
    routers: HashSet<SocketAddr>,
    nodes: HashSet<SocketAddr>,
) {
    let (work_storage, table_actions) = (&mut handler.detached, &mut handler.table_actions);

    let mid_generator = work_storage.aid_generator.generate();
    let action_id = mid_generator.action_id();
    let mut table_bootstrap = TableBootstrap::new(
        work_storage.routing_table.node_id(),
        mid_generator,
        nodes,
        routers,
    );

    // Begin the bootstrap operation
    let bootstrap_status = table_bootstrap.start_bootstrap(&work_storage.out_channel, event_loop);

    work_storage.bootstrapping = true;
    table_actions.insert(action_id, TableAction::Bootstrap(table_bootstrap, 0));

    let bootstrap_complete = match bootstrap_status {
        BootstrapStatus::Idle => true,
        BootstrapStatus::Bootstrapping => false,
        BootstrapStatus::Failed => {
            shutdown_event_loop(event_loop, ShutdownCause::Unspecified);
            false
        }
        BootstrapStatus::Completed => {
            // Check if our bootstrap was actually good
            if should_rebootstrap(&work_storage.routing_table) {
                let (bootstrap, attempts) = match table_actions.get_mut(&action_id) {
                    Some(&mut TableAction::Bootstrap(ref mut bootstrap, ref mut attempts)) => {
                        (bootstrap, attempts)
                    }
                    _ => panic!("bip_dht: Bug, in DhtHandler..."),
                };

                attempt_rebootstrap(bootstrap, attempts, work_storage, event_loop) == Some(false)
            } else {
                true
            }
        }
    };

    if bootstrap_complete {
        broadcast_bootstrap_completed(action_id, table_actions, work_storage, event_loop);
    }
}

fn handle_start_lookup(
    table_actions: &mut HashMap<ActionID, TableAction>,
    work_storage: &mut DetachedDhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
    info_hash: InfoHash,
    should_announce: bool,
) {
    let mid_generator = work_storage.aid_generator.generate();
    let action_id = mid_generator.action_id();

    if work_storage.bootstrapping {
        // Queue it up if we are currently bootstrapping
        work_storage
            .future_actions
            .push(PostBootstrapAction::Lookup(info_hash, should_announce));
    } else {
        // Start the lookup right now if not bootstrapping
        match TableLookup::new(
            work_storage.routing_table.node_id(),
            info_hash,
            mid_generator,
            should_announce,
            &work_storage.routing_table,
            &work_storage.out_channel,
            event_loop,
        ) {
            Some(lookup) => {
                table_actions.insert(action_id, TableAction::Lookup(lookup));
            }
            None => shutdown_event_loop(event_loop, ShutdownCause::Unspecified),
        }
    }
}

fn handle_shutdown(
    handler: &mut DhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
    cause: ShutdownCause,
) {
    let (work_storage, _) = (&mut handler.detached, &mut handler.table_actions);

    work_storage
        .event_tx
        .send(DhtEvent::ShuttingDown(cause))
        .unwrap_or(());

    event_loop.shutdown();
}

fn handle_check_table_refresh(
    table_actions: &mut HashMap<ActionID, TableAction>,
    work_storage: &mut DetachedDhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
    trans_id: TransactionID,
) {
    let opt_refresh_status = match table_actions.get_mut(&trans_id.action_id()) {
        Some(&mut TableAction::Refresh(ref mut refresh)) => Some(refresh.continue_refresh(
            &work_storage.routing_table,
            &work_storage.out_channel,
            event_loop,
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
        Some(RefreshStatus::Failed) => shutdown_event_loop(event_loop, ShutdownCause::Unspecified),
    }
}

fn handle_check_bootstrap_timeout(
    handler: &mut DhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
    trans_id: TransactionID,
) {
    let (work_storage, table_actions) = (&mut handler.detached, &mut handler.table_actions);

    let bootstrap_complete = {
        let opt_bootstrap_info = match table_actions.get_mut(&trans_id.action_id()) {
            Some(&mut TableAction::Bootstrap(ref mut bootstrap, ref mut attempts)) => Some((
                bootstrap.recv_timeout(
                    &trans_id,
                    &work_storage.routing_table,
                    &work_storage.out_channel,
                    event_loop,
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
                shutdown_event_loop(event_loop, ShutdownCause::Unspecified);
                false
            }
            Some((BootstrapStatus::Completed, bootstrap, attempts)) => {
                // Check if our bootstrap was actually good
                if should_rebootstrap(&work_storage.routing_table) {
                    attempt_rebootstrap(bootstrap, attempts, work_storage, event_loop)
                        == Some(false)
                } else {
                    true
                }
            }
        }
    };

    if bootstrap_complete {
        broadcast_bootstrap_completed(
            trans_id.action_id(),
            table_actions,
            work_storage,
            event_loop,
        );
    }
}

fn handle_check_lookup_timeout(
    handler: &mut DhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
    trans_id: TransactionID,
) {
    let (work_storage, table_actions) = (&mut handler.detached, &mut handler.table_actions);

    let opt_lookup_info = match table_actions.get_mut(&trans_id.action_id()) {
        Some(&mut TableAction::Lookup(ref mut lookup)) => Some((
            lookup.recv_timeout(
                &trans_id,
                &work_storage.routing_table,
                &work_storage.out_channel,
                event_loop,
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
        Some((LookupStatus::Completed, info_hash)) => work_storage
            .event_tx
            .send(DhtEvent::LookupCompleted(info_hash))
            .unwrap_or(()),
        Some((LookupStatus::Failed, _)) => {
            shutdown_event_loop(event_loop, ShutdownCause::Unspecified)
        }
        Some((LookupStatus::Values(v), info_hash)) => {
            // Add values to handshaker
            for v4_addr in v {
                let sock_addr = SocketAddr::V4(v4_addr);
                work_storage
                    .event_tx
                    .send(DhtEvent::PeerFound(info_hash, sock_addr))
                    .unwrap_or(());
            }
        }
    }
}

fn handle_check_lookup_endgame(
    handler: &mut DhtHandler,
    event_loop: &mut EventLoop<DhtHandler>,
    trans_id: TransactionID,
) {
    let (work_storage, table_actions) = (&mut handler.detached, &mut handler.table_actions);

    let opt_lookup_info = match table_actions.remove(&trans_id.action_id()) {
        Some(TableAction::Lookup(mut lookup)) => Some((
            lookup.recv_finished(
                work_storage.announce_port,
                &work_storage.routing_table,
                &work_storage.out_channel,
            ),
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
        Some((LookupStatus::Completed, info_hash)) => work_storage
            .event_tx
            .send(DhtEvent::LookupCompleted(info_hash))
            .unwrap_or(()),
        Some((LookupStatus::Failed, _)) => {
            shutdown_event_loop(event_loop, ShutdownCause::Unspecified)
        }
        Some((LookupStatus::Values(v), info_hash)) => {
            // Add values to handshaker
            for v4_addr in v {
                let sock_addr = SocketAddr::V4(v4_addr);
                work_storage
                    .event_tx
                    .send(DhtEvent::PeerFound(info_hash, sock_addr))
                    .unwrap_or(());
            }
        }
    }
}
