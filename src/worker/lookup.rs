use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use tokio::sync::mpsc;

use crate::id::{InfoHash, NodeId, ShaHash, NODE_ID_LEN};
use crate::message::{
    AnnouncePeerRequest, GetPeersRequest, GetPeersResponse, Message, MessageBody, Request,
};
use crate::mio::{EventLoop, Timeout};
use crate::routing::bucket;
use crate::routing::node::{Node, NodeInfo, NodeStatus};
use crate::routing::table::RoutingTable;
use crate::transaction::{MIDGenerator, TransactionID};
use crate::worker::handler::DhtHandler;
use crate::worker::ScheduledTaskCheck;

const LOOKUP_TIMEOUT_MS: u64 = 1500;
const ENDGAME_TIMEOUT_MS: u64 = 1500;

// Currently using the aggressive variant of the standard lookup procedure.
// https://people.kth.se/~rauljc/p2p11/jimenez2011subsecond.pdf

// TODO: Handle case where a request round fails, should we fail the whole lookup (clear acvite lookups?)
// TODO: Clean up the code in this module.

const INITIAL_PICK_NUM: usize = 4; // Alpha
const ITERATIVE_PICK_NUM: usize = 3; // Beta
const ANNOUNCE_PICK_NUM: usize = 8; // # Announces

type Distance = ShaHash;
type DistanceToBeat = ShaHash;

#[derive(Debug, PartialEq, Eq)]
pub enum LookupStatus {
    Searching,
    Values(Vec<SocketAddrV4>),
    Completed,
    Failed,
}

pub struct TableLookup {
    table_id: NodeId,
    target_id: InfoHash,
    in_endgame: bool,
    // If we have received any values in the lookup.
    recv_values: bool,
    id_generator: MIDGenerator,
    will_announce: bool,
    // DistanceToBeat is the distance that the responses of the current lookup needs to beat,
    // interestingly enough (and super important), this distance may not be eqaul to the
    // requested node's distance
    active_lookups: HashMap<TransactionID, (DistanceToBeat, Timeout)>,
    announce_tokens: HashMap<NodeInfo, Vec<u8>>,
    requested_nodes: HashSet<NodeInfo>,
    // Storing whether or not it has ever been pinged so that we
    // can perform the brute force lookup if the lookup failed
    all_sorted_nodes: Vec<(Distance, Node, bool)>,
}

// Gather nodes

impl TableLookup {
    pub fn new(
        table_id: NodeId,
        target_id: InfoHash,
        id_generator: MIDGenerator,
        will_announce: bool,
        table: &RoutingTable,
        out: &mpsc::Sender<(Vec<u8>, SocketAddr)>,
        event_loop: &mut EventLoop<DhtHandler>,
    ) -> Option<TableLookup> {
        // Pick a buckets worth of nodes and put them into the all_sorted_nodes list
        let mut all_sorted_nodes = Vec::with_capacity(bucket::MAX_BUCKET_SIZE);
        for node in table
            .closest_nodes(target_id)
            .filter(|n| n.status() == NodeStatus::Good)
            .take(bucket::MAX_BUCKET_SIZE)
        {
            insert_sorted_node(&mut all_sorted_nodes, target_id, node.clone(), false);
        }

        // Call pick_initial_nodes with the all_sorted_nodes list as an iterator
        let initial_pick_nodes = pick_initial_nodes(all_sorted_nodes.iter_mut());
        let initial_pick_nodes_filtered =
            initial_pick_nodes
                .iter()
                .filter(|&&(_, good)| good)
                .map(|&(ref node, _)| {
                    let distance_to_beat = node.id() ^ target_id;

                    (node, distance_to_beat)
                });

        // Construct the lookup table structure
        let mut table_lookup = TableLookup {
            table_id,
            target_id,
            in_endgame: false,
            recv_values: false,
            id_generator,
            will_announce,
            all_sorted_nodes,
            announce_tokens: HashMap::new(),
            requested_nodes: HashSet::new(),
            active_lookups: HashMap::with_capacity(INITIAL_PICK_NUM),
        };

        // Call start_request_round with the list of initial_nodes (return even if the search completed...for now :D)
        if table_lookup.start_request_round(initial_pick_nodes_filtered, table, out, event_loop)
            != LookupStatus::Failed
        {
            Some(table_lookup)
        } else {
            None
        }
    }

    pub fn info_hash(&self) -> InfoHash {
        self.target_id
    }

    pub fn recv_response(
        &mut self,
        node: Node,
        trans_id: &TransactionID,
        msg: GetPeersResponse,
        table: &RoutingTable,
        out: &mpsc::Sender<(Vec<u8>, SocketAddr)>,
        event_loop: &mut EventLoop<DhtHandler>,
    ) -> LookupStatus {
        // Process the message transaction id
        let (dist_to_beat, timeout) = if let Some(lookup) = self.active_lookups.remove(trans_id) {
            lookup
        } else {
            warn!(
                "bip_dht: Received expired/unsolicited node response for an active table \
                   lookup..."
            );
            return self.current_lookup_status();
        };

        // Cancel the timeout (if this is not an endgame response)
        if !self.in_endgame {
            event_loop.clear_timeout(timeout);
        }

        // Add the announce token to our list of tokens
        self.announce_tokens.insert(*node.info(), msg.token);

        let nodes = msg.nodes;
        let values = msg.values;

        // Check if we beat the distance, get the next distance to beat
        let (iterate_nodes, next_dist_to_beat) = if !nodes.is_empty() {
            let requested_nodes = &self.requested_nodes;

            // Get the closest distance (or the current distance)
            let next_dist_to_beat = nodes
                .iter()
                .filter(|node| !requested_nodes.contains(node))
                .fold(dist_to_beat, |closest, node| {
                    let distance = self.target_id ^ node.id;

                    if distance < closest {
                        distance
                    } else {
                        closest
                    }
                });

            // Check if we got closer (equal to is not enough)
            let iterate_nodes = if next_dist_to_beat < dist_to_beat {
                let iterate_nodes = pick_iterate_nodes(
                    nodes
                        .iter()
                        .filter(|node| !requested_nodes.contains(node))
                        .copied(),
                    self.target_id,
                );

                // Push nodes into the all nodes list
                for node in nodes {
                    let node = Node::as_questionable(node.id, node.addr);
                    let will_ping = iterate_nodes.iter().any(|(n, _)| n == &node);

                    insert_sorted_node(&mut self.all_sorted_nodes, self.target_id, node, will_ping);
                }

                Some(iterate_nodes)
            } else {
                // Push nodes into the all nodes list
                for node in nodes {
                    let node = Node::as_questionable(node.id, node.addr);
                    insert_sorted_node(&mut self.all_sorted_nodes, self.target_id, node, false);
                }

                None
            };

            (iterate_nodes, next_dist_to_beat)
        } else {
            (None, dist_to_beat)
        };

        // Check if we need to iterate (not in the endgame already)
        if !self.in_endgame {
            // If the node gave us a closer id than its own to the target id, continue the search
            if let Some(ref nodes) = iterate_nodes {
                let filtered_nodes = nodes
                    .iter()
                    .filter(|&&(_, good)| good)
                    .map(|&(ref n, _)| (n, next_dist_to_beat));
                if self.start_request_round(filtered_nodes, table, out, event_loop)
                    == LookupStatus::Failed
                {
                    return LookupStatus::Failed;
                }
            }

            // If there are not more active lookups, start the endgame
            if self.active_lookups.is_empty()
                && self.start_endgame_round(table, out, event_loop) == LookupStatus::Failed
            {
                return LookupStatus::Failed;
            }
        }

        if values.is_empty() {
            self.current_lookup_status()
        } else {
            LookupStatus::Values(values)
        }
    }

    pub fn recv_timeout(
        &mut self,
        trans_id: &TransactionID,
        table: &RoutingTable,
        out: &mpsc::Sender<(Vec<u8>, SocketAddr)>,
        event_loop: &mut EventLoop<DhtHandler>,
    ) -> LookupStatus {
        if self.active_lookups.remove(trans_id).is_none() {
            warn!(
                "bip_dht: Received expired/unsolicited node timeout for an active table \
                   lookup..."
            );
            return self.current_lookup_status();
        }

        if !self.in_endgame {
            // If there are not more active lookups, start the endgame
            if self.active_lookups.is_empty()
                && self.start_endgame_round(table, out, event_loop) == LookupStatus::Failed
            {
                return LookupStatus::Failed;
            }
        }

        self.current_lookup_status()
    }

    pub fn recv_finished(
        &mut self,
        port: Option<u16>,
        table: &RoutingTable,
        out: &mpsc::Sender<(Vec<u8>, SocketAddr)>,
    ) -> LookupStatus {
        let mut fatal_error = false;

        // Announce if we were told to
        if self.will_announce {
            // Partial borrow so the filter function doesnt capture all of self
            let announce_tokens = &self.announce_tokens;

            for (_, node, _) in self
                .all_sorted_nodes
                .iter()
                .filter(|(_, node, _)| announce_tokens.contains_key(node.info()))
                .take(ANNOUNCE_PICK_NUM)
            {
                let trans_id = self.id_generator.generate();
                let token = announce_tokens.get(node.info()).unwrap();

                let announce_peer_req = AnnouncePeerRequest {
                    id: self.table_id,
                    info_hash: self.target_id,
                    token: token.clone(),
                    port,
                };
                let announce_peer_msg = Message {
                    transaction_id: trans_id.as_ref().to_vec(),
                    body: MessageBody::Request(Request::AnnouncePeer(announce_peer_req)),
                };
                let announce_peer_msg = announce_peer_msg.encode();

                if out.blocking_send((announce_peer_msg, node.addr())).is_err() {
                    error!(
                        "bip_dht: TableLookup announce request failed to send through the out \
                            channel..."
                    );
                    fatal_error = true;
                }

                if !fatal_error {
                    // We requested from the node, marke it down if the node is in our routing table
                    if let Some(n) = table.find_node(node) {
                        n.local_request()
                    }
                }
            }
        }

        // This may not be cleared since we didnt set a timeout for each node, any nodes that didnt respond would still be in here.
        self.active_lookups.clear();
        self.in_endgame = false;

        if fatal_error {
            LookupStatus::Failed
        } else {
            self.current_lookup_status()
        }
    }

    fn current_lookup_status(&self) -> LookupStatus {
        if self.in_endgame || !self.active_lookups.is_empty() {
            LookupStatus::Searching
        } else {
            LookupStatus::Completed
        }
    }

    fn start_request_round<'a, I>(
        &mut self,
        nodes: I,
        table: &RoutingTable,
        out: &mpsc::Sender<(Vec<u8>, SocketAddr)>,
        event_loop: &mut EventLoop<DhtHandler>,
    ) -> LookupStatus
    where
        I: Iterator<Item = (&'a Node, DistanceToBeat)>,
    {
        // Loop through the given nodes
        let mut messages_sent = 0;
        for (node, dist_to_beat) in nodes {
            // Generate a transaction id for this message
            let trans_id = self.id_generator.generate();

            // Try to start a timeout for the node
            let res_timeout = event_loop.timeout_ms(
                (0, ScheduledTaskCheck::LookupTimeout(trans_id)),
                LOOKUP_TIMEOUT_MS,
            );
            let timeout = if let Ok(t) = res_timeout {
                t
            } else {
                error!("bip_dht: Failed to set a timeout for a table lookup...");
                return LookupStatus::Failed;
            };

            // Associate the transaction id with the distance the returned nodes must beat and the timeout token
            self.active_lookups
                .insert(trans_id, (dist_to_beat, timeout));

            // Send the message to the node
            let get_peers_msg = Message {
                transaction_id: trans_id.as_ref().to_vec(),
                body: MessageBody::Request(Request::GetPeers(GetPeersRequest {
                    id: self.table_id,
                    info_hash: self.target_id,
                })),
            }
            .encode();

            if out.blocking_send((get_peers_msg, node.addr())).is_err() {
                error!("bip_dht: Could not send a lookup message through the channel...");
                return LookupStatus::Failed;
            }

            // We requested from the node, mark it down
            self.requested_nodes.insert(*node.info());

            // Update the node in the routing table
            if let Some(n) = table.find_node(node) {
                n.local_request()
            }

            messages_sent += 1;
        }

        if messages_sent == 0 {
            self.active_lookups.clear();
            LookupStatus::Completed
        } else {
            LookupStatus::Searching
        }
    }

    fn start_endgame_round(
        &mut self,
        table: &RoutingTable,
        out: &mpsc::Sender<(Vec<u8>, SocketAddr)>,
        event_loop: &mut EventLoop<DhtHandler>,
    ) -> LookupStatus {
        // Entering the endgame phase
        self.in_endgame = true;

        // Try to start a global message timeout for the endgame
        let res_timeout = event_loop.timeout_ms(
            (
                0,
                ScheduledTaskCheck::LookupEndGame(self.id_generator.generate()),
            ),
            ENDGAME_TIMEOUT_MS,
        );
        let timeout = if let Ok(t) = res_timeout {
            t
        } else {
            error!("bip_dht: Failed to set a timeout for table lookup endgame...");
            return LookupStatus::Failed;
        };

        // Request all unpinged nodes if we didnt receive any values
        if !self.recv_values {
            for node_info in self
                .all_sorted_nodes
                .iter_mut()
                .filter(|&&mut (_, _, req)| !req)
            {
                let &mut (ref node_dist, ref node, ref mut req) = node_info;

                // Generate a transaction id for this message
                let trans_id = self.id_generator.generate();

                // Associate the transaction id with this node's distance and its timeout token
                // We dont actually need to keep track of this information, but we do still need to
                // filter out unsolicited responses by using the active_lookups map!!!
                self.active_lookups.insert(trans_id, (*node_dist, timeout));

                // Send the message to the node
                let get_peers_msg = Message {
                    transaction_id: trans_id.as_ref().to_vec(),
                    body: MessageBody::Request(Request::GetPeers(GetPeersRequest {
                        id: self.table_id,
                        info_hash: self.target_id,
                    })),
                }
                .encode();

                if out.blocking_send((get_peers_msg, node.addr())).is_err() {
                    error!("bip_dht: Could not send an endgame message through the channel...");
                    return LookupStatus::Failed;
                }

                // Mark that we requested from the node in the RoutingTable
                if let Some(n) = table.find_node(node) {
                    n.local_request()
                }

                // Mark that we requested from the node
                *req = true;
            }
        }

        LookupStatus::Searching
    }
}

/// Picks a number of nodes from the sorted distance iterator to ping on the first round.
fn pick_initial_nodes<'a, I>(sorted_nodes: I) -> [(Node, bool); INITIAL_PICK_NUM]
where
    I: Iterator<Item = &'a mut (Distance, Node, bool)>,
{
    let dummy_id = [0u8; NODE_ID_LEN].into();
    let default = (
        Node::as_bad(dummy_id, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))),
        false,
    );

    let mut pick_nodes = [default.clone(), default.clone(), default.clone(), default];
    for (src, dst) in sorted_nodes.zip(pick_nodes.iter_mut()) {
        dst.0 = src.1.clone();
        dst.1 = true;

        // Mark that the node has been requested from
        src.2 = true;
    }

    pick_nodes
}

/// Picks a number of nodes from the unsorted distance iterator to ping on iterative rounds.
fn pick_iterate_nodes<I>(
    unsorted_nodes: I,
    target_id: InfoHash,
) -> [(Node, bool); ITERATIVE_PICK_NUM]
where
    I: Iterator<Item = NodeInfo>,
{
    let dummy_id = [0u8; NODE_ID_LEN].into();
    let default = (
        Node::as_bad(dummy_id, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))),
        false,
    );

    let mut pick_nodes = [default.clone(), default.clone(), default];
    for node in unsorted_nodes {
        let node = Node::as_questionable(node.id, node.addr);
        insert_closest_nodes(&mut pick_nodes, target_id, node);
    }

    pick_nodes
}

/// Inserts the node into the slice if a slot in the slice is unused or a node
/// in the slice is further from the target id than the node being inserted.
fn insert_closest_nodes(nodes: &mut [(Node, bool)], target_id: InfoHash, new_node: Node) {
    let new_distance = target_id ^ new_node.id();

    for &mut (ref mut old_node, ref mut used) in nodes.iter_mut() {
        if !*used {
            // Slot was not in use, go ahead and place the node
            *old_node = new_node;
            *used = true;
            return;
        } else {
            // Slot is in use, see if our node is closer to the target
            let old_distance = target_id ^ old_node.id();

            if new_distance < old_distance {
                *old_node = new_node;
                return;
            }
        }
    }
}

/// Inserts the Node into the list of nodes based on its distance from the target node.
///
/// Nodes at the start of the list are closer to the target node than nodes at the end.
fn insert_sorted_node(
    nodes: &mut Vec<(Distance, Node, bool)>,
    target: InfoHash,
    node: Node,
    pinged: bool,
) {
    let node_id = node.id();
    let node_dist = target ^ node_id;

    // Perform a search by distance from the target id
    let search_result = nodes.binary_search_by(|&(dist, _, _)| dist.cmp(&node_dist));
    match search_result {
        Ok(dup_index) => {
            // TODO: Bug here, what happens when multiple nodes with the same distance are
            // present, but we dont get the index of the duplicate node (its in the list) from
            // the search, then we would have a duplicate node in the list!
            // Insert only if this node is different (it is ok if they have the same id)
            if nodes[dup_index].1 != node {
                nodes.insert(dup_index, (node_dist, node, pinged));
            }
        }
        Err(ins_index) => nodes.insert(ins_index, (node_dist, node, pinged)),
    };
}
