use super::{
    socket::Socket,
    timer::{Timeout, Timer},
    ScheduledTaskCheck,
};
use crate::id::{InfoHash, ShaHash, NODE_ID_LEN};
use crate::message::{
    AnnouncePeerRequest, GetPeersRequest, GetPeersResponse, Message, MessageBody, Request,
};
use crate::routing::bucket;
use crate::routing::node::{Node, NodeHandle, NodeStatus};
use crate::routing::table::RoutingTable;
use crate::transaction::{MIDGenerator, TransactionID};
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

const LOOKUP_TIMEOUT: Duration = Duration::from_millis(1500);
const ENDGAME_TIMEOUT: Duration = Duration::from_millis(1500);

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
pub(crate) enum LookupStatus {
    Searching,
    Values(Vec<SocketAddr>),
    Completed,
}

pub(crate) struct TableLookup {
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
    announce_tokens: HashMap<NodeHandle, Vec<u8>>,
    requested_nodes: HashSet<NodeHandle>,
    // Storing whether or not it has ever been pinged so that we
    // can perform the brute force lookup if the lookup failed
    all_sorted_nodes: Vec<(Distance, NodeHandle, bool)>,
}

// Gather nodes

impl TableLookup {
    pub async fn new(
        target_id: InfoHash,
        id_generator: MIDGenerator,
        will_announce: bool,
        table: &mut RoutingTable,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> TableLookup {
        // Pick a buckets worth of nodes and put them into the all_sorted_nodes list
        let mut all_sorted_nodes = Vec::with_capacity(bucket::MAX_BUCKET_SIZE);
        for node in table
            .closest_nodes(target_id)
            .filter(|n| n.status() == NodeStatus::Good)
            .take(bucket::MAX_BUCKET_SIZE)
        {
            insert_sorted_node(&mut all_sorted_nodes, target_id, *node.handle(), false);
        }

        // Call pick_initial_nodes with the all_sorted_nodes list as an iterator
        let initial_pick_nodes = pick_initial_nodes(all_sorted_nodes.iter_mut());
        let initial_pick_nodes_filtered =
            initial_pick_nodes
                .iter()
                .filter(|(_, good)| *good)
                .map(|(node, _)| {
                    let distance_to_beat = node.id ^ target_id;

                    (node, distance_to_beat)
                });

        // Construct the lookup table structure
        let mut table_lookup = TableLookup {
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
        table_lookup
            .start_request_round(initial_pick_nodes_filtered, table, socket, timer)
            .await;
        table_lookup
    }

    pub fn info_hash(&self) -> InfoHash {
        self.target_id
    }

    pub async fn recv_response(
        &mut self,
        node: Node,
        trans_id: &TransactionID,
        msg: GetPeersResponse,
        table: &mut RoutingTable,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> LookupStatus {
        // Process the message transaction id
        let (dist_to_beat, timeout) = if let Some(lookup) = self.active_lookups.remove(trans_id) {
            lookup
        } else {
            warn!("Received expired/unsolicited node response for an active table lookup");
            return self.current_lookup_status();
        };

        // Cancel the timeout (if this is not an endgame response)
        if !self.in_endgame {
            timer.cancel(timeout);
        }

        // Add the announce token to our list of tokens
        self.announce_tokens.insert(*node.handle(), msg.token);

        let nodes = match socket.local_addr() {
            Ok(SocketAddr::V4(_)) => msg.nodes_v4,
            Ok(SocketAddr::V6(_)) => msg.nodes_v6,
            Err(error) => {
                error!("Failed to retreive local socket address: {}", error);
                vec![]
            }
        };

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
                    let will_ping = iterate_nodes.iter().any(|(n, _)| n == &node);

                    insert_sorted_node(&mut self.all_sorted_nodes, self.target_id, node, will_ping);
                }

                Some(iterate_nodes)
            } else {
                // Push nodes into the all nodes list
                for node in nodes {
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
            if let Some(nodes) = iterate_nodes {
                let filtered_nodes = nodes
                    .iter()
                    .filter(|(_, good)| *good)
                    .map(|(n, _)| (n, next_dist_to_beat));
                self.start_request_round(filtered_nodes, table, socket, timer)
                    .await;
            }

            // If there are not more active lookups, start the endgame
            if self.active_lookups.is_empty() {
                self.start_endgame_round(table, socket, timer).await;
            }
        }

        if values.is_empty() {
            self.current_lookup_status()
        } else {
            LookupStatus::Values(values)
        }
    }

    pub async fn recv_timeout(
        &mut self,
        trans_id: &TransactionID,
        table: &mut RoutingTable,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
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
            if self.active_lookups.is_empty() {
                self.start_endgame_round(table, socket, timer).await;
            }
        }

        self.current_lookup_status()
    }

    pub async fn recv_finished(
        &mut self,
        port: Option<u16>,
        table: &mut RoutingTable,
        socket: &Socket,
    ) -> LookupStatus {
        // Announce if we were told to
        if self.will_announce {
            // Partial borrow so the filter function doesnt capture all of self
            let announce_tokens = &self.announce_tokens;

            for (_, node, _) in self
                .all_sorted_nodes
                .iter()
                .filter(|(_, node, _)| announce_tokens.contains_key(node))
                .take(ANNOUNCE_PICK_NUM)
            {
                let trans_id = self.id_generator.generate();
                let token = announce_tokens.get(node).unwrap();

                let announce_peer_req = AnnouncePeerRequest {
                    id: table.node_id(),
                    info_hash: self.target_id,
                    token: token.clone(),
                    port,
                };
                let announce_peer_msg = Message {
                    transaction_id: trans_id.as_ref().to_vec(),
                    body: MessageBody::Request(Request::AnnouncePeer(announce_peer_req)),
                };
                let announce_peer_msg = announce_peer_msg.encode();

                match socket.send(&announce_peer_msg, node.addr).await {
                    Ok(()) => {
                        // We requested from the node, marke it down if the node is in our routing table
                        if let Some(n) = table.find_node_mut(node) {
                            n.local_request()
                        }
                    }
                    Err(error) => error!("TableLookup announce request failed to send: {}", error),
                }
            }
        }

        // This may not be cleared since we didnt set a timeout for each node, any nodes that didnt respond would still be in here.
        self.active_lookups.clear();
        self.in_endgame = false;

        self.current_lookup_status()
    }

    fn current_lookup_status(&self) -> LookupStatus {
        if self.in_endgame || !self.active_lookups.is_empty() {
            LookupStatus::Searching
        } else {
            LookupStatus::Completed
        }
    }

    async fn start_request_round<'a, I>(
        &mut self,
        nodes: I,
        table: &mut RoutingTable,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> LookupStatus
    where
        I: Iterator<Item = (&'a NodeHandle, DistanceToBeat)>,
    {
        // Loop through the given nodes
        let mut messages_sent = 0;
        for (node, dist_to_beat) in nodes {
            // Generate a transaction id for this message
            let trans_id = self.id_generator.generate();

            // Try to start a timeout for the node
            let timeout =
                timer.schedule_in(LOOKUP_TIMEOUT, ScheduledTaskCheck::LookupTimeout(trans_id));

            // Associate the transaction id with the distance the returned nodes must beat and the timeout token
            self.active_lookups
                .insert(trans_id, (dist_to_beat, timeout));

            // Send the message to the node
            let get_peers_msg = Message {
                transaction_id: trans_id.as_ref().to_vec(),
                body: MessageBody::Request(Request::GetPeers(GetPeersRequest {
                    id: table.node_id(),
                    info_hash: self.target_id,
                    want: None,
                })),
            }
            .encode();

            if let Err(error) = socket.send(&get_peers_msg, node.addr).await {
                error!("Could not send a lookup message: {}", error);
                continue;
            }

            // We requested from the node, mark it down
            self.requested_nodes.insert(*node);

            // Update the node in the routing table
            if let Some(n) = table.find_node_mut(node) {
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

    async fn start_endgame_round(
        &mut self,
        table: &mut RoutingTable,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> LookupStatus {
        // Entering the endgame phase
        self.in_endgame = true;

        // Try to start a global message timeout for the endgame
        let timeout = timer.schedule_in(
            ENDGAME_TIMEOUT,
            ScheduledTaskCheck::LookupEndGame(self.id_generator.generate()),
        );

        // Request all unpinged nodes if we didnt receive any values
        if !self.recv_values {
            for node_info in self.all_sorted_nodes.iter_mut().filter(|(_, _, req)| !req) {
                let (node_dist, node, req) = node_info;

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
                        id: table.node_id(),
                        info_hash: self.target_id,
                        want: None,
                    })),
                }
                .encode();

                if let Err(error) = socket.send(&get_peers_msg, node.addr).await {
                    error!("Could not send an endgame message: {}", error);
                    continue;
                }

                // Mark that we requested from the node in the RoutingTable
                if let Some(n) = table.find_node_mut(node) {
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
fn pick_initial_nodes<'a, I>(sorted_nodes: I) -> [(NodeHandle, bool); INITIAL_PICK_NUM]
where
    I: Iterator<Item = &'a mut (Distance, NodeHandle, bool)>,
{
    let dummy_id = [0u8; NODE_ID_LEN].into();
    let default = (
        NodeHandle::new(dummy_id, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))),
        false,
    );

    let mut pick_nodes = [default; INITIAL_PICK_NUM];
    for (src, dst) in sorted_nodes.zip(pick_nodes.iter_mut()) {
        dst.0 = src.1;
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
) -> [(NodeHandle, bool); ITERATIVE_PICK_NUM]
where
    I: Iterator<Item = NodeHandle>,
{
    let dummy_id = [0u8; NODE_ID_LEN].into();
    let default = (
        NodeHandle::new(dummy_id, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))),
        false,
    );

    let mut pick_nodes = [default; ITERATIVE_PICK_NUM];
    for node in unsorted_nodes {
        insert_closest_nodes(&mut pick_nodes, target_id, node);
    }

    pick_nodes
}

/// Inserts the node into the slice if a slot in the slice is unused or a node
/// in the slice is further from the target id than the node being inserted.
fn insert_closest_nodes(
    nodes: &mut [(NodeHandle, bool)],
    target_id: InfoHash,
    new_node: NodeHandle,
) {
    let new_distance = target_id ^ new_node.id;

    for &mut (ref mut old_node, ref mut used) in nodes.iter_mut() {
        if !*used {
            // Slot was not in use, go ahead and place the node
            *old_node = new_node;
            *used = true;
            return;
        } else {
            // Slot is in use, see if our node is closer to the target
            let old_distance = target_id ^ old_node.id;

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
    nodes: &mut Vec<(Distance, NodeHandle, bool)>,
    target: InfoHash,
    node: NodeHandle,
    pinged: bool,
) {
    let node_id = node.id;
    let node_dist = target ^ node_id;

    // Perform a search by distance from the target id
    let search_result = nodes.binary_search_by(|(dist, _, _)| dist.cmp(&node_dist));
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
