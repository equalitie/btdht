use super::{
    resolve,
    socket::Socket,
    timer::{Timeout, Timer},
    ActionStatus, ScheduledTaskCheck,
};
use crate::message::{FindNodeRequest, Message, MessageBody, Request};
use crate::routing::bucket::Bucket;
use crate::routing::node::NodeStatus;
use crate::routing::table::{self, RoutingTable};
use crate::transaction::{ActionID, MIDGenerator, TransactionID};
use crate::{id::NodeId, routing::node::NodeHandle};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::Duration,
};

const BOOTSTRAP_INITIAL_TIMEOUT: Duration = Duration::from_millis(2500);
const BOOTSTRAP_NODE_TIMEOUT: Duration = Duration::from_millis(500);

const BOOTSTRAP_PINGS_PER_BUCKET: usize = 8;

pub(crate) struct TableBootstrap {
    id_generator: MIDGenerator,
    starting_nodes: HashSet<SocketAddr>,
    active_messages: HashMap<TransactionID, Timeout>,
    curr_bootstrap_bucket: usize,
    initial_responses: HashSet<SocketAddr>,
    initial_responses_expected: usize,
}

impl TableBootstrap {
    pub fn new(id_generator: MIDGenerator, nodes: HashSet<SocketAddr>) -> TableBootstrap {
        TableBootstrap {
            id_generator,
            starting_nodes: nodes,
            active_messages: HashMap::new(),
            curr_bootstrap_bucket: 0,
            initial_responses: HashSet::new(),
            initial_responses_expected: 0,
        }
    }

    pub async fn start_bootstrap(
        &mut self,
        table_id: NodeId,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
        starting_routers: &HashSet<String>,
        resolved_routers: &mut HashSet<SocketAddr>,
    ) -> ActionStatus {
        // Reset the bootstrap state
        self.active_messages.clear();
        self.curr_bootstrap_bucket = 0;

        // In the initial round, we send the requests to contacts (nodes and routers) who are not in
        // our routing table. Because of that, we don't care who we receive a response from, only
        // that we receive sufficient number of unique ones. Thus we use the same transaction id
        // for all of them.
        // After the initial round we are sending only to nodes from the routing table, so we use
        // unique transaction id per node.
        let trans_id = self.id_generator.generate();

        // Set a timer to begin the actual bootstrap
        let timeout = timer.schedule_in(
            BOOTSTRAP_INITIAL_TIMEOUT,
            ScheduledTaskCheck::BootstrapTimeout(trans_id),
        );

        self.active_messages.insert(trans_id, timeout);

        let find_node_msg = Message {
            transaction_id: trans_id.as_ref().to_vec(),
            body: MessageBody::Request(Request::FindNode(FindNodeRequest {
                id: table_id,
                target: table_id,
                want: None, // we want only contacts of the same address family we have.
            })),
        }
        .encode();

        // Ping all initial routers and nodes
        self.initial_responses_expected = 0;
        self.initial_responses.clear();

        *resolved_routers = resolve(&starting_routers, socket.ip_version()).await;

        for addr in resolved_routers.iter().chain(self.starting_nodes.iter()) {
            match socket.send(&find_node_msg, *addr).await {
                Ok(()) => {
                    if self.initial_responses_expected < BOOTSTRAP_PINGS_PER_BUCKET {
                        self.initial_responses_expected += 1
                    }
                }
                Err(error) => log::error!("Failed to send bootstrap message to router: {}", error),
            }
        }

        if self.initial_responses_expected > 0 {
            ActionStatus::Ongoing
        } else {
            ActionStatus::Completed
        }
    }

    pub fn action_id(&self) -> ActionID {
        self.id_generator.action_id()
    }

    pub async fn recv_response(
        &mut self,
        addr: SocketAddr,
        trans_id: &TransactionID,
        table: &mut RoutingTable,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> ActionStatus {
        // Process the message transaction id
        let timeout = if let Some(t) = self.active_messages.get(trans_id) {
            *t
        } else {
            log::warn!("Received expired/unsolicited node response for an active table bootstrap");
            return ActionStatus::Ongoing;
        };

        // In the initial round all the messages have the same transaction id so clear it only after
        // we receive sufficient number of unique response. After the initial round, every message
        // has its own transaction id so clear it immediately.
        if self.curr_bootstrap_bucket == 0 {
            self.initial_responses.insert(addr);

            if self.initial_responses.len() >= self.initial_responses_expected {
                timer.cancel(timeout);
                self.active_messages.remove(trans_id);
            }
        } else {
            timer.cancel(timeout);
            self.active_messages.remove(trans_id);
        }

        // Check if we need to bootstrap on the next bucket
        if self.active_messages.is_empty() {
            self.bootstrap_next_bucket(table, socket, timer).await
        } else {
            ActionStatus::Ongoing
        }
    }

    pub async fn recv_timeout(
        &mut self,
        trans_id: &TransactionID,
        table: &mut RoutingTable,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> ActionStatus {
        if self.active_messages.remove(trans_id).is_none() {
            log::warn!("Received expired/unsolicited node timeout for an active table bootstrap");
            return ActionStatus::Ongoing;
        }

        // Check if we need to bootstrap on the next bucket
        if self.active_messages.is_empty() {
            self.bootstrap_next_bucket(table, socket, timer).await
        } else {
            ActionStatus::Ongoing
        }
    }

    async fn bootstrap_next_bucket(
        &mut self,
        table: &mut RoutingTable,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> ActionStatus {
        loop {
            if self.curr_bootstrap_bucket >= table::MAX_BUCKETS {
                return ActionStatus::Completed;
            }

            let target_id = table.node_id().flip_bit(self.curr_bootstrap_bucket);

            // Get the optimal iterator to bootstrap the current bucket
            let nodes: Vec<_> =
                if self.curr_bootstrap_bucket == 0 || self.curr_bootstrap_bucket == 1 {
                    table
                        .closest_nodes(target_id)
                        .filter(|n| n.status() == NodeStatus::Questionable)
                        .take(BOOTSTRAP_PINGS_PER_BUCKET)
                        .map(|node| *node.handle())
                        .collect()
                } else {
                    let mut buckets = table.buckets().skip(self.curr_bootstrap_bucket - 2);
                    let dummy_bucket = Bucket::new();

                    // Sloppy probabilities of our target node residing at the node
                    let percent_25_bucket = if let Some(bucket) = buckets.next() {
                        bucket.iter()
                    } else {
                        dummy_bucket.iter()
                    };
                    let percent_50_bucket = if let Some(bucket) = buckets.next() {
                        bucket.iter()
                    } else {
                        dummy_bucket.iter()
                    };
                    let percent_100_bucket = if let Some(bucket) = buckets.next() {
                        bucket.iter()
                    } else {
                        dummy_bucket.iter()
                    };

                    // TODO: Figure out why chaining them in reverse gives us more total nodes on average, perhaps it allows us to fill up the lower
                    // buckets faster at the cost of less nodes in the higher buckets (since lower buckets are very easy to fill)...Although it should
                    // even out since we are stagnating buckets, so doing it in reverse may make sense since on the 3rd iteration, it allows us to ping
                    // questionable nodes in our first buckets right off the bat.
                    percent_25_bucket
                        .chain(percent_50_bucket)
                        .chain(percent_100_bucket)
                        .filter(|n| n.status() == NodeStatus::Questionable)
                        .take(BOOTSTRAP_PINGS_PER_BUCKET)
                        .map(|node| *node.handle())
                        .collect()
                };

            self.curr_bootstrap_bucket += 1;

            // If we failed to send any message, try again on the next bucket.
            if self
                .send_bootstrap_requests(&nodes, target_id, table, socket, timer)
                .await
            {
                return ActionStatus::Ongoing;
            }
        }
    }

    // If this returns `false` it means the request wasn't sent to any node (either because there
    // were no nodes or because all the sends failed). We should proceed to the next bucket in that
    // case.
    async fn send_bootstrap_requests(
        &mut self,
        nodes: &[NodeHandle],
        target_id: NodeId,
        table: &mut RoutingTable,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> bool {
        let mut messages_sent = 0;

        for node in nodes {
            // Generate a transaction id
            let trans_id = self.id_generator.generate();
            let find_node_msg = Message {
                transaction_id: trans_id.as_ref().to_vec(),
                body: MessageBody::Request(Request::FindNode(FindNodeRequest {
                    id: table.node_id(),
                    target: target_id,
                    want: None,
                })),
            }
            .encode();

            // Add a timeout for the node
            let timeout = timer.schedule_in(
                BOOTSTRAP_NODE_TIMEOUT,
                ScheduledTaskCheck::BootstrapTimeout(trans_id),
            );

            // Send the message to the node
            if let Err(error) = socket.send(&find_node_msg, node.addr).await {
                log::error!("Could not send a bootstrap message: {}", error);
                continue;
            }

            // Mark that we requested from the node
            if let Some(node) = table.find_node_mut(node) {
                node.local_request();
            }

            // Create an entry for the timeout in the map
            self.active_messages.insert(trans_id, timeout);

            messages_sent += 1;
        }

        messages_sent > 0
    }
}
