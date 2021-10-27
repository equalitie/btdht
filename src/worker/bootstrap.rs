use super::{
    socket,
    timer::{Timeout, Timer},
    ScheduledTaskCheck,
};
use crate::message::{FindNodeRequest, Message, MessageBody, Request};
use crate::routing::bucket::Bucket;
use crate::routing::node::NodeStatus;
use crate::routing::table::{self, RoutingTable};
use crate::transaction::{MIDGenerator, TransactionID};
use crate::{id::NodeId, routing::node::NodeInfo};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;

const BOOTSTRAP_INITIAL_TIMEOUT: Duration = Duration::from_millis(2500);
const BOOTSTRAP_NODE_TIMEOUT: Duration = Duration::from_millis(500);

const BOOTSTRAP_PINGS_PER_BUCKET: usize = 8;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BootstrapStatus {
    /// Bootstrap has been finished.
    Idle,
    /// Bootstrap is in progress.
    Bootstrapping,
    /// Bootstrap just finished.
    Completed,
    /// Bootstrap failed in a fatal way.
    Failed,
}

pub(crate) struct TableBootstrap {
    table_id: NodeId,
    id_generator: MIDGenerator,
    starting_nodes: HashSet<SocketAddr>,
    active_messages: HashMap<TransactionID, Timeout>,
    starting_routers: HashSet<SocketAddr>,
    curr_bootstrap_bucket: usize,
}

impl TableBootstrap {
    pub fn new(
        table_id: NodeId,
        id_generator: MIDGenerator,
        nodes: HashSet<SocketAddr>,
        routers: HashSet<SocketAddr>,
    ) -> TableBootstrap {
        TableBootstrap {
            table_id,
            id_generator,
            starting_nodes: nodes,
            starting_routers: routers,
            active_messages: HashMap::new(),
            curr_bootstrap_bucket: 0,
        }
    }

    pub fn start_bootstrap(
        &mut self,
        socket: &UdpSocket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> BootstrapStatus {
        // Reset the bootstrap state
        self.active_messages.clear();
        self.curr_bootstrap_bucket = 0;

        // Generate transaction id for the initial bootstrap messages
        let trans_id = self.id_generator.generate();

        // Set a timer to begin the actual bootstrap
        let timeout = timer.schedule_in(
            BOOTSTRAP_INITIAL_TIMEOUT,
            ScheduledTaskCheck::BootstrapTimeout(trans_id),
        );

        // Insert the timeout into the active bootstraps just so we can check if a response was valid (and begin the bucket bootstraps)
        self.active_messages.insert(trans_id, timeout);

        let find_node_msg = Message {
            transaction_id: trans_id.as_ref().to_vec(),
            body: MessageBody::Request(Request::FindNode(FindNodeRequest {
                id: self.table_id,
                target: self.table_id,
            })),
        }
        .encode();

        // Ping all initial routers and nodes
        let mut successes = 0;

        for addr in self
            .starting_routers
            .iter()
            .chain(self.starting_nodes.iter())
        {
            match socket::blocking_send(socket, &find_node_msg, *addr) {
                Ok(()) => {
                    successes += 1;
                }
                Err(error) => error!("Failed to send bootstrap message to router: {}", error),
            }
        }

        if successes > 0 {
            self.current_bootstrap_status()
        } else {
            BootstrapStatus::Failed
        }
    }

    pub fn is_router(&self, addr: &SocketAddr) -> bool {
        self.starting_routers.contains(addr)
    }

    pub fn recv_response(
        &mut self,
        trans_id: &TransactionID,
        table: &mut RoutingTable,
        socket: &UdpSocket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> BootstrapStatus {
        // Process the message transaction id
        let timeout = if let Some(t) = self.active_messages.get(trans_id) {
            *t
        } else {
            warn!(
                "bip_dht: Received expired/unsolicited node response for an active table \
                   bootstrap..."
            );
            return self.current_bootstrap_status();
        };

        // If this response was from the initial bootstrap, we don't want to clear the timeout or remove
        // the token from the map as we want to wait until the proper timeout has been triggered before starting
        if self.curr_bootstrap_bucket != 0 {
            // Message was not from the initial ping
            // Remove the timeout from the event loop
            timer.cancel(timeout);

            // Remove the token from the mapping
            self.active_messages.remove(trans_id);
        }

        // Check if we need to bootstrap on the next bucket
        if self.active_messages.is_empty() {
            return self.bootstrap_next_bucket(table, socket, timer);
        }

        self.current_bootstrap_status()
    }

    pub fn recv_timeout(
        &mut self,
        trans_id: &TransactionID,
        table: &mut RoutingTable,
        socket: &UdpSocket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> BootstrapStatus {
        if self.active_messages.remove(trans_id).is_none() {
            warn!(
                "bip_dht: Received expired/unsolicited node timeout for an active table \
                   bootstrap..."
            );
            return self.current_bootstrap_status();
        }

        // Check if we need to bootstrap on the next bucket
        if self.active_messages.is_empty() {
            return self.bootstrap_next_bucket(table, socket, timer);
        }

        self.current_bootstrap_status()
    }

    // Returns true if there are more buckets to bootstrap, false otherwise
    fn bootstrap_next_bucket(
        &mut self,
        table: &mut RoutingTable,
        socket: &UdpSocket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> BootstrapStatus {
        let target_id = self.table_id.flip_bit(self.curr_bootstrap_bucket);

        // Get the optimal iterator to bootstrap the current bucket
        if self.curr_bootstrap_bucket == 0 || self.curr_bootstrap_bucket == 1 {
            let nodes: Vec<_> = table
                .closest_nodes(target_id)
                .filter(|n| n.status() == NodeStatus::Questionable)
                .take(BOOTSTRAP_PINGS_PER_BUCKET)
                .map(|node| *node.info())
                .collect();

            self.send_bootstrap_requests(&nodes, target_id, table, socket, timer)
        } else {
            let nodes: Vec<_> = {
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
                    .map(|node| *node.info())
                    .collect()
            };

            self.send_bootstrap_requests(&nodes, target_id, table, socket, timer)
        }
    }

    fn send_bootstrap_requests(
        &mut self,
        nodes: &[NodeInfo],
        target_id: NodeId,
        table: &mut RoutingTable,
        socket: &UdpSocket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> BootstrapStatus {
        info!(
            "bip_dht: bootstrap::send_bootstrap_requests {}",
            self.curr_bootstrap_bucket
        );

        let mut messages_sent = 0;

        for node in nodes {
            // Generate a transaction id
            let trans_id = self.id_generator.generate();
            let find_node_msg = Message {
                transaction_id: trans_id.as_ref().to_vec(),
                body: MessageBody::Request(Request::FindNode(FindNodeRequest {
                    id: self.table_id,
                    target: target_id,
                })),
            }
            .encode();

            // Add a timeout for the node
            let timeout = timer.schedule_in(
                BOOTSTRAP_NODE_TIMEOUT,
                ScheduledTaskCheck::BootstrapTimeout(trans_id),
            );

            // Send the message to the node
            if let Err(error) = socket::blocking_send(socket, &find_node_msg, node.addr) {
                error!("Could not send a bootstrap message: {}", error);
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

        self.curr_bootstrap_bucket += 1;
        if self.curr_bootstrap_bucket == table::MAX_BUCKETS {
            BootstrapStatus::Completed
        } else if messages_sent == 0 {
            self.bootstrap_next_bucket(table, socket, timer)
        } else {
            BootstrapStatus::Bootstrapping
        }
    }

    fn current_bootstrap_status(&self) -> BootstrapStatus {
        if self.curr_bootstrap_bucket == table::MAX_BUCKETS || self.active_messages.is_empty() {
            BootstrapStatus::Idle
        } else {
            BootstrapStatus::Bootstrapping
        }
    }
}
