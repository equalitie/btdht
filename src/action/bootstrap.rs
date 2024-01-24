use super::{resolve, IpVersion, Responded, Socket};
use crate::bucket::Bucket;
use crate::message::{FindNodeRequest, Message, MessageBody, Request};
use crate::node::{Node, NodeStatus};
use crate::table::{self, RoutingTable};
use crate::transaction::{MIDGenerator, TransactionID};
use crate::{info_hash::NodeId, node::NodeHandle};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::HashSet,
    net::SocketAddr,
    pin::pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    select,
    sync::{mpsc, watch},
    task, time,
    time::sleep,
};

const INITIAL_TIMEOUT: Duration = Duration::from_millis(2500);
const NODE_TIMEOUT: Duration = Duration::from_millis(500);
const NO_NETWORK_TIMEOUT: Duration = Duration::from_secs(5);
const PERIODIC_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

// We try to rebootstrap when we have fewer nodes than this.
const GOOD_NODE_THRESHOLD: usize = 10;

const PINGS_PER_BUCKET: usize = 8;
const MAX_INITIAL_RESPONSES: usize = 8;

pub(crate) struct TableBootstrap {
    start_tx: watch::Sender<bool>,
    pub state_rx: watch::Receiver<State>,
    //inner: Arc<Mutex<TableBootstrapInner>>,
    worker_handle: task::JoinHandle<()>,
}

impl Drop for TableBootstrap {
    fn drop(&mut self) {
        self.worker_handle.abort();
    }
}

struct TableBootstrapInner {
    this_node_id: NodeId,
    ip_version: IpVersion,
    routers: HashSet<String>,
    id_generator: Mutex<MIDGenerator>,
    starting_nodes: HashSet<SocketAddr>,
    table: Arc<Mutex<RoutingTable>>,
    socket: Arc<Socket>,
    start_rx: watch::Receiver<bool>,
    state_tx: watch::Sender<State>,
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum State {
    AwaitStart,
    InitialContact,
    Bootstrapping,
    Bootstrapped,
    // The starting state or state after a bootstrap has failed and new has been cheduled after a
    // timeout.
    IdleBeforeRebootstrap,
}

impl TableBootstrap {
    pub fn new(
        socket: Arc<Socket>,
        table: Arc<Mutex<RoutingTable>>,
        id_generator: MIDGenerator,
        routers: HashSet<String>,
        nodes: HashSet<SocketAddr>,
    ) -> TableBootstrap {
        let this_node_id = table.lock().unwrap().node_id();

        let (start_tx, start_rx) = watch::channel(false);
        let (state_tx, state_rx) = watch::channel(State::AwaitStart);

        let inner = TableBootstrapInner {
            this_node_id,
            ip_version: socket.ip_version(),
            routers,
            id_generator: Mutex::new(id_generator),
            starting_nodes: nodes,
            table,
            socket,
            start_rx,
            state_tx,
        };

        let worker_handle = task::spawn(inner.run(this_node_id));

        TableBootstrap {
            start_tx,
            state_rx,
            //inner,
            worker_handle,
        }
    }

    /// Return true if the bootstrap state changed.
    pub fn start(&self) {
        // Unwrap OK because the runner exits only if the `start_tx` is destroyed.
        self.start_tx.send(true).unwrap();
    }
}

impl TableBootstrapInner {
    // Return true we switched between being bootsrapped and not being bootstrapped.
    fn set_state(&self, new_state: State, from_line: u32) {
        let old_state = *self.state_tx.borrow();

        if old_state == new_state {
            return;
        }

        self.state_tx.send(new_state).unwrap_or(());

        log::info!(
            "{}: TableBootstrap state change {:?} -> {:?} (from_line: {})",
            self.ip_version,
            old_state,
            new_state,
            from_line
        );
    }

    async fn run(mut self, table_id: NodeId) {
        loop {
            match self.start_rx.changed().await {
                Ok(()) => {
                    if *self.start_rx.borrow() == true {
                        break;
                    }
                }
                Err(_) => return,
            }
        }

        let mut bootstrap_attempt = 0;

        loop {
            // If we have no bootstrap contacts it means we are the first node in the network and
            // other would bootstrap against us. We consider this node as already bootstrapped.
            if self.routers.is_empty() && self.starting_nodes.is_empty() {
                self.set_state(State::Bootstrapped, line!());
                std::future::pending::<()>().await;
                unreachable!();
            }

            let router_addresses = resolve(&self.routers, self.socket.ip_version()).await;
            self.table.lock().unwrap().routers = router_addresses.clone();

            if router_addresses.is_empty() && self.starting_nodes.is_empty() {
                self.set_state(State::IdleBeforeRebootstrap, line!());
                sleep(NO_NETWORK_TIMEOUT).await;
                continue;
            }

            self.set_state(State::InitialContact, line!());
            log::debug!(
                "Have {} routers and {} starting nodes",
                router_addresses.len(),
                self.starting_nodes.len(),
            );

            // In the initial round, we send the requests to contacts (nodes and routers) who are not in
            // our routing table. Because of that, we don't care who we receive a response from, only
            // that we receive sufficient number of unique ones. Thus we use the same transaction id
            // for all of them.
            // After the initial round we are sending only to nodes from the routing table, so we use
            // unique transaction id per node.
            let trans_id = self.id_generator.lock().unwrap().generate();

            let find_node_msg = Self::make_find_node_request(trans_id, table_id, table_id);

            let mut receivers = FuturesUnordered::new();
            let (new_receivers_tx, mut new_receivers_rx) = mpsc::unbounded_channel();

            let contact_count = router_addresses.len() + self.starting_nodes.len();
            let stop_at = std::cmp::min(contact_count, MAX_INITIAL_RESPONSES);
            let mut responses_received = 0;

            let mut send_finished = false;
            let mut new_receivers_closed = false;
            let mut send_to_initial_nodes = pin!(self.send_to_initial_nodes(
                find_node_msg,
                &router_addresses,
                new_receivers_tx
            ));

            loop {
                if send_finished && new_receivers_closed && receivers.is_empty() {
                    break;
                }

                select! {
                    _ = &mut send_to_initial_nodes, if !send_finished => {
                        send_finished = true;
                    },
                    new_receiver = new_receivers_rx.recv(), if !new_receivers_closed => {
                        if let Some(new_receiver) = new_receiver {
                            receivers.push(new_receiver);
                        } else {
                            new_receivers_closed = true;
                        }
                    },
                    ret = receivers.next(), if !receivers.is_empty() => {
                        if let Some(Some((message, from))) = ret {
                            if self.handle_message(message, from) {
                                responses_received += 1;

                                if responses_received >= stop_at {
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if responses_received == 0 {
                self.set_state(State::IdleBeforeRebootstrap, line!());
                time::sleep(self.calculate_retry_duration(bootstrap_attempt)).await;
                bootstrap_attempt += 1;
                continue;
            }

            self.set_state(State::Bootstrapping, line!());

            for bucket_number in 0..table::MAX_BUCKETS {
                log::debug!(
                    "{}: TableBootstrap::bootstrap_next_bucket {}/{}",
                    self.ip_version,
                    bucket_number,
                    table::MAX_BUCKETS
                );

                let (new_receivers_tx, mut new_receivers_rx) = mpsc::unbounded_channel();

                let mut send_bucket_bootstrap =
                    pin!(self.send_bucket_bootstrap_requests(bucket_number, new_receivers_tx));

                let mut send_finished = false;
                let mut new_receivers_closed = false;
                let mut receivers = FuturesUnordered::new();

                loop {
                    if send_finished && new_receivers_closed && receivers.is_empty() {
                        break;
                    }

                    select! {
                        _ = &mut send_bucket_bootstrap, if !send_finished => {
                            send_finished = true;
                        },
                        new_receiver = new_receivers_rx.recv() , if !new_receivers_closed => {
                            if let Some(new_receiver) = new_receiver {
                                receivers.push(new_receiver);
                            } else {
                                new_receivers_closed = true;
                            }
                        },
                        ret = receivers.next(), if !receivers.is_empty() => {
                            if let Some(Some((message, from))) = ret {
                                self.handle_message(message, from);
                            }
                        }
                    }
                }
            }

            let (num_good_nodes, num_questionable_nodes) = {
                let table = self.table.lock().unwrap();
                (table.num_good_nodes(), table.num_questionable_nodes())
            };

            log::debug!(
                "{}: TableBootstrap num_good_nodes:{} and num_questionable_nodes:{}",
                self.ip_version,
                num_good_nodes,
                num_questionable_nodes
            );

            if num_good_nodes < GOOD_NODE_THRESHOLD {
                // If we don't have enought good nodes and the `router_addresses` array is empty
                // then we might be testing or we might be in a country that is blocked from the
                // outside world where no BtDHT exists yet and we're one of the first nodes
                // creating it. In those cases we'll claim that we've bootstrapped and repeat the
                // bootstrap process periodically.
                if !router_addresses.is_empty() {
                    self.set_state(State::IdleBeforeRebootstrap, line!());
                    time::sleep(self.calculate_retry_duration(bootstrap_attempt)).await;
                    bootstrap_attempt += 1;
                    continue;
                }
            }

            self.set_state(State::Bootstrapped, line!());

            // Reset the counter.
            bootstrap_attempt = 0;

            loop {
                time::sleep(PERIODIC_CHECK_TIMEOUT).await;

                if self.table.lock().unwrap().num_good_nodes() < GOOD_NODE_THRESHOLD {
                    break;
                }
            }
        }
    }

    async fn send_to_initial_nodes(
        &self,
        message: Message,
        router_addresses: &HashSet<SocketAddr>,
        new_receivers_tx: mpsc::UnboundedSender<Responded>,
    ) {
        let mut last_send_error = None;
        let mut count = 0;

        for addr in router_addresses.iter().chain(self.starting_nodes.iter()) {
            // Throttle sending if there is too many initial contacts
            if count > PINGS_PER_BUCKET {
                time::sleep(NODE_TIMEOUT.max(Self::nat_friendly_send_duration())).await;
            }

            match self
                .socket
                .send_request(&message, *addr, INITIAL_TIMEOUT)
                .await
            {
                Ok(receiver) => {
                    count += 1;
                    if new_receivers_tx.send(receiver).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    if Some(error.kind()) != last_send_error {
                        log::error!(
                            "{}: Failed to send bootstrap message to router: {}",
                            self.ip_version,
                            error
                        );
                        last_send_error = Some(error.kind());
                    }
                }
            }
        }
    }

    // If this returns `false` it means the request wasn't sent to any node (either because there
    // were no nodes or because all the sends failed). We should proceed to the next bucket in that
    // case.
    async fn send_bucket_bootstrap_requests(
        &self,
        bucket_number: usize,
        new_receivers_tx: mpsc::UnboundedSender<Responded>,
    ) {
        let target_id = self.this_node_id.flip_bit(bucket_number);
        let nodes = self.nodes_to_bootstrap_bucket(bucket_number, target_id);

        for node in nodes {
            // Generate a transaction id
            let trans_id = self.id_generator.lock().unwrap().generate();

            let find_node_msg =
                Self::make_find_node_request(trans_id, self.this_node_id, target_id);

            // Send the message to the node
            match self
                .socket
                .send_request(&find_node_msg, node.addr, NODE_TIMEOUT)
                .await
            {
                Ok(receiver) => {
                    if new_receivers_tx.send(receiver).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    log::error!(
                        "{}: Could not send a bootstrap message: {}",
                        self.ip_version,
                        error
                    );
                    continue;
                }
            }

            // Mark that we requested from the node
            if let Some(node) = self.table.lock().unwrap().find_node_mut(&node) {
                node.local_request();
            }
        }
    }

    fn nat_friendly_send_duration() -> Duration {
        // An answer on serverfault.com[1] says the average home router may have from 2^10
        // to 2^14 NAT entries. To be conservative, and to account for the fact that the
        // user may be running one IPv4 and one IPv6 `MainlineDht`, let's assume we don't
        // want to exceed 256 NAT entries by much. A NAT entry stays open up to 20 secods
        // before it's deleted. Thus let's sleep for 20s/256 so that after 20 seconds if we
        // contact another node, the first nodes we contacted shall begin being removed
        // from the NAT.
        //
        // [1] https://serverfault.com/a/57903
        Duration::from_millis(20_000 / 256)
    }

    fn handle_message(&self, message: Message, from: SocketAddr) -> bool {
        match message.body {
            MessageBody::Response(rsp) => {
                let node = Node::as_good(rsp.id, from);

                let nodes = match self.socket.ip_version() {
                    IpVersion::V4 => &rsp.nodes_v4,
                    IpVersion::V6 => &rsp.nodes_v6,
                };

                self.table.lock().unwrap().add_nodes(node, nodes);

                return true;
            }
            _ => return false,
        }
    }

    fn make_find_node_request(
        transaction_id: TransactionID,
        id: NodeId,
        target: NodeId,
    ) -> Message {
        Message {
            transaction_id: transaction_id.as_ref().to_vec(),
            body: MessageBody::Request(Request::FindNode(FindNodeRequest {
                id,
                target,
                want: None, // we want only contacts of the same address family we have.
            })),
        }
    }

    fn calculate_retry_duration(&self, bootstrap_attempt: u64) -> Duration {
        const BASE: u64 = 2;
        // Max is somewhere around 8.5 mins.
        Duration::from_secs(BASE.pow((bootstrap_attempt + 1).min(9) as u32))
    }

    fn nodes_to_bootstrap_bucket(
        &self,
        bucket_number: usize,
        target_id: NodeId,
    ) -> Vec<NodeHandle> {
        let table = self.table.lock().unwrap();

        // Get the optimal iterator to bootstrap the current bucket
        if bucket_number == 0 || bucket_number == 1 {
            table
                .closest_nodes(target_id)
                .filter(|n| n.status() == NodeStatus::Questionable)
                .take(PINGS_PER_BUCKET)
                .map(|node| *node.handle())
                .collect()
        } else {
            let mut buckets = table.buckets().skip(bucket_number - 2);
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
                .take(PINGS_PER_BUCKET)
                .map(|node| *node.handle())
                .collect()
        }
    }
}
