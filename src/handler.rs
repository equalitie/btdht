use crate::action::{
    bootstrap::{self, TableBootstrap},
    lookup::TableLookup,
    refresh::TableRefresh,
    ActionStatus, IpVersion, OneshotTask, ScheduledTaskCheck, StartLookup, State, WorkerError,
};
use crate::{
    info_hash::{InfoHash, NodeId},
    message::{error_code, Error, Message, MessageBody, Request, Response, Want},
    node::{Node, NodeHandle},
    socket::Socket,
    storage::AnnounceStorage,
    table::RoutingTable,
    timer::Timer,
    token::{Token, TokenStore},
    transaction::{AIDGenerator, ActionID, TransactionID},
};
use futures_util::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    convert::AsRef,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};

/// Storage for our EventLoop to invoke actions upon.
pub(crate) struct DhtHandler {
    this_node_id: NodeId,
    running: bool,
    command_rx: mpsc::UnboundedReceiver<OneshotTask>,
    timer: Timer<ScheduledTaskCheck>,
    read_only: bool,
    announce_port: Option<u16>,
    socket: Arc<Socket>,
    token_store: TokenStore,
    aid_generator: AIDGenerator,
    routing_table: Arc<Mutex<RoutingTable>>,
    active_stores: AnnounceStorage,
    bootstrap: TableBootstrap,

    next_bootstrap_txs_id: u64,
    bootstrap_txs: HashMap<u64, oneshot::Sender<()>>,

    // TableRefresh action.
    refresh: TableRefresh,
    // Ongoing TableLookups.
    lookups: HashMap<ActionID, TableLookup>,
}

impl DhtHandler {
    pub fn new(
        this_node_id: NodeId,
        socket: Socket,
        read_only: bool,
        routers: HashSet<String>,
        nodes: HashSet<SocketAddr>,
        announce_port: Option<u16>,
        command_rx: mpsc::UnboundedReceiver<OneshotTask>,
    ) -> Self {
        let socket = Arc::new(socket);
        let table = Arc::new(Mutex::new(RoutingTable::new(this_node_id)));

        let mut aid_generator = AIDGenerator::new();

        // The refresh task to execute after the bootstrap
        let mid_generator = aid_generator.generate();
        let table_refresh = TableRefresh::new(mid_generator, table.clone());

        let mid_generator = aid_generator.generate();
        let bootstrap =
            TableBootstrap::new(socket.clone(), table.clone(), mid_generator, routers, nodes);

        let timer = Timer::new();

        Self {
            this_node_id,
            running: true,
            command_rx,
            timer,
            read_only,
            announce_port,
            socket,
            token_store: TokenStore::new(),
            aid_generator,
            routing_table: table,
            active_stores: AnnounceStorage::new(),
            bootstrap,
            next_bootstrap_txs_id: 0,
            bootstrap_txs: HashMap::new(),
            refresh: table_refresh,
            lookups: HashMap::new(),
        }
    }

    fn ip_version(&self) -> IpVersion {
        self.socket.ip_version()
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
            result = self.bootstrap.state_rx.changed() => {
                assert!(result.is_ok());
                if self.is_bootstrapped() {
                    self.handle_bootstrap_success().await;
                }
            }
            message = self.socket.recv() => {
                match message {
                    Ok((message, addr)) => if let Err(error) = self.handle_incoming(message, addr).await {
                        log::debug!("{}: Failed to handle incoming message: {} from:{addr:?}", self.ip_version(), error);
                    }
                    Err(error) => log::warn!("{}: Failed to receive incoming message: {}", self.ip_version(), error),
                }
            }
        }
    }

    fn is_bootstrapped(&self) -> bool {
        *self.bootstrap.state_rx.borrow() == bootstrap::State::Bootstrapped
    }

    async fn handle_command(&mut self, task: OneshotTask) {
        match task {
            OneshotTask::StartBootstrap() => {
                self.handle_start_bootstrap();
            }
            OneshotTask::CheckBootstrap(tx) => {
                self.handle_check_bootstrap(tx);
            }
            OneshotTask::StartLookup(lookup) => {
                self.handle_start_lookup(lookup).await;
            }
            OneshotTask::GetLocalAddr(tx) => self.handle_get_local_addr(tx),
            OneshotTask::GetState(tx) => self.handle_get_state(tx),
            OneshotTask::LoadContacts(tx) => self.handle_load_contacts(tx),
        }
    }

    async fn handle_timeout(&mut self, token: ScheduledTaskCheck) {
        match token {
            ScheduledTaskCheck::TableRefresh => {
                self.handle_check_table_refresh().await;
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
        message: Message,
        addr: SocketAddr,
    ) -> Result<(), WorkerError> {
        // Do not process requests if we are read only
        // TODO: Add read only flags to messages we send it we are read only!
        // Also, check for read only flags on responses we get before adding nodes
        // to our RoutingTable.
        if self.read_only && matches!(message.body, MessageBody::Request(_)) {
            return Ok(());
        }

        log::trace!("{}: Received {:?}", self.ip_version(), message);

        // Process the given message
        match message.body {
            MessageBody::Request(Request::Ping(p)) => {
                let node = NodeHandle::new(p.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.lock().unwrap().find_node_mut(&node) {
                    n.remote_request()
                }

                let ping_rsp = Response {
                    id: self.this_node_id,
                    values: vec![],
                    nodes_v4: vec![],
                    nodes_v6: vec![],
                    token: None,
                };
                let ping_msg = Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(ping_rsp),
                };

                self.socket.send(&ping_msg, addr).await?
            }
            MessageBody::Request(Request::FindNode(f)) => {
                let node = NodeHandle::new(f.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.lock().unwrap().find_node_mut(&node) {
                    n.remote_request()
                }

                let (nodes_v4, nodes_v6) = self.find_closest_nodes(f.target, f.want)?;

                let find_node_rsp = Response {
                    id: self.this_node_id,
                    values: vec![],
                    nodes_v4,
                    nodes_v6,
                    token: None,
                };
                let find_node_msg = Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(find_node_rsp),
                };

                self.socket.send(&find_node_msg, addr).await?
            }
            MessageBody::Request(Request::GetPeers(g)) => {
                let node = NodeHandle::new(g.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.lock().unwrap().find_node_mut(&node) {
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

                let get_peers_rsp = Response {
                    id: self.this_node_id,
                    values,
                    nodes_v4,
                    nodes_v6,
                    token: Some(token.as_ref().to_vec()),
                };
                let get_peers_msg = Message {
                    transaction_id: message.transaction_id,
                    body: MessageBody::Response(get_peers_rsp),
                };

                self.socket.send(&get_peers_msg, addr).await?
            }
            MessageBody::Request(Request::AnnouncePeer(a)) => {
                let node = NodeHandle::new(a.id, addr);

                // Node requested from us, mark it in the Routingtable
                if let Some(n) = self.routing_table.lock().unwrap().find_node_mut(&node) {
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
                    log::debug!(
                        "{}: Remote node sent us an invalid token for an AnnounceRequest",
                        self.ip_version()
                    );
                    Message {
                        transaction_id: message.transaction_id,
                        body: MessageBody::Error(Error {
                            code: error_code::PROTOCOL_ERROR,
                            message: "received an invalid token".to_owned(),
                        }),
                    }
                } else if self.active_stores.add_item(a.info_hash, connect_addr) {
                    // Node successfully stored the value with us, send an announce response
                    Message {
                        transaction_id: message.transaction_id,
                        body: MessageBody::Response(Response {
                            id: self.this_node_id,
                            values: vec![],
                            nodes_v4: vec![],
                            nodes_v6: vec![],
                            token: None,
                        }),
                    }
                } else {
                    // Node unsuccessfully stored the value with us, send them an error message
                    // TODO: Spec doesnt actually say what error message to send, or even if we should send one...
                    log::warn!(
                        "{}: AnnounceStorage failed to store contact information because it is full", self.ip_version()
                    );

                    Message {
                        transaction_id: message.transaction_id,
                        body: MessageBody::Error(Error {
                            code: error_code::SERVER_ERROR,
                            message: "announce storage is full".to_owned(),
                        }),
                    }
                };

                self.socket.send(&response_msg, addr).await?
            }
            MessageBody::Response(rsp) => {
                let trans_id = TransactionID::from_bytes(&message.transaction_id)
                    .ok_or(WorkerError::InvalidTransactionId)?;
                self.handle_incoming_response(trans_id, addr, rsp).await?;
            }
            MessageBody::Error(_) => (),
        }

        Ok(())
    }

    async fn handle_incoming_response(
        &mut self,
        trans_id: TransactionID,
        addr: SocketAddr,
        rsp: Response,
    ) -> Result<(), WorkerError> {
        let node = Node::as_good(rsp.id, addr);

        let nodes = match self.socket.ip_version() {
            IpVersion::V4 => &rsp.nodes_v4,
            IpVersion::V6 => &rsp.nodes_v6,
        };

        if let Some(lookup) = self.lookups.get_mut(&trans_id.action_id()) {
            self.routing_table
                .lock()
                .unwrap()
                .add_nodes(node.clone(), nodes);

            match lookup
                .recv_response(node, &trans_id, rsp, &self.socket, &mut self.timer)
                .await
            {
                ActionStatus::Ongoing => (),
                ActionStatus::Completed => self.handle_lookup_completed(trans_id).await,
            }
        } else if self.refresh.action_id() == trans_id.action_id() {
            self.routing_table.lock().unwrap().add_nodes(node, nodes);
        } else {
            return Err(WorkerError::UnsolicitedResponse);
        }

        Ok(())
    }

    fn handle_start_bootstrap(&mut self) {
        self.bootstrap.start();
    }

    fn handle_check_bootstrap(&mut self, tx: oneshot::Sender<()>) {
        if self.is_bootstrapped() {
            tx.send(()).unwrap_or(())
        } else {
            let id = self.next_bootstrap_txs_id;
            self.next_bootstrap_txs_id += 1;
            self.bootstrap_txs.insert(id, tx);
        }
    }

    async fn handle_bootstrap_success(&mut self) {
        // Send notification that the bootstrap has completed.
        for (_, tx) in self.bootstrap_txs.drain() {
            tx.send(()).unwrap_or(())
        }

        // Start the refresh action.
        self.handle_check_table_refresh().await;
    }

    async fn handle_start_lookup(&mut self, lookup: StartLookup) {
        // Start the lookup right now if not bootstrapping
        let mid_generator = self.aid_generator.generate();
        let action_id = mid_generator.action_id();

        let mut lookup = TableLookup::new(
            lookup.info_hash,
            lookup.announce,
            lookup.tx,
            mid_generator,
            self.routing_table.clone(),
            &self.socket,
            &mut self.timer,
        )
        .await;

        if lookup.completed() {
            lookup.recv_finished(self.announce_port, &self.socket).await;
        } else {
            self.lookups.insert(action_id, lookup);
        }
    }

    fn handle_get_state(&self, tx: oneshot::Sender<State>) {
        let table = self.routing_table.lock().unwrap();
        tx.send(State {
            is_running: self.running,
            bootstrapped: self.is_bootstrapped(),
            good_node_count: table.num_good_nodes(),
            questionable_node_count: table.num_questionable_nodes(),
            bucket_count: table.buckets().count(),
        })
        .unwrap_or(())
    }

    fn handle_get_local_addr(&self, tx: oneshot::Sender<SocketAddr>) {
        tx.send(self.socket.local_addr()).unwrap_or(())
    }

    async fn handle_check_lookup_timeout(&mut self, trans_id: TransactionID) {
        let lookup = if let Some(lookup) = self.lookups.get_mut(&trans_id.action_id()) {
            lookup
        } else {
            log::error!(
                "{}: Resolved a TransactionID to a check table lookup but no action found",
                self.ip_version()
            );
            return;
        };

        let lookup_status = lookup
            .recv_timeout(&trans_id, &self.socket, &mut self.timer)
            .await;

        match lookup_status {
            ActionStatus::Ongoing => (),
            ActionStatus::Completed => self.handle_lookup_completed(trans_id).await,
        }
    }

    async fn handle_check_lookup_endgame(&mut self, trans_id: TransactionID) {
        self.handle_lookup_completed(trans_id).await
    }

    async fn handle_lookup_completed(&mut self, trans_id: TransactionID) {
        let mut lookup = if let Some(lookup) = self.lookups.remove(&trans_id.action_id()) {
            lookup
        } else {
            log::error!("{}: Lookup not found", self.ip_version());
            return;
        };

        lookup.recv_finished(self.announce_port, &self.socket).await
    }

    async fn handle_check_table_refresh(&mut self) {
        self.refresh
            .continue_refresh(&self.socket, &mut self.timer)
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
            None => match self.socket.ip_version() {
                IpVersion::V4 => Want::V4,
                IpVersion::V6 => Want::V6,
            },
        };

        let table = self.routing_table.lock().unwrap();

        let nodes_v4 = if matches!(want, Want::V4 | Want::Both) {
            table
                .closest_nodes(target)
                .filter(|node| node.addr().is_ipv4())
                .take(8)
                .map(|node| *node.handle())
                .collect()
        } else {
            vec![]
        };

        let nodes_v6 = if matches!(want, Want::V6 | Want::Both) {
            table
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

    fn handle_load_contacts(
        &self,
        tx: oneshot::Sender<(HashSet<SocketAddr>, HashSet<SocketAddr>)>,
    ) {
        tx.send(self.routing_table.lock().unwrap().load_contacts())
            .unwrap_or(());
    }
}

// ----------------------------------------------------------------------------//
