use super::{ActionStatus, ScheduledTaskCheck};
use crate::{
    message::{AnnouncePeerRequest, Message, MessageBody, Request},
    node::NodeHandle,
    socket::Socket,
    table::RoutingTable,
    timer::{Timeout, Timer},
    transaction::{ActionID, MIDGenerator, TransactionID},
    InfoHash,
};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc;

const ANNOUNCE_PICK_NUM: usize = 8; // # Announces
const REPLY_TIMEOUT: Duration = Duration::from_millis(700);

type Token = Vec<u8>;

pub(crate) struct Announce {
    target_id: InfoHash,
    port_to_announce: Option<u16>,
    // Sorted by proximity to the target InfoHash.
    sorted_nodes: Vec<(NodeHandle, Token, NodeStatus)>,
    id_generator: MIDGenerator,
    // We don't send anything through `tx` here, but its destruction will cause stop to the
    // `await`ed operation that started this `Announce` action.
    _tx: mpsc::UnboundedSender<SocketAddr>,
}

#[derive(PartialEq, Eq, Copy, Clone)]
enum NodeStatus {
    Unused,
    Pending(TransactionID, Timeout),
    Success,
    TimedOut(TransactionID),
    Failure,
}

impl Announce {
    pub async fn new(
        socket: &Socket,
        table: &mut RoutingTable,
        timer: &mut Timer<ScheduledTaskCheck>,
        target_id: InfoHash,
        mut sorted_nodes: Vec<(NodeHandle, Token)>,
        port_to_announce: Option<u16>,
        id_generator: MIDGenerator,
        tx: mpsc::UnboundedSender<SocketAddr>,
    ) -> Option<Self> {
        if sorted_nodes.is_empty() {
            return None;
        }

        let sorted_nodes = sorted_nodes
            .drain(..)
            .map(|(handle, token)| (handle, token, NodeStatus::Unused))
            .collect::<Vec<_>>();

        let mut this = Self {
            target_id,
            port_to_announce,
            sorted_nodes,
            id_generator,
            _tx: tx,
        };

        if this.announce_to_some(socket, timer, table).await == 0 {
            return None;
        }

        Some(this)
    }

    pub fn action_id(&self) -> ActionID {
        self.id_generator.action_id()
    }

    fn compute_status(&self) -> ActionStatus {
        if self.count_success_nodes() >= ANNOUNCE_PICK_NUM {
            return ActionStatus::Completed;
        }

        if self.count_unused_nodes() == 0 {
            // We don't really care if there are any pending tasks here as there is no more unused
            // nodes we could try.
            ActionStatus::Completed
        } else {
            ActionStatus::Ongoing
        }
    }

    pub async fn on_receive_reply(
        &mut self,
        transaction_id: &TransactionID,
        is_success: bool,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> ActionStatus {
        for (_, _, status) in self.sorted_nodes.iter_mut() {
            let tid_and_timeout = match status {
                NodeStatus::Unused => None,
                NodeStatus::Pending(tid, timeout) => Some((tid, Some(*timeout))),
                NodeStatus::Success => None,
                // We can still receive a response even if we decided that this node timed out (it
                // may just be slow).
                NodeStatus::TimedOut(tid) => Some((tid, None)),
                NodeStatus::Failure => None,
            };

            if let Some((tid, timeout)) = tid_and_timeout {
                if *tid == *transaction_id {
                    *status = if is_success {
                        NodeStatus::Success
                    } else {
                        NodeStatus::Failure
                    };
                    if let Some(timeout) = timeout {
                        timer.cancel(timeout);
                    }
                }
            }
        }

        self.compute_status()
    }

    pub async fn on_timeout(
        &mut self,
        transaction_id: &TransactionID,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
        table: &mut RoutingTable,
    ) -> ActionStatus {
        for (_, _, status) in self.sorted_nodes.iter_mut() {
            let tid = match status {
                NodeStatus::Unused => None,
                NodeStatus::Pending(tid, _) => Some(tid),
                NodeStatus::Success => None,
                NodeStatus::TimedOut(_tid) => None, // NoOp
                NodeStatus::Failure => None,
            };

            if tid.cloned() == Some(*transaction_id) {
                *status = NodeStatus::TimedOut(*transaction_id);
            }
        }

        self.announce_to_some(socket, timer, table).await;

        self.compute_status()
    }

    fn count_pending_nodes(&self) -> usize {
        let mut pending = 0;

        for (_, _, status) in self.sorted_nodes.iter() {
            match status {
                NodeStatus::Unused => {}
                NodeStatus::Pending(_, _) => pending += 1,
                NodeStatus::Success => {}
                NodeStatus::TimedOut(_) => {}
                NodeStatus::Failure => {}
            }
        }

        pending
    }

    fn count_success_nodes(&self) -> usize {
        let mut success = 0;

        for (_, _, status) in self.sorted_nodes.iter() {
            match status {
                NodeStatus::Unused => {}
                NodeStatus::Pending(_, _) => {}
                NodeStatus::Success => success += 1,
                NodeStatus::TimedOut(_) => {}
                NodeStatus::Failure => {}
            }
        }

        success
    }

    fn count_unused_nodes(&self) -> usize {
        let mut unused = 0;

        for (_, _, status) in self.sorted_nodes.iter() {
            match status {
                NodeStatus::Unused => unused += 1,
                NodeStatus::Pending(_, _) => {}
                NodeStatus::Success => {}
                NodeStatus::TimedOut(_) => {}
                NodeStatus::Failure => {}
            }
        }

        unused
    }

    /// Pick nodes to which we'll try to to announce.
    fn pick_nodes(
        &mut self,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> Vec<(NodeHandle, Token, TransactionID)> {
        let mut ret = Vec::new();

        let pending = self.count_pending_nodes();
        let success = self.count_success_nodes();

        if pending + success >= ANNOUNCE_PICK_NUM {
            return ret;
        }

        let pick_count = ANNOUNCE_PICK_NUM - pending - success;

        for (node, token, status) in self.sorted_nodes.iter_mut() {
            if status != &NodeStatus::Unused {
                continue;
            }

            let transaction_id = self.id_generator.generate();

            let timeout =
                timer.schedule_in(REPLY_TIMEOUT, ScheduledTaskCheck::Announce(transaction_id));

            *status = NodeStatus::Pending(transaction_id, timeout);

            ret.push((node.clone(), token.clone(), transaction_id));

            if ret.len() >= pick_count {
                break;
            }
        }

        ret
    }

    fn create_announce_message(
        &mut self,
        transaction_id: TransactionID,
        token: &Token,
        this_node_id: InfoHash,
    ) -> Message {
        Message {
            transaction_id: transaction_id.as_ref().to_vec(),
            body: MessageBody::Request(Request::AnnouncePeer(AnnouncePeerRequest {
                id: this_node_id,
                info_hash: self.target_id,
                token: token.clone(),
                port: self.port_to_announce,
            })),
        }
    }

    async fn announce_to_some(
        &mut self,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
        table: &mut RoutingTable,
    ) -> usize {
        let mut count = 0;
        for (node, token, transaction_id) in self.pick_nodes(timer) {
            let message = self.create_announce_message(transaction_id, &token, table.node_id());

            if self.send(&socket, &message, &node, table).await {
                count += 1;
            }
        }
        count
    }

    async fn send(
        &self,
        socket: &Socket,
        message: &Message,
        destination: &NodeHandle,
        table: &mut RoutingTable,
    ) -> bool {
        match socket.send(&message, destination.addr).await {
            Ok(()) => {
                // We requested from the node, mark it down if the node is in our routing table
                if let Some(n) = table.find_node_mut(destination) {
                    n.local_request()
                }
                true
            }
            Err(error) => {
                log::error!(
                    "{}: Announce request failed to send: {}",
                    socket.ip_version(),
                    error
                );
                false
            }
        }
    }
}
