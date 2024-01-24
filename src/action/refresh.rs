use super::ScheduledTaskCheck;
use crate::node::NodeStatus;
use crate::table::{self, RoutingTable};
use crate::transaction::{ActionID, MIDGenerator};
use crate::{
    message::{FindNodeRequest, Message, MessageBody, Request},
    socket::Socket,
    timer::Timer,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const REFRESH_INTERVAL_TIMEOUT: Duration = Duration::from_millis(6000);
const REFRESH_CONCURRENCY: usize = 4;

pub(crate) struct TableRefresh {
    table: Arc<Mutex<RoutingTable>>,
    id_generator: MIDGenerator,
    curr_refresh_bucket: usize,
}

impl TableRefresh {
    pub fn new(id_generator: MIDGenerator, table: Arc<Mutex<RoutingTable>>) -> TableRefresh {
        TableRefresh {
            table,
            id_generator,
            curr_refresh_bucket: 0,
        }
    }

    pub fn action_id(&self) -> ActionID {
        self.id_generator.action_id()
    }

    pub async fn continue_refresh(
        &mut self,
        socket: &Socket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) {
        if self.curr_refresh_bucket == table::MAX_BUCKETS {
            self.curr_refresh_bucket = 0;
        }

        let (this_node_id, target_id, num_good_nodes, num_questionable_nodes, nodes_to_contact) = {
            let table = self.table.lock().unwrap();

            let this_node_id = table.node_id();
            let target_id = this_node_id.flip_bit(self.curr_refresh_bucket);
            let num_good_nodes = table.num_good_nodes();
            let num_questionable_nodes = table.num_questionable_nodes();
            let nodes_to_contact = table
                .closest_nodes(target_id)
                .filter(|n| n.status() == NodeStatus::Questionable)
                .filter(|n| !n.recently_requested_from())
                .take(REFRESH_CONCURRENCY)
                .map(|node| *node.handle())
                .collect::<Vec<_>>();

            (
                this_node_id,
                target_id,
                num_good_nodes,
                num_questionable_nodes,
                nodes_to_contact,
            )
        };

        log::debug!(
            "Performing a refresh for bucket {} (table total: num_good_nodes={}, num_questionable_nodes={})",
            self.curr_refresh_bucket,
            num_good_nodes,
            num_questionable_nodes,
        );

        // Ping the closest questionable nodes
        for node in nodes_to_contact {
            // Generate a transaction id for the request
            let trans_id = self.id_generator.generate();

            // Construct the message
            let find_node_req = FindNodeRequest {
                id: this_node_id,
                target: target_id,
                want: None,
            };
            let find_node_msg = Message {
                transaction_id: trans_id.as_ref().to_vec(),
                body: MessageBody::Request(Request::FindNode(find_node_req)),
            };

            // Send the message
            if let Err(error) = socket.send(&find_node_msg, node.addr).await {
                log::error!("TableRefresh failed to send a refresh message: {}", error);
            }

            // Mark that we requested from the node
            if let Some(node) = self.table.lock().unwrap().find_node_mut(&node) {
                node.local_request();
            }
        }

        // Start a timer for the next refresh
        timer.schedule_in(REFRESH_INTERVAL_TIMEOUT, ScheduledTaskCheck::TableRefresh);

        self.curr_refresh_bucket += 1;
    }
}
