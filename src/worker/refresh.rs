use super::{messenger, timer::Timer, ScheduledTaskCheck};
use crate::message::{FindNodeRequest, Message, MessageBody, Request};
use crate::routing::node::NodeStatus;
use crate::routing::table::{self, RoutingTable};
use crate::transaction::MIDGenerator;
use std::time::Duration;
use tokio::net::UdpSocket;

const REFRESH_INTERVAL_TIMEOUT: Duration = Duration::from_millis(6000);

pub(crate) enum RefreshStatus {
    /// Refresh is in progress.
    Refreshing,
    /// Refresh failed in a fatal way.
    Failed,
}

pub(crate) struct TableRefresh {
    id_generator: MIDGenerator,
    curr_refresh_bucket: usize,
}

impl TableRefresh {
    pub fn new(id_generator: MIDGenerator) -> TableRefresh {
        TableRefresh {
            id_generator,
            curr_refresh_bucket: 0,
        }
    }

    pub fn continue_refresh(
        &mut self,
        table: &RoutingTable,
        socket: &UdpSocket,
        timer: &mut Timer<ScheduledTaskCheck>,
    ) -> RefreshStatus {
        if self.curr_refresh_bucket == table::MAX_BUCKETS {
            self.curr_refresh_bucket = 0;
        }
        let target_id = table.node_id().flip_bit(self.curr_refresh_bucket);

        info!(
            "bip_dht: Performing a refresh for bucket {}",
            self.curr_refresh_bucket
        );
        // Ping the closest questionable node
        for node in table
            .closest_nodes(target_id)
            .filter(|n| n.status() == NodeStatus::Questionable)
            .take(1)
        {
            // Generate a transaction id for the request
            let trans_id = self.id_generator.generate();

            // Construct the message
            let find_node_req = FindNodeRequest {
                id: table.node_id(),
                target: target_id,
            };
            let find_node_msg = Message {
                transaction_id: trans_id.as_ref().to_vec(),
                body: MessageBody::Request(Request::FindNode(find_node_req)),
            };
            let find_node_msg = find_node_msg.encode();

            // Send the message
            if let Err(error) = messenger::blocking_send(socket, &find_node_msg, node.addr()) {
                error!(
                    "bip_dht: TableRefresh failed to send a refresh message: {}",
                    error
                );
                return RefreshStatus::Failed;
            }

            // Mark that we requested from the node
            node.local_request();
        }

        // Generate a dummy transaction id (only the action id will be used)
        let trans_id = self.id_generator.generate();

        // Start a timer for the next refresh
        timer.schedule_in(
            REFRESH_INTERVAL_TIMEOUT,
            ScheduledTaskCheck::TableRefresh(trans_id),
        );

        self.curr_refresh_bucket += 1;

        RefreshStatus::Refreshing
    }
}
