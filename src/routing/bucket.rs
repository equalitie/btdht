use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::slice::Iter;

use crate::id::{NodeId, NODE_ID_LEN};
use crate::routing::node::{Node, NodeStatus};

/// Maximum number of nodes that should reside in any bucket.
pub const MAX_BUCKET_SIZE: usize = 8;

/// Bucket containing Nodes with identical bit prefixes.
pub struct Bucket {
    nodes: [Node; MAX_BUCKET_SIZE],
}

impl Bucket {
    /// Create a new Bucket with all Nodes default initialized.
    pub fn new() -> Bucket {
        let id = NodeId::from([0u8; NODE_ID_LEN]);

        let ip = Ipv4Addr::new(127, 0, 0, 1);
        let addr = SocketAddr::V4(SocketAddrV4::new(ip, 0));

        Bucket {
            nodes: [
                Node::as_bad(id, addr),
                Node::as_bad(id, addr),
                Node::as_bad(id, addr),
                Node::as_bad(id, addr),
                Node::as_bad(id, addr),
                Node::as_bad(id, addr),
                Node::as_bad(id, addr),
                Node::as_bad(id, addr),
            ],
        }
    }

    /// Iterator over all good nodes and questionable nodes in the bucket.
    pub fn pingable_nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|node| node.is_pingable())
    }

    /// Iterator over all good nodes and questionable nodes in the bucket that allos modifying the
    /// nodes.
    pub fn pingable_nodes_mut(&mut self) -> impl Iterator<Item = &mut Node> {
        self.nodes.iter_mut().filter(|node| node.is_pingable())
    }

    /// Iterator over each node within the bucket.
    ///
    /// For buckets newly created, the initial bad nodes are included.
    pub fn iter(&self) -> Iter<Node> {
        self.nodes.iter()
    }

    /// Indicates if the bucket needs to be refreshed.
    #[allow(unused)]
    pub fn needs_refresh(&self) -> bool {
        self.nodes
            .iter()
            .all(|node| node.status() != NodeStatus::Good)
    }

    /// Attempt to add the given Node to the bucket if it is not in a bad state.
    ///
    /// Returns false if the Node could not be placed in the bucket because it is full.
    pub fn add_node(&mut self, new_node: Node) -> bool {
        let new_node_status = new_node.status();
        if new_node_status == NodeStatus::Bad {
            return true;
        }

        // See if this node is already in the table, in that case replace it if it
        // has a higher or equal status to the current node.
        if let Some(index) = self.nodes.iter().position(|node| *node == new_node) {
            // Note, we can't just compare the status and if it's better or equal then replace the
            // old node with the new one. Doing so would erase information already stored locally.
            self.nodes[index].update(new_node);

            return true;
        }

        // See if any lower priority nodes are present in the table, we cant do
        // nodes that have equal status because we have to prefer longer lasting
        // nodes in the case of a good status which helps with stability.
        let replace_index = self
            .nodes
            .iter()
            .position(|node| node.status() < new_node_status);
        if let Some(index) = replace_index {
            self.nodes[index] = new_node;

            true
        } else {
            false
        }
    }

    /// Iterator over all good nodes in the bucket.
    #[cfg(test)]
    fn good_nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes
            .iter()
            .filter(|node| node.status() == NodeStatus::Good)
    }
}

// ----------------------------------------------------------------------------//

#[cfg(test)]
mod tests {

    use crate::routing::bucket::Bucket;
    use crate::routing::node::{Node, NodeStatus};
    use crate::test;

    #[test]
    fn positive_initial_no_nodes() {
        let bucket = Bucket::new();

        assert_eq!(bucket.good_nodes().count(), 0);
        assert_eq!(bucket.pingable_nodes().count(), 0);
    }

    #[test]
    fn positive_all_questionable_nodes() {
        let mut bucket = Bucket::new();

        let dummy_addr = test::dummy_socket_addr_v4();
        let dummy_ids = test::dummy_block_node_ids(super::MAX_BUCKET_SIZE as u8);
        for id in dummy_ids {
            let node = Node::as_questionable(id, dummy_addr);
            bucket.add_node(node);
        }

        assert_eq!(bucket.good_nodes().count(), 0);
        assert_eq!(bucket.pingable_nodes().count(), super::MAX_BUCKET_SIZE);
    }

    #[test]
    fn positive_all_good_nodes() {
        let mut bucket = Bucket::new();

        let dummy_addr = test::dummy_socket_addr_v4();
        let dummy_ids = test::dummy_block_node_ids(super::MAX_BUCKET_SIZE as u8);
        for id in dummy_ids {
            let node = Node::as_good(id, dummy_addr);
            bucket.add_node(node);
        }

        assert_eq!(bucket.good_nodes().count(), super::MAX_BUCKET_SIZE);
        assert_eq!(bucket.pingable_nodes().count(), super::MAX_BUCKET_SIZE);
    }

    #[test]
    fn positive_replace_questionable_node() {
        let mut bucket = Bucket::new();

        let dummy_addr = test::dummy_socket_addr_v4();
        let dummy_ids = test::dummy_block_node_ids(super::MAX_BUCKET_SIZE as u8);
        for id in &dummy_ids {
            let node = Node::as_questionable(*id, dummy_addr);
            bucket.add_node(node);
        }

        assert_eq!(bucket.good_nodes().count(), 0);
        assert_eq!(bucket.pingable_nodes().count(), super::MAX_BUCKET_SIZE);

        let good_node = Node::as_good(dummy_ids[0], dummy_addr);
        bucket.add_node(good_node.clone());

        assert_eq!(bucket.good_nodes().next().unwrap(), &good_node);
        assert_eq!(bucket.good_nodes().count(), 1);
        assert_eq!(bucket.pingable_nodes().count(), super::MAX_BUCKET_SIZE);
    }

    #[test]
    fn positive_resist_good_node_churn() {
        let mut bucket = Bucket::new();

        let dummy_addr = test::dummy_socket_addr_v4();
        let dummy_ids = test::dummy_block_node_ids((super::MAX_BUCKET_SIZE as u8) + 1);
        for id in &dummy_ids {
            let node = Node::as_good(*id, dummy_addr);
            bucket.add_node(node);
        }

        // All the nodes should be good
        assert_eq!(bucket.good_nodes().count(), super::MAX_BUCKET_SIZE);

        // Create a new good node
        let unused_id = dummy_ids[dummy_ids.len() - 1];
        let new_good_node = Node::as_good(unused_id, dummy_addr);

        // Make sure the node is NOT in the bucket
        assert!(!bucket.good_nodes().any(|node| &new_good_node == node));

        // Try to add it
        bucket.add_node(new_good_node.clone());

        // Make sure the node is NOT in the bucket
        assert!(!bucket.good_nodes().any(|node| &new_good_node == node));
    }

    #[test]
    fn positive_resist_questionable_node_churn() {
        let mut bucket = Bucket::new();

        let dummy_addr = test::dummy_socket_addr_v4();
        let dummy_ids = test::dummy_block_node_ids((super::MAX_BUCKET_SIZE as u8) + 1);
        for id in &dummy_ids {
            let node = Node::as_questionable(*id, dummy_addr);
            bucket.add_node(node);
        }

        // All the nodes should be questionable
        assert_eq!(
            bucket
                .pingable_nodes()
                .filter(|node| node.status() == NodeStatus::Questionable)
                .count(),
            super::MAX_BUCKET_SIZE
        );

        // Create a new questionable node
        let unused_id = dummy_ids[dummy_ids.len() - 1];
        let new_questionable_node = Node::as_questionable(unused_id, dummy_addr);

        // Make sure the node is NOT in the bucket
        assert!(!bucket
            .pingable_nodes()
            .any(|node| &new_questionable_node == node));

        // Try to add it
        bucket.add_node(new_questionable_node);

        // Make sure the node is NOT in the bucket
        assert_eq!(
            bucket
                .pingable_nodes()
                .filter(|node| node.status() == NodeStatus::Questionable)
                .count(),
            super::MAX_BUCKET_SIZE
        );
    }
}
