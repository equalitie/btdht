use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::id::NodeId;

// TODO: Should remove as_* functions and replace them with from_requested, from_responded, etc to hide the logic
// of the nodes initial status.

// TODO: Should address the subsecond lookup paper where questionable nodes should not automatically be replaced with
// good nodes, instead, questionable nodes should be pinged twice and then become available to be replaced. This reduces
// GOOD node churn since after 15 minutes, a long lasting node could potentially be replaced by a short lived good node.
// This strategy is actually what is vaguely specified in the standard?

// TODO: Should we be storing a SocketAddr instead of a SocketAddrV4?

/// Maximum wait period before a node becomes questionable.
const MAX_LAST_SEEN_MINS: u64 = 15;

/// Maximum number of requests before a Questionable node becomes Bad.
const MAX_REFRESH_REQUESTS: usize = 2;

/// Status of the node.
/// Ordering of the enumerations is important, variants higher
/// up are considered to be less than those further down.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug, Ord, PartialOrd)]
pub enum NodeStatus {
    Bad,
    Questionable,
    Good,
}

/// Node participating in the dht.
#[derive(Clone)]
pub struct Node {
    info: NodeInfo,
    last_request: Option<Instant>,
    last_response: Option<Instant>,
    refresh_requests: usize,
}

impl Node {
    /// Create a new node that has recently responded to us but never requested from us.
    pub fn as_good(id: NodeId, addr: SocketAddr) -> Node {
        Node {
            info: NodeInfo { id, addr },
            last_response: Some(Instant::now()),
            last_request: None,
            refresh_requests: 0,
        }
    }

    /// Create a questionable node that has responded to us before but never requested from us.
    pub fn as_questionable(id: NodeId, addr: SocketAddr) -> Node {
        let last_response_offset = Duration::from_secs(MAX_LAST_SEEN_MINS * 60);
        let last_response = Instant::now().checked_sub(last_response_offset).unwrap();

        Node {
            info: NodeInfo { id, addr },
            last_response: Some(last_response),
            last_request: None,
            refresh_requests: 0,
        }
    }

    /// Create a new node that has never responded to us or requested from us.
    pub fn as_bad(id: NodeId, addr: SocketAddr) -> Node {
        Node {
            info: NodeInfo { id, addr },
            last_response: None,
            last_request: None,
            refresh_requests: 0,
        }
    }

    /// Record that we sent the node a request.
    pub fn local_request(&mut self) {
        if self.status() != NodeStatus::Good {
            self.refresh_requests = self.refresh_requests.saturating_add(1);
        }
    }

    /// Record that the node sent us a request.
    pub fn remote_request(&mut self) {
        self.last_request = Some(Instant::now());
    }

    /// Record that the node sent us a response.
    #[allow(unused)] // TODO: find out why is this unused
    pub fn remote_response(&mut self) {
        self.last_response = Some(Instant::now());
        self.refresh_requests = 0;
    }

    pub fn id(&self) -> NodeId {
        self.info.id
    }

    pub fn addr(&self) -> SocketAddr {
        self.info.addr
    }

    /// Current status of the node.
    pub fn status(&self) -> NodeStatus {
        let curr_time = Instant::now();

        match recently_responded(self, curr_time) {
            NodeStatus::Good => return NodeStatus::Good,
            NodeStatus::Bad => return NodeStatus::Bad,
            NodeStatus::Questionable => (),
        };

        recently_requested(self, curr_time)
    }

    /// Is node good or questionable?
    pub fn is_pingable(&self) -> bool {
        // Function is moderately expensive
        let status = self.status();
        status == NodeStatus::Good || status == NodeStatus::Questionable
    }

    pub(crate) fn info(&self) -> &NodeInfo {
        &self.info
    }
}

impl Eq for Node {}

impl PartialEq<Node> for Node {
    fn eq(&self, other: &Node) -> bool {
        self.info == other.info
    }
}

impl Hash for Node {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.info.hash(state);
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("Node")
            .field("id", &self.info.id)
            .field("addr", &self.info.addr)
            .field("last_request", &self.last_request)
            .field("last_response", &self.last_response)
            .field("refresh_requests", &self.refresh_requests)
            .finish()
    }
}

/// Node id + its socket address.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: SocketAddr,
}

impl NodeInfo {
    pub fn new(id: NodeId, addr: SocketAddr) -> Self {
        Self { id, addr }
    }
}

// TODO: Verify the two scenarios follow the specification as some cases seem questionable (pun intended), ie, a node
// responds to us once, and then requests from us but never responds to us for the duration of the session. This means they
// could stay marked as a good node even though they could ignore our requests and just sending us periodic requests
// to keep their node marked as good in our routing table...

/// First scenario where a node is good is if it has responded to one of our requests recently.
///
/// Returns the status of the node where a Questionable status means the node has responded
/// to us before, but not recently.
fn recently_responded(node: &Node, curr_time: Instant) -> NodeStatus {
    // Check if node has ever responded to us
    let since_response = match node.last_response {
        Some(response_time) => curr_time - response_time,
        None => return NodeStatus::Bad,
    };

    // Check if node has recently responded to us
    let max_last_response = Duration::from_secs(MAX_LAST_SEEN_MINS * 60);
    if since_response < max_last_response {
        NodeStatus::Good
    } else {
        NodeStatus::Questionable
    }
}

/// Second scenario where a node has ever responded to one of our requests and is good if it
/// has sent us a request recently.
///
/// Returns the final status of the node given that the first scenario found the node to be
/// Questionable.
fn recently_requested(node: &Node, curr_time: Instant) -> NodeStatus {
    let max_last_request = Duration::from_secs(MAX_LAST_SEEN_MINS * 60);

    // Check if the node has recently request from us
    if let Some(request_time) = node.last_request {
        let since_request = curr_time - request_time;

        if since_request < max_last_request {
            return NodeStatus::Good;
        }
    }

    // Check if we have request from node multiple times already without response
    if node.refresh_requests < MAX_REFRESH_REQUESTS {
        NodeStatus::Questionable
    } else {
        NodeStatus::Bad
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::time::{Duration, Instant};

    use crate::compact::Compact;
    use crate::routing::node::{Node, NodeStatus};
    use crate::test;

    #[test]
    fn positive_encode_node() {
        let node_id = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let ip_addr = [127, 0, 0, 1];
        let port = 6881;

        let v4_ip = Ipv4Addr::new(ip_addr[0], ip_addr[1], ip_addr[2], ip_addr[3]);
        let sock_addr = SocketAddr::V4(SocketAddrV4::new(v4_ip, port));

        let node = Node::as_good(node_id.into(), sock_addr);

        let encoded_node = node.info().encode();

        let port_bytes = [(port >> 8) as u8, port as u8];
        for (expected, actual) in node_id
            .iter()
            .chain(ip_addr.iter())
            .chain(port_bytes.iter())
            .zip(encoded_node.iter())
        {
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn positive_as_bad() {
        let node = Node::as_bad(test::dummy_node_id(), test::dummy_socket_addr_v4());

        assert_eq!(node.status(), NodeStatus::Bad);
    }

    #[test]
    fn positive_as_questionable() {
        let node = Node::as_questionable(test::dummy_node_id(), test::dummy_socket_addr_v4());

        assert_eq!(node.status(), NodeStatus::Questionable);
    }

    #[test]
    fn positive_as_good() {
        let node = Node::as_good(test::dummy_node_id(), test::dummy_socket_addr_v4());

        assert_eq!(node.status(), NodeStatus::Good);
    }

    #[test]
    fn positive_response_renewal() {
        let mut node = Node::as_questionable(test::dummy_node_id(), test::dummy_socket_addr_v4());

        node.remote_response();

        assert_eq!(node.status(), NodeStatus::Good);
    }

    #[test]
    fn positive_request_renewal() {
        let mut node = Node::as_questionable(test::dummy_node_id(), test::dummy_socket_addr_v4());

        node.remote_request();

        assert_eq!(node.status(), NodeStatus::Good);
    }

    #[test]
    fn positive_node_idle() {
        let mut node = Node::as_good(test::dummy_node_id(), test::dummy_socket_addr_v4());

        let time_offset = Duration::from_secs(super::MAX_LAST_SEEN_MINS * 60);
        let idle_time = Instant::now() - time_offset;

        node.last_response = Some(idle_time);

        assert_eq!(node.status(), NodeStatus::Questionable);
    }

    #[test]
    fn positive_node_idle_reqeusts() {
        let mut node = Node::as_questionable(test::dummy_node_id(), test::dummy_socket_addr_v4());

        for _ in 0..super::MAX_REFRESH_REQUESTS {
            node.local_request();
        }

        assert_eq!(node.status(), NodeStatus::Bad);
    }

    #[test]
    fn positive_good_status_ordering() {
        assert!(NodeStatus::Good > NodeStatus::Questionable);
        assert!(NodeStatus::Good > NodeStatus::Bad);
    }

    #[test]
    fn positive_questionable_status_ordering() {
        assert!(NodeStatus::Questionable > NodeStatus::Bad);
        assert!(NodeStatus::Questionable < NodeStatus::Good);
    }

    #[test]
    fn positive_bad_status_ordering() {
        assert!(NodeStatus::Bad < NodeStatus::Good);
        assert!(NodeStatus::Bad < NodeStatus::Questionable);
    }
}
