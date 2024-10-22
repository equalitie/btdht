use btdht::{InfoHash, MainlineDht};
use futures_util::StreamExt;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::net::UdpSocket;

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn announce_and_lookup_v4() {
    announce_and_lookup(AddrFamily::V4).await;
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn announce_and_lookup_v6() {
    announce_and_lookup(AddrFamily::V6).await;
}

async fn announce_and_lookup(addr_family: AddrFamily) {
    // Start the router node for the other nodes to bootstrap against.
    let bootstrap_node_socket = UdpSocket::bind(localhost(addr_family)).await.unwrap();
    let bootstrap_node_addr = bootstrap_node_socket.local_addr().unwrap();
    let bootstrap_node = MainlineDht::builder()
        .set_read_only(false)
        .start(bootstrap_node_socket)
        .unwrap();

    assert!(bootstrap_node.bootstrapped().await);

    // Start node A
    let a_socket = UdpSocket::bind(localhost(addr_family)).await.unwrap();
    let a_addr = a_socket.local_addr().unwrap();
    let a_node = MainlineDht::builder()
        .add_node(bootstrap_node_addr)
        .set_read_only(false)
        .start(a_socket)
        .unwrap();

    // Start node B
    let b_socket = UdpSocket::bind(localhost(addr_family)).await.unwrap();
    let b_node = MainlineDht::builder()
        .add_node(bootstrap_node_addr)
        .set_read_only(false)
        .start(b_socket)
        .unwrap();

    // Wait for both nodes to bootstrap
    assert!(a_node.bootstrapped().await);
    assert!(b_node.bootstrapped().await);

    let the_info_hash = InfoHash::sha1(b"foo");

    // Perform a lookup with announce by A. It should not return any peers initially but it should
    // make the network aware that A has the infohash.
    let mut search = a_node.search(the_info_hash, true);
    assert_eq!(search.next().await, None);

    // Now perform the lookup by B. It should find A.
    let mut search = b_node.search(the_info_hash, false);
    assert_eq!(search.next().await, Some(a_addr));
}

#[derive(Copy, Clone)]
enum AddrFamily {
    V4,
    V6,
}

fn localhost(family: AddrFamily) -> SocketAddr {
    match family {
        AddrFamily::V4 => (Ipv4Addr::LOCALHOST, 0).into(),
        AddrFamily::V6 => (Ipv6Addr::LOCALHOST, 0).into(),
    }
}
