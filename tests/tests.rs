use btdht::{DhtEvent, InfoHash, MainlineDht};
use futures_util::StreamExt;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::net::UdpSocket;

#[tokio::test(flavor = "multi_thread")]
async fn announce_and_lookup_v4() {
    announce_and_lookup(AddrFamily::V4).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn announce_and_lookup_v6() {
    announce_and_lookup(AddrFamily::V6).await;
}

async fn announce_and_lookup(addr_family: AddrFamily) {
    // Start the router node for the other nodes to bootstrap against.
    let bootstrap_node_socket = UdpSocket::bind(localhost(addr_family)).await.unwrap();
    let bootstrap_node_addr = bootstrap_node_socket.local_addr().unwrap();
    let (_node, mut bootstrap_node_events) = MainlineDht::builder()
        .set_read_only(false)
        .start(bootstrap_node_socket);
    let mut bootstrap_node_started = false;

    while let Some(event) = bootstrap_node_events.recv().await {
        match event {
            DhtEvent::BootstrapCompleted => {
                bootstrap_node_started = true;
                break;
            }
            DhtEvent::BootstrapFailed => {
                panic!("unexpected event {:?}", event)
            }
        }
    }

    assert!(bootstrap_node_started);

    // Start node A
    let a_socket = UdpSocket::bind(localhost(addr_family)).await.unwrap();
    let a_addr = a_socket.local_addr().unwrap();
    let (a_node, a_events) = MainlineDht::builder()
        .add_node(bootstrap_node_addr)
        .set_read_only(false)
        .start(a_socket);

    // Start node B
    let b_socket = UdpSocket::bind(localhost(addr_family)).await.unwrap();
    let (b_node, b_events) = MainlineDht::builder()
        .add_node(bootstrap_node_addr)
        .set_read_only(false)
        .start(b_socket);

    // Wait for both nodes to bootstrap
    for mut events in [a_events, b_events] {
        let mut bootstrapped = false;

        while let Some(event) = events.recv().await {
            match event {
                DhtEvent::BootstrapCompleted => {
                    bootstrapped = true;
                    break;
                }
                DhtEvent::BootstrapFailed => panic!("bootstrap failed"),
            }
        }

        assert!(bootstrapped);
    }

    let the_info_hash = InfoHash::sha1(b"foo");

    // Perform a lookup with announce by A. It should not return any peers initially but it should
    // make the network aware that A has the infohash.
    let mut search = a_node.search(the_info_hash, true);

    while let Some(peer) = search.next().await {
        panic!("found peer {} but none expected", peer)
    }

    // Now perform the lookup by B. It should find A.
    let mut search = b_node.search(the_info_hash, false);
    let mut peer_found = false;

    while let Some(addr) = search.next().await {
        assert_eq!(addr, a_addr);
        peer_found = true;
    }

    assert!(peer_found);
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
