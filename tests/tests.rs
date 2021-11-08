use btdht::{DhtEvent, InfoHash, MainlineDht};
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
            DhtEvent::BootstrapFailed | DhtEvent::LookupCompleted(_) | DhtEvent::PeerFound(..) => {
                panic!("unexpected event {:?}", event)
            }
        }
    }

    assert!(bootstrap_node_started);

    // Start node A
    let a_socket = UdpSocket::bind(localhost(addr_family)).await.unwrap();
    let a_addr = a_socket.local_addr().unwrap();
    let (a_node, mut a_events) = MainlineDht::builder()
        .add_node(bootstrap_node_addr)
        .set_read_only(false)
        .start(a_socket);

    // Start node B
    let b_socket = UdpSocket::bind(localhost(addr_family)).await.unwrap();
    let (b_node, mut b_events) = MainlineDht::builder()
        .add_node(bootstrap_node_addr)
        .set_read_only(false)
        .start(b_socket);

    let the_info_hash = InfoHash::sha1(b"foo");

    // Perform a lookup with announce by A. It should not return any peers initially but it should
    // make the network aware that A has the infohash.
    a_node.search(the_info_hash, true);

    let mut lookup_completed = false;

    while let Some(event) = a_events.recv().await {
        match event {
            DhtEvent::BootstrapCompleted => (),
            DhtEvent::LookupCompleted(info_hash) => {
                assert_eq!(info_hash, the_info_hash);
                lookup_completed = true;
                break;
            }
            DhtEvent::BootstrapFailed | DhtEvent::PeerFound(..) => {
                panic!("unexpected event {:?}", event)
            }
        }
    }

    assert!(lookup_completed);

    // Now perform the lookup by B. It should find A.
    b_node.search(the_info_hash, false);

    let mut peer_found = false;
    let mut lookup_completed = false;

    while let Some(event) = b_events.recv().await {
        match event {
            DhtEvent::BootstrapCompleted => (),
            DhtEvent::LookupCompleted(info_hash) => {
                assert_eq!(info_hash, the_info_hash);
                lookup_completed = true;
                break;
            }
            DhtEvent::PeerFound(info_hash, addr) => {
                assert_eq!(info_hash, the_info_hash);
                assert_eq!(addr, a_addr);
                peer_found = true;
            }
            DhtEvent::BootstrapFailed => {
                panic!("unexpected event {:?}", event)
            }
        }
    }

    assert!(peer_found);
    assert!(lookup_completed);
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
