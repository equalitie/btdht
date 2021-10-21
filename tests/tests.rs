use bip_dht::{DhtBuilder, DhtEvent, InfoHash};
use std::net::Ipv4Addr;
use tokio::net::UdpSocket;

#[tokio::test(flavor = "multi_thread")]
async fn basic() {
    let a_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let a_addr = a_socket.local_addr().unwrap();

    let b_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let b_addr = b_socket.local_addr().unwrap();

    let node_a = DhtBuilder::with_node(b_addr)
        .set_read_only(false)
        .start_mainline(a_socket)
        .unwrap();
    let mut events_a = node_a.events();

    let node_b = DhtBuilder::with_node(a_addr)
        .set_read_only(false)
        .start_mainline(b_socket)
        .unwrap();
    let mut events_b = node_b.events();

    let the_info_hash = InfoHash::from_bytes(b"foo");

    // Perform a lookup with announce by A. It should not return any peers initially but it should
    // make B aware that A has the infohash.
    node_a.search(the_info_hash, true);

    let mut lookup_completed = false;

    while let Some(event) = events_a.recv().await {
        match event {
            DhtEvent::BootstrapCompleted => (),
            DhtEvent::LookupCompleted(info_hash) => {
                assert_eq!(info_hash, the_info_hash);
                lookup_completed = true;
                break;
            }
            event @ (DhtEvent::PeerFound(..) | DhtEvent::ShuttingDown(_)) => {
                panic!("unexpected event {:?}", event)
            }
        }
    }

    assert!(lookup_completed);

    // Now perform the lookup by B. It should find A.
    node_b.search(the_info_hash, false);

    let mut peer_found = false;
    let mut lookup_completed = false;

    while let Some(event) = events_b.recv().await {
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
            event @ DhtEvent::ShuttingDown(_) => panic!("unexpected event {:?}", event),
        }
    }

    assert!(peer_found);
    assert!(lookup_completed);
}
