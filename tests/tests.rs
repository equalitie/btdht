use btdht::{DhtEvent, InfoHash, MainlineDht};
use std::net::Ipv4Addr;
use tokio::net::UdpSocket;

#[tokio::test(flavor = "multi_thread")]
async fn basic() {
    let a_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let a_addr = a_socket.local_addr().unwrap();

    let b_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let b_addr = b_socket.local_addr().unwrap();

    let (node_a, mut events_a) = MainlineDht::builder()
        .add_node(b_addr)
        .set_read_only(false)
        .set_socket_v4(a_socket)
        .unwrap()
        .start();

    let (node_b, mut events_b) = MainlineDht::builder()
        .add_node(a_addr)
        .set_read_only(false)
        .set_socket_v4(b_socket)
        .unwrap()
        .start();

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
            event @ (DhtEvent::BootstrapFailed | DhtEvent::PeerFound(..)) => {
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
            event @ DhtEvent::BootstrapFailed => {
                panic!("unexpected event {:?}", event)
            }
        }
    }

    assert!(peer_found);
    assert!(lookup_completed);
}
