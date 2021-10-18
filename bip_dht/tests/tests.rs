use bip_dht::{DhtBuilder, DhtEvent};
use bip_util::bt::InfoHash;
use std::net::{Ipv4Addr, SocketAddr};

#[test]
fn basic() {
    // TODO: use random ports
    let a_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 20001));
    let b_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 20002));

    let node_a = DhtBuilder::with_node(b_addr)
        .set_source_addr(a_addr)
        .set_read_only(false)
        .start_mainline()
        .unwrap();
    let events_a = node_a.events();

    let node_b = DhtBuilder::with_node(a_addr)
        .set_source_addr(b_addr)
        .set_read_only(false)
        .start_mainline()
        .unwrap();
    let events_b = node_b.events();

    let the_info_hash = InfoHash::from_bytes(b"foo");

    // Perform a lookup with announce by A. It should not return any peers initially but it should
    // make B aware that A has the infohash.
    node_a.search(the_info_hash, true);

    let mut lookup_completed = false;

    for event in events_a.iter() {
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

    for event in events_b.iter() {
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
