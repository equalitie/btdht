use std::io::{self, Read};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::thread::{self};

use bip_dht::{DhtBuilder, Router};
use bip_util::bt::InfoHash;

fn main() {
    pretty_env_logger::init();

    let hash = InfoHash::from_bytes(b"My Unique Info Hash");

    let dht = DhtBuilder::with_router(Router::uTorrent)
        .set_source_addr(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(0, 0, 0, 0),
            6889,
        )))
        .set_read_only(false)
        .start_mainline()
        .unwrap();

    // Spawn a thread to listen to and report events
    let events = dht.events();
    thread::spawn(move || {
        for event in events {
            println!("\nReceived Dht Event {:?}", event);
        }
    });

    // Let the user announce or search on our info hash
    let stdin = io::stdin();
    let stdin_lock = stdin.lock();
    for byte in stdin_lock.bytes() {
        match &[byte.unwrap()] {
            b"a" => dht.search(hash.into(), true),
            b"s" => dht.search(hash.into(), false),
            _ => (),
        }
    }
}
