//! Implementation of the Bittorrent Mainline Distributed Hash Table.

// Mainline DHT extensions supported on behalf of libtorrent:
// - Always send 'nodes' on a get_peers response even if 'values' is present
// - Unrecognized requests which contain either an 'info_hash' or 'target' arguments are interpreted as 'find_node' TODO
// - Client identification will be present in all outgoing messages in the form of the 'v' key TODO
// const CLIENT_IDENTIFICATION: &'static [u8] = &[b'B', b'I', b'P', 0, 1];

// TODO: The Vuze dht operates over a protocol that is different than the mainline dht.
// It would be possible to create a dht client that can work over both dhts simultaneously,
// this would require essentially a completely separate routing table of course and so it
// might make sense to make this distinction available to the user and allow them to startup
// two dhts using the different protocols on their own.
// const VUZE_DHT: (&'static str, u16) = ("dht.aelitis.com", 6881);

pub mod router;

mod action;
mod bucket;
mod compact;
mod handler;
mod info_hash;
mod mainline_dht;
pub mod message;
mod node;
mod socket;
mod storage;
mod table;
#[cfg(test)]
mod test;
mod time;
mod timer;
mod token;
mod transaction;

pub use crate::action::State;
pub use crate::info_hash::{InfoHash, LengthError, NodeId, INFO_HASH_LEN};
pub use crate::mainline_dht::{DhtBuilder, MainlineDht};

pub type IpVersion = crate::action::IpVersion;

use async_trait::async_trait;
use std::{io, net::SocketAddr};

#[async_trait]
pub trait SocketTrait {
    async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> io::Result<()>;
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)>;
    fn local_addr(&self) -> io::Result<SocketAddr>;
}
