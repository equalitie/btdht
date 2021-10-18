use std::net::SocketAddr;

use bip_util::bt::InfoHash;

/// Trait for peer discovery services to forward peer contact information and metadata.
pub trait Handshaker: Send {
    /// Connect to the given address with the InfoHash.
    fn connect(&mut self, hash: InfoHash, addr: SocketAddr);
}
