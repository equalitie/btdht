use std::net::SocketAddr;

use bip_util::bt::InfoHash;

/// Trait for peer discovery services to forward peer contact information and metadata.
pub trait Handshaker: Send {
    /// Port exposed to peer discovery services.
    ///
    /// It is important that this is the external port that the peer will be sending data
    /// to. This is relevant if the client employs nat traversal via upnp or other means.
    fn port(&self) -> u16;

    /// Connect to the given address with the InfoHash.
    fn connect(&mut self, hash: InfoHash, addr: SocketAddr);
}
