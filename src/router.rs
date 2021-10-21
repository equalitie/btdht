//! Some known public DHT routers.

pub const UTORRENT_DHT: &str = "router.utorrent.com:6881";
// As of recent, this looks to be no longer a CNAME to router.utorrent.com,
// if this is not the case, we should remove it in the future.
pub const BITTORRENT_DHT: &str = "router.bittorrent.com:6881";
pub const BITCOMET_DHT: &str = "router.bitcomet.com:6881";
pub const TRANSMISSION_DHT: &str = "dht.transmissionbt.com:6881";
