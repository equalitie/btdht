use btdht::{router, DhtEvent, InfoHash, LengthError, MainlineDht};
use std::{
    collections::HashSet,
    convert::TryFrom,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    time::Instant,
};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{self, UdpSocket},
    sync::mpsc,
};

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0));
    let socket = UdpSocket::bind(addr).await.unwrap();

    let dht = MainlineDht::builder()
        .add_routers(net::lookup_host(router::BITTORRENT_DHT).await.unwrap())
        .add_routers(net::lookup_host(router::TRANSMISSION_DHT).await.unwrap())
        .set_read_only(false)
        .start(socket)
        .unwrap();
    let mut events = dht.events();

    println!("bootstrapping...");
    let start = Instant::now();

    while let Some(event) = events.recv().await {
        match event {
            DhtEvent::BootstrapCompleted => {
                let elapsed = start.elapsed();
                println!(
                    "bootstrap completed in {}.{:03} seconds",
                    elapsed.as_secs(),
                    elapsed.subsec_millis()
                );
                break;
            }
            DhtEvent::ShuttingDown(cause) => {
                println!("unexpected shutdown (cause: {:?})", cause);
                return;
            }
            DhtEvent::LookupCompleted(_) | DhtEvent::PeerFound(..) => {
                println!("unexpected event: {:?}", event);
            }
        }
    }

    let mut stdout = io::stdout();
    let mut stdin = BufReader::new(io::stdin());
    let mut line = String::new();

    loop {
        stdout.write_all(b"> ").await.unwrap();
        stdout.flush().await.unwrap();

        line.clear();

        if stdin.read_line(&mut line).await.unwrap() > 0 {
            if !handle_command(&dht, &mut events, &line).await.unwrap() {
                break;
            }
        } else {
            break;
        }
    }
}

async fn handle_command(
    dht: &MainlineDht,
    events: &mut mpsc::UnboundedReceiver<DhtEvent>,
    command: &str,
) -> io::Result<bool> {
    match command.parse() {
        Ok(Command::Help) => {
            println!("    h               shows this help message");
            println!("    s <INFO_HASH>   search for the specified info hash");
            println!("    a <INFO_HASH>   announce the specified info hash");
            println!("    q               quit");
        }
        Ok(Command::Search {
            info_hash,
            announce,
        }) => {
            if announce {
                println!("announcing {:?}...", info_hash)
            } else {
                println!("searching for {:?}...", info_hash)
            }

            dht.search(info_hash, announce);

            let start = Instant::now();
            let mut peers = HashSet::new();

            while let Some(event) = events.recv().await {
                match event {
                    DhtEvent::PeerFound(event_info_hash, addr) if event_info_hash == info_hash => {
                        println!("peer found: {}", addr);
                        peers.insert(addr);
                    }
                    DhtEvent::LookupCompleted(event_info_hash) if event_info_hash == info_hash => {
                        let elapsed = start.elapsed();
                        println!(
                            "search completed: found {} peers in {}.{:03} seconds",
                            peers.len(),
                            elapsed.as_secs(),
                            elapsed.subsec_millis()
                        );
                        break;
                    }
                    DhtEvent::BootstrapCompleted
                    | DhtEvent::PeerFound(..)
                    | DhtEvent::LookupCompleted(_) => println!("unexpected event: {:?}", event),
                    DhtEvent::ShuttingDown(cause) => {
                        println!("shutting down (cause: {:?})", cause);
                        return Ok(false);
                    }
                }
            }
        }
        Ok(Command::Quit) => return Ok(false),
        Err(_) => println!("invalid command (use 'h' for help)"),
    }

    Ok(true)
}

enum Command {
    Help,
    Search { info_hash: InfoHash, announce: bool },
    Quit,
}

impl FromStr for Command {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s[..1] {
            "h" | "?" => Ok(Self::Help),
            "s" => Ok(Self::Search {
                info_hash: parse_info_hash(s[1..].trim())?,
                announce: false,
            }),
            "a" => Ok(Self::Search {
                info_hash: parse_info_hash(s[1..].trim())?,
                announce: true,
            }),
            "q" => Ok(Self::Quit),
            _ => Err(ParseError),
        }
    }
}

struct ParseError;

impl From<hex::FromHexError> for ParseError {
    fn from(_: hex::FromHexError) -> Self {
        ParseError
    }
}

impl From<LengthError> for ParseError {
    fn from(_: LengthError) -> Self {
        ParseError
    }
}

fn parse_info_hash(s: &str) -> Result<InfoHash, ParseError> {
    if &s[..1] == "#" {
        Ok(InfoHash::from_bytes(s[1..].trim().as_bytes()))
    } else {
        Ok(InfoHash::try_from(hex::decode(s)?.as_ref())?)
    }
}
