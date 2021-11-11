use btdht::{router, InfoHash, LengthError, MainlineDht};
use futures_util::StreamExt;
use std::{
    collections::HashSet,
    convert::TryFrom,
    net::{Ipv4Addr, SocketAddr},
    str::FromStr,
    time::Instant,
};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{self, UdpSocket},
};

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));
    let socket = UdpSocket::bind(addr).await.unwrap();

    let dht = MainlineDht::builder()
        .add_routers(net::lookup_host(router::BITTORRENT_DHT).await.unwrap())
        .add_routers(net::lookup_host(router::TRANSMISSION_DHT).await.unwrap())
        .set_read_only(false)
        .start(socket);

    println!("bootstrapping...");
    let start = Instant::now();
    let status = dht.bootstrapped().await;
    let elapsed = start.elapsed();

    if status {
        println!(
            "bootstrap completed in {}.{:03} seconds",
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
    } else {
        println!(
            "bootstrap failed in {}.{:03} seconds",
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
        return;
    }

    let mut stdout = io::stdout();
    let mut stdin = BufReader::new(io::stdin());
    let mut line = String::new();

    loop {
        stdout.write_all(b"> ").await.unwrap();
        stdout.flush().await.unwrap();

        line.clear();

        if stdin.read_line(&mut line).await.unwrap() > 0 {
            if !handle_command(&dht, &line).await.unwrap() {
                break;
            }
        } else {
            break;
        }
    }
}

async fn handle_command(dht: &MainlineDht, command: &str) -> io::Result<bool> {
    match command.parse() {
        Ok(Command::Help) => {
            println!("    h               shows this help message");
            println!("    s <INFO_HASH>   search for the specified info hash");
            println!("    a <INFO_HASH>   announce the specified info hash");
            println!("    q               quit");
            println!();
            println!(
                "Note: <INFO_HASH> can be specified either as a 40-character hexadecimal string or \
                 an arbitrary string prefixed with '#'. In the first case it is interpreted \
                 directly as the info hash, in the second the info hash is obtained by computing a \
                 SHA-1 digest of the string excluding the leading '#' and trimming any leading or \
                 trailing whitespace."
            );

            Ok(true)
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

            let mut peers = HashSet::new();
            let start = Instant::now();

            let mut search = dht.search(info_hash, announce);

            while let Some(addr) = search.next().await {
                if peers.insert(addr) {
                    println!("peer found: {}", addr);
                }
            }

            let elapsed = start.elapsed();
            println!(
                "search completed: found {} peers in {}.{:03} seconds",
                peers.len(),
                elapsed.as_secs(),
                elapsed.subsec_millis()
            );

            Ok(true)
        }
        Ok(Command::Quit) => Ok(false),
        Err(_) => {
            println!("invalid command (use 'h' for help)");
            Ok(true)
        }
    }
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
        Ok(InfoHash::sha1(s[1..].trim().as_bytes()))
    } else {
        Ok(InfoHash::try_from(hex::decode(s)?.as_ref())?)
    }
}
