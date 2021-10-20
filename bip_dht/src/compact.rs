//! Compact representation

use crate::{
    id::{NodeId, NODE_ID_LEN},
    message2::NodeInfo,
};
use serde::{
    de::{Deserializer, Error},
    ser::Serializer,
    Deserialize,
};
use serde_bytes::ByteBuf;
use std::{
    convert::{TryFrom, TryInto},
    net::{Ipv4Addr, SocketAddrV4},
};

const SOCKET_ADDR_LEN: usize = 6;
const NODE_INFO_LEN: usize = SOCKET_ADDR_LEN + NODE_ID_LEN;

pub(crate) trait Compact: Sized {
    type Buffer: AsRef<[u8]>;

    fn decode(src: &[u8]) -> Option<Self>;
    fn encode(&self) -> Self::Buffer;
}

impl Compact for SocketAddrV4 {
    type Buffer = [u8; SOCKET_ADDR_LEN];

    fn decode(src: &[u8]) -> Option<Self> {
        let addr = src.get(..4)?;
        let addr = Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3]);
        let port = u16::from_be_bytes(src[4..6].try_into().ok()?);
        Some(SocketAddrV4::new(addr, port))
    }

    fn encode(&self) -> Self::Buffer {
        let mut buffer = [0; SOCKET_ADDR_LEN];
        buffer[..4].copy_from_slice(self.ip().octets().as_ref());
        buffer[4..].copy_from_slice(self.port().to_be_bytes().as_ref());
        buffer
    }
}

impl Compact for NodeId {
    type Buffer = [u8; NODE_ID_LEN];

    fn decode(src: &[u8]) -> Option<Self> {
        Self::try_from(src).ok()
    }

    fn encode(&self) -> Self::Buffer {
        (*self).into()
    }
}

impl Compact for NodeInfo {
    type Buffer = [u8; NODE_INFO_LEN];

    fn decode(src: &[u8]) -> Option<Self> {
        let id = NodeId::decode(src.get(..NODE_ID_LEN)?)?;
        let addr = SocketAddrV4::decode(src.get(NODE_ID_LEN..NODE_ID_LEN + SOCKET_ADDR_LEN)?)?;

        Some(Self { id, addr })
    }

    fn encode(&self) -> Self::Buffer {
        let mut buffer = [0; NODE_INFO_LEN];
        buffer[..NODE_ID_LEN].copy_from_slice(self.id.encode().as_ref());
        buffer[NODE_ID_LEN..].copy_from_slice(self.addr.encode().as_ref());
        buffer
    }
}

impl Compact for Vec<SocketAddrV4> {
    type Buffer = Vec<u8>;

    fn decode(src: &[u8]) -> Option<Self> {
        decode_vec(src, SOCKET_ADDR_LEN)
    }

    fn encode(&self) -> Self::Buffer {
        encode_slice(self)
    }
}

impl Compact for Vec<NodeInfo> {
    type Buffer = Vec<u8>;

    fn decode(src: &[u8]) -> Option<Self> {
        decode_vec(src, NODE_INFO_LEN)
    }

    fn encode(&self) -> Self::Buffer {
        encode_slice(self)
    }
}

fn decode_vec<T: Compact>(src: &[u8], item_len: usize) -> Option<Vec<T>> {
    let num = src.len() / item_len;
    let vec = (0..num)
        .filter_map(|index| T::decode(&src[index * item_len..]))
        .collect();

    Some(vec)
}

fn encode_slice<T: Compact>(items: &[T]) -> Vec<u8> {
    items.iter().fold(Vec::new(), |mut output, item| {
        output.extend_from_slice(item.encode().as_ref());
        output
    })
}

pub(crate) fn serialize<T, S>(value: &T, s: S) -> Result<S::Ok, S::Error>
where
    T: Compact,
    S: Serializer,
{
    s.serialize_bytes(value.encode().as_ref())
}

pub(crate) fn deserialize<'de, T, D>(d: D) -> Result<T, D::Error>
where
    T: Compact,
    D: Deserializer<'de>,
{
    let buf = ByteBuf::deserialize(d)?;
    T::decode(&buf).ok_or_else(|| {
        let msg = format!("multiple of {} or {}", SOCKET_ADDR_LEN, NODE_INFO_LEN);
        D::Error::invalid_length(buf.len(), &msg.as_ref())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_socket_addr() {
        let orig = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6789);

        let encoded = orig.encode();
        let expected = [127, 0, 0, 1, 26, 133];
        assert_eq!(encoded, expected);

        let decoded = SocketAddrV4::decode(&encoded).unwrap();
        assert_eq!(decoded, orig);
    }

    #[test]
    fn compact_node_info() {
        let orig = NodeInfo {
            id: NodeId::from(*b"0123456789abcdefghij"),
            addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6789),
        };

        let encoded = orig.encode();
        let expected = [
            b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a', b'b', b'c', b'd',
            b'e', b'f', b'g', b'h', b'i', b'j', 127, 0, 0, 1, 26, 133,
        ];
        assert_eq!(encoded, expected);

        let decoded = NodeInfo::decode(&encoded).unwrap();
        assert_eq!(decoded, orig);
    }
}
