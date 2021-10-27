//! Compact representation

use crate::{
    id::{NodeId, NODE_ID_LEN},
    routing::node::NodeHandle,
};
use serde::{
    de::{Deserializer, Error},
    ser::Serializer,
    Deserialize,
};
use serde_bytes::ByteBuf;
use std::{
    convert::{TryFrom, TryInto},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
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

impl Compact for NodeHandle {
    type Buffer = [u8; NODE_INFO_LEN];

    fn decode(src: &[u8]) -> Option<Self> {
        let id = NodeId::decode(src.get(..NODE_ID_LEN)?)?;
        let addr = SocketAddrV4::decode(src.get(NODE_ID_LEN..NODE_ID_LEN + SOCKET_ADDR_LEN)?)?;

        Some(Self {
            id,
            addr: SocketAddr::V4(addr),
        })
    }

    fn encode(&self) -> Self::Buffer {
        let addr = match &self.addr {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => panic!("bip_dht: Cannot encode a SocketAddrV6..."),
        };

        let mut buffer = [0; NODE_INFO_LEN];
        buffer[..NODE_ID_LEN].copy_from_slice(self.id.encode().as_ref());
        buffer[NODE_ID_LEN..].copy_from_slice(addr.encode().as_ref());
        buffer
    }
}

impl Compact for Vec<NodeHandle> {
    type Buffer = Vec<u8>;

    fn decode(src: &[u8]) -> Option<Self> {
        let num = src.len() / NODE_INFO_LEN;
        let vec = (0..num)
            .filter_map(|index| NodeHandle::decode(&src[index * NODE_INFO_LEN..]))
            .collect();

        Some(vec)
    }

    fn encode(&self) -> Self::Buffer {
        self.iter().fold(Vec::new(), |mut output, item| {
            output.extend_from_slice(item.encode().as_ref());
            output
        })
    }
}

/// Serialize a `Compact` value as bytes.
pub(crate) fn serialize<T, S>(value: &T, s: S) -> Result<S::Ok, S::Error>
where
    T: Compact,
    S: Serializer,
{
    s.serialize_bytes(value.encode().as_ref())
}

/// Deserialize a `Compact` value from bytes.
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

/// Serialize/deserialize a `Vec` of `Compact` values as a list of bytes.
pub(crate) mod vec {
    use super::Compact;
    use serde::{
        de::{Deserializer, Error as _, SeqAccess, Visitor},
        ser::{SerializeSeq, Serializer},
    };
    use serde_bytes::{ByteBuf, Bytes};
    use std::{fmt, marker::PhantomData};

    pub(crate) fn serialize<T, S>(items: &[T], s: S) -> Result<S::Ok, S::Error>
    where
        T: Compact,
        S: Serializer,
    {
        let mut seq = s.serialize_seq(Some(items.len()))?;
        for item in items {
            seq.serialize_element(Bytes::new(item.encode().as_ref()))?
        }
        seq.end()
    }

    pub(crate) fn deserialize<'de, T, D>(d: D) -> Result<Vec<T>, D::Error>
    where
        T: Compact,
        D: Deserializer<'de>,
    {
        struct CompactVecVisitor<U>(PhantomData<U>);

        impl<'de, U: Compact> Visitor<'de> for CompactVecVisitor<U> {
            type Value = Vec<U>;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "list of bytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut output = Vec::with_capacity(seq.size_hint().unwrap_or(0));

                while let Some(bytes) = seq.next_element::<ByteBuf>()? {
                    let item = U::decode(&bytes)
                        .ok_or_else(|| A::Error::invalid_length(bytes.len(), &self))?;
                    output.push(item);
                }

                Ok(output)
            }
        }

        d.deserialize_seq(CompactVecVisitor(PhantomData))
    }
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
        let orig = NodeHandle {
            id: NodeId::from(*b"0123456789abcdefghij"),
            addr: (Ipv4Addr::LOCALHOST, 6789).into(),
        };

        let encoded = orig.encode();
        let expected = [
            b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a', b'b', b'c', b'd',
            b'e', b'f', b'g', b'h', b'i', b'j', 127, 0, 0, 1, 26, 133,
        ];
        assert_eq!(encoded, expected);

        let decoded = NodeHandle::decode(&encoded).unwrap();
        assert_eq!(decoded, orig);
    }
}
