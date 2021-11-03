//! Compact representation

use crate::id::NODE_ID_LEN;
use std::{
    convert::TryInto,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};

const SOCKET_ADDR_V4_LEN: usize = 6;
const SOCKET_ADDR_V6_LEN: usize = 18;

/// Serialize/deserialize `Vec` of `SocketAddr` in compact format.
pub(crate) mod values {
    use serde::{
        de::{Deserializer, Error as _, SeqAccess, Visitor},
        ser::{SerializeSeq, Serializer},
    };
    use serde_bytes::{ByteBuf, Bytes};
    use std::{fmt, net::SocketAddr};

    pub(crate) fn serialize<S>(addrs: &[SocketAddr], s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = s.serialize_seq(Some(addrs.len()))?;
        for addr in addrs {
            seq.serialize_element(Bytes::new(&super::encode_socket_addr(addr)))?
        }
        seq.end()
    }

    pub(crate) fn deserialize<'de, D>(d: D) -> Result<Vec<SocketAddr>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SocketAddrsVisitor;

        impl<'de> Visitor<'de> for SocketAddrsVisitor {
            type Value = Vec<SocketAddr>;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "list of byte strings")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut output = Vec::with_capacity(seq.size_hint().unwrap_or(0));

                while let Some(bytes) = seq.next_element::<ByteBuf>()? {
                    let item = super::decode_socket_addr(&bytes)
                        .ok_or_else(|| A::Error::invalid_length(bytes.len(), &self))?;
                    output.push(item);
                }

                Ok(output)
            }
        }

        d.deserialize_seq(SocketAddrsVisitor)
    }
}

/// Serialize/deserialize `Vec` of `NodeHandle` in compact format. Specialized for ipv4 addresses.
pub(crate) mod nodes_v4 {
    use crate::routing::node::NodeHandle;
    use serde::{de::Deserializer, ser::Serializer};

    pub(crate) fn serialize<S>(nodes: &[NodeHandle], s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        super::nodes::serialize::<S, { super::SOCKET_ADDR_V4_LEN }>(nodes, s)
    }

    pub(crate) fn deserialize<'de, D>(d: D) -> Result<Vec<NodeHandle>, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::nodes::deserialize::<D, { super::SOCKET_ADDR_V4_LEN }>(d)
    }
}

/// Serialize/deserialize `Vec` of `NodeHandle` in compact format. Specialized for ipv6 addresses.
pub(crate) mod nodes_v6 {
    use crate::routing::node::NodeHandle;
    use serde::{de::Deserializer, ser::Serializer};

    pub(crate) fn serialize<S>(nodes: &[NodeHandle], s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        super::nodes::serialize::<S, { super::SOCKET_ADDR_V6_LEN }>(nodes, s)
    }

    pub(crate) fn deserialize<'de, D>(d: D) -> Result<Vec<NodeHandle>, D::Error>
    where
        D: Deserializer<'de>,
    {
        super::nodes::deserialize::<D, { super::SOCKET_ADDR_V6_LEN }>(d)
    }
}

/// Serialize/deserialize `Vec` of `NodeHandle` in compact format. Generic over address family.
mod nodes {
    use crate::{id::NodeId, routing::node::NodeHandle};
    use serde::{
        de::{Deserialize, Deserializer, Error as _},
        ser::{Error as _, Serializer},
    };
    use serde_bytes::ByteBuf;
    use std::convert::TryFrom;

    pub(crate) fn serialize<S, const ADDR_LEN: usize>(
        nodes: &[NodeHandle],
        s: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buffer = Vec::with_capacity(nodes.len() * (super::NODE_ID_LEN + ADDR_LEN));

        for node in nodes {
            let encoded_addr = super::encode_socket_addr(&node.addr);

            if encoded_addr.len() != ADDR_LEN {
                return Err(S::Error::custom("unexpected address family"));
            }

            buffer.extend(node.id.as_ref());
            buffer.extend(encoded_addr);
        }

        s.serialize_bytes(&buffer)
    }

    pub(crate) fn deserialize<'de, D, const ADDR_LEN: usize>(
        d: D,
    ) -> Result<Vec<NodeHandle>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buffer = ByteBuf::deserialize(d)?;
        let chunks = buffer.chunks_exact(super::NODE_ID_LEN + ADDR_LEN);

        if !chunks.remainder().is_empty() {
            let msg = format!("multiple of {}", (super::NODE_ID_LEN + ADDR_LEN));
            return Err(D::Error::invalid_length(buffer.len(), &msg.as_ref()));
        }

        let nodes = chunks
            .filter_map(|chunk| {
                let id = NodeId::try_from(&chunk[..super::NODE_ID_LEN]).ok()?;
                let addr = super::decode_socket_addr(&chunk[super::NODE_ID_LEN..])?;

                Some(NodeHandle { id, addr })
            })
            .collect();

        Ok(nodes)
    }
}

fn decode_socket_addr(src: &[u8]) -> Option<SocketAddr> {
    if src.len() == SOCKET_ADDR_V4_LEN {
        let addr: [u8; 4] = src.get(..4)?.try_into().ok()?;
        let addr = Ipv4Addr::from(addr);
        let port = u16::from_be_bytes(src.get(4..)?.try_into().ok()?);
        Some((addr, port).into())
    } else if src.len() == SOCKET_ADDR_V6_LEN {
        let addr: [u8; 16] = src.get(..16)?.try_into().ok()?;
        let addr = Ipv6Addr::from(addr);
        let port = u16::from_be_bytes(src.get(16..)?.try_into().ok()?);
        Some((addr, port).into())
    } else {
        None
    }
}

fn encode_socket_addr(addr: &SocketAddr) -> Vec<u8> {
    let mut buffer = match addr {
        SocketAddr::V4(addr) => {
            let mut buffer = Vec::with_capacity(6);
            buffer.extend(addr.ip().octets().as_ref());
            buffer
        }
        SocketAddr::V6(addr) => {
            let mut buffer = Vec::with_capacity(18);
            buffer.extend(addr.ip().octets().as_ref());
            buffer
        }
    };

    buffer.extend(addr.port().to_be_bytes().as_ref());
    buffer
}

#[cfg(test)]
mod tests {
    use crate::{id::NodeId, routing::node::NodeHandle};
    use serde::{Deserialize, Serialize};
    use std::{
        fmt::Debug,
        net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    };

    #[test]
    fn encode_decode_values() {
        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        struct Wrapper {
            #[serde(with = "super::values")]
            values: Vec<SocketAddr>,
        }

        // empty
        encode_decode(&Wrapper { values: Vec::new() }, b"le");
        // one v4
        encode_decode(
            &Wrapper {
                values: vec![(Ipv4Addr::new(127, 0, 0, 1), 6789).into()],
            },
            &[b'l', b'6', b':', 127, 0, 0, 1, 26, 133, b'e'],
        );
        // two v4
        encode_decode(
            &Wrapper {
                values: vec![
                    (Ipv4Addr::new(127, 0, 0, 1), 6789).into(),
                    (Ipv4Addr::new(127, 0, 0, 2), 1234).into(),
                ],
            },
            &[
                b'l', b'6', b':', 127, 0, 0, 1, 26, 133, b'6', b':', 127, 0, 0, 2, 4, 210, b'e',
            ],
        );
        // one v6
        encode_decode(
            &Wrapper {
                values: vec![(
                    Ipv6Addr::new(
                        0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0001,
                    ),
                    6789,
                )
                    .into()],
            },
            &[
                b'l', b'1', b'8', b':', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 26, 133,
                b'e',
            ],
        );
        // two v6
        encode_decode(
            &Wrapper {
                values: vec![
                    (
                        Ipv6Addr::new(
                            0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0001,
                        ),
                        6789,
                    )
                        .into(),
                    (
                        Ipv6Addr::new(
                            0x2001, 0x0db8, 0x85a3, 0x0000, 0x0000, 0x8a2e, 0x0370, 0x7334,
                        ),
                        1234,
                    )
                        .into(),
                ],
            },
            &[
                b'l', b'1', b'8', b':', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 26, 133,
                b'1', b'8', b':', 0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a,
                0x2e, 0x03, 0x70, 0x73, 0x34, 4, 210, b'e',
            ],
        );
        // hybrid (v4 + v6)
        encode_decode(
            &Wrapper {
                values: vec![
                    (Ipv4Addr::new(127, 0, 0, 1), 6789).into(),
                    (
                        Ipv6Addr::new(
                            0x2001, 0x0db8, 0x85a3, 0x0000, 0x0000, 0x8a2e, 0x0370, 0x7334,
                        ),
                        1234,
                    )
                        .into(),
                ],
            },
            &[
                b'l', b'6', b':', 127, 0, 0, 1, 26, 133, b'1', b'8', b':', 0x20, 0x01, 0x0d, 0xb8,
                0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34, 4, 210,
                b'e',
            ],
        );
    }

    #[test]
    fn encode_decode_nodes_v4() {
        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        struct Wrapper {
            #[serde(with = "super::nodes_v4")]
            nodes: Vec<NodeHandle>,
        }

        encode_decode(&Wrapper { nodes: Vec::new() }, b"0:");
        encode_decode(
            &Wrapper {
                nodes: vec![NodeHandle {
                    id: NodeId::from(*b"0123456789abcdefghij"),
                    addr: (Ipv4Addr::new(127, 0, 0, 1), 6789).into(),
                }],
            },
            &[
                b'2', b'6', b':', b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a',
                b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j', 127, 0, 0, 1, 26, 133,
            ],
        );
        encode_decode(
            &Wrapper {
                nodes: vec![
                    NodeHandle {
                        id: NodeId::from(*b"0123456789abcdefghij"),
                        addr: (Ipv4Addr::new(127, 0, 0, 1), 6789).into(),
                    },
                    NodeHandle {
                        id: NodeId::from(*b"klmnopqrstuvwxyz0123"),
                        addr: (Ipv4Addr::new(127, 0, 0, 2), 1234).into(),
                    },
                ],
            },
            &[
                b'5', b'2', b':', b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a',
                b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j', 127, 0, 0, 1, 26, 133, b'k',
                b'l', b'm', b'n', b'o', b'p', b'q', b'r', b's', b't', b'u', b'v', b'w', b'x', b'y',
                b'z', b'0', b'1', b'2', b'3', 127, 0, 0, 2, 4, 210,
            ],
        );
    }

    #[test]
    fn encode_decode_nodes_v6() {
        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        struct Wrapper {
            #[serde(with = "super::nodes_v6")]
            nodes: Vec<NodeHandle>,
        }

        encode_decode(&Wrapper { nodes: Vec::new() }, b"0:");
        encode_decode(
            &Wrapper {
                nodes: vec![NodeHandle {
                    id: NodeId::from(*b"0123456789abcdefghij"),
                    addr: (
                        Ipv6Addr::new(
                            0x2001, 0x0db8, 0x85a3, 0x0000, 0x0000, 0x8a2e, 0x0370, 0x7334,
                        ),
                        6789,
                    )
                        .into(),
                }],
            },
            &[
                b'3', b'8', b':', b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a',
                b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j', 0x20, 0x01, 0x0d, 0xb8, 0x85,
                0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34, 26, 133,
            ],
        );
    }

    #[test]
    fn attempt_to_encode_v4_nodes_as_v6() {
        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        struct Wrapper {
            #[serde(with = "super::nodes_v6")]
            nodes: Vec<NodeHandle>,
        }

        let value = Wrapper {
            nodes: vec![NodeHandle {
                id: NodeId::from(*b"0123456789abcdefghij"),
                addr: (Ipv4Addr::new(127, 0, 0, 1), 1234).into(),
            }],
        };

        assert!(serde_bencode::to_bytes(&value).is_err());
    }

    #[test]
    fn attempt_to_encode_v6_nodes_as_v4() {
        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        struct Wrapper {
            #[serde(with = "super::nodes_v4")]
            nodes: Vec<NodeHandle>,
        }

        let value = Wrapper {
            nodes: vec![NodeHandle {
                id: NodeId::from(*b"0123456789abcdefghij"),
                addr: (
                    Ipv6Addr::new(
                        0x2001, 0x0db8, 0x85a3, 0x0000, 0x0000, 0x8a2e, 0x0370, 0x7334,
                    ),
                    1234,
                )
                    .into(),
            }],
        };

        assert!(serde_bencode::to_bytes(&value).is_err());
    }

    fn encode_decode<'de, T>(value: &T, expected_encoded: &'de [u8])
    where
        T: Serialize + Deserialize<'de> + Eq + Debug,
    {
        let actual_encoded = serde_bencode::to_bytes(value).unwrap();
        assert_eq!(actual_encoded, expected_encoded);

        let actual_decoded: T = serde_bencode::from_bytes(expected_encoded).unwrap();
        assert_eq!(actual_decoded, *value);
    }
}
