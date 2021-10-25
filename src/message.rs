use crate::{
    compact,
    id::{InfoHash, NodeId},
    routing::node::NodeInfo,
};
use serde::{
    de::{Deserializer, Error as _, IgnoredAny, SeqAccess, Visitor},
    ser::{SerializeSeq, Serializer},
    Deserialize, Serialize,
};
use std::{fmt, net::SocketAddrV4};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct Message {
    #[serde(rename = "t", with = "serde_bytes")]
    pub transaction_id: Vec<u8>,
    #[serde(flatten)]
    pub body: MessageBody,
}

impl Message {
    /// Decode the message from bencode.
    pub fn decode(input: &[u8]) -> Result<Self, serde_bencode::Error> {
        serde_bencode::from_bytes(input)
    }

    /// Encode the message into bencode.
    pub fn encode(&self) -> Vec<u8> {
        // `expect` should be fine here as there should be no reason why a serialization into a
        // `Vec` would fail unless we have a bug somewhere.
        serde_bencode::to_bytes(self).expect("failed to serialize message")
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "y")]
pub(crate) enum MessageBody {
    #[serde(rename = "q")]
    Request(Request),
    #[serde(rename = "r", with = "unflatten::response")]
    Response(Response),
    #[serde(rename = "e", with = "unflatten::error")]
    Error(Error),
}

// Opposite of `serde(flatten)` - artificially add one level of nesting to a field.
mod unflatten {
    macro_rules! impl_unflatten {
        ($mod:ident, $field:literal) => {
            pub(crate) mod $mod {
                use serde::{Deserialize, Deserializer, Serialize, Serializer};

                #[derive(Serialize, Deserialize)]
                struct Wrapper<T> {
                    #[serde(rename = $field)]
                    field: T,
                }

                pub(crate) fn serialize<T: Serialize, S: Serializer>(
                    value: &T,
                    s: S,
                ) -> Result<S::Ok, S::Error> {
                    Wrapper { field: value }.serialize(s)
                }

                pub(crate) fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
                    d: D,
                ) -> Result<T, D::Error> {
                    let wrapper = Wrapper::deserialize(d)?;
                    Ok(wrapper.field)
                }
            }
        };
    }

    impl_unflatten!(response, "r");
    impl_unflatten!(error, "e");
}

// TODO: unrecognized requests which contain either an 'info_hash' or 'target' arguments should be
// interpreted as 'find_node' as per Mainline DHT extensions.
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "q", content = "a")]
#[serde(rename_all = "snake_case")]
pub(crate) enum Request {
    Ping(PingRequest),
    FindNode(FindNodeRequest),
    GetPeers(GetPeersRequest),
    AnnouncePeer(AnnouncePeerRequest),
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct PingRequest {
    pub id: NodeId,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct FindNodeRequest {
    pub id: NodeId,
    pub target: NodeId,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct GetPeersRequest {
    pub id: NodeId,
    pub info_hash: InfoHash,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct AnnouncePeerRequest {
    pub id: NodeId,
    pub info_hash: InfoHash,
    #[serde(with = "port", flatten)]
    pub port: Option<u16>,
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

mod port {
    use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Wrapper {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<u16>,

        #[serde(
            default,
            skip_serializing_if = "is_false",
            deserialize_with = "deserialize_bool"
        )]
        implied_port: bool,
    }

    pub(crate) fn serialize<S: Serializer>(port: &Option<u16>, s: S) -> Result<S::Ok, S::Error> {
        Wrapper {
            implied_port: port.is_none(),
            port: *port,
        }
        .serialize(s)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u16>, D::Error> {
        let wrapper = Wrapper::deserialize(d)?;

        if wrapper.implied_port {
            Ok(None)
        } else if wrapper.port.is_some() {
            Ok(wrapper.port)
        } else {
            Err(D::Error::missing_field("port"))
        }
    }

    fn is_false(b: &bool) -> bool {
        !*b
    }

    fn deserialize_bool<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
        let num = u8::deserialize(d)?;
        Ok(num > 0)
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Response {
    // NOTE: the order these variants are listed in is important to make sure they deserialize
    // properly because we use `untagged` enum for this.
    GetPeers(GetPeersResponse),
    FindNode(FindNodeResponse),
    // This is a reponse to either `ping` or `announce_peer`. They are not distinguishable just by
    // looking at the message itself, so that's why we put them into a single variant. To tell which
    // response this is, one has to look up the transaction created when sending the corresponding
    // request.
    Ack(AckResponse),
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct AckResponse {
    pub id: NodeId,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct FindNodeResponse {
    pub id: NodeId,

    #[serde(with = "compact")]
    pub nodes: Vec<NodeInfo>,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct GetPeersResponse {
    pub id: NodeId,

    #[serde(with = "compact::vec", default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<SocketAddrV4>,

    #[serde(with = "compact", default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<NodeInfo>,

    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Error {
    pub code: u8,
    pub message: String,
}

// Using custom Serialize/Deserialize impls because the format is too weird.
impl Serialize for Error {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(2))?;
        seq.serialize_element(&self.code)?;
        seq.serialize_element(&self.message)?;
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Error {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct ErrorVisitor;

        impl<'de> Visitor<'de> for ErrorVisitor {
            type Value = Error;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a list of two elements: an integer and a string")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let code: u8 = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let message = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::invalid_length(1, &self))?;

                // Make sure the list is consumed to the end.
                if seq.next_element::<IgnoredAny>()?.is_some() {
                    return Err(A::Error::invalid_length(3, &self));
                }

                Ok(Error { code, message })
            }
        }

        d.deserialize_seq(ErrorVisitor)
    }
}

pub mod error_code {
    // some of these codes are not used in this crate but we still list them here for completeness.
    #![allow(unused)]

    pub const GENERIC_ERROR: u8 = 201;
    pub const SERVER_ERROR: u8 = 202;
    pub const PROTOCOL_ERROR: u8 = 203;
    pub const METHOD_UNKNOWN: u8 = 204;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn serialize_ping_request() {
        let encoded = "d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::Ping(PingRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded)
    }

    #[test]
    fn serialize_find_node_request() {
        let encoded = "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::FindNode(FindNodeRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
                target: NodeId::from(*b"mnopqrstuvwxyz123456"),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded)
    }

    #[test]
    fn serialize_get_peers_request() {
        let encoded = "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::GetPeers(GetPeersRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
                info_hash: InfoHash::from(*b"mnopqrstuvwxyz123456"),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded)
    }

    #[test]
    fn serialize_announce_peer_request_with_implied_port() {
        let encoded = "d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234565:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::AnnouncePeer(AnnouncePeerRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
                port: None,
                info_hash: InfoHash::from(*b"mnopqrstuvwxyz123456"),
                token: b"aoeusnth".to_vec(),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_announce_peer_request_with_explicit_port() {
        let encoded = "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::AnnouncePeer(AnnouncePeerRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
                port: Some(6881),
                info_hash: InfoHash::from(*b"mnopqrstuvwxyz123456"),
                token: b"aoeusnth".to_vec(),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_ack_response() {
        let encoded = "d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response::Ack(AckResponse {
                id: NodeId::from(*b"mnopqrstuvwxyz123456"),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_find_node_response() {
        let encoded =
            "d1:rd2:id20:0123456789abcdefghij5:nodes26:mnopqrstuvwxyz123456axje.ue1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response::FindNode(FindNodeResponse {
                id: NodeId::from(*b"0123456789abcdefghij"),
                nodes: vec![NodeInfo {
                    id: NodeId::from(*b"mnopqrstuvwxyz123456"),
                    addr: (Ipv4Addr::new(97, 120, 106, 101), 11893).into(),
                }],
            })),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_get_peers_response_with_values() {
        let encoded = "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response::GetPeers(GetPeersResponse {
                id: NodeId::from(*b"abcdefghij0123456789"),
                values: vec![
                    SocketAddrV4::new(Ipv4Addr::new(97, 120, 106, 101), 11893),
                    SocketAddrV4::new(Ipv4Addr::new(105, 100, 104, 116), 28269),
                ],
                nodes: vec![],
                token: b"aoeusnth".to_vec(),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_get_peers_response_with_nodes() {
        let encoded =
            "d1:rd2:id20:abcdefghij01234567895:nodes52:mnopqrstuvwxyz123456axje.u789abcdefghijklmnopqidhtnm5:token8:aoeusnthe1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response::GetPeers(GetPeersResponse {
                id: NodeId::from(*b"abcdefghij0123456789"),
                values: vec![],
                nodes: vec![
                    NodeInfo {
                        id: NodeId::from(*b"mnopqrstuvwxyz123456"),
                        addr: (Ipv4Addr::new(97, 120, 106, 101), 11893).into(),
                    },
                    NodeInfo {
                        id: NodeId::from(*b"789abcdefghijklmnopq"),
                        addr: (Ipv4Addr::new(105, 100, 104, 116), 28269).into(),
                    },
                ],
                token: b"aoeusnth".to_vec(),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_error() {
        let encoded = "d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Error(Error {
                code: error_code::GENERIC_ERROR,
                message: "A Generic Error Ocurred".to_owned(),
            }),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[track_caller]
    fn assert_serialize_deserialize(encoded: &str, decoded: &Message) {
        assert_eq!(serde_bencode::to_string(decoded).unwrap(), encoded);
        assert_eq!(
            serde_bencode::from_str::<Message>(encoded).unwrap(),
            *decoded
        );
    }
}
