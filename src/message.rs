use crate::{
    compact,
    info_hash::{InfoHash, NodeId},
    node::NodeHandle,
};
use serde::{
    Deserialize, Serialize,
    de::{self, Deserializer, Error as _, IgnoredAny, SeqAccess, Visitor},
    ser::{SerializeSeq, Serializer},
};
use std::{fmt, net::SocketAddr};

pub type TransactionId = Vec<u8>;

#[derive(Clone, Eq, PartialEq)]
pub struct Message {
    pub transaction_id: TransactionId,
    pub body: MessageBody,
}

impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        RawMessage::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        dbg!(RawMessage::deserialize(deserializer))?
            .try_into()
            .map_err(RawMessageError::into_de)
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Message")
            .field("transaction_id", &HexFmt(&self.transaction_id))
            .field("body", &self.body)
            .finish()
    }
}

struct HexFmt<'a>(&'a [u8]);

impl fmt::Debug for HexFmt<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum MessageBody {
    Request(Request),
    Response(Response),
    Error(Error),
}

impl fmt::Debug for MessageBody {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Request(Request::Ping(r)) => r.fmt(f),
            Self::Request(Request::FindNode(r)) => r.fmt(f),
            Self::Request(Request::GetPeers(r)) => r.fmt(f),
            Self::Request(Request::AnnouncePeer(r)) => r.fmt(f),
            Self::Response(r) => r.fmt(f),
            Self::Error(e) => e.fmt(f),
        }
    }
}

// TODO: unrecognized requests which contain either an 'info_hash' or 'target' arguments should be
// interpreted as 'find_node' as per Mainline DHT extensions.
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Request {
    // IMPORTANT: Due to how `untagged` enum representation works, the order the variants are listed
    // here matters: variants whose fields are superset of some other variants (taking into account
    // optional fields) must be listed above the other variant.
    FindNode(FindNodeRequest),
    AnnouncePeer(AnnouncePeerRequest),
    GetPeers(GetPeersRequest),
    Ping(PingRequest),
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct PingRequest {
    pub id: NodeId,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct FindNodeRequest {
    pub id: NodeId,
    pub target: NodeId,

    #[serde(with = "want", default, skip_serializing_if = "Option::is_none")]
    pub want: Option<Want>,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct GetPeersRequest {
    pub id: NodeId,
    pub info_hash: InfoHash,

    #[serde(with = "want", default, skip_serializing_if = "Option::is_none")]
    pub want: Option<Want>,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct AnnouncePeerRequest {
    pub id: NodeId,
    pub info_hash: InfoHash,
    #[serde(with = "port", flatten)]
    pub port: Option<u16>,
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Want {
    // The peer wants only ipv4 contacts
    V4,
    // The peer wants only ipv6 contacts
    V6,
    // The peer wants both ipv4 and ipv6 contacts
    Both,
}

mod want {
    use super::Want;
    use serde::{
        Deserializer, Serializer,
        de::{SeqAccess, Visitor},
        ser::SerializeSeq,
    };
    use serde_bytes::Bytes;
    use std::fmt;

    pub(super) fn serialize<S: Serializer>(want: &Option<Want>, s: S) -> Result<S::Ok, S::Error> {
        let len = match want {
            None => 0,
            Some(Want::V4 | Want::V6) => 1,
            Some(Want::Both) => 2,
        };

        let mut seq = s.serialize_seq(Some(len))?;

        if matches!(want, Some(Want::V4 | Want::Both)) {
            seq.serialize_element(Bytes::new(b"n4"))?;
        }

        if matches!(want, Some(Want::V6 | Want::Both)) {
            seq.serialize_element(Bytes::new(b"n6"))?;
        }

        seq.end()
    }

    pub(super) fn deserialize<'de, D>(d: D) -> Result<Option<Want>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WantVisitor;

        impl<'de> Visitor<'de> for WantVisitor {
            type Value = Option<Want>;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a list of strings")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut value = None;

                while let Some(s) = seq.next_element::<String>()? {
                    value = match (value, s.as_str().trim()) {
                        (None, "n4" | "N4") => Some(Want::V4),
                        (None, "n6" | "N6") => Some(Want::V6),
                        (Some(Want::V4), "n6" | "N6") => Some(Want::Both),
                        (Some(Want::V6), "n4" | "N4") => Some(Want::Both),
                        (_, _) => value,
                    }
                }

                Ok(value)
            }
        }

        d.deserialize_seq(WantVisitor)
    }
}

mod port {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Wrapper {
        // Note that the documentation (http://bittorrent.org/beps/bep_0005.html) doesn't say that
        // `port` is optional, and indeed many bittorrent clients will reply with error 203 if it's
        // not present **even when `implied_port` is set to 1**.
        port: u16,

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
            port: port.unwrap_or(0),
        }
        .serialize(s)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u16>, D::Error> {
        let wrapper = Wrapper::deserialize(d)?;

        if wrapper.implied_port {
            Ok(None)
        } else {
            Ok(Some(wrapper.port))
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
pub struct Response {
    pub id: NodeId,

    // Only present in responses to GetPeers
    #[serde(
        with = "compact::values",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub values: Vec<SocketAddr>,

    #[serde(
        rename = "nodes",
        with = "compact::nodes_v4",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub nodes_v4: Vec<NodeHandle>,

    #[serde(
        rename = "nodes6",
        with = "compact::nodes_v6",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub nodes_v6: Vec<NodeHandle>,

    // Only present in responses to GetPeers
    #[serde(with = "serde_bytes", default, skip_serializing_if = "Option::is_none")]
    pub token: Option<Vec<u8>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Error {
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

// Intermediate type to help serialize/deserialize `Message`, to work around
// https://github.com/serde-rs/serde/issues/2881.
#[derive(Serialize, Deserialize, Debug)]
struct RawMessage {
    #[serde(rename = "t", with = "serde_bytes")]
    transaction_id: TransactionId,

    #[serde(rename = "y")]
    message_type: RawMessageType,

    #[serde(rename = "q")]
    request_type: Option<RawRequestType>,

    #[serde(rename = "a")]
    request: Option<Request>,

    #[serde(rename = "r")]
    response: Option<Response>,

    #[serde(rename = "e")]
    error: Option<Error>,
}

impl<'a> From<&'a Message> for RawMessage {
    fn from(value: &'a Message) -> Self {
        match &value.body {
            MessageBody::Request(request) => Self {
                transaction_id: value.transaction_id.clone(),
                message_type: RawMessageType::Request,
                request_type: Some(RawRequestType::from(request)),
                request: Some(request.clone()),
                response: None,
                error: None,
            },
            MessageBody::Response(response) => Self {
                transaction_id: value.transaction_id.clone(),
                message_type: RawMessageType::Response,
                request_type: None,
                request: None,
                response: Some(response.clone()),
                error: None,
            },
            MessageBody::Error(error) => Self {
                transaction_id: value.transaction_id.clone(),
                message_type: RawMessageType::Error,
                request_type: None,
                request: None,
                response: None,
                error: Some(error.clone()),
            },
        }
    }
}

impl TryFrom<RawMessage> for Message {
    type Error = RawMessageError;

    fn try_from(value: RawMessage) -> Result<Self, Self::Error> {
        let body = match value.message_type {
            RawMessageType::Request => {
                let request_type = value
                    .request_type
                    .ok_or(RawMessageError::MissingRequestType)?;
                let request = value.request.ok_or(RawMessageError::MissingRequestArgs)?;

                match (request_type, &request) {
                    (RawRequestType::Ping, Request::Ping(_))
                    | (RawRequestType::FindNode, Request::FindNode(_))
                    | (RawRequestType::GetPeers, Request::GetPeers(_))
                    | (RawRequestType::AnnouncePeer, Request::AnnouncePeer(_)) => {
                        MessageBody::Request(request)
                    }
                    _ => Err(RawMessageError::InvalidRequest)?,
                }
            }
            RawMessageType::Response => {
                MessageBody::Response(value.response.ok_or(RawMessageError::MissingResponse)?)
            }
            RawMessageType::Error => {
                MessageBody::Error(value.error.ok_or(RawMessageError::MissingError)?)
            }
        };

        Ok(Self {
            transaction_id: value.transaction_id,
            body,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum RawMessageType {
    #[serde(rename = "q")]
    Request,
    #[serde(rename = "r")]
    Response,
    #[serde(rename = "e")]
    Error,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum RawRequestType {
    Ping,
    FindNode,
    GetPeers,
    AnnouncePeer,
}

impl<'a> From<&'a Request> for RawRequestType {
    fn from(value: &'a Request) -> Self {
        match value {
            Request::Ping(_) => Self::Ping,
            Request::FindNode(_) => Self::FindNode,
            Request::GetPeers(_) => Self::GetPeers,
            Request::AnnouncePeer(_) => Self::AnnouncePeer,
        }
    }
}

enum RawMessageError {
    MissingRequestType,
    MissingRequestArgs,
    MissingResponse,
    MissingError,
    InvalidRequest,
}

impl RawMessageError {
    fn into_de<E: de::Error>(self) -> E {
        match self {
            Self::MissingRequestType => E::missing_field("q"),
            Self::MissingRequestArgs => E::missing_field("a"),
            Self::MissingResponse => E::missing_field("r"),
            Self::MissingError => E::missing_field("e"),
            Self::InvalidRequest => E::custom("invalid query arguments"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bencode;

    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

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
                want: None,
            })),
        };

        assert_serialize_deserialize(encoded, &decoded)
    }

    #[test]
    fn serialize_find_node_request_with_want() {
        let encoded = "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz1234564:wantl2:n42:n6ee1:q9:find_node1:t2:aa1:y1:qe";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::FindNode(FindNodeRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
                target: NodeId::from(*b"mnopqrstuvwxyz123456"),
                want: Some(Want::Both),
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
                want: None,
            })),
        };

        assert_serialize_deserialize(encoded, &decoded)
    }

    #[test]
    fn serialize_get_peers_request_with_want() {
        let encoded = "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:wantl2:n4ee1:q9:get_peers1:t2:aa1:y1:qe";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::GetPeers(GetPeersRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
                info_hash: InfoHash::from(*b"mnopqrstuvwxyz123456"),
                want: Some(Want::V4),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded)
    }

    #[test]
    fn serialize_announce_peer_request_with_implied_port() {
        let encoded = "d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti0e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe";
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
    fn serialize_other_response_none() {
        let encoded = "d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response {
                id: NodeId::from(*b"mnopqrstuvwxyz123456"),
                values: vec![],
                nodes_v4: vec![],
                nodes_v6: vec![],
                token: None,
            }),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_other_response_v4() {
        let encoded =
            "d1:rd2:id20:0123456789abcdefghij5:nodes26:mnopqrstuvwxyz012345axje.ue1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response {
                id: NodeId::from(*b"0123456789abcdefghij"),
                values: vec![],
                nodes_v4: vec![NodeHandle {
                    id: NodeId::from(*b"mnopqrstuvwxyz012345"),
                    addr: (Ipv4Addr::new(97, 120, 106, 101), 11893).into(),
                }],
                nodes_v6: vec![],
                token: None,
            }),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_other_response_v6() {
        let encoded = "d1:rd2:id20:0123456789abcdefghij6:nodes638:mnopqrstuvwxyz012345abcdefghijklmnop.ue1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response {
                id: NodeId::from(*b"0123456789abcdefghij"),
                values: vec![],
                nodes_v4: vec![],
                nodes_v6: vec![NodeHandle {
                    id: NodeId::from(*b"mnopqrstuvwxyz012345"),
                    addr: (
                        Ipv6Addr::new(
                            0x6162, 0x6364, 0x6566, 0x6768, 0x696a, 0x6b6c, 0x6d6e, 0x6f70,
                        ),
                        11893,
                    )
                        .into(),
                }],
                token: None,
            }),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_other_response_both() {
        let encoded = "d1:rd2:id20:0123456789abcdefghij5:nodes26:mnopqrstuvwxyz012345axje.u6:nodes638:6789abcdefghijklmnopabcdefghijklmnop.ue1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response {
                id: NodeId::from(*b"0123456789abcdefghij"),
                values: vec![],
                nodes_v4: vec![NodeHandle {
                    id: NodeId::from(*b"mnopqrstuvwxyz012345"),
                    addr: (Ipv4Addr::new(97, 120, 106, 101), 11893).into(),
                }],
                nodes_v6: vec![NodeHandle {
                    id: NodeId::from(*b"6789abcdefghijklmnop"),
                    addr: (
                        Ipv6Addr::new(
                            0x6162, 0x6364, 0x6566, 0x6768, 0x696a, 0x6b6c, 0x6d6e, 0x6f70,
                        ),
                        11893,
                    )
                        .into(),
                }],
                token: None,
            }),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_get_peers_response_with_values() {
        let encoded = "d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response {
                id: NodeId::from(*b"abcdefghij0123456789"),
                values: vec![
                    (Ipv4Addr::new(97, 120, 106, 101), 11893).into(),
                    (Ipv4Addr::new(105, 100, 104, 116), 28269).into(),
                ],
                nodes_v4: vec![],
                nodes_v6: vec![],
                token: Some(b"aoeusnth".to_vec()),
            }),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_get_peers_response_with_nodes_v4() {
        let encoded = "d1:rd2:id20:abcdefghij01234567895:nodes52:mnopqrstuvwxyz123456axje.u789abcdefghijklmnopqidhtnm5:token8:aoeusnthe1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response {
                id: NodeId::from(*b"abcdefghij0123456789"),
                values: vec![],
                nodes_v4: vec![
                    NodeHandle {
                        id: NodeId::from(*b"mnopqrstuvwxyz123456"),
                        addr: (Ipv4Addr::new(97, 120, 106, 101), 11893).into(),
                    },
                    NodeHandle {
                        id: NodeId::from(*b"789abcdefghijklmnopq"),
                        addr: (Ipv4Addr::new(105, 100, 104, 116), 28269).into(),
                    },
                ],
                nodes_v6: vec![],
                token: Some(b"aoeusnth".to_vec()),
            }),
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
        assert_eq!(
            str::from_utf8(&bencode::encode(decoded).expect("encode failed"))
                .expect("invalid utf8"),
            encoded
        );

        assert_eq!(
            bencode::decode::<Message>(encoded.as_bytes()).expect("decode failed"),
            *decoded
        );
    }
}
