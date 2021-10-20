use crate::{
    compact,
    id::{InfoHash, NodeId},
};
use serde::{
    de::{Deserializer, Error as _, IgnoredAny, SeqAccess, Visitor},
    ser::{SerializeSeq, Serializer},
    Deserialize, Serialize,
};
use std::{convert::TryFrom, fmt, net::SocketAddrV4};
use thiserror::Error;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(try_from = "RawMessage", into = "RawMessage")]
pub struct Message {
    transaction_id: Vec<u8>,
    body: MessageBody,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum MessageBody {
    Request(Request),
    Response(Response),
    Error(Error),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Request {
    Ping(PingRequest),
    FindNode(FindNodeRequest),
    GetPeers(GetPeersRequest),
    AnnouncePeer(AnnouncePeerRequest),
}

impl Request {
    fn into_raw(self) -> (RequestType, RawRequest) {
        match self {
            Request::Ping(request) => (
                RequestType::Ping,
                RawRequest {
                    id: request.id,
                    target: None,
                    info_hash: None,
                    implied_port: false,
                    port: None,
                    token: None,
                },
            ),
            Request::FindNode(request) => (
                RequestType::FindNode,
                RawRequest {
                    id: request.id,
                    target: Some(request.target),
                    info_hash: None,
                    implied_port: false,
                    port: None,
                    token: None,
                },
            ),
            Request::GetPeers(request) => (
                RequestType::GetPeers,
                RawRequest {
                    id: request.id,
                    target: None,
                    info_hash: Some(request.info_hash),
                    implied_port: false,
                    port: None,
                    token: None,
                },
            ),
            Request::AnnouncePeer(request) => (
                RequestType::AnnouncePeer,
                RawRequest {
                    id: request.id,
                    target: None,
                    info_hash: Some(request.info_hash),
                    implied_port: request.port.is_none(),
                    port: request.port,
                    token: Some(request.token),
                },
            ),
        }
    }

    fn from_raw(request_type: RequestType, raw: RawRequest) -> Result<Self, DecodeError> {
        let id = raw.id;

        match request_type {
            RequestType::Ping => Ok(Self::Ping(PingRequest { id })),
            RequestType::FindNode => {
                let target = raw.target.ok_or(DecodeError::MissingField("target"))?;
                Ok(Self::FindNode(FindNodeRequest { id, target }))
            }
            RequestType::GetPeers => {
                let info_hash = raw
                    .info_hash
                    .ok_or(DecodeError::MissingField("info_hash"))?;
                Ok(Self::GetPeers(GetPeersRequest { id, info_hash }))
            }
            RequestType::AnnouncePeer => {
                let info_hash = raw
                    .info_hash
                    .ok_or(DecodeError::MissingField("info_hash"))?;
                let implied_port = raw.implied_port;
                let port = if implied_port {
                    None
                } else {
                    Some(raw.port.ok_or(DecodeError::MissingField("port"))?)
                };
                let token = raw.token.ok_or(DecodeError::MissingField("token"))?;

                Ok(Self::AnnouncePeer(AnnouncePeerRequest {
                    id,
                    info_hash,
                    port,
                    token,
                }))
            }
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct PingRequest {
    pub id: NodeId,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct FindNodeRequest {
    pub id: NodeId,
    pub target: NodeId,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct GetPeersRequest {
    pub id: NodeId,
    pub info_hash: InfoHash,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct AnnouncePeerRequest {
    pub id: NodeId,
    pub info_hash: InfoHash,
    pub port: Option<u16>,
    pub token: Vec<u8>,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Response {
    FindNode(FindNodeResponse),
    GetPeers(GetPeersResponse),
    // NOTE: `Ping` must be last here to prevent other variants to be deserialized as `Ping` due to
    //       this enum being `untagged`.
    Ping(PingResponse),
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PingResponse {
    pub id: NodeId,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct FindNodeResponse {
    pub id: NodeId,

    #[serde(with = "compact")]
    pub nodes: Vec<NodeInfo>,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct GetPeersResponse {
    pub id: NodeId,

    #[serde(with = "compact", default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<SocketAddrV4>,

    #[serde(with = "compact", default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<NodeInfo>,

    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: SocketAddrV4,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Error {
    code: u32,
    message: String,
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
                let code = seq
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

// Intermediate format to help serializing/deserializing. This shouldn't be necessary because we
// should be able to achieve what we need via a combination of `flatten` and `tag` serde attributes,
// but it seems the `serde_bencode` crate doesn't handle those too well.
// TODO: fix the issues upstream, then do this properly.
#[derive(Debug, Serialize, Deserialize)]
struct RawMessage {
    #[serde(rename = "t", with = "serde_bytes")]
    transaction_id: Vec<u8>,

    #[serde(rename = "y")]
    message_type: MessageType,

    #[serde(rename = "q", default, skip_serializing_if = "Option::is_none")]
    request_type: Option<RequestType>,

    #[serde(rename = "a", default, skip_serializing_if = "Option::is_none")]
    request: Option<RawRequest>,

    #[serde(rename = "r", default, skip_serializing_if = "Option::is_none")]
    response: Option<Response>,

    #[serde(rename = "e", default, skip_serializing_if = "Option::is_none")]
    error: Option<Error>,
}

impl From<Message> for RawMessage {
    fn from(msg: Message) -> Self {
        match msg.body {
            MessageBody::Request(request) => {
                let (request_type, request) = request.into_raw();

                Self {
                    transaction_id: msg.transaction_id,
                    message_type: MessageType::Request,
                    request_type: Some(request_type),
                    request: Some(request),
                    response: None,
                    error: None,
                }
            }
            MessageBody::Response(response) => Self {
                transaction_id: msg.transaction_id,
                message_type: MessageType::Response,
                request_type: None,
                request: None,
                response: Some(response),
                error: None,
            },
            MessageBody::Error(error) => Self {
                transaction_id: msg.transaction_id,
                message_type: MessageType::Error,
                request_type: None,
                request: None,
                response: None,
                error: Some(error),
            },
        }
    }
}

impl TryFrom<RawMessage> for Message {
    type Error = DecodeError;

    fn try_from(raw: RawMessage) -> Result<Self, Self::Error> {
        match raw.message_type {
            MessageType::Request => {
                let request_type = raw.request_type.ok_or(DecodeError::MissingField("q"))?;
                let request = raw.request.ok_or(DecodeError::MissingField("a"))?;
                let request = Request::from_raw(request_type, request)?;

                Ok(Self {
                    transaction_id: raw.transaction_id,
                    body: MessageBody::Request(request),
                })
            }
            MessageType::Response => {
                let response = raw.response.ok_or(DecodeError::MissingField("r"))?;

                Ok(Self {
                    transaction_id: raw.transaction_id,
                    body: MessageBody::Response(response),
                })
            }
            MessageType::Error => {
                let error = raw.error.ok_or(DecodeError::MissingField("e"))?;

                Ok(Self {
                    transaction_id: raw.transaction_id,
                    body: MessageBody::Error(error),
                })
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum MessageType {
    #[serde(rename = "q")]
    Request,
    #[serde(rename = "r")]
    Response,
    #[serde(rename = "e")]
    Error,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RequestType {
    Ping,
    FindNode,
    GetPeers,
    AnnouncePeer,
}

#[derive(Debug, Serialize, Deserialize)]
struct RawRequest {
    id: NodeId,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    target: Option<NodeId>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    info_hash: Option<InfoHash>,

    #[serde(
        default,
        skip_serializing_if = "is_false",
        deserialize_with = "deserialize_bool"
    )]
    implied_port: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    port: Option<u16>,

    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    token: Option<Vec<u8>>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

fn deserialize_bool<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    let num = u8::deserialize(d)?;
    Ok(num > 0)
}

#[derive(Debug, Error)]
enum DecodeError {
    #[error("field `{0}` is missing")]
    MissingField(&'static str),
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
    fn serialize_ping_response() {
        let encoded = "d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response::Ping(PingResponse {
                id: NodeId::from(*b"mnopqrstuvwxyz123456"),
            })),
        };

        assert_serialize_deserialize(encoded, &decoded);
    }

    #[test]
    fn serialize_find_node_response() {
        let encoded = b"d1:rd2:id20:0123456789abcdefghij5:nodes26:\x6d\x6e\x6f\x70\x71\x72\x73\x74\x75\x76\x77\x78\x79\x7a\x31\x32\x33\x34\x35\x36\x7f\x00\x00\x01\x1a\xe1e1:t2:aa1:y1:re";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Response(Response::FindNode(FindNodeResponse {
                id: NodeId::from(*b"0123456789abcdefghij"),
                nodes: vec![NodeInfo {
                    id: NodeId::from(*b"mnopqrstuvwxyz123456"),
                    addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6881),
                }],
            })),
        };

        let actual_encoded = serde_bencode::to_bytes(&decoded).unwrap();
        assert_eq!(actual_encoded, encoded);

        let actual_decoded: Message = serde_bencode::from_bytes(&encoded[..]).unwrap();
        assert_eq!(actual_decoded, decoded);
    }

    #[test]
    fn serialize_get_peers_response_with_values() {
        let encoded = b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re";
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

        let actual_encoded = serde_bencode::to_bytes(&decoded).unwrap();
        // assert_eq!(actual_encoded, encoded);

        let actual_decoded: Message = serde_bencode::from_bytes(&encoded[..]).unwrap();
        assert_eq!(actual_decoded, decoded);
    }

    #[test]
    fn serialize_error() {
        let encoded = "d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Error(Error {
                code: 201,
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
