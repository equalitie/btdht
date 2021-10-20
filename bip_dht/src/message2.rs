use crate::id::{InfoHash, NodeId};
use serde::{
    de::{Deserializer, Error as _, IgnoredAny, SeqAccess, Visitor},
    ser::{SerializeSeq, Serializer},
    Deserialize, Serialize,
};
use std::{convert::TryFrom, fmt};
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

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Response {}

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
                    error: None,
                }
            }
            MessageBody::Response(_) => todo!(),
            MessageBody::Error(error) => Self {
                transaction_id: msg.transaction_id,
                message_type: MessageType::Error,
                request_type: None,
                request: None,
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
            MessageType::Response => todo!(),
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

    // DEBUG:
    // #[test]
    // fn foo() {
    //     let encoded = "d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";
    //     let raw: RawMessage = serde_bencode::from_str(encoded).unwrap();
    //     println!("{:?}", raw);
    // }

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
    fn serialize_error() {
        let encoded = "d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee";
        let decoded = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Error(Error {
                code: 201,
                message: "A Generic Error Ocurred".to_owned(),
            }),
        };

        assert_serialize_deserialize(encoded, &decoded)
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
