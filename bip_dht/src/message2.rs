use crate::id::{InfoHash, NodeId};
use serde::{de::Deserializer, Deserialize, Serialize};
use std::convert::TryFrom;
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

#[derive(Debug, Serialize, Deserialize)]
struct RawMessage {
    #[serde(rename = "t", with = "serde_bytes")]
    transaction_id: Vec<u8>,

    #[serde(
        rename = "v",
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "serde_bytes"
    )]
    version: Vec<u8>,

    #[serde(rename = "y")]
    message_type: MessageType,

    #[serde(rename = "q", default, skip_serializing_if = "Option::is_none")]
    request_type: Option<RequestType>,

    #[serde(rename = "a", default, skip_serializing_if = "Option::is_none")]
    request: Option<RawRequest>,
}

impl From<Message> for RawMessage {
    fn from(msg: Message) -> Self {
        match msg.body {
            MessageBody::Request(request) => {
                let (request_type, request) = request.into_raw();

                Self {
                    transaction_id: msg.transaction_id,
                    version: Vec::new(),
                    message_type: MessageType::Request,
                    request_type: Some(request_type),
                    request: Some(request),
                }
            }
            MessageBody::Response(_) => todo!(),
            MessageBody::Error(_) => todo!(),
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
            MessageType::Error => todo!(),
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
        let expected = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::Ping(PingRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
            })),
        };

        test_serialize_deserialize(encoded, &expected)
    }

    #[test]
    fn serialize_find_node_request() {
        let encoded = "d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";
        let expected = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::FindNode(FindNodeRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
                target: NodeId::from(*b"mnopqrstuvwxyz123456"),
            })),
        };

        test_serialize_deserialize(encoded, &expected)
    }

    #[test]
    fn serialize_get_peers_request() {
        let encoded = "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe";
        let expected = Message {
            transaction_id: b"aa".to_vec(),
            body: MessageBody::Request(Request::GetPeers(GetPeersRequest {
                id: NodeId::from(*b"abcdefghij0123456789"),
                info_hash: InfoHash::from(*b"mnopqrstuvwxyz123456"),
            })),
        };

        test_serialize_deserialize(encoded, &expected)
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

        test_serialize_deserialize(encoded, &decoded);
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

        test_serialize_deserialize(encoded, &decoded);
    }

    // #[test]
    // fn serialize_error() {
    //     let encoded = "d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee";
    //     let expected = Message {
    //         transaction_id: b"aa",
    //         body: MessageBody::Error(Error {
    //             code: 201,
    //             message: "A Generic Error Ocurred",
    //         }),
    //     };

    //     test_serialize_deserialize(encoded, &expected)
    // }

    #[track_caller]
    fn test_serialize_deserialize(encoded: &str, decoded: &Message) {
        assert_eq!(serde_bencode::to_string(decoded).unwrap(), encoded);
        assert_eq!(
            serde_bencode::from_str::<Message>(encoded).unwrap(),
            *decoded
        );
    }
}
