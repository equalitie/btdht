use bip_bencode::BencodeConvertError;
use thiserror::Error;

use crate::message::error::ErrorMessage;

#[derive(Debug, Error)]
pub enum DhtError {
    #[error("bencode")]
    Bencode(#[from] BencodeConvertError),
    #[error("node sent an invalid message with message code {code}")]
    InvalidMessage { code: String },
    #[error("node sent us an invalid request message with code {:?} and message {}", .msg.error_code(), .msg.error_message())]
    InvalidRequest { msg: ErrorMessage<'static> },
    #[error("node sent us an invalid response: {details}")]
    InvalidResponse { details: String },
    #[error("node sent us an unsolicited response")]
    UnsolicitedResponse,
}

pub type DhtResult<T, E = DhtError> = std::result::Result<T, E>;
