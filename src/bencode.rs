use serde::{Deserialize, Serialize};

pub type Error = serde_bencode::Error;

#[inline]
pub(crate) fn encode<T>(value: &T) -> Result<Vec<u8>, Error>
where
    T: Serialize,
{
    serde_bencode::to_bytes(value)
}

#[inline]
pub(crate) fn decode<'de, T>(bytes: &'de [u8]) -> Result<T, Error>
where
    T: Deserialize<'de>,
{
    serde_bencode::from_bytes(bytes)
}
