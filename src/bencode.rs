use serde::{Deserialize, Serialize};

#[inline]
pub(crate) fn encode<T>(value: &T) -> Result<Vec<u8>, serde_bencode::Error>
where
    T: Serialize,
{
    serde_bencode::to_bytes(value)
}

#[inline]
pub(crate) fn decode<'de, T>(bytes: &'de [u8]) -> Result<T, serde_bencode::Error>
where
    T: Deserialize<'de>,
{
    serde_bencode::from_bytes(bytes)
}
