use rand::{
    distributions::{Distribution, Standard},
    Rng,
};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{
    convert::{TryFrom, TryInto},
    fmt,
    ops::BitXor,
};
use thiserror::Error;

/// Length of a SHA-1 hash.
pub const SHA_HASH_LEN: usize = 20;

/// SHA-1 hash wrapper type for performing operations on the hash.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ShaHash(#[serde(with = "byte_array")] [u8; SHA_HASH_LEN]);

impl ShaHash {
    /// Create a ShaHash by hashing the given bytes using SHA-1.
    pub fn sha1(bytes: &[u8]) -> Self {
        let hash = Sha1::digest(bytes);
        Self(hash.into())
    }

    /// Panics if index is out of bounds.
    pub fn flip_bit(self, index: usize) -> Self {
        let mut bytes = self.0;
        let (byte_index, bit_index) = (index / 8, index % 8);

        let actual_bit_index = 7 - bit_index;
        bytes[byte_index] ^= 1 << actual_bit_index;

        bytes.into()
    }

    /// Number of leading zero bits.
    pub fn leading_zeros(&self) -> u32 {
        let mut bits = 0;

        for byte in self.0 {
            bits += byte.leading_zeros();

            if byte != 0 {
                break;
            }
        }

        bits
    }
}

impl AsRef<[u8]> for ShaHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<ShaHash> for [u8; SHA_HASH_LEN] {
    fn from(hash: ShaHash) -> [u8; SHA_HASH_LEN] {
        hash.0
    }
}

impl From<[u8; SHA_HASH_LEN]> for ShaHash {
    fn from(hash: [u8; SHA_HASH_LEN]) -> ShaHash {
        Self(hash)
    }
}

#[derive(Debug, Error)]
#[error("invalid SHA-1 hash length")]
pub struct LengthError;

impl<'a> TryFrom<&'a [u8]> for ShaHash {
    type Error = LengthError;

    fn try_from(slice: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self(slice.try_into().map_err(|_| LengthError)?))
    }
}

impl BitXor for ShaHash {
    type Output = Self;

    fn bitxor(mut self, rhs: Self) -> Self {
        for (src, dst) in rhs.0.iter().zip(self.0.iter_mut()) {
            *dst ^= *src;
        }

        self
    }
}

impl Distribution<ShaHash> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> ShaHash {
        ShaHash(rng.gen())
    }
}

impl fmt::LowerHex for ShaHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{:02x}", b)?;
        }

        Ok(())
    }
}

impl fmt::Debug for ShaHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:x}", self)
    }
}

mod byte_array {
    use super::SHA_HASH_LEN;
    use serde::{
        de::{Deserialize, Deserializer, Error},
        ser::{Serialize, Serializer},
    };
    use serde_bytes::{ByteBuf, Bytes};
    use std::convert::TryInto;

    pub(super) fn serialize<S: Serializer>(
        bytes: &[u8; SHA_HASH_LEN],
        s: S,
    ) -> Result<S::Ok, S::Error> {
        Bytes::new(bytes.as_ref()).serialize(s)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<[u8; SHA_HASH_LEN], D::Error> {
        let buf = ByteBuf::deserialize(d)?;
        let buf = buf.into_vec();
        let len = buf.len();

        buf.try_into().map_err(|_| {
            let expected = format!("{}", SHA_HASH_LEN);
            D::Error::invalid_length(len, &expected.as_ref())
        })
    }
}

// ----------------------------------------------------------------------------//

/// Bittorrent `NodeId`.
pub type NodeId = ShaHash;

/// Bittorrent `InfoHash`.
pub type InfoHash = ShaHash;

/// Length of a `NodeId`.
pub const NODE_ID_LEN: usize = SHA_HASH_LEN;

/// Length of an `InfoHash`.
pub const INFO_HASH_LEN: usize = SHA_HASH_LEN;

// ----------------------------------------------------------------------------//

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_no_leading_zeroes() {
        let zero_bits = ShaHash::from([0u8; SHA_HASH_LEN]);
        let one_bits = ShaHash::from([255u8; SHA_HASH_LEN]);

        let xor_hash = zero_bits ^ one_bits;

        assert_eq!(xor_hash.leading_zeros(), 0)
    }

    #[test]
    fn positive_all_leading_zeroes() {
        let first_one_bits = ShaHash::from([255u8; SHA_HASH_LEN]);
        let second_one_bits = ShaHash::from([255u8; SHA_HASH_LEN]);

        let xor_hash = first_one_bits ^ second_one_bits;

        assert_eq!(xor_hash.leading_zeros() as usize, SHA_HASH_LEN * 8);
    }

    #[test]
    fn positive_one_leading_zero() {
        let zero_bits = ShaHash::from([0u8; SHA_HASH_LEN]);

        let mut bytes = [255u8; SHA_HASH_LEN];
        bytes[0] = 127;
        let mostly_one_bits = ShaHash::from(bytes);

        let xor_hash = zero_bits ^ mostly_one_bits;

        assert_eq!(xor_hash.leading_zeros(), 1);
    }

    #[test]
    fn positive_one_trailing_zero() {
        let zero_bits = ShaHash::from([0u8; SHA_HASH_LEN]);

        let mut bytes = [255u8; SHA_HASH_LEN];
        bytes[super::SHA_HASH_LEN - 1] = 254;
        let mostly_zero_bits = ShaHash::from(bytes);

        let xor_hash = zero_bits ^ mostly_zero_bits;

        assert_eq!(xor_hash.leading_zeros(), 0);
    }
}
