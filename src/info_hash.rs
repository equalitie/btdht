use rand::{
    distributions::{Distribution, Standard},
    Rng,
};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{
    convert::{TryFrom, TryInto},
    fmt,
    net::IpAddr,
    ops::BitXor,
};
use thiserror::Error;

/// Length of `InfoHash` in bytes.
pub const INFO_HASH_LEN: usize = 20;

/// 20-byte long identifier of nodes and objects on the DHT
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct InfoHash(#[serde(with = "byte_array")] [u8; INFO_HASH_LEN]);

impl InfoHash {
    /// Generate InfoHash from the IP address as described by BEP42
    /// https://www.bittorrent.org/beps/bep_0042.html
    pub fn from_ip(ip: IpAddr) -> Self {
        let v4_mask: [u8; 8] = [0x03, 0x0f, 0x3f, 0xff, 0, 0, 0, 0];
        let v6_mask: [u8; 8] = [0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff];

        let (mut ip, num_octets, mask) = {
            let mut array = [0; 8];
            let (mask, num_octets) = match ip {
                IpAddr::V4(ip) => {
                    let num = 4;
                    let octets = ip.octets();
                    for i in 0..num {
                        array[i] = octets[i];
                    }
                    (v4_mask, num)
                }
                IpAddr::V6(ip) => {
                    let num = 8;
                    let octets = ip.octets();
                    for i in 0..num {
                        array[i] = octets[i];
                    }
                    (v6_mask, num)
                }
            };
            (array, num_octets, mask)
        };

        for i in 0..num_octets {
            ip[i] &= mask[i];
        }

        let rand = rand::random::<u8>();
        ip[0] |= (rand & 0x7) << 5;

        let crc = crc32c::crc32c_append(0, &ip[0..num_octets]);

        let mut node_id: [u8; INFO_HASH_LEN] = [0; INFO_HASH_LEN];

        node_id[0] = (crc >> 24).to_le_bytes()[0];
        node_id[1] = (crc >> 16).to_le_bytes()[0];
        node_id[2] = (crc >> 8).to_le_bytes()[0] & 0xf8 | (rand::random::<u8>() & 0x7);

        for i in 3..19 {
            node_id[i] = rand::random::<u8>();
        }

        node_id[19] = rand;

        Self(node_id)
    }

    /// Create a DhtId by hashing the given bytes using SHA-1.
    pub fn sha1(bytes: &[u8]) -> Self {
        let hash = Sha1::digest(bytes);
        Self(hash.into())
    }

    /// Flip the bit at the given index.
    ///
    /// # Panics
    ///
    /// Panics if index is out of bounds (>= 160)
    pub(crate) fn flip_bit(self, index: usize) -> Self {
        let mut bytes = self.0;
        let (byte_index, bit_index) = (index / 8, index % 8);

        let actual_bit_index = 7 - bit_index;
        bytes[byte_index] ^= 1 << actual_bit_index;

        bytes.into()
    }

    /// Number of leading zero bits.
    pub(crate) fn leading_zeros(&self) -> u32 {
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

impl AsRef<[u8]> for InfoHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<InfoHash> for [u8; INFO_HASH_LEN] {
    fn from(hash: InfoHash) -> [u8; INFO_HASH_LEN] {
        hash.0
    }
}

impl From<[u8; INFO_HASH_LEN]> for InfoHash {
    fn from(hash: [u8; INFO_HASH_LEN]) -> InfoHash {
        Self(hash)
    }
}

#[derive(Debug, Error)]
#[error("invalid id length")]
pub struct LengthError;

impl<'a> TryFrom<&'a [u8]> for InfoHash {
    type Error = LengthError;

    fn try_from(slice: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self(slice.try_into().map_err(|_| LengthError)?))
    }
}

impl BitXor for InfoHash {
    type Output = Self;

    fn bitxor(mut self, rhs: Self) -> Self {
        for (src, dst) in rhs.0.iter().zip(self.0.iter_mut()) {
            *dst ^= *src;
        }

        self
    }
}

impl Distribution<InfoHash> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> InfoHash {
        InfoHash(rng.gen())
    }
}

impl fmt::LowerHex for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }

        Ok(())
    }
}

impl fmt::Debug for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:x}")
    }
}

mod byte_array {
    use super::INFO_HASH_LEN;
    use serde::{
        de::{Deserialize, Deserializer, Error},
        ser::{Serialize, Serializer},
    };
    use serde_bytes::{ByteBuf, Bytes};
    use std::convert::TryInto;

    pub(super) fn serialize<S: Serializer>(
        bytes: &[u8; INFO_HASH_LEN],
        s: S,
    ) -> Result<S::Ok, S::Error> {
        Bytes::new(bytes.as_ref()).serialize(s)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<[u8; INFO_HASH_LEN], D::Error> {
        let buf = ByteBuf::deserialize(d)?;
        let buf = buf.into_vec();
        let len = buf.len();

        buf.try_into().map_err(|_| {
            let expected = format!("{INFO_HASH_LEN}");
            D::Error::invalid_length(len, &expected.as_ref())
        })
    }
}

// ----------------------------------------------------------------------------//

/// Bittorrent `NodeId`.
pub type NodeId = InfoHash;

/// Length of a `NodeId`.
pub const NODE_ID_LEN: usize = INFO_HASH_LEN;

// ----------------------------------------------------------------------------//

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_no_leading_zeroes() {
        let zero_bits = InfoHash::from([0u8; INFO_HASH_LEN]);
        let one_bits = InfoHash::from([255u8; INFO_HASH_LEN]);

        let xor_hash = zero_bits ^ one_bits;

        assert_eq!(xor_hash.leading_zeros(), 0)
    }

    #[test]
    fn positive_all_leading_zeroes() {
        let first_one_bits = InfoHash::from([255u8; INFO_HASH_LEN]);
        let second_one_bits = InfoHash::from([255u8; INFO_HASH_LEN]);

        let xor_hash = first_one_bits ^ second_one_bits;

        assert_eq!(xor_hash.leading_zeros() as usize, INFO_HASH_LEN * 8);
    }

    #[test]
    fn positive_one_leading_zero() {
        let zero_bits = InfoHash::from([0u8; INFO_HASH_LEN]);

        let mut bytes = [255u8; INFO_HASH_LEN];
        bytes[0] = 127;
        let mostly_one_bits = InfoHash::from(bytes);

        let xor_hash = zero_bits ^ mostly_one_bits;

        assert_eq!(xor_hash.leading_zeros(), 1);
    }

    #[test]
    fn positive_one_trailing_zero() {
        let zero_bits = InfoHash::from([0u8; INFO_HASH_LEN]);

        let mut bytes = [255u8; INFO_HASH_LEN];
        bytes[super::INFO_HASH_LEN - 1] = 254;
        let mostly_zero_bits = InfoHash::from(bytes);

        let xor_hash = zero_bits ^ mostly_zero_bits;

        assert_eq!(xor_hash.leading_zeros(), 0);
    }
}
