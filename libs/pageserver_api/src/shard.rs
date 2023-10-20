use serde::{Deserialize, Serialize};

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct ShardNumber(pub u8);

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct ShardCount(pub u8);

impl ShardNumber {
    pub const MAX: Self = Self(u8::MAX);
}
