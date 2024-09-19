use std::time::SystemTime;
use utils::{serde_percent::Percent, serde_system_time};

/// Pageserver current utilization and scoring for how good candidate the pageserver would be for
/// the next tenant.
///
/// See and maintain pageserver openapi spec for `/v1/utilization_score` as the truth.
///
/// `format: int64` fields must use `ser_saturating_u63` because openapi generated clients might
/// not handle full u64 values properly.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PageserverUtilization {
    /// Used disk space (physical, ground truth from statfs())
    #[serde(serialize_with = "ser_saturating_u63")]
    pub disk_usage_bytes: u64,
    /// Free disk space
    #[serde(serialize_with = "ser_saturating_u63")]
    pub free_space_bytes: u64,

    /// Wanted disk space, based on the tenant shards currently present on this pageserver: this
    /// is like disk_usage_bytes, but it is stable and does not change with the cache state of
    /// tenants, whereas disk_usage_bytes may reach the disk eviction `max_usage_pct` and stay
    /// there, or may be unrealistically low if the pageserver has attached tenants which haven't
    /// downloaded layers yet.
    #[serde(serialize_with = "ser_saturating_u63", default)]
    pub disk_wanted_bytes: u64,

    // What proportion of total disk space will this pageserver use before it starts evicting data?
    #[serde(default = "unity_percent")]
    pub disk_usable_pct: Percent,

    // How many shards are currently on this node?
    #[serde(default)]
    pub shard_count: u32,

    // How many shards should this node be able to handle at most?
    #[serde(default)]
    pub max_shard_count: u32,

    /// Cached result of [`Self::score`]
    pub utilization_score: Option<u64>,

    /// When was this snapshot captured, pageserver local time.
    ///
    /// Use millis to give confidence that the value is regenerated often enough.
    pub captured_at: serde_system_time::SystemTime,
}

fn unity_percent() -> Percent {
    Percent::new(0).unwrap()
}

pub type RawScore = u64;

impl PageserverUtilization {
    const UTILIZATION_FULL: u64 = 1000000;

    /// Calculate a utilization score.  The result is to be inrepreted as a fraction of
    /// Self::UTILIZATION_FULL.
    ///
    /// Lower values are more affine to scheduling more work on this node.
    /// - UTILIZATION_FULL represents an ideal node which is fully utilized but should not receive any more work.
    /// - 0.0 represents an empty node.
    /// - Negative values are forbidden
    /// - Values over UTILIZATION_FULL indicate an overloaded node, which may show degraded performance due to
    ///   layer eviction.
    pub fn score(&self) -> RawScore {
        let disk_usable_capacity = ((self.disk_usage_bytes + self.free_space_bytes)
            * self.disk_usable_pct.get() as u64)
            / 100;
        let disk_utilization_score =
            self.disk_wanted_bytes * Self::UTILIZATION_FULL / disk_usable_capacity;

        let shard_utilization_score =
            self.shard_count as u64 * Self::UTILIZATION_FULL / self.max_shard_count as u64;
        std::cmp::max(disk_utilization_score, shard_utilization_score)
    }

    pub fn cached_score(&mut self) -> RawScore {
        match self.utilization_score {
            None => {
                let s = self.score();
                self.utilization_score = Some(s);
                s
            }
            Some(s) => s,
        }
    }

    /// If a node is currently hosting more work than it can comfortably handle.  This does not indicate that
    /// it will fail, but it is a strong signal that more work should not be added unless there is no alternative.
    ///
    /// When a node is overloaded, we may override soft affinity preferences and do things like scheduling
    /// into a node in a less desirable AZ, if all the nodes in the preferred AZ are overloaded.
    pub fn is_overloaded(score: RawScore) -> bool {
        // Why the factor of two?  This is unscientific but reflects behavior of real systems:
        // - In terms of shard counts, a node's preferred max count is a soft limit intended to keep
        //   startup and housekeeping jobs nice and responsive.  We can go to double this limit if needed
        //   until some more nodes are deployed.
        // - In terms of disk space, the node's utilization heuristic assumes every tenant needs to
        //   hold its biggest timeline fully on disk, which is tends to be an over estimate when
        //   some tenants are very idle and have dropped layers from disk.  In practice going up to
        //   double is generally better than giving up and scheduling in a sub-optimal AZ.
        score >= 2 * Self::UTILIZATION_FULL
    }

    pub fn adjust_shard_count_max(&mut self, shard_count: u32) {
        if self.shard_count < shard_count {
            self.shard_count = shard_count;

            // Dirty cache: this will be calculated next time someone retrives the score
            self.utilization_score = None;
        }
    }

    /// A utilization structure that has a full utilization score: use this as a placeholder when
    /// you need a utilization but don't have real values yet.
    pub fn full() -> Self {
        Self {
            disk_usage_bytes: 1,
            free_space_bytes: 0,
            disk_wanted_bytes: 1,
            disk_usable_pct: Percent::new(100).unwrap(),
            shard_count: 1,
            max_shard_count: 1,
            utilization_score: Some(Self::UTILIZATION_FULL),
            captured_at: serde_system_time::SystemTime(SystemTime::now()),
        }
    }
}

/// Test helper
pub mod test_utilization {
    use super::PageserverUtilization;
    use std::time::SystemTime;
    use utils::{
        serde_percent::Percent,
        serde_system_time::{self},
    };

    // Parameters of the imaginary node used for test utilization instances
    const TEST_DISK_SIZE: u64 = 1024 * 1024 * 1024 * 1024;
    const TEST_SHARDS_MAX: u32 = 1000;

    /// Unit test helper.  Unconditionally compiled because cfg(test) doesn't carry across crates.  Do
    /// not abuse this function from non-test code.
    ///
    /// Emulates a node with a 1000 shard limit and a 1TB disk.
    pub fn simple(shard_count: u32, disk_wanted_bytes: u64) -> PageserverUtilization {
        PageserverUtilization {
            disk_usage_bytes: disk_wanted_bytes,
            free_space_bytes: TEST_DISK_SIZE - std::cmp::min(disk_wanted_bytes, TEST_DISK_SIZE),
            disk_wanted_bytes,
            disk_usable_pct: Percent::new(100).unwrap(),
            shard_count,
            max_shard_count: TEST_SHARDS_MAX,
            utilization_score: None,
            captured_at: serde_system_time::SystemTime(SystemTime::now()),
        }
    }
}

/// openapi knows only `format: int64`, so avoid outputting a non-parseable value by generated clients.
///
/// Instead of newtype, use this because a newtype would get require handling deserializing values
/// with the highest bit set which is properly parsed by serde formats, but would create a
/// conundrum on how to handle and again serialize such values at type level. It will be a few
/// years until we can use more than `i64::MAX` bytes on a disk.
fn ser_saturating_u63<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    const MAX_FORMAT_INT64: u64 = i64::MAX as u64;

    let value = (*value).min(MAX_FORMAT_INT64);

    serializer.serialize_u64(value)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn u64_max_is_serialized_as_u63_max() {
        let doc = PageserverUtilization {
            disk_usage_bytes: u64::MAX,
            free_space_bytes: 0,
            disk_wanted_bytes: u64::MAX,
            utilization_score: Some(13),
            disk_usable_pct: Percent::new(90).unwrap(),
            shard_count: 100,
            max_shard_count: 200,
            captured_at: serde_system_time::SystemTime(
                std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1708509779),
            ),
        };

        let s = serde_json::to_string(&doc).unwrap();

        let expected = "{\"disk_usage_bytes\":9223372036854775807,\"free_space_bytes\":0,\"disk_wanted_bytes\":9223372036854775807,\"disk_usable_pct\":90,\"shard_count\":100,\"max_shard_count\":200,\"utilization_score\":13,\"captured_at\":\"2024-02-21T10:02:59.000Z\"}";

        assert_eq!(s, expected);
    }
}
