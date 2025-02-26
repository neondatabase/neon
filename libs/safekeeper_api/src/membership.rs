//! Types defining safekeeper membership, see
//! rfcs/035-safekeeper-dynamic-membership-change.md
//! for details.

use std::collections::HashSet;
use std::fmt::Display;

use anyhow;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use utils::id::NodeId;

/// 1 is the first valid generation, 0 is used as
/// a placeholder before we fully migrate to generations.
pub const INVALID_GENERATION: SafekeeperGeneration = SafekeeperGeneration::new(0);
pub const INITIAL_GENERATION: SafekeeperGeneration = SafekeeperGeneration::new(1);

/// Number uniquely identifying safekeeper configuration.
/// Note: it is a part of sk control file.
///
/// Like tenant generations, but for safekeepers.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SafekeeperGeneration(u32);

impl SafekeeperGeneration {
    pub const fn new(v: u32) -> Self {
        Self(v)
    }

    #[track_caller]
    pub fn previous(&self) -> Option<Self> {
        Some(Self(self.0.checked_sub(1)?))
    }

    #[track_caller]
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn into_inner(self) -> u32 {
        self.0
    }
}

impl Display for SafekeeperGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Membership is defined by ids so e.g. walproposer uses them to figure out
/// quorums, but we also carry host and port to give wp idea where to connect.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SafekeeperId {
    pub id: NodeId,
    pub host: String,
    /// We include here only port for computes -- that is, pg protocol tenant
    /// only port, or wide pg protocol port if the former is not configured.
    pub pg_port: u16,
}

impl Display for SafekeeperId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[id={}, ep={}:{}]", self.id, self.host, self.pg_port)
    }
}

/// Set of safekeepers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct MemberSet {
    pub m: Vec<SafekeeperId>,
}

impl MemberSet {
    pub fn empty() -> Self {
        MemberSet { m: Vec::new() }
    }

    pub fn new(members: Vec<SafekeeperId>) -> anyhow::Result<Self> {
        let hs: HashSet<NodeId> = HashSet::from_iter(members.iter().map(|sk| sk.id));
        if hs.len() != members.len() {
            bail!("duplicate safekeeper id in the set {:?}", members);
        }
        Ok(MemberSet { m: members })
    }

    pub fn contains(&self, sk: NodeId) -> bool {
        self.m.iter().any(|m| m.id == sk)
    }

    pub fn add(&mut self, sk: SafekeeperId) -> anyhow::Result<()> {
        if self.contains(sk.id) {
            bail!(format!(
                "sk {} is already member of the set {}",
                sk.id, self
            ));
        }
        self.m.push(sk);
        Ok(())
    }
}

impl Display for MemberSet {
    /// Display as a comma separated list of members.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sks_str = self.m.iter().map(|sk| sk.to_string()).collect::<Vec<_>>();
        write!(f, "({})", sks_str.join(", "))
    }
}

/// Safekeeper membership configuration.
/// Note: it is a part of both control file and http API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Configuration {
    /// Unique id.
    pub generation: SafekeeperGeneration,
    /// Current members of the configuration.
    pub members: MemberSet,
    /// Some means it is a joint conf.
    pub new_members: Option<MemberSet>,
}

impl Configuration {
    /// Used for pre-generations timelines, will be removed eventually.
    pub fn empty() -> Self {
        Configuration {
            generation: INVALID_GENERATION,
            members: MemberSet::empty(),
            new_members: None,
        }
    }

    /// Is `sk_id` member of the configuration?
    pub fn contains(&self, sk_id: NodeId) -> bool {
        self.members.contains(sk_id) || self.new_members.as_ref().is_some_and(|m| m.contains(sk_id))
    }
}

impl Display for Configuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "gen={}, members={}, new_members={}",
            self.generation,
            self.members,
            self.new_members
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or(String::from("none"))
        )
    }
}

#[cfg(test)]
mod tests {
    use utils::id::NodeId;

    use super::{MemberSet, SafekeeperId};

    #[test]
    fn test_member_set() {
        let mut members = MemberSet::empty();
        members
            .add(SafekeeperId {
                id: NodeId(42),
                host: String::from("lala.org"),
                pg_port: 5432,
            })
            .unwrap();

        members
            .add(SafekeeperId {
                id: NodeId(42),
                host: String::from("lala.org"),
                pg_port: 5432,
            })
            .expect_err("duplicate must not be allowed");

        members
            .add(SafekeeperId {
                id: NodeId(43),
                host: String::from("bubu.org"),
                pg_port: 5432,
            })
            .unwrap();

        println!("members: {}", members);

        let j = serde_json::to_string(&members).expect("failed to serialize");
        println!("members json: {}", j);
        assert_eq!(
            j,
            r#"[{"id":42,"host":"lala.org","pg_port":5432},{"id":43,"host":"bubu.org","pg_port":5432}]"#
        );
    }
}
