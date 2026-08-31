/// Type of the storage node (pageserver or safekeeper) that we are updating DNS records for. Different types of nodes will have
/// different-looking DNS names in the DNS zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NodeType {
    Pageserver,
    Safekeeper,
}
