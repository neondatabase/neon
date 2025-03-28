mod aws_keys;
pub use aws_keys::AwsRemoteKeyClient;

/// A string uniquely identifying a key
#[derive(Debug, PartialEq, Eq)]
pub struct KeyId(pub String);