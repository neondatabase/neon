#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
    References,
    Trigger,
    Usage,
    Create,
    Connect,
    Temporary,
    Execute,
}
