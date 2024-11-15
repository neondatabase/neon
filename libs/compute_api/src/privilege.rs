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

impl Privilege {
    pub fn as_str(&self) -> &'static str {
        match self {
            Privilege::Select => "SELECT",
            Privilege::Insert => "INSERT",
            Privilege::Update => "UPDATE",
            Privilege::Delete => "DELETE",
            Privilege::Truncate => "TRUNCATE",
            Privilege::References => "REFERENCES",
            Privilege::Trigger => "TRIGGER",
            Privilege::Usage => "USAGE",
            Privilege::Create => "CREATE",
            Privilege::Connect => "CONNECT",
            Privilege::Temporary => "TEMPORARY",
            Privilege::Execute => "EXECUTE",
        }
    }
}
