use std::str::FromStr;

#[derive(Debug)]
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

impl FromStr for Privilege {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SELECT" => Ok(Privilege::Select),
            "INSERT" => Ok(Privilege::Insert),
            "UPDATE" => Ok(Privilege::Update),
            "DELETE" => Ok(Privilege::Delete),
            "TRUNCATE" => Ok(Privilege::Truncate),
            "REFERENCES" => Ok(Privilege::References),
            "TRIGGER" => Ok(Privilege::Trigger),
            "USAGE" => Ok(Privilege::Usage),
            "CREATE" => Ok(Privilege::Create),
            "CONNECT" => Ok(Privilege::Connect),
            "TEMPORARY" => Ok(Privilege::Temporary),
            "EXECUTE" => Ok(Privilege::Execute),
            _ => Err("Invalid privilege"),
        }
    }
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
    pub fn to_string(&self) -> String {
        self.as_str().to_string()
    }
}
