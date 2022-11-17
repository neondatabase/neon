use url::Url;

#[derive(Debug)]
pub struct PgConnectionConfig {
    url: Url,
}

impl PgConnectionConfig {
    pub fn host(&self) -> &str {
        self.url.host_str().expect("BUG: no host")
    }

    pub fn port(&self) -> u16 {
        self.url.port().expect("BUG: no port")
    }

    /// Return a `<host>:<port>` string.
    pub fn raw_address(&self) -> String {
        format!("{}:{}", self.host(), self.port())
    }

    /// Connect using postgres protocol with TLS disabled.
    pub fn connect_no_tls(&self) -> Result<postgres::Client, postgres::Error> {
        postgres::Client::connect(self.url.as_str(), postgres::NoTls)
    }
}

impl std::str::FromStr for PgConnectionConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut url: Url = s.parse()?;

        match url.scheme() {
            "postgres" | "postgresql" => {}
            other => anyhow::bail!("invalid scheme: {other}"),
        }

        // It's not a valid connection url if host is unavailable.
        if url.host().is_none() {
            anyhow::bail!(url::ParseError::EmptyHost);
        }

        // E.g. `postgres:bar`.
        if url.cannot_be_a_base() {
            anyhow::bail!("URL cannot be a base");
        }

        // Set the default PG port if it's missing.
        if url.port().is_none() {
            url.set_port(Some(5432))
                .expect("BUG: couldn't set the default port");
        }

        Ok(Self { url })
    }
}
