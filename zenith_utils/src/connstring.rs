use postgres::Config;

pub fn connection_host_port(config: &Config) -> (String, u16) {
    assert_eq!(config.get_hosts().len(), 1, "only one pair of host and port is supported in connection string");
    assert_eq!(config.get_ports().len(), 1, "only one pair of host and port is supported in connection string");
    let host = match &config.get_hosts()[0] {
        postgres::config::Host::Tcp(host) => host.as_ref(),
        postgres::config::Host::Unix(host) => host.to_str().unwrap(),
    };
    (host.to_owned(), config.get_ports()[0])
}

pub fn connection_address(config: &Config) -> String {
    let (host, port) = connection_host_port(config);
    format!("{}:{}", host, port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_host_port() {
        let config: Config = "postgresql://no_user@localhost:64000/no_db".parse().unwrap();
        assert_eq!(connection_host_port(&config), ("localhost".to_owned(), 64000));
    }

    #[test]
    #[should_panic(expected = "only one pair of host and port is supported in connection string")]
    fn test_connection_host_port_multiple_ports() {
        let config: Config = "postgresql://no_user@localhost:64000,localhost:64001/no_db".parse().unwrap();
        assert_eq!(connection_host_port(&config), ("localhost".to_owned(), 64000));
    }
}
