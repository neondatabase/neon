use std::env::{VarError, var};
use std::error::Error;
use std::net::IpAddr;
use std::str::FromStr;

/// Name of the environment variable containing the reachable IP address of the node. If set, the IP address contained in this
/// environment variable is used as the reachable IP address of the pageserver or safekeeper node during node registration.
/// In a Kubernetes environment, this environment variable should be set by Kubernetes to the Pod IP (specified in the Pod
/// template).
pub const HADRON_NODE_IP_ADDRESS: &str = "HADRON_NODE_IP_ADDRESS";

/// Read the reachable IP address of this page server from env var HADRON_NODE_IP_ADDRESS.
/// In Kubernetes this environment variable is set to the Pod IP (specified in the Pod template).
pub fn read_node_ip_addr_from_env() -> Result<Option<IpAddr>, Box<dyn Error>> {
    match var(HADRON_NODE_IP_ADDRESS) {
        Ok(v) => {
            if let Ok(addr) = IpAddr::from_str(&v) {
                Ok(Some(addr))
            } else {
                Err(format!("Invalid IP address string: {v}. Cannot be parsed as either an IPv4 or an IPv6 address.").into())
            }
        }
        Err(VarError::NotPresent) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_read_node_ip_addr_from_env() {
        // SAFETY: test code
        unsafe {
            // Test with a valid IPv4 address
            env::set_var(HADRON_NODE_IP_ADDRESS, "192.168.1.1");
            let result = read_node_ip_addr_from_env().unwrap();
            assert_eq!(result, Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));

            // Test with a valid IPv6 address
            env::set_var(
                HADRON_NODE_IP_ADDRESS,
                "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            );
        }
        let result = read_node_ip_addr_from_env().unwrap();
        assert_eq!(
            result,
            Some(IpAddr::V6(
                Ipv6Addr::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap()
            ))
        );

        // Test with an invalid IP address
        // SAFETY: test code
        unsafe {
            env::set_var(HADRON_NODE_IP_ADDRESS, "invalid_ip");
        }
        let result = read_node_ip_addr_from_env();
        assert!(result.is_err());

        // Test with no environment variable set
        // SAFETY: test code
        unsafe {
            env::remove_var(HADRON_NODE_IP_ADDRESS);
        }
        let result = read_node_ip_addr_from_env().unwrap();
        assert_eq!(result, None);
    }
}
