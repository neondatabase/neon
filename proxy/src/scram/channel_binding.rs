//! Definition and parser for channel binding flag (a part of GS2 header).

/// Channel binding flag (possibly with params).
#[derive(Debug, PartialEq, Eq)]
pub enum ChannelBinding<T> {
    /// Client doesn't support channel binding.
    NotSupportedClient,
    /// Client thinks server doesn't support channel binding.
    NotSupportedServer,
    /// Client wants to use this type of channel binding.
    Required(T),
}

impl<T> ChannelBinding<T> {
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> ChannelBinding<R> {
        use ChannelBinding::*;
        match self {
            NotSupportedClient => NotSupportedClient,
            NotSupportedServer => NotSupportedServer,
            Required(x) => Required(f(x)),
        }
    }
}

impl<'a> ChannelBinding<&'a str> {
    // NB: FromStr doesn't work with lifetimes
    pub fn parse(input: &'a str) -> Option<Self> {
        use ChannelBinding::*;
        Some(match input {
            "n" => NotSupportedClient,
            "y" => NotSupportedServer,
            other => Required(other.strip_prefix("p=")?),
        })
    }
}

impl<T: AsRef<str>> ChannelBinding<T> {
    /// Encode channel binding data as base64 for subsequent checks.
    pub fn encode(&self, get_cbind_data: impl FnOnce(&str) -> String) -> String {
        use ChannelBinding::*;
        match self {
            NotSupportedClient => {
                // base64::encode("n,,")
                "biws".into()
            }
            NotSupportedServer => {
                // base64::encode("y,,")
                "eSws".into()
            }
            Required(s) => {
                let s = s.as_ref();
                let msg = format!("p={mode},,{data}", mode = s, data = get_cbind_data(s));
                base64::encode(msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_binding_encode() {
        use ChannelBinding::*;

        let cases = [
            (NotSupportedClient, base64::encode("n,,")),
            (NotSupportedServer, base64::encode("y,,")),
            (Required("foo"), base64::encode("p=foo,,bar")),
        ];

        for (cb, input) in cases {
            assert_eq!(cb.encode(|_| "bar".to_owned()), input);
        }
    }
}
