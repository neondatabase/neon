//! Definition and parser for channel binding flag (a part of the `GS2` header).

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
    pub fn and_then<R, E>(self, f: impl FnOnce(T) -> Result<R, E>) -> Result<ChannelBinding<R>, E> {
        use ChannelBinding::*;
        Ok(match self {
            NotSupportedClient => NotSupportedClient,
            NotSupportedServer => NotSupportedServer,
            Required(x) => Required(f(x)?),
        })
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

impl<T: std::fmt::Display> ChannelBinding<T> {
    /// Encode channel binding data as base64 for subsequent checks.
    pub fn encode<E>(
        &self,
        get_cbind_data: impl FnOnce(&T) -> Result<String, E>,
    ) -> Result<std::borrow::Cow<'static, str>, E> {
        use ChannelBinding::*;
        Ok(match self {
            NotSupportedClient => {
                // base64::encode("n,,")
                "biws".into()
            }
            NotSupportedServer => {
                // base64::encode("y,,")
                "eSws".into()
            }
            Required(mode) => {
                let msg = format!(
                    "p={mode},,{data}",
                    mode = mode,
                    data = get_cbind_data(mode)?
                );
                base64::encode(msg).into()
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_binding_encode() -> anyhow::Result<()> {
        use ChannelBinding::*;

        let cases = [
            (NotSupportedClient, base64::encode("n,,")),
            (NotSupportedServer, base64::encode("y,,")),
            (Required("foo"), base64::encode("p=foo,,bar")),
        ];

        for (cb, input) in cases {
            assert_eq!(cb.encode(|_| anyhow::Ok("bar".to_owned()))?, input);
        }

        Ok(())
    }
}
