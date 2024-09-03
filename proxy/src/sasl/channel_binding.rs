//! Definition and parser for channel binding flag (a part of the `GS2` header).

/// Channel binding flag (possibly with params).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChannelBinding<T> {
    /// Client doesn't support channel binding.
    NotSupportedClient,
    /// Client thinks server doesn't support channel binding.
    NotSupportedServer,
    /// Client wants to use this type of channel binding.
    Required(T),
}

impl<T> ChannelBinding<T> {
    pub(crate) fn and_then<R, E>(
        self,
        f: impl FnOnce(T) -> Result<R, E>,
    ) -> Result<ChannelBinding<R>, E> {
        Ok(match self {
            Self::NotSupportedClient => ChannelBinding::NotSupportedClient,
            Self::NotSupportedServer => ChannelBinding::NotSupportedServer,
            Self::Required(x) => ChannelBinding::Required(f(x)?),
        })
    }
}

impl<'a> ChannelBinding<&'a str> {
    // NB: FromStr doesn't work with lifetimes
    pub(crate) fn parse(input: &'a str) -> Option<Self> {
        Some(match input {
            "n" => Self::NotSupportedClient,
            "y" => Self::NotSupportedServer,
            other => Self::Required(other.strip_prefix("p=")?),
        })
    }
}

impl<T: std::fmt::Display> ChannelBinding<T> {
    /// Encode channel binding data as base64 for subsequent checks.
    pub(crate) fn encode<'a, E>(
        &self,
        get_cbind_data: impl FnOnce(&T) -> Result<&'a [u8], E>,
    ) -> Result<std::borrow::Cow<'static, str>, E> {
        Ok(match self {
            Self::NotSupportedClient => {
                // base64::encode("n,,")
                "biws".into()
            }
            Self::NotSupportedServer => {
                // base64::encode("y,,")
                "eSws".into()
            }
            Self::Required(mode) => {
                use std::io::Write;
                let mut cbind_input = vec![];
                write!(&mut cbind_input, "p={mode},,",).unwrap();
                cbind_input.extend_from_slice(get_cbind_data(mode)?);
                base64::encode(&cbind_input).into()
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
            assert_eq!(cb.encode(|_| anyhow::Ok(b"bar"))?, input);
        }

        Ok(())
    }
}
