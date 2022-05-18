use anyhow::bail;
use url::form_urlencoded::Serializer;

/// A [url](url::Url) type with additional guarantees.
#[derive(Debug, Clone)]
pub struct ApiUrl(url::Url);

impl ApiUrl {
    /// Consume the wrapper and return inner [url](url::Url).
    pub fn into_inner(self) -> url::Url {
        self.0
    }

    /// See [`url::Url::query_pairs_mut`].
    pub fn query_pairs_mut(&mut self) -> Serializer<'_, url::UrlQuery<'_>> {
        self.0.query_pairs_mut()
    }

    /// See [`url::Url::path_segments_mut`].
    pub fn path_segments_mut(&mut self) -> url::PathSegmentsMut {
        // We've already verified that it works during construction.
        self.0.path_segments_mut().expect("bad API url")
    }
}

/// This instance imposes additional requirements on the url.
impl std::str::FromStr for ApiUrl {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let mut url: url::Url = s.parse()?;

        // Make sure that we can build upon this URL.
        if url.path_segments_mut().is_err() {
            bail!("bad API url provided");
        }

        Ok(Self(url))
    }
}

/// This instance is safe because it doesn't allow us to modify the object.
impl std::ops::Deref for ApiUrl {
    type Target = url::Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for ApiUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_url() {
        let url = "test:foobar";
        url.parse::<url::Url>().expect("unexpected parsing failure");
        let _ = url.parse::<ApiUrl>().expect_err("should not parse");
    }

    #[test]
    fn good_url() {
        let url = "test://foobar";
        let mut a = url.parse::<url::Url>().expect("unexpected parsing failure");
        let mut b = url.parse::<ApiUrl>().expect("unexpected parsing failure");

        a.path_segments_mut().unwrap().push("method");
        a.query_pairs_mut().append_pair("key", "value");

        b.path_segments_mut().push("method");
        b.query_pairs_mut().append_pair("key", "value");

        assert_eq!(a, b.into_inner());
    }
}
