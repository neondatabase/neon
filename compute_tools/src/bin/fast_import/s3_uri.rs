use anyhow::Result;
use std::str::FromStr;

/// Struct to hold parsed S3 components
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Uri {
    pub bucket: String,
    pub key: String,
}

impl FromStr for S3Uri {
    type Err = anyhow::Error;

    /// Parse an S3 URI into a bucket and key
    fn from_str(uri: &str) -> Result<Self> {
        // Ensure the URI starts with "s3://"
        if !uri.starts_with("s3://") {
            return Err(anyhow::anyhow!("Invalid S3 URI scheme"));
        }

        // Remove the "s3://" prefix
        let stripped_uri = &uri[5..];

        // Split the remaining string into bucket and key parts
        if let Some((bucket, key)) = stripped_uri.split_once('/') {
            Ok(S3Uri {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })
        } else {
            Err(anyhow::anyhow!(
                "Invalid S3 URI format, missing bucket or key"
            ))
        }
    }
}

impl S3Uri {
    pub fn append(&self, suffix: &str) -> Self {
        Self {
            bucket: self.bucket.clone(),
            key: format!("{}{}", self.key, suffix),
        }
    }
}

impl std::fmt::Display for S3Uri {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}

impl clap::builder::TypedValueParser for S3Uri {
    type Value = Self;

    fn parse_ref(
        &self,
        _cmd: &clap::Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let value_str = value.to_str().ok_or_else(|| {
            clap::Error::raw(
                clap::error::ErrorKind::InvalidUtf8,
                "Invalid UTF-8 sequence",
            )
        })?;
        S3Uri::from_str(value_str).map_err(|e| {
            clap::Error::raw(
                clap::error::ErrorKind::InvalidValue,
                format!("Failed to parse S3 URI: {}", e),
            )
        })
    }
}
