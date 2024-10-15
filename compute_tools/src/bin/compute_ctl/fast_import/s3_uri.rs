use anyhow::Result;
use std::str::FromStr;

/// Struct to hold parsed S3 components
#[derive(Debug, PartialEq)]
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

impl ToString for S3Uri {
    fn to_string(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_uri_valid() {
        let uri = "s3://my-bucket/some/path/to/object.txt";
        let result = S3Uri::from_str(uri).unwrap();

        assert_eq!(
            result,
            S3Uri {
                bucket: "my-bucket".to_string(),
                key: "some/path/to/object.txt".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_s3_uri_invalid_scheme() {
        let uri = "http://my-bucket/some/path/to/object.txt";
        let result = S3Uri::from_str(uri);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_uri_missing_key() {
        let uri = "s3://my-bucket";
        let result = S3Uri::from_str(uri);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_uri_empty_uri() {
        let uri = "";
        let result = S3Uri::from_str(uri);
        assert!(result.is_err());
    }
}
