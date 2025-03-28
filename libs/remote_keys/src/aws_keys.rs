use aws_config::BehaviorVersion;

use crate::KeyId;

pub struct AwsRemoteKeyClient {
    client: aws_sdk_kms::Client,
}

impl AwsRemoteKeyClient {
    pub async fn new() -> Self {
        let sdk_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .retry_config(
                aws_config::retry::RetryConfig::standard()
                    .with_max_attempts(5) // Retry up to 5 times
                    .with_initial_backoff(std::time::Duration::from_millis(200)) // Start with 200ms delay
                    .with_max_backoff(std::time::Duration::from_secs(5)), // Cap at 5 seconds
            )
            .load()
            .await;
        let client = aws_sdk_kms::Client::new(&sdk_config);
        Self { client }
    }

    pub async fn decrypt(&self, key_id: &KeyId, ciphertext: impl Into<Vec<u8>>) -> Vec<u8> {
        let output = self
            .client
            .decrypt()
            .key_id(&key_id.0)
            .ciphertext_blob(aws_smithy_types::Blob::new(ciphertext.into()))
            .send()
            .await
            .expect("decrypt");
        output.plaintext.expect("plaintext").into_inner()
    }

    pub async fn encrypt(&self, key_id: &KeyId, ciphertext: impl Into<Vec<u8>>) -> Vec<u8> {
        let output = self
            .client
            .encrypt()
            .key_id(&key_id.0)
            .plaintext(aws_smithy_types::Blob::new(ciphertext.into()))
            .send()
            .await
            .expect("decrypt");
        output.ciphertext_blob.expect("ciphertext").into_inner()
    }
}
