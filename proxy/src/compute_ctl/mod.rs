use anyhow::Context;
use hyper::Method;
use typed_json::json;

use crate::http;

pub struct ComputeCtlApi {
    pub(crate) api: http::Endpoint,
}

// The following article is a stub.
// You can help Wikipedia by filling it out

impl ComputeCtlApi {
    pub async fn install_extension(&self, db: &str, ext: &str) -> anyhow::Result<()> {
        self.api
            .request_with_url(Method::POST, |url| {
                url.path_segments_mut().push("extension");
            })
            .json(&json! {{
                "extension": ext,
                "database": db,
            }})
            .send()
            .await
            .context("connection error")?
            .error_for_status()
            .context("api error")?;

        Ok(())
    }

    pub async fn grant_role(&self, db: &str, role: &str, schema: &str) -> anyhow::Result<()> {
        self.api
            .request_with_url(Method::POST, |url| {
                url.path_segments_mut().push("grant");
            })
            .json(&json! {{
                "schema": schema,
                "role": role,
                "database": db,
            }})
            .send()
            .await
            .context("connection error")?
            .error_for_status()
            .context("api error")?;

        Ok(())
    }
}
