

pub struct InMemoryLayerRaw {
}

impl InMemoryLayerRaw {
    pub async fn new() -> Self {
        Self {

        }
    }

    pub async fn put_value(
        &self,
        key: Key,
        lsn: Lsn,
        val: &Value,
        ctx: &RequestContext,
    ) -> Result<()> {

        Ok(())
    }
}
