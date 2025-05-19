struct SafekeeperMigrator {}

impl SafekeeperMigrator {
    pub fn new(service: Arc<Service>) -> Self {
        Self {}
    }

    pub async fn migrate(&self) -> Result<(), String> {}


pub async fn apply_on_quorum<O>(&self, safekeepers: &[&Safekeeper], op: O, quorum: i32) -> Result<(), String> 
where O: FnMut(SafekeeperClient) -> std::future::Future<Output = mgmt_api::Result<()>>,
{
    let tasks : FuturesUnordered;
    
    let cancel = self.service.cancel.clone();
    

    for sk in safekeepers {
        tasks.push(async {
            sk.with_client_retries(
            op,
            &self.service.http_client,
            &self.service.config.jwt_token,
            1,
            1,
            timeout,
            cancel,
        )?;
        sk.id
    });
        tasks.push(self.apply_on_safekeeper(sk));
    }

    let mut success = 0;

    let applied_on = Vec::new();


    while let Some(res) = tasks.next().await {
        match res {
            Ok(res) => {
                applied_on.push(res);
                success += 1;
                if success >= quorum {
                    break;
                }
            }
            Err(e) => {
                // Handle error
                return Err(format!("Error applying operation: {}", e));
            }
        }
    }

    cancel.cancel();
    let mut deadline = Instant::now() + Duration::from_secs(5);

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => {
                break;
            }
            res = tasks.next() => {

            }
        }
    }
    
    Ok(())
}
