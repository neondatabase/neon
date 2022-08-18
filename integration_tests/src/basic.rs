#[cfg(test)]
mod tests {
    use pg_bin::LocalPostgres;
    use std::path::PathBuf;
    use tokio_postgres::NoTls;

    #[tokio::test]
    async fn test_postgres_select_1() -> anyhow::Result<()> {
        // Test setup
        let pg_datadir = PathBuf::from("/home/bojan/tmp/t1/");
        let pg_prefix = PathBuf::from("/home/bojan/src/neondatabase/neon/tmp_install/bin/");

        // Get a postgres
        let mut postgres = LocalPostgres::new(pg_datadir, pg_prefix);
        postgres.start();
        let config = postgres.admin_conn_info();

        if let Err(e) = config.connect(NoTls).await {
            eprintln!("error error {:?}", e);
        }

        // Get client, run connection
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Run "select 1"
        let rows = client.query("SELECT 'hello';", &[]).await?;
        let value: &str = rows[0].get(0);
        assert_eq!(value, "hello");

        Ok(())
    }
}
