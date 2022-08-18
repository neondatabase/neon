#[cfg(test)]
mod tests {
    use pg_bin::PgDatadir;
    use std::path::PathBuf;
    use tokio_postgres::NoTls;

    #[tokio::test]
    async fn test_postgres_select_1() -> anyhow::Result<()> {
        // Test setup
        let output = PathBuf::from("/home/bojan/tmp/");
        let pg_prefix = PathBuf::from("/home/bojan/src/neondatabase/neon/tmp_install/bin/");

        // Init datadir
        let pg_datadir_path = PathBuf::from("/home/bojan/tmp/t1/");
        let pg_datadir = PgDatadir::new_initdb(pg_datadir_path, &pg_prefix, &output, true);

        // Get a postgres
        let postgres = pg_datadir.spawn_postgres(pg_prefix, output);
        let conn_info = postgres.admin_conn_info();

        // Get client, run connection
        let (client, connection) = conn_info.connect(NoTls).await?;
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
