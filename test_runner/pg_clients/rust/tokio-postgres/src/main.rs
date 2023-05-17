use std::env::VarError;
use tokio_postgres;

fn get_env(key: &str) -> String {
    match std::env::var(key) {
        Ok(val) => val,
        Err(VarError::NotPresent) => panic!("{key} env variable not set"),
        Err(VarError::NotUnicode(_)) => panic!("{key} is not valid unicode"),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), tokio_postgres::Error> {
    let host = get_env("NEON_HOST");
    let database = get_env("NEON_DATABASE");
    let user = get_env("NEON_USER");
    let password = get_env("NEON_PASSWORD");

    let url = format!("postgresql://{user}:{password}@{host}/{database}");

    // Use the native TLS implementation (Neon requires TLS)
    let tls_connector =
        postgres_native_tls::MakeTlsConnector::new(native_tls::TlsConnector::new().unwrap());

    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(&url, tls_connector).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let result = client.query("SELECT 1", &[]).await?;

    let value: i32 = result[0].get(0);
    assert_eq!(value, 1);
    println!("{value}");

    Ok(())
}
