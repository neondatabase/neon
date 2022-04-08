use anyhow::*;
use clap::{App, Arg};
use wal_generate::*;

fn main() -> Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("wal_generate=info"),
    )
    .init();
    let arg_matches = App::new("Postgres WAL generator")
        .about("Generates Postgres databases with specific WAL properties")
        .arg(
            Arg::new("datadir")
                .short('D')
                .long("datadir")
                .takes_value(true)
                .help("Data directory for the Postgres server")
                .required(true),
        )
        .arg(
            Arg::new("pg-distrib-dir")
                .long("pg-distrib-dir")
                .takes_value(true)
                .help("Directory with Postgres distribution (bin and lib directories, e.g. tmp_install)")
                .default_value("/usr/local"),
        )
        .get_matches();

    let cfg = Conf {
        pg_distrib_dir: arg_matches.value_of("pg-distrib-dir").unwrap().into(),
        datadir: arg_matches.value_of("datadir").unwrap().into(),
    };
    cfg.initdb()?;
    let mut srv = cfg.start_server()?;
    generate_last_wal_record_crossing_segment(&mut srv.connect_with_timeout()?)?;
    srv.kill();
    Ok(())
}
