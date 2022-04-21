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
                .required(true)
        )
        .arg(
            Arg::new("pg-distrib-dir")
                .long("pg-distrib-dir")
                .takes_value(true)
                .help("Directory with Postgres distribution (bin and lib directories, e.g. tmp_install)")
                .default_value("/usr/local")
        )
        .arg(
            Arg::new("type")
                .long("type")
                .takes_value(true)
                .help("Type of WAL to generate")
                .possible_values(["simple", "last_wal_record_crossing_segment", "wal_record_crossing_segment_followed_by_small_one"])
                .required(true)
        )
        .get_matches();

    let cfg = Conf {
        pg_distrib_dir: arg_matches.value_of("pg-distrib-dir").unwrap().into(),
        datadir: arg_matches.value_of("datadir").unwrap().into(),
    };
    cfg.initdb()?;
    let mut srv = cfg.start_server()?;
    let lsn = match arg_matches.value_of("type").unwrap() {
        "simple" => generate_simple(&mut srv.connect_with_timeout()?)?,
        "last_wal_record_crossing_segment" => {
            generate_last_wal_record_crossing_segment(&mut srv.connect_with_timeout()?)?
        }
        "wal_record_crossing_segment_followed_by_small_one" => {
            generate_wal_record_crossing_segment_followed_by_small_one(
                &mut srv.connect_with_timeout()?,
            )?
        }
        a => panic!("Unknown --type argument: {}", a),
    };
    println!("end_of_wal = {}", lsn);
    srv.kill();
    Ok(())
}
