use anyhow::*;
use clap::{value_parser, Arg, ArgMatches, Command};
use postgres::Client;
use std::{path::PathBuf, str::FromStr};
use wal_craft::*;

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("wal_craft=info"))
        .init();
    let arg_matches = cli().get_matches();

    let wal_craft = |arg_matches: &ArgMatches, client: &mut Client| {
        let intermediate_lsns = match arg_matches
            .get_one::<String>("type")
            .map(|s| s.as_str())
            .context("'type' is required")?
        {
            Simple::NAME => Simple::craft(client)?,
            LastWalRecordXlogSwitch::NAME => LastWalRecordXlogSwitch::craft(client)?,
            LastWalRecordXlogSwitchEndsOnPageBoundary::NAME => {
                LastWalRecordXlogSwitchEndsOnPageBoundary::craft(client)?
            }
            WalRecordCrossingSegmentFollowedBySmallOne::NAME => {
                WalRecordCrossingSegmentFollowedBySmallOne::craft(client)?
            }
            LastWalRecordCrossingSegment::NAME => LastWalRecordCrossingSegment::craft(client)?,
            a => panic!("Unknown --type argument: {a}"),
        };
        let end_of_wal_lsn = client.pg_current_wal_insert_lsn()?;
        for lsn in intermediate_lsns {
            println!("intermediate_lsn = {lsn}");
        }
        println!("end_of_wal = {end_of_wal_lsn}");
        Ok(())
    };

    match arg_matches.subcommand() {
        None => panic!("No subcommand provided"),
        Some(("print-postgres-config", _)) => {
            for cfg in REQUIRED_POSTGRES_CONFIG.iter() {
                println!("{cfg}");
            }
            Ok(())
        }

        Some(("with-initdb", arg_matches)) => {
            let cfg = Conf {
                pg_version: *arg_matches
                    .get_one::<u32>("pg-version")
                    .context("'pg-version' is required")?,
                pg_distrib_dir: arg_matches
                    .get_one::<PathBuf>("pg-distrib-dir")
                    .context("'pg-distrib-dir' is required")?
                    .to_owned(),
                datadir: arg_matches
                    .get_one::<PathBuf>("datadir")
                    .context("'datadir' is required")?
                    .to_owned(),
            };
            cfg.initdb()?;
            let srv = cfg.start_server()?;
            wal_craft(arg_matches, &mut srv.connect_with_timeout()?)?;
            srv.kill();
            Ok(())
        }
        Some(("in-existing", arg_matches)) => wal_craft(
            arg_matches,
            &mut postgres::Config::from_str(
                arg_matches
                    .get_one::<String>("connection")
                    .context("'connection' is required")?,
            )
            .context(
                "'connection' argument value could not be parsed as a postgres connection string",
            )?
            .connect(postgres::NoTls)?,
        ),
        Some(_) => panic!("Unknown subcommand"),
    }
}

fn cli() -> Command {
    let type_arg = &Arg::new("type")
        .help("Type of WAL to craft")
        .value_parser([
            Simple::NAME,
            LastWalRecordXlogSwitch::NAME,
            LastWalRecordXlogSwitchEndsOnPageBoundary::NAME,
            WalRecordCrossingSegmentFollowedBySmallOne::NAME,
            LastWalRecordCrossingSegment::NAME,
        ])
        .required(true);

    Command::new("Postgres WAL crafter")
        .about("Crafts Postgres databases with specific WAL properties")
        .subcommand(
            Command::new("print-postgres-config")
                .about("Print the configuration required for PostgreSQL server before running this script")
        )
        .subcommand(
            Command::new("with-initdb")
                .about("Craft WAL in a new data directory first initialized with initdb")
                .arg(type_arg)
                .arg(
                    Arg::new("datadir")
                        .help("Data directory for the Postgres server")
                        .value_parser(value_parser!(PathBuf))
                        .required(true)
                )
                .arg(
                    Arg::new("pg-distrib-dir")
                        .long("pg-distrib-dir")
                        .value_parser(value_parser!(PathBuf))
                        .help("Directory with Postgres distributions (bin and lib directories, e.g. pg_install containing subpath `v14/bin/postgresql`)")
                        .default_value("/usr/local")
                )
                .arg(
                    Arg::new("pg-version")
                    .long("pg-version")
                    .help("Postgres version to use for the initial tenant")
                    .value_parser(value_parser!(u32))
                    .required(true)

                )
        )
        .subcommand(
            Command::new("in-existing")
                .about("Craft WAL at an existing recently created Postgres database. Note that server may append new WAL entries on shutdown.")
                .arg(type_arg)
                .arg(
                    Arg::new("connection")
                        .help("Connection string to the Postgres database to populate")
                        .required(true)
                )
        )
}

#[test]
fn verify_cli() {
    cli().debug_assert();
}
