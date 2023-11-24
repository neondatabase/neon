use clap::Parser;

mod getpage_latest_lsn;

/// Component-level performance test for pageserver.
#[derive(clap::Parser)]
enum Args {
    GetPageLatestLsn(getpage_latest_lsn::Args),
}

fn main() {
    let args = Args::parse();
    match args {
        Args::GetPageLatestLsn(args) => getpage_latest_lsn::main(args),
    }
    .unwrap()
}
