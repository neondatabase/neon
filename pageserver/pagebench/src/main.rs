use clap::Parser;

pub(crate) mod util;

mod basebackup;
mod getpage_latest_lsn;

/// Component-level performance test for pageserver.
#[derive(clap::Parser)]
enum Args {
    Basebackup(basebackup::Args),
    GetPageLatestLsn(getpage_latest_lsn::Args),
}

fn main() {
    let args = Args::parse();
    match args {
        Args::Basebackup(args) => basebackup::main(args),
        Args::GetPageLatestLsn(args) => getpage_latest_lsn::main(args),
    }
    .unwrap()
}
