use clap::Parser;

mod getpage_latest_lsn;

mod linear_histo;

/// Component-level performance test for pageserver.
#[derive(clap::Parser)]
enum Args {
    GetPageLatestLsn(getpage_latest_lsn::Args),
}

fn main() {
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let jh = match args {
        Args::GetPageLatestLsn(args) => rt.spawn(getpage_latest_lsn::main(args)),
    };
    rt.block_on(jh).unwrap().unwrap();
}
