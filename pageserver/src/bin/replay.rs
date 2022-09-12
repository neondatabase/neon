use std::{
    fs::{read_dir, File},
    io::BufReader,
    path::PathBuf,
    str::FromStr,
};

use clap::{App, Arg};
use utils::zid::{ZTenantId, ZTimelineId};

async fn replay_trace<R: std::io::Read>(reader: R) -> anyhow::Result<()> {
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // TODO upgrade to struct macro arg parsing
    let arg_matches = App::new("Pageserver trace replay tool")
        .about("Replays wal or read traces to test pageserver performance")
        .arg(
            Arg::new("traces_dir")
                .takes_value(true)
                .help("Directory where the read traces are stored"),
        )
        .get_matches();

    let traces_dir = PathBuf::from(arg_matches.value_of("traces_dir").unwrap());
    for tenant_dir in read_dir(traces_dir)? {
        let entry = tenant_dir?;
        let path = entry.path();
        let _tenant_id = ZTenantId::from_str(path.file_name().unwrap().to_str().unwrap())?;

        for timeline_dir in read_dir(path)? {
            let entry = timeline_dir?;
            let path = entry.path();
            let _timeline_id = ZTimelineId::from_str(path.file_name().unwrap().to_str().unwrap())?;

            for trace_dir in read_dir(path)? {
                let entry = trace_dir?;
                let path = entry.path();
                let _conn_id = ZTimelineId::from_str(path.file_name().unwrap().to_str().unwrap())?;

                let file = File::open(path)?;
                let reader = BufReader::new(file);
                replay_trace(reader).await?;
            }
        }
    }

    Ok(())
}
