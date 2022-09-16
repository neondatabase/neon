use clap::{App, Arg};
use pageserver::{page_service::PagestreamFeMessage, repository::Key};
use std::io::Write;
use std::{
    fs::{read_dir, File},
    io::BufReader,
    path::PathBuf,
    str::FromStr,
};
use svg_fmt::*;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
    pq_proto::{BeMessage, FeMessage},
};

fn main() -> anyhow::Result<()> {
    // TODO upgrade to struct macro arg parsing
    let arg_matches = App::new("Pageserver trace visualization tool")
        .about("Makes a svg file that displays the read pattern")
        .arg(
            Arg::new("traces_dir")
                .takes_value(true)
                .help("Directory where the read traces are stored"),
        )
        .get_matches();

    // (blkno, lsn)
    let mut dots = Vec::<(u32, Lsn)>::new();

    println!("scanning trace ...");
    let traces_dir = PathBuf::from(arg_matches.value_of("traces_dir").unwrap());
    for tenant_dir in read_dir(traces_dir)? {
        let entry = tenant_dir?;
        let path = entry.path();
        // let tenant_id = TenantId::from_str(path.file_name().unwrap().to_str().unwrap())?;
        // println!("tenant: {tenant_id}");

        println!("opening {path:?}");
        for timeline_dir in read_dir(path)? {
            let entry = timeline_dir?;
            let path = entry.path();
            // let timeline_id = TimelineId::from_str(path.file_name().unwrap().to_str().unwrap())?;
            // println!("hi");
            // println!("timeline: {timeline_id}");

            println!("opening {path:?}");
            for trace_dir in read_dir(path)? {
                let entry = trace_dir?;
                let path = entry.path();
                // let _conn_id = TimelineId::from_str(path.file_name().unwrap().to_str().unwrap())?;

                println!("opening {path:?}");
                let file = File::open(path.clone())?;
                let mut reader = BufReader::new(file);
                while let Ok(msg) = PagestreamFeMessage::parse(&mut reader) {
                    // println!("Parsed message {:?}", msg);
                    match msg {
                        PagestreamFeMessage::Exists(_) => {}
                        PagestreamFeMessage::Nblocks(_) => {}
                        PagestreamFeMessage::GetPage(req) => dots.push((req.blkno, req.lsn)),
                        PagestreamFeMessage::DbSize(_) => {}
                    };

                    // HACK
                    if dots.len() > 100 {
                        break;
                    }
                }
            }
        }
    }

    // Analyze
    let blkno_max = (&dots).into_iter().map(|(blkno, _)| blkno).max().unwrap();
    let blkno_min = (&dots).into_iter().map(|(blkno, _)| blkno).min().unwrap();
    let lsn_max = (&dots).into_iter().map(|(_, lsn)| lsn).max().unwrap();
    let lsn_min = (&dots).into_iter().map(|(_, lsn)| lsn).min().unwrap();

    // Draw
    println!("drawing trace ...");
    let mut svg_file = File::create("out.svg").expect("can't make file");
    writeln!(
        &mut svg_file,
        "{}",
        BeginSvg {
            w: (blkno_max - blkno_min + 1) as f32,
            h: (lsn_max.0 - lsn_min.0 + 1) as f32,
        }
    )?;
    for dot in &dots {
        writeln!(
            &mut svg_file,
            "    {}",
            rectangle(
                (dot.0 - blkno_min) as f32,
                (dot.1 .0 - lsn_min.0) as f32,
                1.0,
                1.0
            )
            .fill(Fill::Color(red()))
            .stroke(Stroke::Color(black(), 0.0))
            .border_radius(0.5)
        )?;
        // println!("    {}",
        // rectangle(key_start as f32 + stretch * margin,
        // stretch * (lsn_max as f32 - 1.0 - (lsn_start as f32 + margin - lsn_offset)),
        // key_diff as f32 - stretch * 2.0 * margin,
        // stretch * (lsn_diff - 2.0 * margin))
        // // .fill(rgb(200, 200, 200))
        // .fill(fill)
        // .stroke(Stroke::Color(rgb(200, 200, 200), 0.1))
        // .border_radius(0.4)
        // );
    }

    writeln!(&mut svg_file, "{}", EndSvg)?;

    Ok(())
}
