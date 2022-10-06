use clap::{App, Arg};
use futures::TryFutureExt;
use pageserver::{page_service::PagestreamFeMessage, repository::Key};

use std::{collections::{BTreeMap, BTreeSet, HashMap}, ops::Range, path::PathBuf};
use std::io::Write;
use std::{
    fs::{read_dir, File},
    io::BufReader,
    str::FromStr,
};
use svg_fmt::*;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
    pq_proto::{BeMessage, FeMessage},
};

fn analyze<T: Ord + Copy>(coords: Vec<T>) -> (usize, BTreeMap<T, usize>) {
    let set: BTreeSet<T> = coords.into_iter().collect();

    let mut map: BTreeMap<T, usize> = BTreeMap::new();
    for (i, e) in set.iter().enumerate() {
        map.insert(*e, i);
    }

    (set.len(), map)
}

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


    let mut dump_file = File::create("dump.txt").expect("can't make file");
    let mut deltas = HashMap::<i32, u32>::new();
    let mut prev1: u32 = 0;
    let mut prev2: u32 = 0;
    let mut prev3: u32 = 0;

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
                        PagestreamFeMessage::GetPage(req) => {
                            writeln!(&mut dump_file, "{} {} {}", req.rel, req.blkno, req.lsn)?;
                            // dots.push((req.blkno, req.lsn));
                            // HACK
                            dots.push((req.blkno, Lsn::from(dots.len() as u64)));

                            let delta1 = (req.blkno as i32) - (prev1 as i32);
                            let delta2 = (req.blkno as i32) - (prev2 as i32);
                            let delta3 = (req.blkno as i32) - (prev3 as i32);
                            let mut delta = if i32::abs(delta1) < i32::abs(delta2) {
                                delta1
                            } else {
                                delta2
                            };
                            if i32::abs(delta3) < i32::abs(delta) {
                                delta = delta3;
                            }
                            let delta = delta1;

                            prev3 = prev2;
                            prev2 = prev1;
                            prev1 = req.blkno;

                            match deltas.get_mut(&delta) {
                                Some(c) => {*c += 1;},
                                None => {deltas.insert(delta, 1);},
                            };

                            if delta == 9 {
                                println!("{} {} {} {}", dots.len(), req.rel, req.blkno, req.lsn);
                            }
                        },
                        PagestreamFeMessage::DbSize(_) => {}
                    };

                    // HACK
                    // if dots.len() > 1000 {
                        // break;
                    // }
                }
            }
        }
    }

    let mut other = deltas.len();
    deltas.retain(|_, count| *count > 3);
    other -= deltas.len();
    dbg!(other);
    dbg!(deltas);

    // Collect all coordinates
    let mut keys: Vec<u32> = vec![];
    let mut lsns: Vec<Lsn> = vec![];
    for dot in &dots {
        keys.push(dot.0);
        lsns.push(dot.1);
    }

    // Analyze
    let (key_max, key_map) = analyze(keys);
    let (lsn_max, lsn_map) = analyze(lsns);

    // Draw
    println!("drawing trace ...");
    let mut svg_file = File::create("out.svg").expect("can't make file");
    writeln!(
        &mut svg_file,
        "{}",
        BeginSvg {
            w: (key_max + 1) as f32,
            h: (lsn_max + 1) as f32,
        }
    )?;
    for (key, lsn) in &dots {
        let key = key_map.get(&key).unwrap();
        let lsn = lsn_map.get(&lsn).unwrap();
        writeln!(
            &mut svg_file,
            "    {}",
            rectangle(
                *key as f32,
                *lsn as f32,
                10.0,
                10.0
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
