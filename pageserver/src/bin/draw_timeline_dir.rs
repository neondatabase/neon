use svg_fmt::*;
use clap::{App, Arg};
use anyhow::Result;
use std::{collections::{BTreeMap, BTreeSet}, ops::Range, path::PathBuf};
use utils::{lsn::Lsn, project_git_version};
use pageserver::{layered_repository::get_range, repository::{Key, key_range_size}};

project_git_version!(GIT_VERSION);


fn analyze<T: Ord + Copy>(coords: Vec<T>) -> (usize, BTreeMap<T, usize>) {
    let set: BTreeSet<T> = coords.into_iter().collect();

    let mut map: BTreeMap<T, usize> = BTreeMap::new();
    for (i, e) in set.iter().enumerate() {
        map.insert(*e, i);
    }

    (set.len(), map)
}

fn main() -> Result<()> {
    let arg_matches = App::new("Neon draw_timeline_dir utility")
        .about("Draws the domains of the image and delta layers in a directory")
        .version(GIT_VERSION)
        .arg(
            Arg::new("path")
                .help("Path to timeline directory")
                .required(true)
                .index(1),
        )
        .get_matches();

    // Get ranges
    let mut ranges: Vec<(Range<Key>, Range<Lsn>)> = vec![];
    let timeline_path = PathBuf::from(arg_matches.value_of("path").unwrap());
    for entry in std::fs::read_dir(timeline_path).unwrap() {
        let entry = entry?;
        let path: PathBuf = entry.path();

        if let Ok(range) = get_range(&path) {
            ranges.push(range);
        }
    }

    let mut sum: u64 = 0;
    let mut count = 0;

    // Collect all coordinates
    let mut keys: Vec<Key> = vec![];
    let mut lsns: Vec<Lsn> = vec![];
    for (keyr, lsnr) in &ranges {
        keys.push(keyr.start);
        keys.push(keyr.end);
        lsns.push(lsnr.start);
        lsns.push(lsnr.end);

        sum += key_range_size(keyr) as u64;
        count += 1;
    }

    let ave = sum / count;
    eprintln!("average size: {}", ave);

    // Analyze
    let (key_max, key_map) = analyze(keys);
    let (lsn_max, lsn_map) = analyze(lsns);


    // TODO
    // 1. Why is there empty space below deltas?
    // - does image happen at LSN **after** last delta?
    // 2. Why are images overriding each other?
    // 4. Am I hiding rectangles behind others?
    // 5. Why does it start with images, not deltas?
    // 6. Time flows down. Does that make sense?
    // 7. Why so many deltas at the same time? Compaction?

    // Initialize stats
    let mut num_deltas = 0;
    let mut num_images = 0;

    // Draw
    let stretch = 3.0;
    println!("{}", BeginSvg { w: key_max as f32, h: stretch * lsn_max as f32 });
    for (keyr, lsnr) in &ranges {
        let key_start = *key_map.get(&keyr.start).unwrap();
        let key_end = *key_map.get(&keyr.end).unwrap();
        let key_diff = key_end - key_start;

        if key_start >= key_end {
            panic!("AAA");
        }

        let lsn_start = *lsn_map.get(&lsnr.start).unwrap();
        let lsn_end = *lsn_map.get(&lsnr.end).unwrap();

        let mut lsn_diff = (lsn_end - lsn_start) as f32;
        let mut fill = Fill::None;
        let mut margin = 0.05 * lsn_diff;
        let mut lsn_offset = 0.0;
        if lsn_start == lsn_end {
            num_images += 1;
            lsn_diff = 0.3;
            lsn_offset = lsn_diff * 2.0;
            margin = 0.05;
            fill = Fill::Color(rgb(200, 200, 200));
        } else if lsn_start < lsn_end {
            num_deltas += 1;
            // fill = Fill::Color(rgb(200, 200, 200));
        } else {
            panic!("AAA");
        }

        println!("    {}",
            rectangle(key_start as f32 + stretch * margin,
                      stretch * (lsn_max as f32 - 1.0 - (lsn_start as f32 + margin - lsn_offset)),
                      key_diff as f32 - stretch * 2.0 * margin,
                      stretch * (lsn_diff - 2.0 * margin))
                // .fill(rgb(200, 200, 200))
                .fill(fill)
                .stroke(Stroke::Color(rgb(200, 200, 200), 0.1))
                .border_radius(0.4)
        );
    }
    println!("{}", EndSvg);

    eprintln!("num_images: {}", num_images);
    eprintln!("num_deltas: {}", num_deltas);

    Ok(())
}
