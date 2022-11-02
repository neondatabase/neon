//! Pricing model related to kept history size testing ground.
//!
//! Has a number of scenarios and a `main` for invoking these by number, calculating the history
//! size or price proxy, outputs graphviz graph. Makefile in directory shows how to use graphviz to
//! turn scenarios into pngs.

use pricing_model::{Segment, SegmentPrice, Storage};

// Main branch only. Some updates on it.
fn scenario_1() -> (Vec<Segment>, SegmentPrice) {
    // Create main branch
    let mut storage = Storage::new("main");

    // Bulk load 5 GB of data to it
    storage.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        storage.update("main", 1_000);
    }

    let price = storage.price(1000);

    (storage.into_segments(), price)
}

// Main branch only. Some updates on it.
fn scenario_2() -> (Vec<Segment>, SegmentPrice) {
    // Create main branch
    let mut storage = Storage::new("main");

    // Bulk load 5 GB of data to it
    storage.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        storage.update("main", 1_000);
    }

    // Branch
    storage.branch("main", "child");
    storage.update("child", 1_000);

    // More updates on parent
    storage.update("main", 1_000);

    let price = storage.price(1000);

    (storage.into_segments(), price)
}

// Like 2, but more updates on main
fn scenario_3() -> (Vec<Segment>, SegmentPrice) {
    // Create main branch
    let mut storage = Storage::new("main");

    // Bulk load 5 GB of data to it
    storage.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        storage.update("main", 1_000);
    }

    // Branch
    storage.branch("main", "child");
    storage.update("child", 1_000);

    // More updates on parent
    for _ in 0..5 {
        storage.update("main", 1_000);
    }

    let price = storage.price(1000);

    (storage.into_segments(), price)
}

// Diverged branches
fn scenario_4() -> (Vec<Segment>, SegmentPrice) {
    // Create main branch
    let mut storage = Storage::new("main");

    // Bulk load 5 GB of data to it
    storage.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        storage.update("main", 1_000);
    }

    // Branch
    storage.branch("main", "child");
    storage.update("child", 1_000);

    // More updates on parent
    for _ in 0..8 {
        storage.update("main", 1_000);
    }

    let price = storage.price(1000);

    (storage.into_segments(), price)
}

fn scenario_5() -> (Vec<Segment>, SegmentPrice) {
    let mut storage = Storage::new("a");
    storage.insert("a", 5000);
    storage.branch("a", "b");
    storage.update("b", 4000);
    storage.update("a", 2000);
    storage.branch("a", "c");
    storage.insert("c", 4000);
    storage.insert("a", 2000);

    let price = storage.price(5000);

    (storage.into_segments(), price)
}

fn scenario_6() -> (Vec<Segment>, SegmentPrice) {
    use std::borrow::Cow;

    const NO_OP: Cow<'static, str> = Cow::Borrowed("");

    let branches = [
        Some(0x7ff1edab8182025f15ae33482edb590a_u128),
        Some(0xb1719e044db05401a05a2ed588a3ad3f),
        Some(0xb68d6691c895ad0a70809470020929ef),
    ];

    // compared to other scenarios, this one uses bytes instead of kB

    let mut storage = Storage::new(None);

    storage.branch(&None, branches[0]); // at 0
    storage.modify_branch(&branches[0], NO_OP, 108951064, 43696128); // at 108951064
    storage.branch(&branches[0], branches[1]); // at 108951064
    storage.modify_branch(&branches[1], NO_OP, 15560408, -1851392); // at 124511472
    storage.modify_branch(&branches[0], NO_OP, 174464360, -1531904); // at 283415424
    storage.branch(&branches[0], branches[2]); // at 283415424
    storage.modify_branch(&branches[2], NO_OP, 15906192, 8192); // at 299321616
    storage.modify_branch(&branches[0], NO_OP, 18909976, 32768); // at 302325400

    let price = storage.price(100_000);

    (storage.into_segments(), price)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let scenario = if args.len() < 2 { "1" } else { &args[1] };

    let (segments, price) = match scenario {
        "1" => scenario_1(),
        "2" => scenario_2(),
        "3" => scenario_3(),
        "4" => scenario_4(),
        "5" => scenario_5(),
        "6" => scenario_6(),
        other => {
            eprintln!("invalid scenario {}", other);
            std::process::exit(1);
        }
    };

    graphviz_price_tree(&segments, &price);
}

fn graphviz_price_recurse(segments: &[Segment], node: &SegmentPrice) {
    use pricing_model::SegmentMethod::*;

    let seg_id = node.seg_id;
    let seg = segments.get(seg_id).unwrap();
    let lsn = seg.end_lsn;
    let size = seg.end_size;
    let method = node.method;

    println!("  {{");
    println!("    node [width=0.1 height=0.1 shape=oval]");

    let price = node.total_children();

    let penwidth = if seg.needed { 6 } else { 3 };
    let x = match method {
        SnapshotAfter =>
            format!("label=\"lsn: {lsn}\\nsize: {size}\\nprice: {price}\" style=filled penwidth={penwidth}"),
        Wal =>
            format!("label=\"lsn: {lsn}\\nsize: {size}\\nprice: {price}\" color=\"black\" penwidth={penwidth}"),
        WalNeeded =>
            format!("label=\"lsn: {lsn}\\nsize: {size}\\nprice: {price}\" color=\"black\" penwidth={penwidth}"),
        Skipped =>
            format!("label=\"lsn: {lsn}\\nsize: {size}\\nprice: {price}\" color=\"gray\" penwidth={penwidth}"),
    };

    println!("    \"seg{seg_id}\" [{x}]");
    println!("  }}");

    // Recurse. Much of the data is actually on the edge
    for child in node.children.iter() {
        let child_id = child.seg_id;
        graphviz_price_recurse(segments, child);

        let edge_color = match child.method {
            SnapshotAfter => "gray",
            Wal => "black",
            WalNeeded => "black",
            Skipped => "gray",
        };

        println!("  {{");
        println!("    edge [] ");
        print!("    \"seg{seg_id}\" -> \"seg{child_id}\" [");
        print!("color={edge_color}");
        if child.method == WalNeeded {
            print!(" penwidth=6");
        }
        if child.method == Wal {
            print!(" penwidth=3");
        }

        let next = segments.get(child_id).unwrap();

        if next.op.is_empty() {
            print!(
                " label=\"{} / {}\"",
                next.end_lsn - seg.end_lsn,
                (next.end_size as i128 - seg.end_size as i128)
            );
        } else {
            print!(" label=\"{}: {}\"", next.op, next.end_lsn - seg.end_lsn);
        }
        println!("]");
        println!("  }}");
    }
}

fn graphviz_price_tree(segments: &[Segment], tree: &SegmentPrice) {
    println!("digraph G {{");
    println!("  fontname=\"Helvetica,Arial,sans-serif\"");
    println!("  node [fontname=\"Helvetica,Arial,sans-serif\"]");
    println!("  edge [fontname=\"Helvetica,Arial,sans-serif\"]");
    println!("  graph [center=1 rankdir=LR]");
    println!("  edge [dir=none]");

    graphviz_price_recurse(segments, tree);

    println!("}}");
}

#[test]
fn scenarios_return_same_price() {
    type ScenarioFn = fn() -> (Vec<Segment>, SegmentPrice);
    let truths: &[(u32, ScenarioFn, _)] = &[
        (line!(), scenario_1, 8000),
        (line!(), scenario_2, 9000),
        (line!(), scenario_3, 13000),
        (line!(), scenario_4, 16000),
        (line!(), scenario_5, 17000),
        (line!(), scenario_6, 333_792_000),
    ];

    for (line, scenario, expected) in truths {
        let (_, price) = scenario();
        assert_eq!(*expected, price.total_children(), "scenario on line {line}");
    }
}
