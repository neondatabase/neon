//! Tenant size model tests.

use tenant_size_model::{Segment, SizeResult, StorageModel};

use std::collections::HashMap;

struct ScenarioBuilder {
    segments: Vec<Segment>,

    /// Mapping from the branch name to the index of a segment describing its latest state.
    branches: HashMap<String, usize>,
}

impl ScenarioBuilder {
    /// Creates a new storage with the given default branch name.
    pub fn new(initial_branch: &str) -> ScenarioBuilder {
        let init_segment = Segment {
            parent: None,
            lsn: 0,
            size: Some(0),
            needed: false, // determined later
        };

        ScenarioBuilder {
            segments: vec![init_segment],
            branches: HashMap::from([(initial_branch.into(), 0)]),
        }
    }

    /// Advances the branch with the named operation, by the relative LSN and logical size bytes.
    pub fn modify_branch(&mut self, branch: &str, lsn_bytes: u64, size_bytes: i64) {
        let lastseg_id = *self.branches.get(branch).unwrap();
        let newseg_id = self.segments.len();
        let lastseg = &mut self.segments[lastseg_id];

        let newseg = Segment {
            parent: Some(lastseg_id),
            lsn: lastseg.lsn + lsn_bytes,
            size: Some((lastseg.size.unwrap() as i64 + size_bytes) as u64),
            needed: false,
        };

        self.segments.push(newseg);
        *self.branches.get_mut(branch).expect("read already") = newseg_id;
    }

    pub fn insert(&mut self, branch: &str, bytes: u64) {
        self.modify_branch(branch, bytes, bytes as i64);
    }

    pub fn update(&mut self, branch: &str, bytes: u64) {
        self.modify_branch(branch, bytes, 0i64);
    }

    pub fn _delete(&mut self, branch: &str, bytes: u64) {
        self.modify_branch(branch, bytes, -(bytes as i64));
    }

    /// Panics if the parent branch cannot be found.
    pub fn branch(&mut self, parent: &str, name: &str) {
        // Find the right segment
        let branchseg_id = *self
            .branches
            .get(parent)
            .expect("should had found the parent by key");
        let _branchseg = &mut self.segments[branchseg_id];

        // Create branch name for it
        self.branches.insert(name.to_string(), branchseg_id);
    }

    pub fn calculate(&mut self, retention_period: u64) -> (StorageModel, SizeResult) {
        // Phase 1: Mark all the segments that need to be retained
        for (_branch, &last_seg_id) in self.branches.iter() {
            let last_seg = &self.segments[last_seg_id];
            let cutoff_lsn = last_seg.lsn.saturating_sub(retention_period);
            let mut seg_id = last_seg_id;
            loop {
                let seg = &mut self.segments[seg_id];
                if seg.lsn <= cutoff_lsn {
                    break;
                }
                seg.needed = true;
                if let Some(prev_seg_id) = seg.parent {
                    seg_id = prev_seg_id;
                } else {
                    break;
                }
            }
        }

        // Perform the calculation
        let storage_model = StorageModel {
            segments: self.segments.clone(),
        };
        let size_result = storage_model.calculate();
        (storage_model, size_result)
    }
}

// Main branch only. Some updates on it.
#[test]
fn scenario_1() {
    // Create main branch
    let mut scenario = ScenarioBuilder::new("main");

    // Bulk load 5 GB of data to it
    scenario.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    // Calculate the synthetic size with retention horizon 1000
    let (_model, result) = scenario.calculate(1000);

    // The end of the branch is at LSN 10000. Need to retain
    // a logical snapshot at LSN 9000, plus the WAL between 9000-10000.
    // The logical snapshot has size 5000.
    assert_eq!(result.total_size, 5000 + 1000);
}

// Main branch only. Some updates on it.
#[test]
fn scenario_2() {
    // Create main branch
    let mut scenario = ScenarioBuilder::new("main");

    // Bulk load 5 GB of data to it
    scenario.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    // Branch
    scenario.branch("main", "child");
    scenario.update("child", 1_000);

    // More updates on parent
    scenario.update("main", 1_000);

    //
    // The history looks like this now:
    //
    //         10000          11000
    // *----*----*--------------*    main
    //           |
    //           |            11000
    //           +--------------     child
    //
    //
    // With retention horizon 1000, we need to retain logical snapshot
    // at the branch point, size 5000, and the WAL from 10000-11000 on
    // both branches.
    let (_model, result) = scenario.calculate(1000);

    assert_eq!(result.total_size, 5000 + 1000 + 1000);
}

// Like 2, but more updates on main
#[test]
fn scenario_3() {
    // Create main branch
    let mut scenario = ScenarioBuilder::new("main");

    // Bulk load 5 GB of data to it
    scenario.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    // Branch
    scenario.branch("main", "child");
    scenario.update("child", 1_000);

    // More updates on parent
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    //
    // The history looks like this now:
    //
    //         10000                                 15000
    // *----*----*------------------------------------*    main
    //           |
    //           |            11000
    //           +--------------     child
    //
    //
    // With retention horizon 1000, it's still cheapest to retain
    // - snapshot at branch point (size 5000)
    // - WAL on child between 10000-11000
    // - WAL on main between 10000-15000
    //
    // This is in total 5000 + 1000 + 5000
    //
    let (_model, result) = scenario.calculate(1000);

    assert_eq!(result.total_size, 5000 + 1000 + 5000);
}

// Diverged branches
#[test]
fn scenario_4() {
    // Create main branch
    let mut scenario = ScenarioBuilder::new("main");

    // Bulk load 5 GB of data to it
    scenario.insert("main", 5_000);

    // Stream of updates
    for _ in 0..5 {
        scenario.update("main", 1_000);
    }

    // Branch
    scenario.branch("main", "child");
    scenario.update("child", 1_000);

    // More updates on parent
    for _ in 0..8 {
        scenario.update("main", 1_000);
    }

    //
    // The history looks like this now:
    //
    //         10000                                 18000
    // *----*----*------------------------------------*    main
    //           |
    //           |            11000
    //           +--------------     child
    //
    //
    // With retention horizon 1000, it's now cheapest to retain
    // separate snapshots on both branches:
    // - snapshot on main branch at LSN 17000 (size 5000)
    // - WAL on main between 17000-18000
    // - snapshot on child branch at LSN 10000 (size 5000)
    // - WAL on child between 10000-11000
    //
    // This is in total 5000 + 1000 + 5000 + 1000 = 12000
    //
    // (If we used the method from the previous scenario, and
    // kept only snapshot at the branch point, we'd need to keep
    // all the WAL between 10000-18000 on the main branch, so
    // the total size would be 5000 + 1000 + 8000 = 14000. The
    // calculation always picks the cheapest alternative)

    let (_model, result) = scenario.calculate(1000);

    assert_eq!(result.total_size, 5000 + 1000 + 5000 + 1000);
}

#[test]
fn scenario_5() {
    let mut scenario = ScenarioBuilder::new("a");
    scenario.insert("a", 5000);
    scenario.branch("a", "b");
    scenario.update("b", 4000);
    scenario.update("a", 2000);
    scenario.branch("a", "c");
    scenario.insert("c", 4000);
    scenario.insert("a", 2000);

    let (_model, result) = scenario.calculate(1000);

    assert_eq!(result.total_size, 17000);
}

#[test]
fn scenario_6() {
    let branches = [
        "7ff1edab8182025f15ae33482edb590a",
        "b1719e044db05401a05a2ed588a3ad3f",
        "0xb68d6691c895ad0a70809470020929ef",
    ];

    // compared to other scenarios, this one uses bytes instead of kB

    let mut scenario = ScenarioBuilder::new("");

    scenario.branch("", branches[0]); // at 0
    scenario.modify_branch(branches[0], 108951064, 43696128); // at 108951064
    scenario.branch(branches[0], branches[1]); // at 108951064
    scenario.modify_branch(branches[1], 15560408, -1851392); // at 124511472
    scenario.modify_branch(branches[0], 174464360, -1531904); // at 283415424
    scenario.branch(branches[0], branches[2]); // at 283415424
    scenario.modify_branch(branches[2], 15906192, 8192); // at 299321616
    scenario.modify_branch(branches[0], 18909976, 32768); // at 302325400

    let (model, result) = scenario.calculate(100_000);

    // FIXME: We previously calculated 333_792_000. But with this PR, we get
    // a much lower number. At a quick look at the model output and the
    // calculations here, the new result seems correct to me.
    eprintln!(
        " MODEL: {}",
        serde_json::to_string(&model.segments).unwrap()
    );
    eprintln!(
        "RESULT: {}",
        serde_json::to_string(&result.segments).unwrap()
    );

    assert_eq!(result.total_size, 136_236_928);
}
