use neon_shmem::hash::HashMapAccess;
use neon_shmem::hash::HashMapInit;
use neon_shmem::hash::entry::Entry;
use rand::prelude::*;
use rand::distr::{Distribution, StandardUniform};
use plotters::prelude::*;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
#[repr(C)]
pub struct FileCacheKey {
    pub _spc_id: u32,
    pub _db_id: u32,
    pub _rel_number: u32,
    pub _fork_num: u32,
    pub _block_num: u32,
}

impl Distribution<FileCacheKey> for StandardUniform {
	// questionable, but doesn't need to be good randomness
	fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> FileCacheKey {
		FileCacheKey {
			_spc_id: rng.random(),
			_db_id: rng.random(),
			_rel_number: rng.random(),
			_fork_num: rng.random(),
			_block_num: rng.random()
		}
    }
}

#[derive(Clone, Debug)]
#[repr(C)]
pub struct FileCacheEntry {
    pub _offset: u32,
    pub _access_count: u32,
    pub _prev: *mut FileCacheEntry,
    pub _next: *mut FileCacheEntry,
    pub _state: [u32; 8],
}

impl FileCacheEntry {
	fn dummy() -> Self {
		Self {
			_offset: 0,
			_access_count: 0,
			_prev: std::ptr::null_mut(),
			_next: std::ptr::null_mut(),
			_state: [0; 8]				
		}
	}
}

// Utilities for applying operations.

#[derive(Clone, Debug)]
struct TestOp<K,V>(K, Option<V>);

fn apply_op<K: Clone + std::hash::Hash + Eq, V, S: std::hash::BuildHasher>(
    op: TestOp<K,V>,
    map: &mut HashMapAccess<K,V,S>,
) {
	let hash = map.get_hash_value(&op.0);
	let entry = map.entry_with_hash(op.0, hash);

    match op.1 {
		Some(new) => {
			match entry {
				Entry::Occupied(mut e) => Some(e.insert(new)),
				Entry::Vacant(e) => { e.insert(new).unwrap(); None },
			}
		},
		None => {
			match entry {
				Entry::Occupied(e) => Some(e.remove()),
				Entry::Vacant(_) => None,
			}
		},
	};
}

#[cfg(feature = "stats")]
fn main() {
	let ideal_filled = 16_000_000;
	let size = 20_000_000;
	let mut writer = HashMapInit::new_resizeable(size, size).attach_writer();
	let mut rng = rand::rng();		
	while writer.get_num_buckets_in_use() < ideal_filled as usize {
		let key: FileCacheKey = rng.random();
		let val = FileCacheEntry::dummy();
		apply_op(TestOp(key, Some(val)), &mut writer);
	}
	println!("Inserted {ideal_filled} entries into a map with capacity {size}.");
	let (distr, max) = writer.chain_distribution();

	let root_area = BitMapBackend::new("chain_distr.png", (800, 400))
		.into_drawing_area();
    root_area.fill(&WHITE).unwrap();
	
    let mut ctx = ChartBuilder::on(&root_area)
        .set_label_area_size(LabelAreaPosition::Left, 40)
        .set_label_area_size(LabelAreaPosition::Bottom, 40)
        .build_cartesian_2d((0..max).into_segmented(), (0..ideal_filled * 2).log_scale())
        .unwrap();

    ctx.configure_mesh()
		.y_label_formatter(&|y| format!("{:e}", y))
		.draw().unwrap();

    ctx.draw_series(
        Histogram::vertical(&ctx)
			.margin(10)
			.data(distr.iter().map(|x| (x.1, 1)))
    ).unwrap();

	// let root_area = BitMapBackend::new("dict_distr.png", (2000, 400))
	// 	.into_drawing_area();
    // root_area.fill(&WHITE).unwrap();
	
    // let mut ctx = ChartBuilder::on(&root_area)
    //     .set_label_area_size(LabelAreaPosition::Left, 40)
    //     .set_label_area_size(LabelAreaPosition::Bottom, 40)
    //     .build_cartesian_2d((0..writer.dict_len()), (0..(max as f32 * 1.5) as usize))
    //     .unwrap();

    // ctx.configure_mesh().draw().unwrap();

	// ctx.draw_series(LineSeries::new(
	// 	distr.iter().map(|(bin, count)| (*bin, *count)),
	// 	&RED,
	// )).unwrap();
	
	// println!("Longest chain: {}", writer.longest_chain());
}

#[cfg(not(feature = "stats"))]
fn main() {
	println!("Enable the `stats` feature to use this binary!");
}

