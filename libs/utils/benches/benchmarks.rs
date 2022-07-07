use criterion::{criterion_group, criterion_main, Criterion};

use utils::pg_checksum_page::pg_checksum_page;
use utils::zid;

pub fn bench_zid_stringify(c: &mut Criterion) {
    // Can only use public methods.
    let ztl = zid::ZTenantTimelineId::generate();

    c.bench_function("zid.to_string", |b| {
        b.iter(|| {
            // FIXME measurement overhead?
            //for _ in 0..1000 {
            //    ztl.tenant_id.to_string();
            //}
            ztl.tenant_id.to_string();
        })
    });
}

// NB: adding `black_box` around arguments doesn't seem to change anything.
pub fn pg_checksum_page_basic(c: &mut Criterion) {
    const BLCKSZ: usize = 8192;
    let mut page: [u8; BLCKSZ] = [0; BLCKSZ];
    for (i, byte) in page.iter_mut().enumerate().take(BLCKSZ) {
        *byte = i as u8;
    }

    c.bench_function("pg_checksum_page_basic", |b| {
        b.iter(|| {
            unsafe { pg_checksum_page(&page[..], 0) };
        })
    });
}

criterion_group!(benches, pg_checksum_page_basic, bench_zid_stringify);
criterion_main!(benches);
