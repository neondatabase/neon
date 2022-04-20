#![allow(unused)]

use criterion::{criterion_group, criterion_main, Criterion};
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

criterion_group!(benches, bench_zid_stringify);
criterion_main!(benches);
