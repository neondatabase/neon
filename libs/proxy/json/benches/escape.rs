use std::hint::black_box;

use criterion::{Bencher, Criterion, criterion_group, criterion_main};

#[derive(Debug)]
#[allow(dead_code)]
struct Foo {
    some_field: Bar,
    some_other_field: String,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Bar {
    nested_fields: String,
    some_other_value: i32,
}

pub fn escape(c: &mut Criterion) {
    c.bench_function("small", |b| bench_json_encode_inner(b, "object_key"));
    c.bench_function("large_fmt", |b| {
        let value = Foo {
            some_field: Bar {
                nested_fields: "could not connect to database, control plane error \"foo bar\""
                    .to_string()
                    .to_string(),
                some_other_value: -1,
            },
            some_other_field: "error".to_string(),
        };

        bench_json_encode_inner(b, format_args!("{:?}", &value));
    });
}

criterion_group!(benches, escape);
criterion_main!(benches);

fn bench_json_encode_inner(b: &mut Bencher<'_>, v: impl json::ValueEncoder + Copy) {
    let mut output = Vec::new();

    // write it once so we don't alloc during the benchmark.
    json::ValueSer::new(&mut output).value(black_box(v));

    b.iter(|| {
        output.clear();
        json::ValueSer::new(&mut output).value(black_box(v));
        black_box(&output[..]);
    });
}
