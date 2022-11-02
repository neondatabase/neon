all: 1.svg 2.svg 3.svg 4.svg 1.png 2.png 3.png 4.png

../../target/debug/tenant_size_model: Cargo.toml src/main.rs src/lib.rs
	cargo build --bin tenant_size_model

%.svg: %.dot
	dot -Tsvg $< > $@

%.png: %.dot
	dot -Tpng $< > $@

%.dot: ../../target/debug/tenant_size_model
	../../target/debug/tenant_size_model $* > $@
