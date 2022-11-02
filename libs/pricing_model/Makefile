all: 1.svg 2.svg 3.svg 4.svg 1.png 2.png 3.png 4.png

target/debug/neon-price-simulator: Cargo.toml src/main.rs
	cargo build

%.svg: %.dot
	dot -Tsvg $< > $@

%.png: %.dot
	dot -Tpng $< > $@

%.dot: target/debug/neon-price-simulator
	./target/debug/neon-price-simulator $* > $@
