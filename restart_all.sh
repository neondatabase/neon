pkill -9 -f postgres
pkill -9 -f zenith

rm -rf .zenith

./target/debug/zenith init

./target/debug/zenith start

./target/debug/zenith pg start main
