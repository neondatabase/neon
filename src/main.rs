use std::thread;

mod page_cache;
mod waldecoder;
mod walreceiver;

use std::io::Error;

fn main() -> Result<(), Error> {


    // Launch the WAL receiver thread. It will try to connect to the WAL safekeeper,
    // and stream the WAL. If the connection is lost, it will reconnect on its own.
    // We just fire and forget it here.
    let handler = thread::spawn(|| {
        // thread code
        walreceiver::thread_main();
    });

    let _unused = handler.join();     // never returns.

    Ok(())
}
