//
// Main entry point for the Page Server executable
//

use std::thread;

mod page_cache;
mod page_service;
mod restore_s3;
mod waldecoder;
mod walreceiver;
mod walredo;

use std::io::Error;

fn main() -> Result<(), Error> {

    // First, restore the latest base backup from S3. (We don't persist anything
    // to local disk at the moment, so we need to do this at every startup)
    restore_s3::restore_main();
    

    let mut threads = Vec::new();

    // Launch the WAL receiver thread. It will try to connect to the WAL safekeeper,
    // and stream the WAL. If the connection is lost, it will reconnect on its own.
    // We just fire and forget it here.
    let walreceiver_thread = thread::spawn(|| {
        // thread code
        walreceiver::thread_main();
    });
    threads.push(walreceiver_thread);

    // GetPage@LSN requests are served by another thread. (It uses async I/O,
    // but the code in page_service sets up it own thread pool for that)

    let page_server_thread = thread::spawn(|| {
        // thread code
        page_service::thread_main();
    });
    threads.push(page_server_thread);

    // never returns.
    for t in threads {
        t.join().unwrap()
    }
    Ok(())
}
