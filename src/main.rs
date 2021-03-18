//
// Main entry point for the Page Server executable
//

use std::thread;

mod page_cache;
mod page_service;
mod waldecoder;
mod walreceiver;
mod walredo;

use std::io::Error;
use std::time::Duration;

fn main() -> Result<(), Error> {
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

    // Since the GetPage@LSN network interface isn't working yet, mock that
    // by calling the GetPage@LSN function with a random block every 5 seconds.
    loop {
        thread::sleep(Duration::from_secs(5));

        page_cache::test_get_page_at_lsn();
    }

    // never returns.
    //for t in threads {
    //    t.join().unwrap()
    //}
    //let _unused = handler.join();     // never returns.
    //Ok(())
}
