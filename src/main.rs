use std::thread;

mod page_cache;
mod waldecoder;
mod walreceiver;
mod page_service;

use std::io::Error;

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
