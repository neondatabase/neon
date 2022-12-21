use std::{collections::HashSet, ffi::OsString};

use shmempipe::shared::TryLockError;

fn main() {
    let mut args = std::env::args_os().fuse();

    let _me = args.next().expect("not supported: running without argv[0]");

    let mode = args.next();

    let mode = mode.as_ref().map(|x| {
        x.to_str()
            .expect("first argument must be convertable to str")
    });

    match mode.as_deref() {
        Some("inner") => as_inner(args),
        Some("outer") | None => as_outer(),
        Some(other) => unreachable!("unknown mode: {other:?}"),
    };
}

/// Runs as the child process, processing requests and responding.
fn as_inner<I>(mut args: I)
where
    I: Iterator<Item = OsString>,
{
    use bytes::Buf;
    use sha2::Digest;

    let path = args.next().expect("need path name used for shm_open");
    let path: &std::path::Path = path.as_ref();

    let shm = shmempipe::open_existing(path).unwrap();

    let mut responder = shm.try_acquire_responder().unwrap();

    let mut response = Vec::with_capacity(8192);

    let mut buffer = bytes::BytesMut::with_capacity(16 * 1024);

    loop {
        buffer.resize(4, 0);
        let mut read = 0;
        while read < 4 {
            read += responder.read(&mut buffer);
        }
        let len = buffer.get_u32();

        buffer.resize(len as usize, 0);
        let mut read = 0;
        while read < len as usize {
            read += responder.read(&mut buffer[read..]);
        }

        let sha = <[u8; 32]>::from(sha2::Sha256::digest(&buffer[..]));
        response.clear();
        response.extend(sha);
        response.resize(8192, 0);
        responder.write_all(&response);
    }
}

/// Starts the child process, sending requests and receiving responses.
fn as_outer() {
    let some_path = std::path::Path::new("/some_any_file_name");

    let shm = shmempipe::create(some_path).unwrap();

    // leave the lock open, but don't forget the guard, because ... well, that should work actually
    // ok if any one of the processes stays alive, it should work, perhaps

    let myself = std::env::args_os().nth(0).expect("already checked");

    let mut child: Option<std::process::Child> = None;

    let mut previously_died_slots = HashSet::new();
    let mut previous_locked_slot = HashSet::new();

    loop {
        if let Some(child_mut) = child.as_mut() {
            match child_mut.try_wait() {
                Ok(Some(es)) => {
                    println!("child had exited: {es:?}");
                    child.take();
                }
                Ok(None) => { /* not yet exited */ }
                Err(e) => println!("child probably hasn't exited yet: {e:?}"),
            }
        }

        if child.is_none() {
            child = Some(
                std::process::Command::new(&myself)
                    .arg("inner")
                    .arg(some_path.as_os_str())
                    .stdin(std::process::Stdio::null())
                    // rest can be inherited
                    .spawn()
                    .unwrap(),
            );
        }

        // we must not try to lock our own slot
        for (i, slot) in shm.participants.iter().enumerate().skip(1) {
            let slot = unsafe { std::pin::Pin::new_unchecked(slot) };

            match slot.try_lock() {
                Ok(g) => {
                    if previously_died_slots.contains(&i) {
                        // we cannot detect ABA but just stay silent, assuming no change
                    } else {
                        println!("slot#{i} is free, last: {:?}", *g);
                    }
                }
                Err(TryLockError::PreviousOwnerDied(g)) => {
                    println!("slot#{i} had previously died: {:?}", *g);
                    previously_died_slots.insert(i);
                    previous_locked_slot.remove(&i);
                }
                Err(TryLockError::WouldBlock) => {
                    if previously_died_slots.remove(&i) {
                        println!("previously died slot#{i} has been reused");
                    } else if previous_locked_slot.insert(i) {
                        println!("slot#{i} is locked");
                    }
                }
            }
        }

        if previous_locked_slot.is_empty() {
            std::thread::sleep(std::time::Duration::from_millis(500));
            continue;
        }

        // otherwise start producing random values for 1..64kB, expecting to get back the hash of
        // the random values repeated for 8192 bytes.

        // let distr = rand::distributions::Uniform::<u32>::new_inclusive(230, 64 * 1024);

        let owned = shm.try_acquire_requester().expect("I am the only one");

        let reqs = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let _jhs = (0..4)
            .map(|_| (owned.clone(), reqs.clone()))
            .map(|(owned, reqs)| {
                std::thread::spawn(move || {
                    use bytes::BufMut;
                    #[allow(unused)]
                    use rand::{Rng, RngCore};
                    use sha2::Digest;
                    let mut req = Vec::with_capacity(128 * 1024);
                    let mut resp = Vec::with_capacity(8192);
                    let mut rng = rand::thread_rng();
                    loop {
                        req.clear();
                        let len = 1132 - 4; // rng.sample(&distr);
                        req.put_u32(len);
                        req.resize(4 + len as usize, 0);

                        rng.fill_bytes(&mut req[4..][..len as usize]);

                        resp.clear();
                        resp.resize(8192, 1);

                        // let started = std::time::Instant::now();

                        owned.request_response(&req, &mut resp);

                        // let elapsed = started.elapsed();
                        reqs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        let expected =
                            <[u8; 32]>::from(sha2::Sha256::digest(&req[4..][..len as usize]));
                        assert_eq!(resp.len(), 8192);
                        assert_eq!(expected.as_slice(), &resp[..32]);
                        assert!(resp[32..].iter().all(|&x| x == 0));
                    }
                })
            })
            .collect::<Vec<_>>();

        loop {
            let started = std::time::Instant::now();
            std::thread::sleep(std::time::Duration::from_secs(1));
            let mut read = reqs.load(std::sync::atomic::Ordering::Relaxed);

            loop {
                match reqs.compare_exchange(
                    read,
                    0,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(y) => read = y,
                }
            }

            let elapsed = started.elapsed();

            println!(
                "{:.2} rps = {:?} per one",
                read as f64 / elapsed.as_secs_f64(),
                Some(read)
                    .filter(|&x| x != 0)
                    .map(|x| elapsed.div_f64(x as f64))
            );
        }
    }
}
