use std::{collections::HashSet, ffi::OsString, pin::Pin};

use shmempipe::shared::PreviousOwnerDied;

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
    use shmempipe::shared::TryLockError;

    let path = args.next().expect("need path name used for shm_open");
    let path: &std::path::Path = path.as_ref();

    let shm = shmempipe::open_existing(path).unwrap();

    assert_eq!(
        shm.magic.load(std::sync::atomic::Ordering::SeqCst),
        0xcafebabe
    );

    let mut won = None;

    while won.is_none() {
        for (i, slot) in shm.participants.iter().enumerate() {
            // if slot == 0 { check refcount and go away? }
            let slot = unsafe { std::pin::Pin::new_unchecked(slot) };
            match slot.try_lock() {
                Ok(g) => {
                    println!("child: slot#{i} won");
                    won = Some(g);
                    break;
                }
                Err(TryLockError::PreviousOwnerDied(g)) => {
                    println!("child: slot#{i} recovered");
                    won = Some(g);
                    break;
                }
                Err(TryLockError::WouldBlock) => {
                    continue;
                }
            };
        }

        // because of the longer sleep here, it's likely that outer is able to launch subprocesses
        // faster than this get's to react, and loses constantly.
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    let mut g = won.unwrap();

    *g = Some(std::process::id());

    provide_responses(&shm).unwrap();
}

fn provide_responses(
    shm: &shmempipe::SharedMemPipePtr<shmempipe::Ready>,
) -> Result<(), Box<dyn std::error::Error + 'static>> {
    // FIXME: ringbuf is probably similar to seqlock's unsound, as there is no atomic memcpy,
    // it will not participate in the atomic operations order, no matter which fences are
    // added.
    let mut c = unsafe { ringbuf::Consumer::new(&shm.to_worker) };
    let mut p = unsafe { ringbuf::Producer::new(&shm.from_worker) };

    // let writer_mutex = unsafe { Pin::new_unchecked(&shm.to_worker_writer) };
    let writer_cond = unsafe { Pin::new_unchecked(&shm.to_worker_cond) };

    let resp_mutex = unsafe { Pin::new_unchecked(&shm.from_worker_writer) };
    let resp_cond = unsafe { Pin::new_unchecked(&shm.from_worker_cond) };

    let mut response = Vec::with_capacity(8192);

    loop {
        let mut tx_loops = 0;
        let mut rx_loops = 0;

        let mut len = [0u8; 4];

        {
            let mut written_len = &mut len[..];

            while !written_len.is_empty() {
                let written = c.pop_slice(&mut written_len);
                written_len = &mut written_len[written..];

                if written == 0 {
                    rx_loops += 1;
                    // println!("waking up writer_cond");
                    writer_cond.notify_one();
                }
            }
        }

        use bytes::Buf;

        let len = std::io::Cursor::new(&len).get_u32();
        let remaining = len;

        use sha2::Digest;
        let mut sha = sha2::Sha256::default();

        consume_from_consumer(
            &mut c,
            remaining as usize,
            |slice| {
                sha.update(slice);
                Ok(slice.len())
            },
            || {
                // println!("waking up writer_cond later");
                writer_cond.notify_one();
            },
        )
        .unwrap();

        let sha = <[u8; 32]>::from(sha.finalize());

        let resp = sha.iter().copied().chain(std::iter::repeat(0)).take(8192);
        response.clear();
        response.extend(resp);

        let mut resp = &response[..];

        while !resp.is_empty() {
            let pushed = p.push_slice(&mut resp);
            resp = &resp[pushed..];
            tx_loops += 1;
            if !resp.is_empty() {
                if tx_loops % 100 == 0 {
                    let g = resp_mutex.lock().unwrap_or_else(|e| e.into_inner());
                    // println!("waiting on from_worker_cond");
                    let _ = resp_cond.wait_while(g, |_| p.free_len() == 0);
                } else {
                    std::hint::spin_loop();
                }
            }
        }

        drop(rx_loops);
        drop(tx_loops);

        // println!("client rx/tx {rx_loops}/{tx_loops} for {len}");
    }
}

/// Starts the child process, sending requests and receiving responses.
fn as_outer() {
    use shmempipe::shared::TryLockError;

    let some_path = std::path::Path::new("/some_any_file_name");

    let shm = shmempipe::create(some_path).unwrap();

    let lock = unsafe { Pin::new_unchecked(&shm.participants[0]) };
    let mut g = lock
        .try_lock()
        .expect("should be the first locker after creation");
    *g = Some(std::process::id());

    // leave the lock open, but don't forget the guard, because ... well, that should work actually
    // ok if any one of the processes stays alive, it should work, perhaps

    let myself = std::env::args_os().nth(0).expect("already checked");

    let mut child: Option<std::process::Child> = None;

    let mut previously_died_slots = HashSet::new();
    let mut previous_locked_slot = HashSet::new();

    let mut req = Vec::with_capacity(64 * 1024);
    let mut response = Vec::with_capacity(64 * 1024);

    let mut sum = std::time::Duration::ZERO;
    let mut n = 0;

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

        let mut rng = rand::thread_rng();
        use bytes::BufMut;
        use rand::{Rng, RngCore};
        use sha2::Digest;

        // let distr = rand::distributions::Uniform::<u32>::new_inclusive(230, 64 * 1024);

        loop {
            req.clear();
            let len = 1132 - 4; // rng.sample(&distr);

            req.put_u32(len);
            req.resize(4 + len as usize, 0);

            rng.fill_bytes(&mut req[4..][..len as usize]);
            let expected = <[u8; 32]>::from(sha2::Sha256::digest(&req[4..][..len as usize]));

            response.clear();

            let started = std::time::Instant::now();

            run_rpc(&shm, &req, |partial_resp| {
                response.extend(partial_resp);
                Ok(partial_resp.len())
            })
            .unwrap();

            sum += started.elapsed();
            n += 1;

            assert_eq!(response.len(), 8192);
            assert_eq!(expected.as_slice(), &response[..32]);

            assert!(response[32..].iter().all(|&x| x == 0));

            if n == 10000 {
                println!("avg over 10000: {:?}", sum / n);
                n = 0;
                sum = std::time::Duration::ZERO;
            }
        }
    }
}

fn run_rpc<F>(
    shm: &shmempipe::SharedMemPipePtr<shmempipe::Ready>,
    mut req: &[u8],
    partial_resp: F,
) -> Result<(), Box<dyn std::error::Error + 'static>>
where
    F: FnMut(&[u8]) -> Result<usize, Box<dyn std::error::Error + 'static>>,
{
    let mut tx_loops = 0;
    let mut rx_loops = 0;

    let mut p = unsafe { ringbuf::Producer::new(&shm.to_worker) };

    let write_mutex = unsafe { Pin::new_unchecked(&shm.to_worker_writer) };
    let write_cond = unsafe { Pin::new_unchecked(&shm.to_worker_cond) };

    let read_cond = unsafe { Pin::new_unchecked(&shm.from_worker_cond) };

    assert!(!req.is_empty());

    loop {
        let pushed = p.push_slice(req);
        req = &req[pushed..];

        if req.is_empty() {
            break;
        } else {
            tx_loops += 1;

            if tx_loops % 100 == 0 {
                // we must now wait for the other side to advance
                // TODO: unsure how
                // println!("pushed, going to wait until p.len() > 0");
                let g = write_mutex.lock().unwrap_or_else(|e| e.into_inner());
                let _ = write_cond.wait_while(g, |_| p.free_len() == 0);
            } else {
                std::hint::spin_loop();
            }
        }
    }

    drop(p);

    {
        let mut c = unsafe { ringbuf::Consumer::new(&shm.from_worker) };

        consume_from_consumer(&mut c, 8192, partial_resp, || {
            rx_loops += 1;
            // println!("notifying from_worker_cond");
            read_cond.notify_one();
            std::hint::spin_loop();
        })?;
    }

    drop(tx_loops);
    drop(rx_loops);
    // println!("outer: tx/rx {tx_loops}/{rx_loops}");

    Ok(())
}

fn consume_from_consumer<F, SF, R>(
    consumer: &mut ringbuf::Consumer<u8, R>,
    mut remaining: usize,
    mut partial_resp: F,
    mut spin: SF,
) -> Result<(), Box<dyn std::error::Error + 'static>>
where
    F: FnMut(&[u8]) -> Result<usize, Box<dyn std::error::Error + 'static>>,
    SF: FnMut(),
    R: ringbuf::ring_buffer::RbRef,
    R::Rb: ringbuf::ring_buffer::RbRead<u8>,
{
    while remaining != 0 {
        let (mut s0, mut s1) = consumer.as_slices();

        if s0.is_empty() {
            assert!(s1.is_empty());
            spin();
            continue;
        } else {
            let limit = std::cmp::min(remaining, s0.len() + s1.len());

            // shrink the view according to the limit, not to read of the *next* response
            if limit < s0.len() {
                s0 = &s0[..limit];
                s1 = &[][..];
            } else {
                let rem_s1 = limit - s0.len();
                if rem_s1 < s1.len() {
                    s1 = &s1[..rem_s1];
                }
            }

            let mut read = partial_resp(s0)?;

            assert!(read <= s0.len());

            if !s1.is_empty() && read == s0.len() {
                let read_s1 = partial_resp(s1)?;

                assert!(read_s1 <= s1.len());
                read += read_s1;
            }

            assert!(read <= remaining);
            remaining -= read;

            unsafe { consumer.advance(read) };
        }
    }
    Ok(())
}
