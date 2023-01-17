use std::sync::atomic::Ordering::Relaxed;
use std::{collections::HashSet, ffi::OsString, io::BufRead};

use rand::{Rng, RngCore};
use sha2::Digest;

use shmempipe::OwnedResponder;

/// Whether the worker will hash the input, and return hash + (8192 - 32) zeroes, or just return all
/// zeroes.
const SHA_INPUT: bool = false;

/// Whether or not launch a process or just run the "worker" in a thread, which is nicer to debug.
const SPAWN_PROCESS: bool = false;

/// What kind of requests to send from the "owner" to the "worker."
///
/// There is no logic to the messages, the in-band signalling of the shmempipe is used to make sure
/// all of the message is received.
const INPUT_SIZE: InputSize = InputSize::Fixed(1132);

#[allow(unused)]
enum InputSize {
    /// Generate the length with an rng
    Random,
    /// This many random bytes
    Fixed(u32),
}

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
    let path = args.next().expect("need path name used for shm_open");
    let path: &std::path::Path = path.as_ref();

    let shm = shmempipe::open_existing(path).unwrap();

    let responder = shm.try_acquire_responder().unwrap();

    inner_respond(responder);
}

fn inner_respond(mut responder: OwnedResponder) -> ! {
    let mut response = vec![0; 8192];

    const INITIAL_BUF_SIZE: usize = 16 * 1024;
    let mut buffer = vec![0; INITIAL_BUF_SIZE];

    #[cfg(not_now)]
    let mut histogram = hdrhistogram::Histogram::<u64>::new(3).unwrap();
    #[cfg(not_now)]
    let mut serialized = Vec::new();
    #[cfg(not_now)]
    let mut b64 = String::new();

    // mostly helpful if you try to hunt down memory corruption, or hdrhistogram'ing
    #[cfg(not_now)]
    let mut seq = 0;

    loop {
        #[cfg(not_now)]
        let started_at = std::time::Instant::now();

        // read the messages stricly one by one, this is less efficient but guaranteed to have less
        // bugs here.
        let mut buffered = 0;

        let mut len = [0u8; 4];

        while buffered < 4 {
            buffered += responder.read(&mut len[buffered..]);
        }

        let len = u32::from_ne_bytes(len);

        if len as usize > buffer.len() {
            // the buffer size is never shrunk
            buffer.resize(len as usize, 0);
        }
        buffered = 0;

        while buffered < len as usize {
            let until = len as usize - buffered;
            buffered += responder.read(&mut buffer[buffered..][..until]);
        }

        assert_eq!(buffered, len as usize);

        // this is quite ridiculous that sha256 seems slower than walredo for short/short
        if SHA_INPUT {
            let sha = <[u8; 32]>::from(sha2::Sha256::digest(&buffer[..len as usize]));
            response[..32].copy_from_slice(&sha);
        }

        responder.write_all(&response);

        #[cfg(not_now)]
        {
            let elapsed = started_at.elapsed();
            let nanos = u64::try_from(elapsed.as_nanos()).unwrap();

            histogram += nanos;

            seq += 1;

            if seq % 1_000_000 == 0 {
                use hdrhistogram::serialization::Serializer;
                println!();

                serialized.clear();
                let mut s = hdrhistogram::serialization::V2DeflateSerializer::new();
                s.serialize(&histogram, &mut serialized).unwrap();

                b64.clear();
                base64::encode_engine_string(
                    &serialized,
                    &mut b64,
                    &base64::engine::DEFAULT_ENGINE,
                );
                println!("{}", b64);

                println!();
            }
        }
    }
}

enum Child {
    Process(std::process::Child),
    Thread(std::thread::JoinHandle<()>),
}

/// Starts the child process, sending requests and receiving responses.
fn as_outer() {
    let some_path = std::path::Path::new("/some_any_file_name");

    // having to arc this is quite unfortunate, but we cannot access ArcInner...
    let shm = shmempipe::create(some_path).unwrap();

    // leave the lock open, but don't forget the guard, because ... well, that should work actually
    // ok if any one of the processes stays alive, it should work, perhaps

    let myself = std::env::args_os().nth(0).expect("already checked");

    let mut child: Option<Child> = None;

    let mut previous_locked_slot = HashSet::new();

    loop {
        match child.as_mut() {
            Some(Child::Process(child_mut)) => {
                match child_mut.try_wait() {
                    Ok(Some(_es)) => {
                        println!("child had exited: {_es:?}");
                        child.take();
                    }
                    Ok(None) => { /* not yet exited */ }
                    Err(e) => {
                        println!("child probably hasn't exited yet: {e:?}")
                    }
                }
            }
            Some(Child::Thread(_jh)) => {}
            None => {}
        }

        if child.is_none() {
            if SPAWN_PROCESS {
                let mut spawn = std::process::Command::new(&myself)
                    .arg("inner")
                    .arg(some_path.as_os_str())
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                    .unwrap();

                let stdout = spawn.stdout.take().unwrap();
                let stderr = spawn.stderr.take().unwrap();

                child = Some(Child::Process(spawn));

                std::thread::spawn(move || {
                    let mut reader = std::io::BufReader::new(stdout);

                    let mut buf = String::new();
                    loop {
                        buf.clear();
                        let read = reader.read_line(&mut buf).unwrap();
                        if read == 0 {
                            break;
                        }
                        println!("{}", buf.trim());
                    }
                });

                std::thread::spawn(move || {
                    let mut reader = std::io::BufReader::new(stderr);

                    let mut buf = String::new();
                    loop {
                        buf.clear();
                        let read = reader.read_line(&mut buf).unwrap();
                        if read == 0 {
                            break;
                        }
                        println!("{}", buf.trim());
                    }
                });
            } else {
                // scoped threads are not yet on our rust
                let fake_joined = unsafe { shm.as_joined() };
                child = Some(Child::Thread(std::thread::spawn(move || {
                    // dropping and munmapping the created ptr before child would be wildly ub
                    let responder = fake_joined
                        .try_acquire_responder()
                        .expect("failed to acquire responder");
                    inner_respond(responder);
                })));
            }
        }

        // we must not try to lock our own slot
        for (i, slot) in shm.participants.iter().enumerate().skip(1) {
            match slot.load(Relaxed) {
                0 => {
                    previous_locked_slot.remove(&i);
                }
                _pid => {
                    previous_locked_slot.insert(i);
                }
            }
        }

        if previous_locked_slot.is_empty() {
            // if the child process or thread is not yet around, sleep a bit
            std::thread::sleep(std::time::Duration::from_millis(500));
            continue;
        }

        // otherwise start producing random values for 1..64kB, expecting to get back the hash of
        // the random values repeated for 8192 bytes.

        #[allow(unused)]
        let distr = rand::distributions::Uniform::<u32>::new_inclusive(230, 64 * 1024);

        let owned = shm.try_acquire_requester().expect("I am the only one");

        let reqs = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let _jhs = (0..1)
            .map(|_| (owned.clone(), reqs.clone()))
            .map(|(owned, reqs)| {
                std::thread::spawn(move || {
                    let mut req = vec![0; 64 * 1024];
                    let mut rng = rand::thread_rng();
                    rng.fill_bytes(&mut req[..]);

                    // initially fill the response up with ones to differentiate from a situation where we
                    // didn't read all of the 8192 bytes. this used to be done after every clear,
                    // however to cut down profiling noise it's only done once.
                    let mut resp = vec![1u8; 8192];

                    resp.resize(8192, 1);
                    loop {
                        let len = match INPUT_SIZE {
                            InputSize::Random => rng.sample(&distr),
                            InputSize::Fixed(fixed) => fixed,
                        };

                        let head = len.to_ne_bytes();
                        let tail = &req[..len as usize];

                        let payload = [&head[..], tail];

                        let id = owned.request_response(&payload, &mut resp);

                        reqs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        assert_eq!(resp.len(), 8192);

                        if SHA_INPUT {
                            let expected = <[u8; 32]>::from(sha2::Sha256::digest(&tail));
                            if expected.as_slice() != &resp[..32] {
                                println!(
                                    "{id} -- hash mismatch, expected {:?}, got {:?} (len: {len})",
                                    Hex(expected.as_slice()),
                                    Hex(&resp[..32])
                                );
                                break;
                            }
                            assert!(resp[32..].iter().all(|&x| x == 0));
                        } else if !SHA_INPUT {
                            debug_assert!(resp[..].iter().all(|&x| x == 0));
                        } else {
                            // println!("{id} -- ok, len: {len}");
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        println!(
            "starting up with {} threads, using subprocess: {SPAWN_PROCESS}",
            _jhs.len()
        );

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

        // this will be unreachable if the above is loop, or not if it isn't
        #[allow(unreachable_code)]
        {
            _jhs.into_iter().for_each(|x| x.join().unwrap());

            match child {
                Some(Child::Process(mut c)) => {
                    c.kill().unwrap();
                    c.wait().unwrap();
                }
                Some(Child::Thread(_jh)) => {}
                None => {
                    unreachable!();
                }
            }

            break;
        }
    }
}

struct Hex<'a>(&'a [u8]);

impl std::fmt::Debug for Hex<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        static LUT: &[u8; 16] = b"0123456789abcdef";

        let mut temp = [0u8; 32];

        for chunk in self.0.chunks(temp.len() / 2) {
            chunk
                .iter()
                .flat_map(|&b| [b >> 4, b & 0x0f])
                .map(|nib| LUT[nib as usize])
                .zip(temp.iter_mut())
                .for_each(|(nib, out)| *out = nib);

            f.write_str(unsafe { std::str::from_utf8_unchecked(&temp[..chunk.len() * 2]) })?;
        }

        Ok(())
    }
}
