use std::{collections::HashSet, ffi::OsString};

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

        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    let mut g = won.unwrap();

    *g = Some(std::process::id());

    println!("child#{:?} now entering asleep", *g);

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1000));
    }
}

/// Starts the child process, sending requests and receiving responses.
fn as_outer() {
    use shmempipe::shared::TryLockError;

    let some_path = std::path::Path::new("/some_any_file_name");

    let shm = shmempipe::create(some_path).unwrap();

    let myself = std::env::args_os().nth(0).expect("already checked");

    let child = std::process::Command::new(myself)
        .arg("inner")
        .arg(some_path.as_os_str())
        .stdin(std::process::Stdio::null())
        // rest can be inherited
        .spawn()
        .unwrap();

    let mut child = Some(child);

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

        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}
