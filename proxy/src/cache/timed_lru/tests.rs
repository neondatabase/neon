use super::*;

/// Check that we can define the cache for certain types.
#[test]
fn definition() {
    // Check for trivial yet possible types.
    let cache = TimedLru::<(), ()>::new("test", 128, Duration::from_secs(0));
    let _ = cache.insert(Default::default(), Default::default());
    let _ = cache.get(&());

    // Now for something less trivial.
    let cache = TimedLru::<String, String>::new("test", 128, Duration::from_secs(0));
    let _ = cache.insert(Default::default(), Default::default());
    let _ = cache.get(&String::default());
    let _ = cache.get("str should work");

    // It should also work for non-cloneable values.
    struct NoClone;
    let cache = TimedLru::<Box<str>, NoClone>::new("test", 128, Duration::from_secs(0));
    let _ = cache.insert(Default::default(), NoClone.into());
    let _ = cache.get(&Box::<str>::from("boxed str"));
    let _ = cache.get("str should work");
}

#[test]
fn insert() {
    const CAPACITY: usize = 2;
    let cache = TimedLru::<String, u32>::new("test", CAPACITY, Duration::from_secs(10));
    assert_eq!(cache.size(), 0);

    let key = Arc::new(String::from("key"));

    let (old, cached) = cache.insert(key.clone(), 42.into());
    assert_eq!(old, None);
    assert_eq!(*cached, 42);
    assert_eq!(cache.size(), 1);

    let (old, cached) = cache.insert(key, 1.into());
    assert_eq!(old.as_deref(), Some(&42));
    assert_eq!(*cached, 1);
    assert_eq!(cache.size(), 1);

    let (old, cached) = cache.insert(Arc::new("N1".to_owned()), 10.into());
    assert_eq!(old, None);
    assert_eq!(*cached, 10);
    assert_eq!(cache.size(), 2);

    let (old, cached) = cache.insert(Arc::new("N2".to_owned()), 20.into());
    assert_eq!(old, None);
    assert_eq!(*cached, 20);
    assert_eq!(cache.size(), 2);
}

#[test]
fn get_none() {
    let cache = TimedLru::<String, u32>::new("test", 2, Duration::from_secs(10));
    let cached = cache.get("missing");
    assert!(matches!(cached, None));
}

#[test]
fn invalidation_simple() {
    let cache = TimedLru::<String, u32>::new("test", 2, Duration::from_secs(10));
    let (_, cached) = cache.insert(String::from("key").into(), 100.into());
    assert_eq!(cache.size(), 1);

    cached.invalidate();

    assert_eq!(cache.size(), 0);
    assert!(matches!(cache.get("key"), None));
}

#[test]
fn invalidation_preserve_newer() {
    let cache = TimedLru::<String, u32>::new("test", 2, Duration::from_secs(10));
    let key = Arc::new(String::from("key"));

    let (_, cached) = cache.insert(key.clone(), 100.into());
    assert_eq!(cache.size(), 1);
    let _ = cache.insert(key.clone(), 200.into());
    assert_eq!(cache.size(), 1);
    cached.invalidate();
    assert_eq!(cache.size(), 1);

    let cached = cache.get(key.as_ref());
    assert_eq!(cached.as_deref(), Some(&200));
}

#[test]
fn auto_expiry() {
    let lifetime = Duration::from_millis(300);
    let cache = TimedLru::<String, u32>::new("test", 2, lifetime);

    let key = Arc::new(String::from("key"));
    let _ = cache.insert(key.clone(), 42.into());

    let cached = cache.get(key.as_ref());
    assert_eq!(cached.as_deref(), Some(&42));

    std::thread::sleep(lifetime);

    let cached = cache.get(key.as_ref());
    assert_eq!(cached.as_deref(), None);
}
