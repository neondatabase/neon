pub(crate) type ThreadLocalStats<T> = Arc<Mutex<T>>;
pub(crate) type AllThreadLocalStats<T> = Arc<Mutex<Vec<ThreadLocalStats<T>>>>;

macro_rules! declare {
    ($THREAD_LOCAL_NAME:ident: $T:ty) => {
        thread_local! {
            pub static $THREAD_LOCAL_NAME: std::cell::RefCell<crate::util::tokio_thread_local_stats::ThreadLocalStats<$T>> = std::cell::RefCell::new(
                std::sync::Arc::new(std::sync::Mutex::new(Default::default()))
            );
        }
    };
}

use std::sync::{Arc, Mutex};

pub(crate) use declare;

macro_rules! main {
    ($THREAD_LOCAL_NAME:ident, $main_impl:expr) => {{
        let main_impl = $main_impl;
        let all = Arc::new(Mutex::new(Vec::new()));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .on_thread_start({
                let all = Arc::clone(&all);
                move || {
                    // pre-initialize the thread local stats by accessesing them
                    // (some stats like requests_stats::Stats are quite costly to initialize,
                    //  we don't want to pay that cost during the measurement period)
                    $THREAD_LOCAL_NAME.with(|stats| {
                        let stats: Arc<_> = Arc::clone(&*stats.borrow());
                        all.lock().unwrap().push(stats);
                    });
                }
            })
            .enable_all()
            .build()
            .unwrap();

        let main_task = rt.spawn(main_impl(all));
        rt.block_on(main_task).unwrap()
    }};
}

pub(crate) use main;
