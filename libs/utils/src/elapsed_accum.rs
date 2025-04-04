use std::time::{Duration, Instant};

#[derive(Default)]
pub struct ElapsedAccum {
    accum: Duration,
}

impl ElapsedAccum {
    pub fn get(&self) -> Duration {
        self.accum
    }
    pub fn guard(&mut self) -> impl Drop + '_ {
        let start = Instant::now();
        scopeguard::guard(start, |last_wait_at| {
            self.accum += Instant::now() - last_wait_at;
        })
    }

    pub async fn measure<Fut, O>(&mut self, fut: Fut) -> O
    where
        Fut: Future<Output = O>,
    {
        let _guard = self.guard();
        fut.await
    }
}
