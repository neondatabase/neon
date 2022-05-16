use std::{marker::PhantomData, ops::Range, time::{Duration, Instant}};
use serde::{Deserialize, Serialize};



pub trait Job: std::fmt::Debug + Send + Copy + Clone + PartialEq + Eq + 'static {
    type ErrorType: AsRef<dyn std::error::Error + 'static>;
    fn run(&self) -> Result<(), Self::ErrorType>;
}

pub enum Schedule {
    Every(Duration),
}

/// A job that repeats on a schedule
pub struct Chore<J: Job> {
    pub job: J,
    pub schedule: Schedule,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
pub struct ChoreHandle<J: Job> {
    _marker: PhantomData<J>,
    chore_id: u64,
}

pub enum Status<J: Job> {
    Scheduled(Instant),
    Error(J::ErrorType),
}

pub trait Scheduler<J: Job> {
    fn add_chore(&self, chore: Chore<J>) -> ChoreHandle<J>;
    fn remove_chore(&self, ch: ChoreHandle<J>);
    fn get_status(&self, ch: ChoreHandle<J>) -> Status<J>;
}

pub struct SimpleScheduler<J: Job> {
    _marker: PhantomData<J>,
}
impl<J: Job> Scheduler<J> for SimpleScheduler<J> {
    fn add_chore(&self, chore: Chore<J>) -> ChoreHandle<J> {
        todo!()
    }

    fn remove_chore(&self, ch: ChoreHandle<J>) {
        todo!()
    }

    fn get_status(&self, ch: ChoreHandle<J>) -> Status<J> {
        todo!()
    }
}
