use std::time::Duration;

use super::worker::Job;


enum JobStatus {
    Active,
    Stuck,
}

struct Scheduler<J: Job> {
    interval: Duration,
    jobs: Vec<(J, JobStatus)>
}
