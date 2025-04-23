




#[derive(Clone, Copy, Debug)]
pub(crate) enum RequestKind {
    Get = 0,
    Put = 1,
    Delete = 2,
    List = 3,
    Copy = 4,
    TimeTravel = 5,
    Head = 6,
}


use scopeguard::ScopeGuard;


#[derive(Debug, Clone, Copy)]
pub(crate) enum AttemptOutcome {
    Ok,
    Err,
    Cancelled,
}

impl<T, E> From<&Result<T, E>> for AttemptOutcome {
    fn from(value: &Result<T, E>) -> Self {
        match value {
            Ok(_) => AttemptOutcome::Ok,
            Err(_) => AttemptOutcome::Err,
        }
    }
}









/// On drop (cancellation) add time to [`BucketMetrics::req_seconds`].
pub(crate) fn start_measuring_requests(
    _kind: RequestKind,
) -> ScopeGuard<std::time::Instant, impl FnOnce(std::time::Instant), scopeguard::OnSuccess> {
    scopeguard::guard_on_success(std::time::Instant::now(), move |_started_at| {
        
    })
}



