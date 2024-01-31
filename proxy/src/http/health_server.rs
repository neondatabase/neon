use anyhow::{anyhow, bail};
use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use hyper::{Body, Request, Response, StatusCode};
use once_cell::sync::Lazy;
use std::{convert::Infallible, ffi::CString, net::TcpListener, sync::Mutex};
use tikv_jemalloc_ctl::{
    opt,
    // Access, AsName as _, MibStr,
    prof,
};
use tracing::info;
use utils::http::{endpoint, error::ApiError, json::json_response, RouterBuilder, RouterService};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

async fn prof_dump(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    static PROF_MIB: Lazy<prof::dump_mib> = Lazy::new(|| {
        // b"prof.dump\0"
        //     .name()
        //     .mib_str::<[usize; 2]>()
        //     .expect("could not create prof.dump MIB")
        prof::dump::mib().expect("could not create prof.dump MIB")
    });
    static PROF_DIR: Lazy<Utf8TempDir> =
        Lazy::new(|| camino_tempfile::tempdir().expect("could not create tempdir"));
    static PROF_FILE: Lazy<Utf8PathBuf> = Lazy::new(|| PROF_DIR.path().join("prof.dump"));
    static PROF_FILE0: Lazy<CString> = Lazy::new(|| CString::new(PROF_FILE.as_str()).unwrap());
    static DUMP_LOCK: Mutex<()> = Mutex::new(());

    tokio::task::spawn_blocking(|| {
        let _guard = DUMP_LOCK.lock();
        PROF_MIB
            .write(&PROF_FILE0)
            .expect("could not trigger prof.dump");
        let prof_dump = std::fs::read_to_string(&*PROF_FILE).expect("could not open prof.dump");

        Response::new(Body::from(prof_dump))
    })
    .await
    .map_err(|e| ApiError::InternalServerError(e.into()))
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    dbg!(opt::prof::read().unwrap());
    prof::active::write(true).unwrap();
    // let _: bool = dbg!(b"opt.prof\0".name().read().unwrap());
    // b"prof.active\0".name().write(true).unwrap();

    endpoint::make_router()
        .get("/v1/status", status_handler)
        .get("/v1/jemalloc/prof.dump", prof_dump)
}

pub async fn task_main(http_listener: TcpListener) -> anyhow::Result<Infallible> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService::new(make_router().build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    bail!("hyper server without shutdown handling cannot shutdown successfully");
}
