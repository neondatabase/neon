// Copyright 2023 Folke Behrens
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Contains HTTP handler for jeprof support (/pprof/heap).
//! Based on <https://gperftools.github.io/gperftools/pprof_remote_servers.html>,
//! <https://jemalloc.net/jemalloc.3.html#mallctl_namespace>,
//! <https://github.com/jemalloc/jemalloc/blob/master/bin/jeprof.in>.

use crate::profiling::mallctl;
use http::{header, Method, Request, Response, StatusCode};
use std::{collections::HashMap, env, fmt};

#[inline]
pub fn router(req: Request<Vec<u8>>) -> http::Result<Response<Vec<u8>>> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/pprof/conf") => JeprofHandler(get_pprof_conf_handler).call(req),
        (&Method::POST, "/pprof/conf") => JeprofHandler(post_pprof_conf_handler).call(req),
        (&Method::GET, "/pprof/heap") => JeprofHandler(get_pprof_heap_handler).call(req),
        (&Method::GET, "/pprof/cmdline") => JeprofHandler(get_pprof_cmdline_handler).call(req),
        (&Method::GET, "/pprof/symbol") => JeprofHandler(get_pprof_symbol_handler).call(req),
        (&Method::POST, "/pprof/symbol") => JeprofHandler(post_pprof_symbol_handler).call(req),
        (&Method::GET, "/pprof/stats") => JeprofHandler(get_pprof_stats_handler).call(req),
        _ => {
            let body = b"Bad Request\r\n";
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, body.len())
                .body(body.to_vec())
        }
    }
}

#[cfg(feature = "actix-handlers")]
#[inline]
pub fn actix_routes(cfg: &mut actix_web::web::ServiceConfig) {
    cfg.service(
        actix_web::web::scope("/pprof")
            .route(
                "/conf",
                actix_web::web::get().to(JeprofHandler(get_pprof_conf_handler)),
            )
            .route(
                "/conf",
                actix_web::web::post().to(JeprofHandler(post_pprof_conf_handler)),
            )
            .route(
                "/heap",
                actix_web::web::get().to(JeprofHandler(get_pprof_heap_handler)),
            )
            .route(
                "/cmdline",
                actix_web::web::get().to(JeprofHandler(get_pprof_cmdline_handler)),
            )
            .route(
                "/symbol",
                actix_web::web::get().to(JeprofHandler(get_pprof_symbol_handler)),
            )
            .route(
                "/symbol",
                actix_web::web::post().to(JeprofHandler(post_pprof_symbol_handler)),
            )
            .route(
                "/stats",
                actix_web::web::get().to(JeprofHandler(get_pprof_stats_handler)),
            ),
    );
}

#[derive(Debug)]
pub struct ErrorResponse(String);

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ERROR: {}", self.0)
    }
}

#[cfg(feature = "actix-handlers")]
impl actix_web::ResponseError for ErrorResponse {}

#[derive(Clone, Debug)]
struct JeprofHandler<F>(F)
where
    F: Fn(&[u8], &HashMap<String, String>) -> Result<(Vec<u8>, Option<String>), ErrorResponse>
        + Clone
        + 'static;

impl<F> JeprofHandler<F>
where
    F: Fn(&[u8], &HashMap<String, String>) -> Result<(Vec<u8>, Option<String>), ErrorResponse>
        + Clone
        + 'static,
{
    fn call(&self, req: Request<Vec<u8>>) -> http::Result<Response<Vec<u8>>> {
        let params: HashMap<String, String> = parse_malloc_conf_query(req.uri().query())
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.unwrap_or_default().to_string()))
            .collect();
        match self.0(req.body(), &params) {
            Ok((body, Some(content_disposition))) => response_ok_binary(body, &content_disposition),
            Ok((body, None)) => response_ok(body),
            Err(err) => response_err(&err.0),
        }
    }
}

#[cfg(feature = "actix-handlers")]
impl<F>
    actix_web::Handler<(
        actix_web::web::Payload,
        actix_web::web::Query<HashMap<String, String>>,
    )> for JeprofHandler<F>
where
    F: Fn(&[u8], &HashMap<String, String>) -> Result<(Vec<u8>, Option<String>), ErrorResponse>
        + Clone
        + 'static,
{
    type Output = Result<actix_web::HttpResponse, ErrorResponse>;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output>>>;

    fn call(
        &self,
        (mut body, query): (
            actix_web::web::Payload,
            actix_web::web::Query<HashMap<String, String>>,
        ),
    ) -> Self::Future {
        use futures_util::StreamExt as _;

        let f = self.0.clone();
        Box::pin(async move {
            let mut data = Vec::<u8>::new();
            while let Some(item) = body.next().await {
                data.extend_from_slice(&item.map_err(|e| ErrorResponse(e.to_string()))?);
            }
            f(&data, &query.0).map(|(body, content_disposition)| {
                let mut resp = actix_web::HttpResponse::Ok();
                if let Some(filename) = content_disposition {
                    resp.insert_header(actix_web::http::header::ContentDisposition::attachment(
                        filename,
                    ));
                }
                resp.body(actix_web::web::Bytes::from(body))
            })
        })
    }
}

#[inline]
pub fn get_pprof_conf_handler(
    _body: &[u8],
    _params: &HashMap<String, String>,
) -> Result<(Vec<u8>, Option<String>), ErrorResponse> {
    match mallctl::enabled() {
        Ok(true) => (),
        _ => return Err(ErrorResponse("jemalloc profiling not enabled".to_owned())),
    };

    let Ok(state) = mallctl::active() else {
        return Err(ErrorResponse("failed to read prof.active\r\n".to_owned()));
    };
    let Ok(sample) = mallctl::sample_interval() else {
        return Err(ErrorResponse(
            "failed to read prof.lg_sample\r\n".to_owned(),
        ));
    };
    let body = format!("prof.active:{state},prof.lg_sample:{sample}\r\n");
    Ok((body.into_bytes(), None))
}

#[inline]
pub fn post_pprof_conf_handler(
    _body: &[u8],
    params: &HashMap<String, String>,
) -> Result<(Vec<u8>, Option<String>), ErrorResponse> {
    match mallctl::enabled() {
        Ok(true) => (),
        _ => {
            return Err(ErrorResponse(
                "jemalloc profiling not enabled\r\n".to_owned(),
            ))
        }
    };

    for (name, value) in params {
        if let Err(e) = match name.as_str() {
            "prof.reset" => {
                if value.is_empty() {
                    mallctl::reset(None)
                } else {
                    let sample = value.parse().map_err(|_| {
                        ErrorResponse(format!("invalid prof.reset value: {value:?}\r\n"))
                    })?;
                    mallctl::reset(Some(sample))
                }
            }
            "prof.active" => {
                let Some(state) = value.parse().ok() else {
                    return Err(ErrorResponse(format!(
                        "invalid prof.active value: {value:?}\r\n"
                    )));
                };
                mallctl::set_active(state)
            }
            _ => {
                return Err(ErrorResponse(format!("{name}={value:?} unknown\r\n")));
            }
        } {
            return Err(ErrorResponse(format!("{name}={value:?} failed: {e}\r\n")));
        }
    }

    Ok((b"OK\r\n".to_vec(), None))
}

#[inline]
pub fn get_pprof_heap_handler(
    _body: &[u8],
    _params: &HashMap<String, String>,
) -> Result<(Vec<u8>, Option<String>), ErrorResponse> {
    match mallctl::enabled() {
        Ok(true) => (),
        _ => {
            return Err(ErrorResponse(
                "jemalloc profiling not enabled\r\n".to_owned(),
            ))
        }
    };

    let Ok(f) = tempfile::Builder::new()
        .prefix("jemalloc.")
        .suffix(".prof")
        .tempfile()
    else {
        return Err(ErrorResponse(
            "cannot create temporary file for profile dump\r\n".to_owned(),
        ));
    };

    let Ok(profile) = mallctl::dump(f.path().to_str()) else {
        return Err(ErrorResponse("failed to dump profile\r\n".to_owned()));
    };

    let filename = f.path().file_name().expect("proper filename from tempfile");
    Ok((
        profile.expect("profile not None"),
        Some(filename.to_string_lossy().to_string()),
    ))
}

/// HTTP handler for GET /pprof/cmdline.
#[inline]
pub fn get_pprof_cmdline_handler(
    _body: &[u8],
    _params: &HashMap<String, String>,
) -> Result<(Vec<u8>, Option<String>), ErrorResponse> {
    let mut body = String::new();
    for arg in env::args() {
        body.push_str(arg.as_str());
        body.push_str("\r\n");
    }
    Ok((body.into_bytes(), None))
}

/// HTTP handler for GET /pprof/symbol.
#[inline]
pub fn get_pprof_symbol_handler(
    _body: &[u8],
    _params: &HashMap<String, String>,
) -> Result<(Vec<u8>, Option<String>), ErrorResponse> {
    // TODO: any quick way to check if binary is stripped?
    let body = b"num_symbols: 1\r\n";
    Ok((body.to_vec(), None))
}

/// HTTP handler for POST /pprof/symbol.
#[inline]
pub fn post_pprof_symbol_handler(
    body: &[u8],
    _params: &HashMap<String, String>,
) -> Result<(Vec<u8>, Option<String>), ErrorResponse> {
    fn lookup_symbol(addr: u64) -> Option<String> {
        let mut s: Option<String> = None;
        backtrace::resolve(addr as *mut _, |symbol| {
            s = symbol.name().map(|n| n.to_string());
        });
        s
    }

    let body = String::from_utf8_lossy(body);
    let addrs = body
        .split('+')
        .filter_map(|addr| u64::from_str_radix(addr.trim_start_matches("0x"), 16).ok())
        .map(|addr| (addr, lookup_symbol(addr)))
        .filter_map(|(addr, sym)| sym.map(|sym| (addr, sym)));

    let mut body = String::new();
    for (addr, sym) in addrs {
        body.push_str(format!("{addr:#x}\t{sym}\r\n").as_str());
    }

    Ok((body.into_bytes(), None))
}

/// HTTP handler for GET /pprof/stats.
#[inline]
pub fn get_pprof_stats_handler(
    _body: &[u8],
    _params: &HashMap<String, String>,
) -> Result<(Vec<u8>, Option<String>), ErrorResponse> {
    let body = match mallctl::stats() {
        Ok(body) => body,
        Err(e) => return Err(ErrorResponse(format!("failed to print stats: {e}\r\n"))),
    };
    Ok((body, None))
}

fn parse_malloc_conf_query(query: Option<&str>) -> Vec<(&str, Option<&str>)> {
    query
        .map(|q| {
            q.split(',')
                .map(|kv| kv.splitn(2, ':').collect::<Vec<_>>())
                .map(|v| match v.len() {
                    1 => (v[0], None),
                    2 => (v[0], Some(v[1])),
                    _ => unreachable!(),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn response_ok(body: Vec<u8>) -> http::Result<Response<Vec<u8>>> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=UTF-8")
        .header(header::CONTENT_LENGTH, body.len())
        .body(body)
}

fn response_ok_binary(body: Vec<u8>, filename: &str) -> http::Result<Response<Vec<u8>>> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .header(header::CONTENT_LENGTH, body.len())
        .body(body)
}

fn response_err(msg: &str) -> http::Result<Response<Vec<u8>>> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(header::CONTENT_TYPE, "text/plain; charset=UTF-8")
        .header(header::CONTENT_LENGTH, msg.len())
        .body(msg.as_bytes().to_owned())
}
