use http::StatusCode;

pub trait HttpCodeError {
    fn get_http_status_code(&self) -> StatusCode;
}
