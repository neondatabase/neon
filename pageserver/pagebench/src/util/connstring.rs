pub(crate) fn connstring(host_port: &str, jwt: Option<&str>) -> String {
    let colon_and_jwt = if let Some(jwt) = jwt {
        format!(":{jwt}") // TODO: urlescape
    } else {
        format!("")
    };
    format!("postgres://postgres{colon_and_jwt}@{host_port}")
}
