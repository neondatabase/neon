use std::net::SocketAddr;

/// Routes registered users to cplane-provided compute node
pub struct DefaultRouter {
    pub listen_address: SocketAddr,
    pub auth_endpoint: String,
}

/// Routes new user to cplane-provided compute node, authenticating via link
pub struct LinkRouter {
    pub listen_address: SocketAddr,
    pub redirect_uri: String,
}

/// Route to existing postgres instance. For local development only.
pub struct StaticRouter {
    pub listen_address: SocketAddr,
    pub postgres_host: String,
    pub postgres_port: u16,
}

// TODO try a trait instead?
pub enum Router {
    Default(DefaultRouter),
    Link(LinkRouter),
    Static(StaticRouter),
}
