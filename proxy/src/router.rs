use std::net::SocketAddr;

/// For users who already have credentials
pub struct DefaultRouter {
    pub listen_address: SocketAddr,
    pub auth_endpoint: String,
}

/// For quick-starting new users
pub struct LinkRouter {
    pub listen_address: SocketAddr,
    pub redirect_uri: String,
}

/// For local development
pub struct StaticRouter {
    pub listen_address: SocketAddr,
    pub postgres_host: String,
    pub postgres_port: u16,
}

// TODO try a trait instead
pub enum Router {
    Default(DefaultRouter),
    Link(LinkRouter),
    Static(StaticRouter),
}
