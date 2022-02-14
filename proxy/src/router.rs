/// Routes registered users to cplane-provided compute node
pub struct DefaultRouter {
    pub auth_endpoint: String,
}

/// Routes new user to cplane-provided compute node, authenticating via link
pub struct LinkRouter {
    pub redirect_uri: String,
}

/// DefaultRouter for usernames ending in @zenith, otherwise LinkRouter
pub struct MixedRouter {
    pub default: DefaultRouter,
    pub link: LinkRouter,
}

/// Route to existing postgres instance. For local development only.
pub struct StaticRouter {
    pub postgres_host: String,
    pub postgres_port: u16,
}

// TODO try a trait instead?
// a router is anything that can construct an auth
#[non_exhaustive]
pub enum Router {
    Default(DefaultRouter),
    Link(LinkRouter),
    Mixed(MixedRouter),
    Static(StaticRouter),
}
