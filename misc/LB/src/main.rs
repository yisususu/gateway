use async_trait::async_trait;
use pingora_core::server::Server;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::Result;
use pingora_proxy::{ProxyHttp, Session};
use std::env;

pub struct Gateway;

#[async_trait]
impl ProxyHttp for Gateway {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let addr = env::var("BACKEND_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
        let addr: std::net::SocketAddr = addr
            .parse()
            .map_err(|e| pingora_core::Error::because(
                pingora_core::ErrorType::InternalError,
                format!("invalid BACKEND_ADDR: {e}"),
                e,
            ))?;

        Ok(Box::new(HttpPeer::new(addr, false, String::new())))
    }
}

fn main() {
    let listen_port: u16 = env::var("LISTEN_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6198);

    let mut server = Server::new(None).unwrap();
    server.bootstrap();

    let mut proxy = pingora_proxy::http_proxy_service(&server.configuration, Gateway);
    proxy.add_tcp(&format!("0.0.0.0:{listen_port}"));

    server.add_service(proxy);
    server.run_forever();
}
