use log::{info};
use std::convert::Infallible;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use crate::metrics::MetricsWriter;

pub struct MetricsServer;

impl MetricsServer {
    pub async fn start(port: u16) {
        let bind_address = ([0, 0, 0, 0], port).into();

        let server = Server::bind(&bind_address)
            .serve(make_service_fn(|_conn| {
                async { Ok::<_, Infallible>(service_fn(metrics)) }
            }));

        info!("metrics listening 0.0.0.0:{}", port);

        server.await.unwrap();
    }
}

async fn metrics(_: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut result = String::with_capacity(2048);

    MetricsWriter::write(&mut result);

    Ok(Response::new(Body::from(result)))
}