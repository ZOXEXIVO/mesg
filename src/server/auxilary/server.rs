use crate::metrics::StaticMetricsWriter;
use crate::server::transport::proto::PROTOFILE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use log::info;
use std::convert::Infallible;

pub struct AuxiliaryServer;

impl AuxiliaryServer {
    pub async fn start(port: u16) {
        let bind_address = ([0, 0, 0, 0], port).into();

        let server = Server::bind(&bind_address).serve(make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(handle_func))
        }));

        info!("protofile: http://0.0.0.0:{}/proto", port);
        info!("metrics endpoint: http://0.0.0.0:{}/metrics", port);

        server.await.unwrap();
    }
}

async fn handle_func(request: Request<Body>) -> Result<Response<Body>, Infallible> {
    match request.uri().to_string().as_ref() {
        "/proto" => proto(request),
        "/metrics" => metrics(request),
        _ => Ok(Response::new(Body::empty())),
    }
}

fn proto(_: Request<Body>) -> Result<Response<Body>, Infallible> {
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/protobuf")
        .body(Body::from(PROTOFILE))
        .unwrap();

    Ok(response)
}

fn metrics(_: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut result = String::with_capacity(4096);

    StaticMetricsWriter::write(&mut result);

    Ok(Response::new(Body::from(result)))
}
