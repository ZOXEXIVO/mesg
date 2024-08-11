use crate::metrics::StaticMetricsWriter;
use crate::server::transport::proto::PROTOFILE;
use std::{convert::Infallible, net::SocketAddr};
use http_body_util::Full;
use hyper::{body::{Bytes}, server::conn::http1, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use log::info;
use tokio::net::TcpListener;
use tower::ServiceBuilder;

pub struct AuxiliaryServer;

impl AuxiliaryServer {
    pub async fn start(port: u16) {
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();

        let listener = TcpListener::bind(&addr).await.unwrap();

        info!("protofile: http://127.0.0.1:{}/proto", port);
        info!("metrics endpoint: http://127.0.0.1:{}/metrics", port);

        loop {
            let (stream, _) = listener.accept().await.unwrap();

            tokio::spawn(async move {
                let svc = ServiceBuilder::new().service(
                    hyper::service::service_fn(handle_func)
                );

                if let Err(err) = http1::Builder::new().serve_connection(TokioIo::new(stream), svc).await {
                    eprintln!("server error: {}", err);
                }
            });
        }
    }
}

async fn handle_func(request: Request<impl hyper::body::Body>) -> Result<Response<Full<Bytes>>, Infallible> {
    match request.uri().to_string().as_ref() {
        "/proto" => proto(request),
        "/metrics" => metrics(request),
        _ => Ok(Response::new(Full::new(Bytes::from("Hello World!"))))
    }
}

fn proto(_: Request<impl hyper::body::Body>) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/protobuf")
        .body(Full::from(Bytes::from(PROTOFILE)))
        .unwrap();

    Ok(response)
}

fn metrics(_: Request<impl hyper::body::Body>) -> Result<Response<Full<Bytes>>, Infallible> {
    let mut result = String::with_capacity(4096);

    StaticMetricsWriter::write(&mut result);

    Ok(Response::new(Full::from(result)))
}
