use crate::metrics::StaticMetricsWriter;
use crate::server::transport::proto::PROTOFILE;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::{Service};
use hyper::{body::Incoming as IncomingBody, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use log::info;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::net::TcpListener;

pub struct AuxiliaryServer;

impl AuxiliaryServer {
    pub async fn start(port: u16) {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));

        let listener = TcpListener::bind(addr).await.unwrap();

        info!("protofile: http://0.0.0.0:{}/proto", port);
        info!("metrics endpoint: http://0.0.0.0:{}/metrics", port);

        let svc = AuxiliarySvc::new();

        loop {
            let (stream, _) = listener.accept().await.unwrap();

            let io = TokioIo::new(stream);

            let svc_clone = svc.clone();

            // Spawn a tokio task to serve multiple connections concurrently
            tokio::task::spawn(async move {
                if let Err(err) = http1::Builder::new().serve_connection(io, svc_clone).await {
                    println!("Failed to serve connection: {:?}", err);
                }
            });
        }
    }
}

#[derive(Debug, Clone)]
struct AuxiliarySvc {}

impl AuxiliarySvc {
    pub fn new() -> Self {
        AuxiliarySvc {}
    }
}

impl Service<Request<IncomingBody>> for AuxiliarySvc {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<IncomingBody>) -> Self::Future {
        fn mk_response(s: String) -> Result<Response<Full<Bytes>>, hyper::Error> {
            Ok(Response::builder().body(Full::new(Bytes::from(s))).unwrap())
        }

        let res = match req.uri().path() {
            "/proto" => proto(req),
            "/metrics" => metrics(req),
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("")))
                .unwrap()),
        };

        Box::pin(async { res })
    }
}

fn proto(_body: Request<IncomingBody>) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/protobuf")
        .body(Full::new(Bytes::from(PROTOFILE)))
        .unwrap();

    Ok(response)
}

fn metrics(_body: Request<IncomingBody>) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let mut result = String::with_capacity(4096);

    StaticMetricsWriter::write(&mut result);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; charset=UTF-8")
        .body(Full::new(Bytes::from(result)))
        .unwrap();

    Ok(response)
}
