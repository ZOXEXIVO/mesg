mod auxilary;
mod service;
mod transport;

use tonic::transport::Server;

use crate::cluster::cluster::Cluster;
use log::info;

use crate::controller::MesgController;
use crate::server::auxilary::AuxiliaryServer;
use crate::server::service::MesgService;
use crate::server::transport::grpc_impl::MesgGrpcImplService;
use crate::server::transport::mesg_protocol_server::MesgProtocolServer;
use crate::storage::Storage;
use std::thread::JoinHandle;
use tokio::runtime::Runtime;

pub use crate::server::transport::grpc::PullResponse;
use std::sync::Arc;

pub struct MesgServerOptions {
    pub db_path: String,
    pub port: u16,
    pub metric_port: u16,
}

pub struct MesgServer {
    cluster: Cluster,
    storage: Option<Arc<Storage>>,
    metrics_server_thread: Option<JoinHandle<()>>,
}

impl MesgServer {
    pub fn new() -> Self {
        MesgServer {
            cluster: Cluster::new(),
            storage: None,
            metrics_server_thread: None,
        }
    }

    pub async fn run(&mut self, options: MesgServerOptions) -> Result<(), std::io::Error> {
        let service_port = options.port;

        self.metrics_server_thread = Some(MesgServer::start_service_server(options.metric_port));

        let addr = format!("0.0.0.0:{0}", service_port).parse().unwrap();

        info!("listening: {0}", addr);

        self.storage = Some(Arc::new(Storage::new()));

        let cloned_storage = Arc::clone(self.storage.as_ref().unwrap());

        let controller = MesgController::new(cloned_storage);

        controller.start_jobs();

        let service = MesgService::new(controller);

        Server::builder()
            .add_service(MesgProtocolServer::new(MesgGrpcImplService::new(service)))
            .serve(addr)
            .await
            .unwrap();

        Ok(())
    }

    fn start_service_server(port: u16) -> JoinHandle<()> {
        std::thread::spawn(move || {
            Runtime::new()
                .unwrap()
                .block_on(AuxiliaryServer::start(port));
        })
    }
}
