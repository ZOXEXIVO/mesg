/// Push
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushRequest {
    #[prost(string, tag = "1")]
    pub queue: std::string::String,
    #[prost(bytes, tag = "2")]
    pub data: std::vec::Vec<u8>,
    #[prost(int32, tag = "3")]
    pub len: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushResponse {
    #[prost(bool, tag = "1")]
    pub ack: bool,
}
// Pull

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PullRequest {
    #[prost(string, tag = "1")]
    pub queue: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PullResponse {
    #[prost(string, tag = "1")]
    pub message_id: std::string::String,
    #[prost(bytes, tag = "2")]
    pub data: std::vec::Vec<u8>,
    #[prost(int32, tag = "3")]
    pub len: i32,
}
// Commit

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitRequest {
    #[prost(string, tag = "1")]
    pub queue: std::string::String,
    #[prost(string, tag = "2")]
    pub message_id: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitResponse {}
#[doc = r" Generated server implementations."]
pub mod mesg_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with MesgServiceServer."]
    #[async_trait]
    pub trait MesgService: Send + Sync + 'static {
        async fn push(
            &self,
            request: tonic::Request<super::PushRequest>,
        ) -> Result<tonic::Response<super::PushResponse>, tonic::Status>;
        #[doc = "Server streaming response type for the Pull method."]
        type PullStream: Stream<Item = Result<super::PullResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn pull(
            &self,
            request: tonic::Request<super::PullRequest>,
        ) -> Result<tonic::Response<Self::PullStream>, tonic::Status>;
        async fn commit(
            &self,
            request: tonic::Request<super::CommitRequest>,
        ) -> Result<tonic::Response<super::CommitResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MesgServiceServer<T: MesgService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: MesgService> MesgServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for MesgServiceServer<T>
    where
        T: MesgService,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/grpc.MesgService/Push" => {
                    #[allow(non_camel_case_types)]
                    struct PushSvc<T: MesgService>(pub Arc<T>);
                    impl<T: MesgService> tonic::server::UnaryService<super::PushRequest> for PushSvc<T> {
                        type Response = super::PushResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PushRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).push(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = PushSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/grpc.MesgService/Pull" => {
                    #[allow(non_camel_case_types)]
                    struct PullSvc<T: MesgService>(pub Arc<T>);
                    impl<T: MesgService> tonic::server::ServerStreamingService<super::PullRequest> for PullSvc<T> {
                        type Response = super::PullResponse;
                        type ResponseStream = T::PullStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PullRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).pull(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = PullSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/grpc.MesgService/Commit" => {
                    #[allow(non_camel_case_types)]
                    struct CommitSvc<T: MesgService>(pub Arc<T>);
                    impl<T: MesgService> tonic::server::UnaryService<super::CommitRequest> for CommitSvc<T> {
                        type Response = super::CommitResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CommitRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).commit(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = CommitSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: MesgService> Clone for MesgServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: MesgService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MesgService> tonic::transport::NamedService for MesgServiceServer<T> {
        const NAME: &'static str = "grpc.MesgService";
    }
}
