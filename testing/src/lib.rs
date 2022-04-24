mod grpc;

#[cfg(test)]
mod tests {
    use crate::grpc::{mesg_protocol_client::MesgProtocolClient, PullRequest, PushRequest};
    use std::env;

    const DEFAULT_MESG_URL: &'static str = "http://localhost:35000";

    // #[tokio::test]
    // async fn push_pull_once_success() {
    //     let queue = String::from("queue1");
    //
    //     let mut client = create_client().await;
    //
    //     let mut pull_stream = client
    //         .pull(PullRequest {
    //             queue: String::clone(&queue),
    //             application: String::from("app1"),
    //             invisibility_timeout: 100,
    //         })
    //         .await
    //         .unwrap()
    //         .into_inner();
    //
    //     let push_response = client
    //         .push(tonic::Request::new(PushRequest {
    //             queue: String::clone(&queue),
    //             data: vec![1, 2, 3],
    //         }))
    //         .await
    //         .unwrap()
    //         .into_inner();
    //
    //     assert_eq!(true, push_response.success);
    //
    //     if let Ok(stream_item) = pull_stream.message().await {
    //         let item = stream_item.unwrap();
    //
    //         assert_eq!(3, item.data.len());
    //         assert_eq!(1, item.data[0]);
    //         assert_eq!(2, item.data[1]);
    //         assert_eq!(3, item.data[2]);
    //     }
    // }

    #[tokio::test]
    async fn push_pull_many_success() {
        let queue = String::from("queue2");

        let mut client = create_client().await;

        let mut pull_stream = client
            .pull(PullRequest {
                queue: String::clone(&queue),
                application: String::from("app2"),
                invisibility_timeout: 100,
            })
            .await
            .unwrap()
            .into_inner();
  
        for _ in 0..2 {
            let push_response = client
                .push(tonic::Request::new(PushRequest {
                    queue: String::clone(&queue),
                    data: vec![3, 2, 1],
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(true, push_response.success);
        }

        for _ in 0..2 {
            if let Ok(stream_item) = pull_stream.message().await {
                let item = stream_item.unwrap();

                assert_eq!(3, item.data.len());
                assert_eq!(3, item.data[0]);
                assert_eq!(2, item.data[1]);
                assert_eq!(1, item.data[2]);
            }
        }
    }

    async fn create_client() -> MesgProtocolClient<tonic::transport::Channel> {
        let mesg_url = env::var("MESG_URL").unwrap_or(String::from(DEFAULT_MESG_URL));

        MesgProtocolClient::connect(mesg_url).await.unwrap()
    }
}
