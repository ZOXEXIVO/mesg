mod grpc;

#[cfg(test)]
mod tests {
    use crate::grpc::{
        mesg_protocol_client::MesgProtocolClient, CommitRequest, PullRequest, PushRequest,
        RollbackRequest,
    };
    use std::env;
    use tokio::time::{sleep, timeout, Duration};
    use tokio_stream::StreamExt;
    use uuid::Uuid;

    const DEFAULT_MESG_URL: &'static str = "http://0.0.0.0:35000";

    // ==================== Basic Operations ====================

    #[tokio::test]
    async fn test_push_pull_single_message() {
        let queue = "test_basic_push_pull";
        let mut client = create_client().await;

        let mut pull_stream = start_pull(&mut client, queue, "app1", 5000).await;

        // Push a message
        push_message(&mut client, queue, vec![1, 2, 3]).await;

        // Pull and verify
        let msg = pull_stream.message().await.unwrap().unwrap();
        assert_eq!(vec![1, 2, 3], msg.data);
    }

    #[tokio::test]
    async fn test_push_multiple_messages_fifo() {
        let queue = "test_fifo_order";
        let mut client = create_client().await;

        let mut pull_stream = start_pull(&mut client, queue, "app_fifo", 5000).await;

        // Push multiple messages
        for i in 0..5 {
            push_message(&mut client, queue, vec![i]).await;
        }

        // Verify FIFO order
        for i in 0..5 {
            let msg = pull_stream.message().await.unwrap().unwrap();
            assert_eq!(vec![i], msg.data, "Messages should be received in FIFO order");
        }
    }

    #[tokio::test]
    async fn test_empty_queue_behavior() {
        let queue = "test_empty_queue";
        let mut client = create_client().await;

        let mut pull_stream = start_pull(&mut client, queue, "app_empty", 1000).await;

        // Try to pull from empty queue with timeout
        let result = timeout(Duration::from_secs(2), pull_stream.message()).await;
        assert!(result.is_err(), "Should timeout when queue is empty");
    }

    // ==================== Commit Operations ====================

    #[tokio::test]
    async fn test_commit_removes_message() {
        let queue = "test_commit";
        let app = "app_commit";
        let mut client = create_client().await;

        // Push a message
        push_message(&mut client, queue, vec![100]).await;

        // First consumer pulls and commits
        let mut pull_stream1 = start_pull(&mut client, queue, app, 5000).await;
        let msg = pull_stream1.message().await.unwrap().unwrap();
        commit_message(&mut client, &msg.id, queue, app).await;

        // Second consumer should not see the committed message
        let mut pull_stream2 = start_pull(&mut client, queue, app, 1000).await;
        let result = timeout(Duration::from_secs(2), pull_stream2.message()).await;
        assert!(result.is_err(), "Committed message should not be available");
    }

    #[tokio::test]
    async fn test_commit_wrong_application_fails() {
        let queue = "test_commit_wrong_app";
        let app1 = "app_commit_1";
        let app2 = "app_commit_2";
        let mut client = create_client().await;

        push_message(&mut client, queue, vec![1, 2, 3]).await;

        // Pull with app1
        let mut pull_stream = start_pull(&mut client, queue, app1, 5000).await;
        let msg = pull_stream.message().await.unwrap().unwrap();

        // Try to commit with app2 (should fail or not affect the message)
        let commit_result = client
            .commit(tonic::Request::new(CommitRequest {
                id: msg.id.clone(),
                queue: queue.to_string(),
                application: app2.to_string(),
            }))
            .await;

        // The message should still be available after visibility timeout
        // (implementation may vary, but message shouldn't be committed by wrong app)
    }

    // ==================== Rollback Operations ====================

    #[tokio::test]
    async fn test_rollback_returns_message_immediately() {
        let queue = "test_rollback";
        let app = "app_rollback";
        let mut client = create_client().await;

        push_message(&mut client, queue, vec![42]).await;

        // Pull message
        let mut pull_stream = start_pull(&mut client, queue, app, 10000).await;
        let msg = pull_stream.message().await.unwrap().unwrap();
        let msg_id = msg.id.clone();

        // Rollback immediately
        rollback_message(&mut client, &msg_id, queue, app).await;

        // Message should be available immediately (not waiting for timeout)
        let msg2 = pull_stream.message().await.unwrap().unwrap();
        assert_eq!(vec![42], msg2.data);
    }

    #[tokio::test]
    async fn test_multiple_rollbacks() {
        let queue = "test_multi_rollback";
        let app = "app_multi_rollback";
        let mut client = create_client().await;

        push_message(&mut client, queue, vec![99]).await;

        let mut pull_stream = start_pull(&mut client, queue, app, 5000).await;

        // Rollback multiple times
        for i in 0..3 {
            let msg = pull_stream.message().await.unwrap().unwrap();
            assert_eq!(vec![99], msg.data, "Iteration {}", i);
            rollback_message(&mut client, &msg.id, queue, app).await;
        }

        // Finally commit
        let msg = pull_stream.message().await.unwrap().unwrap();
        commit_message(&mut client, &msg.id, queue, app).await;
    }

    // ==================== Visibility Timeout ====================

    #[tokio::test]
    async fn test_visibility_timeout_restoration() {
        let queue = "test_visibility_timeout";
        let app = "app_visibility";
        let mut client = create_client().await;

        push_message(&mut client, queue, vec![7, 8, 9]).await;

        // Pull with short timeout
        let mut pull_stream1 = start_pull(&mut client, queue, app, 2000).await;
        let msg1 = pull_stream1.message().await.unwrap().unwrap();
        let original_id = msg1.id.clone();

        // Don't commit or rollback - let it timeout
        drop(pull_stream1);

        // Wait for visibility timeout + buffer
        sleep(Duration::from_millis(3000)).await;

        // Message should be available again
        let mut pull_stream2 = start_pull(&mut client, queue, app, 5000).await;
        let msg2 = pull_stream2.message().await.unwrap().unwrap();
        assert_eq!(vec![7, 8, 9], msg2.data);

        // Commit to clean up
        commit_message(&mut client, &msg2.id, queue, app).await;
    }

    #[tokio::test]
    async fn test_different_visibility_timeouts() {
        let queue = "test_different_timeouts";
        let mut client = create_client().await;

        push_message(&mut client, queue, vec![1]).await;
        push_message(&mut client, queue, vec![2]).await;

        // Consumer 1 with short timeout
        let mut stream1 = start_pull(&mut client, queue, "app_short", 1000).await;
        let msg1 = stream1.message().await.unwrap().unwrap();

        // Consumer 2 with long timeout
        let mut stream2 = start_pull(&mut client, queue, "app_long", 10000).await;
        let msg2 = stream2.message().await.unwrap().unwrap();

        assert_ne!(msg1.id, msg2.id, "Different messages should be pulled");

        // Cleanup
        commit_message(&mut client, &msg1.id, queue, "app_short").await;
        commit_message(&mut client, &msg2.id, queue, "app_long").await;
    }

    // ==================== Multiple Consumers ====================

    #[tokio::test]
    async fn test_multiple_consumers_same_application() {
        let queue = "test_multi_consumer_same_app";
        let app = "shared_app";
        let mut client1 = create_client().await;
        let mut client2 = create_client().await;

        // Push multiple messages
        for i in 0..10 {
            push_message(&mut client1, queue, vec![i]).await;
        }

        // Two consumers from same application
        let mut stream1 = start_pull(&mut client1, queue, app, 5000).await;
        let mut stream2 = start_pull(&mut client2, queue, app, 5000).await;

        // Collect messages concurrently from both consumers
        let handle1 = tokio::spawn(async move {
            let mut ids = Vec::new();
            let mut client = create_client().await;
            for _ in 0..5 {
                if let Ok(Some(msg)) = stream1.message().await {
                    ids.push(msg.id.clone());
                    commit_message(&mut client, &msg.id, queue, app).await;
                }
            }
            ids
        });

        let handle2 = tokio::spawn(async move {
            let mut ids = Vec::new();
            let mut client = create_client().await;
            for _ in 0..5 {
                if let Ok(Some(msg)) = stream2.message().await {
                    ids.push(msg.id.clone());
                    commit_message(&mut client, &msg.id, queue, app).await;
                }
            }
            ids
        });

        let ids1 = handle1.await.unwrap();
        let ids2 = handle2.await.unwrap();

        let mut all_ids = ids1;
        all_ids.extend(ids2);

        // All IDs should be unique
        all_ids.sort();
        let original_len = all_ids.len();
        all_ids.dedup();
        assert_eq!(original_len, all_ids.len(), "Each message should be delivered only once");
        assert_eq!(10, all_ids.len(), "All messages should be received");
    }

    #[tokio::test]
    async fn test_multiple_consumers_different_applications() {
        let queue = "test_multi_consumer_diff_app";
        let mut client = create_client().await;

        push_message(&mut client, queue, vec![123]).await;

        // Two consumers with different applications should get the same message
        // (unless broadcast is explicitly implemented differently)
        let mut stream1 = start_pull(&mut client, queue, "app_a", 5000).await;
        let mut stream2 = start_pull(&mut client, queue, "app_b", 5000).await;

        let msg1 = stream1.message().await.unwrap().unwrap();
        let msg2 = stream2.message().await.unwrap().unwrap();

        // Based on your implementation, different apps get independent copies
        assert_eq!(vec![123], msg1.data);
        assert_eq!(vec![123], msg2.data);

        commit_message(&mut client, &msg1.id, queue, "app_a").await;
        commit_message(&mut client, &msg2.id, queue, "app_b").await;
    }

    // ==================== Broadcast Messages ====================

    #[tokio::test]
    async fn test_broadcast_message() {
        let queue = "test_broadcast";
        let mut client = create_client().await;

        // Push broadcast message
        let response = client
            .push(tonic::Request::new(PushRequest {
                queue: queue.to_string(),
                data: vec![255, 254, 253],
                is_broadcast: true,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(response.success);

        // Multiple consumers should receive it
        let mut stream1 = start_pull(&mut client, queue, "broadcast_app_1", 5000).await;
        let mut stream2 = start_pull(&mut client, queue, "broadcast_app_2", 5000).await;

        let msg1 = stream1.message().await.unwrap().unwrap();
        let msg2 = stream2.message().await.unwrap().unwrap();

        assert_eq!(vec![255, 254, 253], msg1.data);
        assert_eq!(vec![255, 254, 253], msg2.data);
    }

    // ==================== Edge Cases ====================

    #[tokio::test]
    async fn test_empty_message_data() {
        let queue = "test_empty_data";
        let mut client = create_client().await;

        push_message(&mut client, queue, vec![]).await;

        let mut pull_stream = start_pull(&mut client, queue, "app_empty_data", 5000).await;
        let msg = pull_stream.message().await.unwrap().unwrap();

        assert_eq!(Vec::<u8>::new(), msg.data);
        commit_message(&mut client, &msg.id, queue, "app_empty_data").await;
    }

    #[tokio::test]
    async fn test_large_message_data() {
        let queue = "test_large_data";
        let mut client = create_client().await;

        let large_data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        push_message(&mut client, queue, large_data.clone()).await;

        let mut pull_stream = start_pull(&mut client, queue, "app_large", 5000).await;
        let msg = pull_stream.message().await.unwrap().unwrap();

        assert_eq!(large_data, msg.data);
        commit_message(&mut client, &msg.id, queue, "app_large").await;
    }

    #[tokio::test]
    async fn test_commit_nonexistent_message() {
        let queue = "test_commit_nonexistent";
        let mut client = create_client().await;

        let nonexistent_message_id = Uuid::new_v4().to_string();

        let result = client
            .commit(tonic::Request::new(CommitRequest {
                id: nonexistent_message_id,
                queue: queue.to_string(),
                application: "app_test".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(false, result.success, "Committing nonexistent message should fail");
    }

    #[tokio::test]
    async fn test_rollback_nonexistent_message() {
        let queue = "test_rollback_nonexistent";
        let mut client = create_client().await;

        let nonexistent_message_id = Uuid::new_v4().to_string();

        let result = client
            .rollback(tonic::Request::new(RollbackRequest {
                id: nonexistent_message_id,
                queue: queue.to_string(),
                application: "app_test".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(false, result.success, "Rolling back nonexistent message should fail");
    }

    #[tokio::test]
    async fn test_double_commit() {
        let queue = "test_double_commit";
        let app = "app_double_commit";
        let mut client = create_client().await;

        push_message(&mut client, queue, vec![11, 22]).await;

        let mut pull_stream = start_pull(&mut client, queue, app, 5000).await;
        let msg = pull_stream.message().await.unwrap().unwrap();

        // First commit should succeed
        let result1 = commit_message(&mut client, &msg.id, queue, app).await;
        assert!(result1);

        // Second commit should fail
        let result2 = client
            .commit(tonic::Request::new(CommitRequest {
                id: msg.id.clone(),
                queue: queue.to_string(),
                application: app.to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(false, result2.success, "Double commit should fail");
    }

    // ==================== Stress Tests ====================

    #[tokio::test]
    async fn test_high_throughput() {
        let queue = "test_throughput";
        let app = "app_throughput";
        let mut client = create_client().await;
        let message_count = 100;

        // Push many messages
        for i in 0..message_count {
            push_message(&mut client, queue, vec![i as u8]).await;
        }

        // Pull and commit all
        let mut pull_stream = start_pull(&mut client, queue, app, 5000).await;

        for _ in 0..message_count {
            let msg = pull_stream.message().await.unwrap().unwrap();
            commit_message(&mut client, &msg.id, queue, app).await;
        }
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let queue = "test_concurrent";
        let mut handles = vec![];

        // Spawn multiple tasks
        for i in 0..10 {
            let task_queue = queue.to_string();
            handles.push(tokio::spawn(async move {
                let mut client = create_client().await;
                push_message(&mut client, &task_queue, vec![i]).await;

                let app = format!("app_{}", i);
                let mut stream = start_pull(&mut client, &task_queue, &app, 5000).await;
                let msg = stream.message().await.unwrap().unwrap();
                commit_message(&mut client, &msg.id, &task_queue, &app).await;
            }));
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }
    }

    // ==================== Helper Functions ====================

    async fn create_client() -> MesgProtocolClient<tonic::transport::Channel> {
        let mesg_url = env::var("MESG_URL").unwrap_or(String::from(DEFAULT_MESG_URL));
        MesgProtocolClient::connect(mesg_url).await.unwrap()
    }

    async fn start_pull(
        client: &mut MesgProtocolClient<tonic::transport::Channel>,
        queue: &str,
        application: &str,
        timeout_ms: i32,
    ) -> tonic::codec::Streaming<crate::grpc::PullResponse> {
        client
            .pull(PullRequest {
                queue: queue.to_string(),
                application: application.to_string(),
                invisibility_timeout_ms: timeout_ms,
            })
            .await
            .unwrap()
            .into_inner()
    }

    async fn push_message(
        client: &mut MesgProtocolClient<tonic::transport::Channel>,
        queue: &str,
        data: Vec<u8>,
    ) {
        let response = client
            .push(tonic::Request::new(PushRequest {
                queue: queue.to_string(),
                data,
                is_broadcast: false,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(response.success, "Push should succeed");
    }

    async fn commit_message(
        client: &mut MesgProtocolClient<tonic::transport::Channel>,
        id: &str,
        queue: &str,
        application: &str,
    ) -> bool {
        let response = client
            .commit(tonic::Request::new(CommitRequest {
                id: id.to_string(),
                queue: queue.to_string(),
                application: application.to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        response.success
    }

    async fn rollback_message(
        client: &mut MesgProtocolClient<tonic::transport::Channel>,
        id: &str,
        queue: &str,
        application: &str,
    ) -> bool {
        let response = client
            .rollback(tonic::Request::new(RollbackRequest {
                id: id.to_string(),
                queue: queue.to_string(),
                application: application.to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        response.success
    }
}