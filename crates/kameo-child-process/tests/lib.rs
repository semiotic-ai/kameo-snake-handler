//! High-concurrency async tests for core IPC protocol logic (no real process spawning)

// On-wire uses serde-brief; tests derive serde only
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::Once;
use std::time::Duration;
use tracing::trace;

static INIT: Once = Once::new();

fn init_tracing() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    });
}

// Import core types from the crate
use kameo_child_process::framing::{LengthPrefixedRead, LengthPrefixedWrite};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DummyMsg {
    id: u64,
}

#[derive(Clone)]
struct DummyHandler;
#[async_trait::async_trait]
impl kameo_child_process::callback::TypedCallbackHandler<DummyMsg> for DummyHandler {
    type Response = ();

    async fn handle_callback(
        &self,
        _cb: DummyMsg,
    ) -> Result<
        Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<
                            Self::Response,
                            kameo_child_process::error::PythonExecutionError,
                        >,
                    > + Send,
            >,
        >,
        kameo_child_process::error::PythonExecutionError,
    > {
        use futures::stream;
        Ok(Box::pin(stream::once(async move { Ok(()) })))
    }

    fn type_name(&self) -> &'static str {
        "DummyMsg"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DummyParentMsg {
    id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct DummyParentOk {
    id: u64,
}

impl kameo_child_process::KameoChildProcessMessage for DummyParentMsg {
    type Ok = DummyParentOk;
}

// Implement KameoChildProcessMessage for DummyMsg so it can be used with setup_test_ipc_pair
impl kameo_child_process::KameoChildProcessMessage for DummyMsg {
    type Ok = ();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_callback_message() {
    trace!(
        event = "test_start",
        name = "test_single_callback_message",
        "Starting test"
    );
    init_tracing();
    tokio::time::timeout(Duration::from_secs(5), async {
        // Set up a pair of connected UnixStreams
        let (sock1, sock2) = tokio::net::UnixStream::pair().unwrap();
        let parent_duplex = kameo_child_process::DuplexUnixStream::new(sock1);
        let child_duplex = kameo_child_process::DuplexUnixStream::new(sock2);

        // Create dynamic callback module and register handler
        let mut callback_module = kameo_child_process::callback::DynamicCallbackModule::new();
        callback_module
            .register_handler::<DummyMsg, DummyHandler>("test", DummyHandler)
            .unwrap();

        // Create typed callback receiver
        let (read_half, write_half) = parent_duplex.into_split();
        let reader = LengthPrefixedRead::new(read_half);
        let writer = LengthPrefixedWrite::new(write_half);
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let receiver = kameo_child_process::callback::TypedCallbackReceiver::new(
            std::sync::Arc::new(callback_module),
            reader,
            writer,
            cancellation_token.clone(),
            "actor_type",
        );

        let receiver_task = tokio::spawn(async move {
            receiver.run().await.unwrap();
        });

        // Spawn a fake child artefact using the other end
        let child_task = tokio::spawn(async move {
            let (read_half, write_half) = child_duplex.into_split();
            let mut writer = LengthPrefixedWrite::new(write_half);

            // Create typed callback envelope
            let callback_data = serde_brief::to_vec(&DummyMsg { id: 42 }).unwrap();
            let envelope = kameo_child_process::callback::TypedCallbackEnvelope {
                callback_path: "test.DummyMsg".to_string(),
                correlation_id: 1,
                callback_data,
                context: Default::default(),
            };

            trace!(
                event = "test_child",
                step = "send_callback",
                correlation_id = 1,
                "Child sending callback envelope"
            );
            if let Err(e) = writer.write_msg(&envelope).await {
                panic!("Child write error: {:?}", e);
            }

            // Read response
            let mut reader = LengthPrefixedRead::new(read_half);
            match reader
                .read_msg::<kameo_child_process::callback::TypedCallbackResponse>()
                .await
            {
                Ok(reply_envelope) => {
                    trace!(
                        event = "test_child",
                        step = "received_reply",
                        correlation_id = reply_envelope.correlation_id,
                        "Child received reply"
                    );
                    assert_eq!(reply_envelope.correlation_id, 1);
                    assert_eq!(reply_envelope.callback_path, "test.DummyMsg");
                }
                Err(e) => panic!("Child read error: {:?}", e),
            }
            // Drop reader/writer to close
            drop(reader);
            drop(writer);
        });

        child_task.await.unwrap();
        cancellation_token.cancel();
        receiver_task.await.unwrap();
    })
    .await
    .expect("Test timed out");
}

// Helper to create a UnixStream pair, perform handshake, and return ready-to-use sockets
pub async fn setup_test_ipc_pair<M>() -> (tokio::net::UnixStream, tokio::net::UnixStream)
where
    M: kameo_child_process::KameoChildProcessMessage + Send + Sync + 'static,
{
    let (parent_stream, child_stream) = tokio::net::UnixStream::pair().unwrap();
    // Remove handshake here - artefacts handle it
    (parent_stream, child_stream)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_parent_child_ipc() {
    trace!(
        event = "test_start",
        name = "test_parent_child_ipc",
        "Starting test"
    );
    init_tracing();

    // Initialize metrics
    kameo_child_process::metrics::init_metrics();

    use futures::future::join_all;
    use kameo_child_process::SubprocessIpcBackend;
    use kameo_child_process::SubprocessIpcChild;
    use std::time::Duration;
    use tokio::time::timeout;

    // Start a task to log metrics periodically
    let _metrics_task = tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        for _ in 0..90 {
            // Log for up to 3 minutes max
            interval.tick().await;
            kameo_child_process::metrics::MetricsReporter::log_metrics_state();
        }
    });

    #[derive(Clone)]
    struct DummyChildHandler;
    #[async_trait::async_trait]
    impl kameo_child_process::ChildProcessMessageHandler<DummyParentMsg> for DummyChildHandler {
        async fn handle_child_message(
            &mut self,
            msg: DummyParentMsg,
        ) -> Result<DummyParentOk, kameo_child_process::error::PythonExecutionError> {
            trace!(event = "handler", id = msg.id, "Handling message in child");
            Ok(DummyParentOk { id: msg.id })
        }
    }

    let (parent_stream, child_stream) = tokio::net::UnixStream::pair().unwrap();
    let p = kameo_child_process::DuplexUnixStream::new(parent_stream);
    let c = kameo_child_process::DuplexUnixStream::new(child_stream);

    let child_handler = DummyChildHandler;
    let backend = SubprocessIpcBackend::<DummyParentMsg>::from_duplex(p);

    let child_task = tokio::spawn(async move {
        let child = SubprocessIpcChild::<DummyParentMsg>::from_duplex(c);
        child.run(child_handler).await
    });

    // Run many concurrent requests
    let requests = 10_000;
    let futures = (0..requests)
        .map(|i| {
            let backend = backend.clone();
            async move {
                let msg = DummyParentMsg { id: i as u64 };
                let result = backend.send(msg).await;
                trace!(event = "parent_test", id = i, ?result, "Got result");
                result
            }
        })
        .collect::<Vec<_>>();

    let results = timeout(Duration::from_secs(120), join_all(futures))
        .await
        .expect("test timed out");

    for (i, result) in results.into_iter().enumerate() {
        assert!(result.is_ok(), "Request {} failed: {:?}", i, result);
        let ok = result.unwrap();
        assert_eq!(ok.id, i as u64, "Result has wrong id");
    }

    // Shutdown and wait for child task
    backend.shutdown();
    match timeout(Duration::from_secs(5), child_task).await {
        Ok(res) => {
            trace!(event = "test_shutdown", ?res, "Child task completed");
            res.expect("child task panicked")
                .expect("child task returned error");
        }
        Err(_) => {
            panic!("Child task failed to shut down in time");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_high_volume_multiple_callback_types() {
    trace!(
        event = "test_start",
        name = "test_high_volume_multiple_callback_types",
        "Starting test"
    );
    init_tracing();

    // Initialize metrics
    kameo_child_process::metrics::init_metrics();

    // Define multiple callback types for testing
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DataFetchCallback {
        id: u64,
        fetch_type: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ComputeCallback {
        id: u64,
        operation: String,
        value: f64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct BatchCallback {
        id: u64,
        batch_size: usize,
    }

    // Handlers for each callback type
    #[derive(Clone)]
    struct DataFetchHandler;
    #[async_trait::async_trait]
    impl kameo_child_process::callback::TypedCallbackHandler<DataFetchCallback> for DataFetchHandler {
        type Response = String;

        async fn handle_callback(
            &self,
            cb: DataFetchCallback,
        ) -> Result<
            Pin<
                Box<
                    dyn futures::Stream<
                            Item = Result<
                                Self::Response,
                                kameo_child_process::error::PythonExecutionError,
                            >,
                        > + Send,
                >,
            >,
            kameo_child_process::error::PythonExecutionError,
        > {
            use futures::stream;
            trace!(
                event = "data_fetch_handler",
                id = cb.id,
                fetch_type = cb.fetch_type,
                "Handling data fetch"
            );
            let responses = vec![
                format!("data_chunk_1_for_{}", cb.id),
                format!("data_chunk_2_for_{}", cb.id),
                format!("data_chunk_3_for_{}", cb.id),
            ];
            Ok(Box::pin(stream::iter(responses.into_iter().map(Ok))))
        }

        fn type_name(&self) -> &'static str {
            "DataFetchCallback"
        }
    }

    #[derive(Clone)]
    struct ComputeHandler;
    #[async_trait::async_trait]
    impl kameo_child_process::callback::TypedCallbackHandler<ComputeCallback> for ComputeHandler {
        type Response = f64;

        async fn handle_callback(
            &self,
            cb: ComputeCallback,
        ) -> Result<
            Pin<
                Box<
                    dyn futures::Stream<
                            Item = Result<
                                Self::Response,
                                kameo_child_process::error::PythonExecutionError,
                            >,
                        > + Send,
                >,
            >,
            kameo_child_process::error::PythonExecutionError,
        > {
            use futures::stream;
            trace!(
                event = "compute_handler",
                id = cb.id,
                operation = cb.operation,
                value = cb.value,
                "Handling compute"
            );
            let result = match cb.operation.as_str() {
                "square" => cb.value * cb.value,
                "sqrt" => cb.value.sqrt(),
                "double" => cb.value * 2.0,
                _ => cb.value,
            };
            Ok(Box::pin(stream::once(async move { Ok(result) })))
        }

        fn type_name(&self) -> &'static str {
            "ComputeCallback"
        }
    }

    #[derive(Clone)]
    struct BatchHandler;
    #[async_trait::async_trait]
    impl kameo_child_process::callback::TypedCallbackHandler<BatchCallback> for BatchHandler {
        type Response = u64;

        async fn handle_callback(
            &self,
            cb: BatchCallback,
        ) -> Result<
            Pin<
                Box<
                    dyn futures::Stream<
                            Item = Result<
                                Self::Response,
                                kameo_child_process::error::PythonExecutionError,
                            >,
                        > + Send,
                >,
            >,
            kameo_child_process::error::PythonExecutionError,
        > {
            use futures::stream;
            trace!(
                event = "batch_handler",
                id = cb.id,
                batch_size = cb.batch_size,
                "Handling batch"
            );
            let items: Vec<u64> = (0..cb.batch_size as u64)
                .map(|i| cb.id * 1000 + i)
                .collect();
            Ok(Box::pin(stream::iter(items.into_iter().map(Ok))))
        }

        fn type_name(&self) -> &'static str {
            "BatchCallback"
        }
    }

    tokio::time::timeout(Duration::from_secs(180), async {
        tracing::info!("Test started - setting up high volume multi-type callback test");

        // No additional imports needed for this test

        // Start a task to log metrics periodically
        let _metrics_task = tokio::spawn(async {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            for _ in 0..90 {
                // Log for up to 3 minutes max
                interval.tick().await;
                kameo_child_process::metrics::MetricsReporter::log_metrics_state();
            }
        });

        tracing::info!("Creating socket pair");
        // Create the sockets
        let (parent_stream, child_stream) = tokio::net::UnixStream::pair().unwrap();
        let parent_duplex = kameo_child_process::DuplexUnixStream::new(parent_stream);
        let child_duplex = kameo_child_process::DuplexUnixStream::new(child_stream);

        // Create dynamic callback module and register multiple handlers
        let mut callback_module = kameo_child_process::callback::DynamicCallbackModule::new();
        callback_module
            .register_handler::<DataFetchCallback, DataFetchHandler>("data", DataFetchHandler)
            .unwrap();
        callback_module
            .register_handler::<ComputeCallback, ComputeHandler>("compute", ComputeHandler)
            .unwrap();
        callback_module
            .register_handler::<BatchCallback, BatchHandler>("batch", BatchHandler)
            .unwrap();

        // Create typed callback receiver
        let (read_half, write_half) = parent_duplex.into_split();
        let reader = LengthPrefixedRead::new(read_half);
        let writer = LengthPrefixedWrite::new(write_half);
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let receiver = kameo_child_process::callback::TypedCallbackReceiver::new(
            std::sync::Arc::new(callback_module),
            reader,
            writer,
            cancellation_token.clone(),
            "actor_type",
        );

        tracing::info!("Spawning receiver task");
        let receiver_task = tokio::spawn(async move {
            tracing::info!("Receiver task started");
            let result = receiver.run().await;
            tracing::info!("Receiver task completed with result: {:?}", result);
            result
        });

        tracing::info!("Starting high volume callback processing");
        // Test with 10k callbacks across multiple types by sending them through the child socket
        let total_callbacks = 10_000;
        let batch_size = 500;
        let mut all_sent = 0;

        // Create child writer to send callbacks
        let (child_read, child_write) = child_duplex.into_split();
        let mut writer = LengthPrefixedWrite::new(child_write);
        let mut reader = LengthPrefixedRead::new(child_read);

        for batch_start in (0..total_callbacks).step_by(batch_size) {
            let end = std::cmp::min(batch_start + batch_size, total_callbacks);
            tracing::info!("Processing batch {}-{}", batch_start, end);

            // Send all callbacks in this batch
            for i in batch_start..end {
                // Rotate between different callback types
                let (callback_path, callback_data) = match i % 3 {
                    0 => {
                        let callback = DataFetchCallback {
                            id: i as u64,
                            fetch_type: format!("type_{}", i % 5),
                        };
                        (
                            "data.DataFetchCallback",
                            serde_brief::to_vec(&callback).unwrap(),
                        )
                    }
                    1 => {
                        let operations = ["square", "sqrt", "double"];
                        let callback = ComputeCallback {
                            id: i as u64,
                            operation: operations[i % operations.len()].to_string(),
                            value: (i as f64) * 0.5,
                        };
                        (
                            "compute.ComputeCallback",
                            serde_brief::to_vec(&callback).unwrap(),
                        )
                    }
                    _ => {
                        let callback = BatchCallback {
                            id: i as u64,
                            batch_size: (i % 10) + 1,
                        };
                        (
                            "batch.BatchCallback",
                            serde_brief::to_vec(&callback).unwrap(),
                        )
                    }
                };

                // Send callback envelope
                let envelope = kameo_child_process::callback::TypedCallbackEnvelope {
                    callback_path: callback_path.to_string(),
                    correlation_id: i as u64,
                    callback_data,
                    context: Default::default(),
                };

                if let Err(e) = writer.write_msg(&envelope).await {
                    panic!("Failed to send callback {}: {}", i, e);
                }
                all_sent += 1;
            }

            tracing::info!("Sent {} callbacks in batch", end - batch_start);
        }

        tracing::info!("Sent all {} callbacks, now reading responses", all_sent);

        // Read responses (each callback may produce multiple responses)
        let mut total_responses = 0;
        let mut responses_by_callback: std::collections::HashMap<u64, u32> =
            std::collections::HashMap::new();

        // Read responses for a reasonable time
        let response_timeout = Duration::from_secs(30);
        let start_time = std::time::Instant::now();

        while start_time.elapsed() < response_timeout && total_responses < all_sent * 5 {
            match tokio::time::timeout(
                Duration::from_millis(100),
                reader.read_msg::<kameo_child_process::callback::TypedCallbackResponse>(),
            )
            .await
            {
                Ok(Ok(response)) => {
                    *responses_by_callback
                        .entry(response.correlation_id)
                        .or_insert(0) += 1;
                    total_responses += 1;

                    if total_responses % 1000 == 0 {
                        tracing::info!("Received {} responses so far", total_responses);
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!("Error reading response: {}", e);
                    break;
                }
                Err(_) => {
                    // Timeout waiting for response - this is normal when responses are done
                    if total_responses > 0 {
                        tracing::info!(
                            "Timeout waiting for more responses, stopping (got {} responses)",
                            total_responses
                        );
                        break;
                    }
                }
            }
        }

        tracing::info!(
            "Received {} total responses for {} unique callbacks",
            total_responses,
            responses_by_callback.len()
        );

        // Verify we got responses for most callbacks
        assert!(
            responses_by_callback.len() > total_callbacks / 2,
            "Expected responses for at least half of callbacks, got responses for {}/{}",
            responses_by_callback.len(),
            total_callbacks
        );
        assert!(
            total_responses > total_callbacks,
            "Expected more responses than callbacks due to streaming, got {}/{}",
            total_responses,
            total_callbacks
        );

        // Shutdown receiver
        tracing::info!("Shutting down receiver");
        cancellation_token.cancel();

        // Wait for receiver to complete
        tracing::info!("Waiting for receiver task");
        match tokio::time::timeout(Duration::from_secs(10), receiver_task).await {
            Ok(res) => {
                tracing::info!("Receiver task completed: {:?}", res);
                let _ = res.expect("receiver task join error");
            }
            Err(_) => {
                tracing::error!("Receiver task timed out");
                panic!("Receiver task failed to shut down in time");
            }
        }

        tracing::info!("High volume multi-type callback test completed successfully");
    })
    .await
    .expect("Test timeout");
}

// Refactor to use in-process simulation
#[tokio::test]
async fn test_child_process_exits_on_parent_disconnect() {
    use kameo_child_process::{run_child_actor_loop, KameoChildProcessMessage};
    use std::time::Duration;
    use tokio::net::UnixStream;
    use tokio::time::timeout;
    init_tracing();

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct DummyMsg;
    impl KameoChildProcessMessage for DummyMsg {
        type Ok = ();
    }

    #[derive(Clone)]
    struct DummyHandler;
    #[async_trait::async_trait]
    impl kameo_child_process::ChildProcessMessageHandler<DummyMsg> for DummyHandler {
        async fn handle_child_message(
            &mut self,
            _msg: DummyMsg,
        ) -> Result<(), kameo_child_process::error::PythonExecutionError> {
            Ok(())
        }
    }

    let (parent_stream, child_stream) =
        UnixStream::pair().expect("Failed to create UnixStream pair");
    let child_conn = Box::new(child_stream);
    let child_task =
        tokio::spawn(async move { run_child_actor_loop(DummyHandler, child_conn, None).await });

    // Simulate parent disconnect by dropping parent_stream
    drop(parent_stream);

    // Wait for child to exit
    let result = timeout(Duration::from_secs(5), child_task)
        .await
        .expect("timeout")
        .expect("join");
    assert!(
        result.is_ok(),
        "Child did not exit cleanly on parent disconnect"
    );
}

#[tokio::test]
async fn test_streaming_basic() {
    init_tracing();

    // Test the ReplySlot streaming functionality directly
    use kameo_child_process::ReplySlot;

    // Create a streaming reply slot
    let mut slot = ReplySlot::new();

    // Send some items through the streaming channel
    assert!(slot.try_send_stream_item(Ok(DummyParentOk { id: 100 })));
    assert!(slot.try_send_stream_item(Ok(DummyParentOk { id: 101 })));
    assert!(slot.try_send_stream_item(Ok(DummyParentOk { id: 102 })));

    // Close the stream
    slot.close_stream();

    // Get the receiver after sending items
    let receiver = slot.take_stream_receiver().expect("Should have receiver");

    // Read from the receiver
    let mut items = Vec::new();
    let mut rx = receiver;

    loop {
        match rx.recv().await {
            Some(Ok(item)) => items.push(Ok(item)),
            Some(Err(e)) => items.push(Err(e)),
            None => break, // Channel closed
        }
    }

    // Verify we got the expected items
    assert_eq!(items.len(), 3);
    assert!(matches!(items[0], Ok(DummyParentOk { id: 100 })));
    assert!(matches!(items[1], Ok(DummyParentOk { id: 101 })));
    assert!(matches!(items[2], Ok(DummyParentOk { id: 102 })));

    trace!(
        event = "test_complete",
        name = "test_streaming_basic",
        "Basic streaming test completed successfully"
    );
}
