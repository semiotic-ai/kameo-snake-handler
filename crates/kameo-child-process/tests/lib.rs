//! High-concurrency async tests for core IPC protocol logic (no real process spawning)

use std::time::Duration;
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use std::sync::Once;
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

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
struct DummyMsg {
    id: u64
}

#[derive(Clone)]
struct DummyHandler;
#[async_trait::async_trait]
impl kameo_child_process::callback::CallbackHandler<DummyMsg> for DummyHandler {
    async fn handle(&self, _cb: DummyMsg) -> Result<(), kameo_child_process::error::PythonExecutionError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
struct DummyParentMsg {
    id: u64
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
struct DummyParentOk {
    id: u64
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
    trace!(event = "test_start", name = "test_single_callback_message", "Starting test");
    init_tracing();
    tokio::time::timeout(Duration::from_secs(5), async {
        // Set up a pair of connected UnixStreams
        let (sock1, sock2) = tokio::net::UnixStream::pair().unwrap();
        let parent_duplex = kameo_child_process::DuplexUnixStream::new(sock1);
        let child_duplex = kameo_child_process::DuplexUnixStream::new(sock2);
        let receiver = kameo_child_process::callback::CallbackReceiver::<DummyMsg, DummyHandler>::from_duplex(parent_duplex, DummyHandler);

        let token = receiver.cancellation_token();
        let receiver_task = tokio::spawn(async move {
            receiver.run().await.unwrap();
        });

        // Spawn a fake child artefact using the other end
        let child_task = tokio::spawn(async move {
            let (read_half, write_half) = child_duplex.into_inner().into_split();
            let mut writer = LengthPrefixedWrite::new(write_half);
            let envelope = kameo_child_process::MultiplexEnvelope {
                correlation_id: 1,
                inner: DummyMsg { id: 42 },
                context: Default::default(),
            };
            trace!(event = "test_child", step = "send_callback", correlation_id = 1, "Child sending callback envelope");
            if let Err(e) = writer.write_msg(&envelope).await {
                panic!("Child write error: {:?}", e);
            }
            // Explicitly flush and shutdown write to signal end if needed, but for single message, just read reply
            let mut reader = LengthPrefixedRead::new(read_half);
            match reader.read_msg::<kameo_child_process::MultiplexEnvelope<Result<(), kameo_child_process::error::PythonExecutionError>>>().await {
                Ok(reply_envelope) => {
                    trace!(event = "test_child", step = "received_reply", correlation_id = reply_envelope.correlation_id, ?reply_envelope.inner, "Child received reply");
                    assert_eq!(reply_envelope.correlation_id, 1);
                    assert!(reply_envelope.inner.is_ok());
                }
                Err(e) => panic!("Child read error: {:?}", e),
            }
            // Drop reader/writer to close
            drop(reader);
            drop(writer);
        });

        child_task.await.unwrap();
        token.cancel();
        receiver_task.await.unwrap();
    }).await.expect("Test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_high_concurrency_callback_protocol() {
    trace!(event = "test_start", name = "test_high_concurrency_callback_protocol", "Starting high concurrency callback test");
    init_tracing();
    
    // Initialize metrics
    kameo_child_process::metrics::init_metrics();
    
    tokio::time::timeout(Duration::from_secs(180), async {
        tracing::info!("Test started - setting up high concurrency callback protocol test");
        tracing::info!("This test will process 10,000 concurrent callbacks");
        
        use kameo_child_process::callback::{CallbackReceiver, CallbackIpcChild, CallbackHandler};
        use futures::stream::{self, StreamExt};
        
        // Start a task to log metrics periodically
        let _metrics_task = tokio::spawn(async {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            for _ in 0..90 {  // Log for up to 3 minutes max
                interval.tick().await;
                kameo_child_process::metrics::MetricsReporter::log_metrics_state();
            }
        });
        
        #[derive(Clone)]
        struct ParentHandler;
        #[async_trait::async_trait]
        impl CallbackHandler<DummyMsg> for ParentHandler {
            async fn handle(&self, cb: DummyMsg) -> Result<(), kameo_child_process::error::PythonExecutionError> {
                trace!(event = "parent_handler", id = cb.id, "Parent handling callback");
                Ok(())
            }
        }
        
        tracing::info!("Creating socket pair");
        // Create the sockets
        let (parent_stream, child_stream) = tokio::net::UnixStream::pair().unwrap();
        let parent_socket = kameo_child_process::DuplexUnixStream::new(parent_stream);
        let child_socket = kameo_child_process::DuplexUnixStream::new(child_stream);
        
        tracing::info!("Setting up child IPC");
        // Set up child side
        let child_ipc = CallbackIpcChild::<DummyMsg>::from_duplex(child_socket);
        
        tracing::info!("Setting up parent receiver");
        // Set up parent side
        let parent_handler = ParentHandler;
        let receiver = CallbackReceiver::from_duplex(parent_socket, parent_handler);
        
        tracing::info!("Spawning receiver task");
        // Spawn parent receiver task
        let receiver_task = tokio::spawn(async move {
            tracing::info!("Receiver task started");
            let result = receiver.run().await;
            tracing::info!("Receiver task completed with result: {:?}", result);
            result
        });
        
        tracing::info!("Starting to process 10k concurrent callbacks");
        // Run 10k callbacks with high concurrency
        let total_callbacks = 10_000;
        
        // Create all futures at once for maximum concurrency
        let futures = (0..total_callbacks).map(|i| {
            let child_ipc = child_ipc.clone();
            async move {
                let msg = DummyMsg { id: i as u64 };
                let result = child_ipc.handle(msg).await;
                trace!(event = "callback_completed", id = i, ?result, "Callback completed");
                result
            }
        });
        
        tracing::info!("Collecting all callback futures");
        // Process all callbacks with high concurrency
        let start_time = std::time::Instant::now();
        let results: Vec<_> = stream::iter(futures)
            .buffer_unordered(1000) // Process 1000 concurrently for high throughput
            .collect()
            .await;
        let elapsed = start_time.elapsed();
        tracing::info!("Processed {} callbacks in {:?}", total_callbacks, elapsed);
        
        tracing::info!("All callbacks processed, checking results");
        assert_eq!(results.len(), total_callbacks, "Expected {} results, got {}", total_callbacks, results.len());
        
        // Since buffer_unordered returns results in completion order, we just check that all succeeded
        for result in results.into_iter() {
            assert!(result.is_ok(), "Callback failed: {:?}", result);
        }
        
        // Shutdown receiver
        tracing::info!("Shutting down child IPC");
        child_ipc.shutdown();
        
        // Wait for receiver to complete
        tracing::info!("Waiting for receiver task");
        match tokio::time::timeout(Duration::from_secs(5), receiver_task).await {
            Ok(res) => {
                tracing::info!("Receiver task completed: {:?}", res);
                let _ = res.expect("receiver task join error"); // Propagate any errors
            },
            Err(_) => {
                tracing::error!("Receiver task timed out");
                panic!("Receiver task failed to shut down in time");
            }
        }
        
        tracing::info!("High concurrency callback test completed successfully");
    }).await.expect("test timeout");
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
    trace!(event = "test_start", name = "test_parent_child_ipc", "Starting test");
    init_tracing();
    
    // Initialize metrics
    kameo_child_process::metrics::init_metrics();
    
    use kameo_child_process::SubprocessIpcBackend;
    use kameo_child_process::SubprocessIpcChild;
    use tokio::time::timeout;
    use std::time::Duration;
    use futures::future::join_all;
    

    // Start a task to log metrics periodically
    let _metrics_task = tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        for _ in 0..90 {  // Log for up to 3 minutes max
            interval.tick().await;
            kameo_child_process::metrics::MetricsReporter::log_metrics_state();
        }
    });
    
    #[derive(Clone)]
    struct DummyChildHandler;
    #[async_trait::async_trait]
    impl kameo_child_process::ChildProcessMessageHandler<DummyParentMsg> for DummyChildHandler {
        async fn handle_child_message(&mut self, msg: DummyParentMsg) -> Result<DummyParentOk, kameo_child_process::error::PythonExecutionError> {
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
    let futures = (0..requests).map(|i| {
        let backend = backend.clone();
        async move {
            let msg = DummyParentMsg { id: i };
            let result = backend.send(msg).await;
            trace!(event = "parent_test", id = i, ?result, "Got result");
            result
        }
    }).collect::<Vec<_>>();
    
    let results = timeout(Duration::from_secs(120), join_all(futures)).await
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
            res.expect("child task panicked").expect("child task returned error");
        },
        Err(_) => {
            panic!("Child task failed to shut down in time");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_high_concurrency_parent_child_ipc() {
    trace!(event = "test_start", name = "test_high_concurrency_parent_child_ipc", "Starting high concurrency parent-child IPC test");
    init_tracing();
    
    // Initialize metrics
    kameo_child_process::metrics::init_metrics();
    
    use kameo_child_process::SubprocessIpcBackend;
    use kameo_child_process::SubprocessIpcChild;
    use tokio::time::timeout;
    use std::time::Duration;
    use futures::stream::{self, StreamExt};
    
    tokio::time::timeout(Duration::from_secs(180), async {
        // Start a task to log metrics periodically
        let _metrics_task = tokio::spawn(async {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            for _ in 0..90 {  // Log for up to 3 minutes max
                interval.tick().await;
                kameo_child_process::metrics::MetricsReporter::log_metrics_state();
            }
        });
        
        #[derive(Clone)]
        struct DummyChildHandler;
        #[async_trait::async_trait]
        impl kameo_child_process::ChildProcessMessageHandler<DummyParentMsg> for DummyChildHandler {
            async fn handle_child_message(&mut self, msg: DummyParentMsg) -> Result<DummyParentOk, kameo_child_process::error::PythonExecutionError> {
                // Simplified handler without complex tracing
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
        
        tracing::info!("Starting to process 10k concurrent parent-child messages");
        // Run 10k concurrent requests with high concurrency
        let total_requests = 10_000;
        tracing::info!("This test will process {} concurrent parent-child messages", total_requests);
        
        // Create all futures at once for maximum concurrency
        let futures = (0..total_requests).map(|i| {
            let backend = backend.clone();
            async move {
                let msg = DummyParentMsg { id: i as u64 };
                let result = backend.send(msg).await;
                result
            }
        });
        
        tracing::info!("Collecting all parent-child message futures");
        // Process all messages with high concurrency
        let start_time = std::time::Instant::now();
        let results: Vec<_> = stream::iter(futures)
            .buffer_unordered(1000) // Process 1000 concurrently for high throughput
            .collect()
            .await;
        let elapsed = start_time.elapsed();
        tracing::info!("Processed {} parent-child messages in {:?}", total_requests, elapsed);
        
        tracing::info!("All parent-child messages processed, checking results");
        assert_eq!(results.len(), total_requests, "Expected {} results, got {}", total_requests, results.len());
        
        // Since buffer_unordered returns results in completion order, we need to track which IDs we got
        let mut received_ids = std::collections::HashSet::new();
        for result in results.into_iter() {
            assert!(result.is_ok(), "Request failed: {:?}", result);
            let ok = result.unwrap();
            assert!(received_ids.insert(ok.id), "Duplicate ID received: {}", ok.id);
            assert!(ok.id < total_requests as u64, "ID out of range: {}", ok.id);
        }
        assert_eq!(received_ids.len(), total_requests, "Expected {} unique IDs, got {}", total_requests, received_ids.len());
        
        // Shutdown and wait for child task
        tracing::info!("Shutting down backend");
        backend.shutdown();
        match timeout(Duration::from_secs(5), child_task).await {
            Ok(res) => {
                tracing::info!("Child task completed: {:?}", res);
                res.expect("child task panicked").expect("child task returned error");
            },
            Err(_) => {
                panic!("Child task failed to shut down in time");
            }
        }
        
            tracing::info!("High concurrency parent-child IPC test completed successfully");
    }).await.expect("test timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_high_concurrency_parent_child_ipc_simplified() {
    trace!(event = "test_start", name = "test_high_concurrency_parent_child_ipc_simplified", "Starting simplified high concurrency parent-child IPC test");
    init_tracing();
    
    // Initialize metrics
    kameo_child_process::metrics::init_metrics();
    
    use kameo_child_process::SubprocessIpcBackend;
    use kameo_child_process::SubprocessIpcChild;
    use tokio::time::timeout;
    use std::time::Duration;
    use futures::stream::{self, StreamExt};
    
    tokio::time::timeout(Duration::from_secs(180), async {
        // Start a task to log metrics periodically
        let _metrics_task = tokio::spawn(async {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            for _ in 0..90 {  // Log for up to 3 minutes max
                interval.tick().await;
                kameo_child_process::metrics::MetricsReporter::log_metrics_state();
            }
        });
        
        #[derive(Clone)]
        struct DummyChildHandler;
        #[async_trait::async_trait]
        impl kameo_child_process::ChildProcessMessageHandler<DummyParentMsg> for DummyChildHandler {
            async fn handle_child_message(&mut self, msg: DummyParentMsg) -> Result<DummyParentOk, kameo_child_process::error::PythonExecutionError> {
                // Minimal handler - just return the ID
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
        
        tracing::info!("Starting to process 10k concurrent parent-child messages (simplified)");
        let total_requests = 10_000;
        tracing::info!("This test will process {} concurrent parent-child messages with minimal overhead", total_requests);
        
        // Create all futures at once for maximum concurrency
        let futures = (0..total_requests).map(|i| {
            let backend = backend.clone();
            async move {
                let msg = DummyParentMsg { id: i as u64 };
                backend.send(msg).await
            }
        });
        
        tracing::info!("Collecting all parent-child message futures (simplified)");
        let start_time = std::time::Instant::now();
        let results: Vec<_> = stream::iter(futures)
            .buffer_unordered(1000) // Process 1000 concurrently for high throughput
            .collect()
            .await;
        let elapsed = start_time.elapsed();
        tracing::info!("Processed {} parent-child messages in {:?} (simplified)", total_requests, elapsed);
        
        tracing::info!("All parent-child messages processed, checking results");
        assert_eq!(results.len(), total_requests, "Expected {} results, got {}", total_requests, results.len());
        
        // Since buffer_unordered returns results in completion order, we need to track which IDs we got
        let mut received_ids = std::collections::HashSet::new();
        for result in results.into_iter() {
            assert!(result.is_ok(), "Request failed: {:?}", result);
            let ok = result.unwrap();
            assert!(received_ids.insert(ok.id), "Duplicate ID received: {}", ok.id);
            assert!(ok.id < total_requests as u64, "ID out of range: {}", ok.id);
        }
        assert_eq!(received_ids.len(), total_requests, "Expected {} unique IDs, got {}", total_requests, received_ids.len());
        
        // Shutdown and wait for child task
        tracing::info!("Shutting down backend");
        backend.shutdown();
        match timeout(Duration::from_secs(5), child_task).await {
            Ok(res) => {
                tracing::info!("Child task completed: {:?}", res);
                res.expect("child task panicked").expect("child task returned error");
            },
            Err(_) => {
                panic!("Child task failed to shut down in time");
            }
        }
        
        tracing::info!("Simplified high concurrency parent-child IPC test completed successfully");
    }).await.expect("test timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_high_concurrency_parent_child_ipc_no_control_wrapper() {
    trace!(event = "test_start", name = "test_high_concurrency_parent_child_ipc_no_control_wrapper", "Starting parent-child IPC test without Control wrapper");
    init_tracing();
    
    // Initialize metrics
    kameo_child_process::metrics::init_metrics();
    
    use kameo_child_process::SubprocessIpcBackend;
    use kameo_child_process::SubprocessIpcChild;
    use tokio::time::timeout;
    use std::time::Duration;
    use futures::stream::{self, StreamExt};
    
    tokio::time::timeout(Duration::from_secs(180), async {
        // Start a task to log metrics periodically
        let _metrics_task = tokio::spawn(async {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            for _ in 0..90 {  // Log for up to 3 minutes max
                interval.tick().await;
                kameo_child_process::metrics::MetricsReporter::log_metrics_state();
            }
        });
        
        #[derive(Clone)]
        struct DummyChildHandler;
        #[async_trait::async_trait]
        impl kameo_child_process::ChildProcessMessageHandler<DummyParentMsg> for DummyChildHandler {
            async fn handle_child_message(&mut self, msg: DummyParentMsg) -> Result<DummyParentOk, kameo_child_process::error::PythonExecutionError> {
                // Minimal handler - just return the ID
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
        
        tracing::info!("Starting to process 10k concurrent parent-child messages (no Control wrapper)");
        let total_requests = 10_000;
        tracing::info!("This test will process {} concurrent parent-child messages without Control wrapper overhead", total_requests);
        
        // Create all futures at once for maximum concurrency
        let futures = (0..total_requests).map(|i| {
            let backend = backend.clone();
            async move {
                let msg = DummyParentMsg { id: i as u64 };
                backend.send(msg).await
            }
        });
        
        tracing::info!("Collecting all parent-child message futures (no Control wrapper)");
        let start_time = std::time::Instant::now();
        let results: Vec<_> = stream::iter(futures)
            .buffer_unordered(1000) // Process 1000 concurrently for high throughput
            .collect()
            .await;
        let elapsed = start_time.elapsed();
        tracing::info!("Processed {} parent-child messages in {:?} (no Control wrapper)", total_requests, elapsed);
        
        tracing::info!("All parent-child messages processed, checking results");
        assert_eq!(results.len(), total_requests, "Expected {} results, got {}", total_requests, results.len());
        
        // Since buffer_unordered returns results in completion order, we need to track which IDs we got
        let mut received_ids = std::collections::HashSet::new();
        for result in results.into_iter() {
            assert!(result.is_ok(), "Request failed: {:?}", result);
            let ok = result.unwrap();
            assert!(received_ids.insert(ok.id), "Duplicate ID received: {}", ok.id);
            assert!(ok.id < total_requests as u64, "ID out of range: {}", ok.id);
        }
        assert_eq!(received_ids.len(), total_requests, "Expected {} unique IDs, got {}", total_requests, received_ids.len());
        
        // Shutdown and wait for child task
        tracing::info!("Shutting down backend");
        backend.shutdown();
        match timeout(Duration::from_secs(5), child_task).await {
            Ok(res) => {
                tracing::info!("Child task completed: {:?}", res);
                res.expect("child task panicked").expect("child task returned error");
            },
            Err(_) => {
                panic!("Child task failed to shut down in time");
            }
        }
        
        tracing::info!("Parent-child IPC test without Control wrapper completed successfully");
    }).await.expect("test timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_callback_protocol_full_duplex() {
    trace!(event = "test_start", name = "test_callback_protocol_full_duplex", "Starting test");
    init_tracing();
    
    // Initialize metrics
    kameo_child_process::metrics::init_metrics();
    
    tokio::time::timeout(Duration::from_secs(180), async {
        tracing::info!("Test started - setting up callback protocol test");
        // test body
        use kameo_child_process::callback::{CallbackReceiver, CallbackIpcChild, CallbackHandler};
        
        
        use futures::stream::{self, StreamExt};
        
        // Start a task to log metrics periodically
        let _metrics_task = tokio::spawn(async {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            for _ in 0..90 {  // Log for up to 3 minutes max
                interval.tick().await;
                kameo_child_process::metrics::MetricsReporter::log_metrics_state();
            }
        });
        
        #[derive(Clone)]
        struct ParentHandler;
        #[async_trait::async_trait]
        impl CallbackHandler<DummyMsg> for ParentHandler {
            async fn handle(&self, cb: DummyMsg) -> Result<(), kameo_child_process::error::PythonExecutionError> {
                trace!(event = "parent_handler", id = cb.id, "Parent handling callback");
                Ok(())
            }
        }
        
        tracing::info!("Creating socket pair");
        // Create the sockets
        let (parent_stream, child_stream) = tokio::net::UnixStream::pair().unwrap();
        let parent_socket = kameo_child_process::DuplexUnixStream::new(parent_stream);
        let child_socket = kameo_child_process::DuplexUnixStream::new(child_stream);
        
        tracing::info!("Setting up child IPC");
        // Set up child side
        let child_ipc = CallbackIpcChild::<DummyMsg>::from_duplex(child_socket);
        
        tracing::info!("Setting up parent receiver");
        // Set up parent side
        let parent_handler = ParentHandler;
        let receiver = CallbackReceiver::from_duplex(parent_socket, parent_handler);
        
        tracing::info!("Spawning receiver task");
        // Spawn parent receiver task
        let receiver_task = tokio::spawn(async move {
            tracing::info!("Receiver task started");
            let result = receiver.run().await;
            tracing::info!("Receiver task completed with result: {:?}", result);
            result
        });
        
        tracing::info!("Starting to process callbacks");
        // Run 10k callbacks for debugging purposes
        let total_callbacks = 10_000;
        let batch_size = 500;
        let mut all_results = vec![];
        
        for batch_start in (0..total_callbacks).step_by(batch_size) {
            let end = std::cmp::min(batch_start + batch_size, total_callbacks);
            tracing::info!("Processing batch {}-{}", batch_start, end);
            
            let futures = (batch_start..end).map(|i| {
                let child_ipc = child_ipc.clone();
                async move {
                    tracing::info!("Starting callback {}", i);
                    let msg = DummyMsg { id: i as u64 };
                    let result = child_ipc.handle(msg).await;
                    tracing::info!("Completed callback {} with result: {:?}", i, result);
                    result
                }
            });
            
            tracing::info!("Collecting batch futures");
            // Process this batch with controlled concurrency using buffer_unordered
            let batch_results: Vec<_> = stream::iter(futures)
                .buffer_unordered(100) // Process 100 at a time within each batch
                .collect()
                .await;
            
            tracing::info!("Batch completed, got {} results", batch_results.len());
            all_results.extend(batch_results);
        }
        
        tracing::info!("All batches processed, checking results");
        for (i, result) in all_results.into_iter().enumerate() {
            assert!(result.is_ok(), "Callback {} failed: {:?}", i, result);
        }
        
        // Shutdown receiver
        tracing::info!("Shutting down child IPC");
        child_ipc.shutdown();
        
        // Wait for receiver to complete
        tracing::info!("Waiting for receiver task");
        match tokio::time::timeout(Duration::from_secs(5), receiver_task).await {
            Ok(res) => {
                tracing::info!("Receiver task completed: {:?}", res);
                let _ = res.expect("receiver task join error"); // Propagate any errors
            },
            Err(_) => {
                tracing::error!("Receiver task timed out");
                panic!("Receiver task failed to shut down in time");
            }
        }
        
        tracing::info!("Test completed successfully");
    }).await.expect("test timeout");
}

// Refactor to use in-process simulation
#[tokio::test]
async fn test_child_process_exits_on_parent_disconnect() {
    use kameo_child_process::{KameoChildProcessMessage, run_child_actor_loop};
    use tokio::net::UnixStream;
    use std::time::Duration;
    use tokio::time::timeout;
    init_tracing();

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
    struct DummyMsg;
    impl KameoChildProcessMessage for DummyMsg {
        type Ok = ();
    }

    #[derive(Clone)]
    struct DummyHandler;
    #[async_trait::async_trait]
    impl kameo_child_process::ChildProcessMessageHandler<DummyMsg> for DummyHandler {
        async fn handle_child_message(&mut self, _msg: DummyMsg) -> Result<(), kameo_child_process::error::PythonExecutionError> {
            Ok(())
        }
    }

    let (parent_stream, child_stream) = UnixStream::pair().expect("Failed to create UnixStream pair");
    let child_conn = Box::new(child_stream);
    let child_task = tokio::spawn(async move {
        run_child_actor_loop(DummyHandler, child_conn, None).await
    });

    // Simulate parent disconnect by dropping parent_stream
    drop(parent_stream);

    // Wait for child to exit
    let result = timeout(Duration::from_secs(5), child_task).await.expect("timeout").expect("join");
    assert!(result.is_ok(), "Child did not exit cleanly on parent disconnect");
}
