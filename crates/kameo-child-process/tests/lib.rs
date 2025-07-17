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
struct DummyMsg(u64);

#[derive(Clone)]
struct DummyHandler;
#[async_trait::async_trait]
impl kameo_child_process::callback::CallbackHandler<DummyMsg> for DummyHandler {
    async fn handle(&self, _cb: DummyMsg) -> Result<(), kameo_child_process::error::PythonExecutionError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
struct DummyParentMsg(u64);
impl kameo_child_process::KameoChildProcessMessage for DummyParentMsg {
    type Reply = u64;
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
            let envelope = kameo_child_process::callback::CallbackEnvelope {
                correlation_id: 1,
                inner: DummyMsg(42),
                context: Default::default(),
            };
            trace!(event = "test_child", step = "send_callback", correlation_id = 1, "Child sending callback envelope");
            if let Err(e) = writer.write_msg(&envelope).await {
                panic!("Child write error: {:?}", e);
            }
            // Explicitly flush and shutdown write to signal end if needed, but for single message, just read reply
            let mut reader = LengthPrefixedRead::new(read_half);
            match reader.read_msg::<kameo_child_process::callback::CallbackEnvelope<Result<(), kameo_child_process::error::PythonExecutionError>>>().await {
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

// Implement KameoChildProcessMessage for DummyMsg so it can be used with setup_test_ipc_pair
impl kameo_child_process::KameoChildProcessMessage for DummyMsg {
    type Reply = ();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_parent_child_ipc() {
    trace!(event = "test_start", name = "test_parent_child_ipc", "Starting test");
    init_tracing();
    use kameo_child_process::SubprocessIpcBackend;
    use kameo_child_process::{SubprocessIpcChild, ChildProcessMessageHandler, DuplexUnixStream};
    use tokio::time::timeout;
    use std::time::Duration;

    #[derive(Clone)]
    struct DummyChildHandler;
    #[async_trait::async_trait]
    impl ChildProcessMessageHandler<DummyParentMsg> for DummyChildHandler {
        type Reply = Result<u64, kameo_child_process::error::PythonExecutionError>;
        async fn handle_child_message(&mut self, msg: DummyParentMsg) -> Self::Reply {
            tracing::info!(?msg, "DummyChildHandler received message");
            Ok(msg.0 + 100)
        }
    }

    // Use helper to get handshake-complete sockets
    let (parent_stream, child_stream) = setup_test_ipc_pair::<DummyParentMsg>().await;
    let backend = SubprocessIpcBackend::<DummyParentMsg>::from_duplex(DuplexUnixStream::new(parent_stream));
    let child = SubprocessIpcChild::<DummyParentMsg>::from_duplex(DuplexUnixStream::new(child_stream));
    let child_task = tokio::spawn(async move {
        tracing::info!("Child about to run");
        let result = child.run(DummyChildHandler).await;
        tracing::info!(?result, "Child finished running");
        result
    });

    // Yield to ensure the child task is polled before parent sends
    tokio::task::yield_now().await;

    tracing::info!("Parent about to send message");
    let reply = timeout(Duration::from_secs(2), backend.send(DummyParentMsg(123))).await;
    tracing::info!(?reply, "Parent got reply or timeout");
    let reply = reply.expect("timeout").expect("reply");
    assert_eq!(reply, 223);
    backend.shutdown();
    let child_result = child_task.await;
    tracing::info!(?child_result, "Child task join result");
    if let Err(e) = child_result {
        panic!("Child task panicked: {:?}", e);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_callback_protocol_full_duplex() {
    trace!(event = "test_start", name = "test_callback_protocol_full_duplex", "Starting test");
    init_tracing();
    tokio::time::timeout(Duration::from_secs(5), async {
        // test body
        use kameo_child_process::callback::{CallbackReceiver, CallbackIpcChild, CallbackHandler};
        use kameo_child_process::DuplexUnixStream;
        
        #[derive(Clone)]
        struct ParentHandler;
        #[async_trait::async_trait]
        impl CallbackHandler<DummyMsg> for ParentHandler {
            async fn handle(&self, cb: DummyMsg) -> Result<(), kameo_child_process::error::PythonExecutionError> {
                tracing::info!(?cb, "ParentHandler received callback");
                Ok(())
            }
        }
        let (sock1, sock2) = tokio::net::UnixStream::pair().unwrap();
        let parent_duplex = DuplexUnixStream::new(sock1);
        let child_duplex = DuplexUnixStream::new(sock2);
        let receiver = CallbackReceiver::<DummyMsg, ParentHandler>::from_duplex(parent_duplex, ParentHandler);
        let child_ipc = CallbackIpcChild::<DummyMsg>::from_duplex(child_duplex);
        let token = receiver.cancellation_token();
        let receiver_task = tokio::spawn(async move {
            receiver.run().await.unwrap();
        });
        let child_task = tokio::spawn(async move {
            let result = child_ipc.handle(DummyMsg(42)).await;
            tracing::info!(?result, "Child got reply");
            assert!(result.is_ok());
            child_ipc.shutdown();
        });
        child_task.await.unwrap();
        token.cancel();
        receiver_task.await.unwrap();
    }).await.expect("Test timed out");
}

// Refactor to use in-process simulation
#[tokio::test]
async fn test_child_process_exits_on_parent_disconnect() {
    use kameo_child_process::{KameoChildProcessMessage, run_child_actor_loop, DuplexUnixStream};
    use tokio::net::UnixStream;
    use std::time::Duration;
    use tokio::time::timeout;
    init_tracing();

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
    struct DummyMsg;
    impl KameoChildProcessMessage for DummyMsg {
        type Reply = ();
    }

    #[derive(Clone)]
    struct DummyHandler;
    #[async_trait::async_trait]
    impl kameo_child_process::ChildProcessMessageHandler<DummyMsg> for DummyHandler {
        type Reply = Result<(), kameo_child_process::error::PythonExecutionError>;
        async fn handle_child_message(&mut self, _msg: DummyMsg) -> Self::Reply {
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
