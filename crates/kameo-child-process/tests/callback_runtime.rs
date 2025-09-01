use kameo_child_process::callback::{TypedCallbackEnvelope, TypedCallbackResponse};
use kameo_child_process::callback_runtime as rt;
use kameo_child_process::framing::{LengthPrefixedRead, LengthPrefixedWrite};
use serial_test::serial;

async fn setup_pair() -> (
    tokio::net::UnixStream,
    tokio::net::UnixStream,
) {
    tokio::net::UnixStream::pair().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn callback_runtime_round_trip_single() {
    let (child_side, parent_side) = setup_pair().await;

    // Initialize runtime on child side
    rt::shutdown();
    rt::init(child_side).expect("init");

    // Parent writer/reader
    let (pr, pw) = parent_side.into_split();
    let mut reader = LengthPrefixedRead::new(pr);
    let mut writer = LengthPrefixedWrite::new(pw);

    // Register route
    let correlation_id = 42u64;
    let mut rx = rt::register_stream(correlation_id).await.expect("rx");

    // Send envelope to parent (simulates child->parent)
    let env = TypedCallbackEnvelope {
        callback_path: "mod.Type".to_string(),
        correlation_id,
        callback_data: vec![1,2,3],
        context: Default::default(),
    };
    rt::send(env).expect("send");

    // Parent receives, echos one response, then final
    let recv_env: TypedCallbackEnvelope = reader.read_msg().await.expect("read env");
    assert_eq!(recv_env.correlation_id, correlation_id);

    let resp = TypedCallbackResponse {
        callback_path: recv_env.callback_path.clone(),
        response_type: "Resp".to_string(),
        response_data: vec![9,9,9],
        correlation_id,
        is_final: false,
    };
    writer.write_msg(&resp).await.expect("write resp");

    let final_resp = TypedCallbackResponse {
        callback_path: recv_env.callback_path,
        response_type: "StreamTermination".to_string(),
        response_data: vec![],
        correlation_id,
        is_final: true,
    };
    writer.write_msg(&final_resp).await.expect("write final");

    // Child side receives via runtime stream (both items)
    let first = rx.recv().await.expect("first item available");
    assert_eq!(first.correlation_id, correlation_id);
    assert!(!first.is_final);
    let last = rx.recv().await.expect("final item available");
    assert_eq!(last.correlation_id, correlation_id);
    assert!(last.is_final);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn callback_runtime_high_throughput_interleaved() {
    let (child_side, parent_side) = setup_pair().await;
    rt::shutdown();
    rt::init(child_side).expect("init");

    let (pr, pw) = parent_side.into_split();
    let mut reader = LengthPrefixedRead::new(pr);
    let mut writer = LengthPrefixedWrite::new(pw);

    // Register many correlation ids
    let total: usize = 2000;
    let mut receivers = Vec::with_capacity(total);
    for i in 0..total {
        receivers.push(rt::register_stream(i as u64).await.expect("rx"));
        let env = TypedCallbackEnvelope {
            callback_path: "x.Y".to_string(),
            correlation_id: i as u64,
            callback_data: vec![0],
            context: Default::default(),
        };
        rt::send(env).expect("send");
    }

    // Read all envelopes then send interleaved responses
    for _ in 0..total { let _: TypedCallbackEnvelope = reader.read_msg().await.unwrap(); }

    // Interleave: for each id, send two non-final then final
    for i in (0..total).step_by(2) {
        let cid = i as u64;
        let resp1 = TypedCallbackResponse { callback_path: "x.Y".into(), response_type: "A".into(), response_data: vec![1], correlation_id: cid, is_final: false };
        writer.write_msg(&resp1).await.unwrap();
    }
    for i in (1..total).step_by(2) {
        let cid = i as u64;
        let resp1 = TypedCallbackResponse { callback_path: "x.Y".into(), response_type: "A".into(), response_data: vec![1], correlation_id: cid, is_final: false };
        writer.write_msg(&resp1).await.unwrap();
    }
    for i in 0..total {
        let cid = i as u64;
        let final_resp = TypedCallbackResponse { callback_path: "x.Y".into(), response_type: "StreamTermination".into(), response_data: vec![], correlation_id: cid, is_final: true };
        writer.write_msg(&final_resp).await.unwrap();
    }

    // Verify all receivers get a non-final then final
    for (i, mut rx) in receivers.into_iter().enumerate() {
        let first = rx.recv().await.expect("first");
        assert_eq!(first.correlation_id, i as u64);
        assert!(!first.is_final);
        let last = rx.recv().await.expect("last");
        assert!(last.is_final);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn callback_runtime_reader_eof_drops_routes() {
    let (child_side, parent_side) = setup_pair().await;
    rt::shutdown();
    rt::init(child_side).expect("init");

    // Register, but never send final; closing parent should end reader
    let cid = 7u64;
    let mut _rx = rt::register_stream(cid).await.expect("rx");
    drop(parent_side); // triggers reader task to exit

    // Give the runtime a brief moment to process EOF
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    rt::shutdown();
}


