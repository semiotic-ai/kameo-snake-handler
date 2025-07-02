use super::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::process::Command;
use tracing::{debug, error, instrument};
use uuid::Uuid;

pub fn unique_socket_path(actor_name: &str) -> PathBuf {
    let mut path = std::path::PathBuf::from("/tmp");
    let short_name = &actor_name[0..std::cmp::min(8, actor_name.len())];
    path.push(format!(
        "kameo-{}-{}.sock",
        short_name,
        Uuid::new_v4().simple()
    ));
    path
}

#[instrument(skip(exe), fields(actor_name), parent = tracing::Span::current())]
pub async fn host<M, R>(
    actor_name: &str,
    exe: &str,
) -> std::io::Result<(Box<dyn AsyncReadWrite>, tokio::process::Child, PathBuf)>
where
    M: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    R: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    let socket_path = unique_socket_path(actor_name);
    let socket_path_str = socket_path.to_string_lossy().into_owned();

    debug!(status = "starting", socket_path = %socket_path_str, actor_type = actor_name);

    let mut cmd = Command::new(exe);
    cmd.env("KAMEO_CHILD_ACTOR", actor_name);
    cmd.env("KAMEO_ACTOR_SOCKET", socket_path_str.clone());
    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        cmd.env("RUST_LOG", rust_log);
    }

    let endpoint = parity_tokio_ipc::Endpoint::new(socket_path_str.clone());
    let mut incoming = endpoint.incoming()?;

    debug!(status = "spawning", actor_type = actor_name);
    let child = cmd.spawn()?;

    debug!(status = "waiting", actor_type = actor_name);
    let conn = incoming.next().await.transpose()?.ok_or_else(|| {
        error!(
            actor_type = actor_name,
            message = "No child connection received"
        );
        std::io::Error::new(std::io::ErrorKind::Other, "No child connection")
    })?;

    debug!(status = "completed", actor_type = actor_name);
    Ok((Box::new(conn), child, socket_path))
}

#[instrument(fields(pid= std::process::id(), actor_name = ?std::env::var("KAMEO_CHILD_ACTOR").ok()), parent = tracing::Span::current())]
pub async fn child_request() -> std::io::Result<Box<dyn AsyncReadWrite>> {
    let req_env = std::env::var("KAMEO_REQUEST_SOCKET");
    let cb_env = std::env::var("KAMEO_CALLBACK_SOCKET");
    let actor_env = std::env::var("KAMEO_CHILD_ACTOR");
    debug!(pid = std::process::id(), request_socket = ?req_env, callback_socket = ?cb_env, actor_name = ?actor_env, "Child handshake env vars");
    if req_env.is_err() || cb_env.is_err() || actor_env.is_err() {
        error!(pid = std::process::id(), request_socket = ?req_env, callback_socket = ?cb_env, actor_name = ?actor_env, "Missing required handshake env var(s), aborting child early");
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!(
            "Missing required handshake env var(s): KAMEO_REQUEST_SOCKET={:?}, KAMEO_CALLBACK_SOCKET={:?}, KAMEO_CHILD_ACTOR={:?}",
            req_env, cb_env, actor_env
        )));
    }
    let socket_path = req_env.unwrap();
    debug!(which = "request", socket_path = %socket_path, "Child got KAMEO_REQUEST_SOCKET env var");
    debug!(which = "request", socket_path = %socket_path, "Child attempting to connect to request socket");
    let stream = parity_tokio_ipc::Endpoint::connect(&socket_path).await?;
    Ok(Box::new(stream) as Box<dyn AsyncReadWrite>)
}

#[instrument(fields(actor_name = ?std::env::var("KAMEO_CHILD_ACTOR").ok()), parent = tracing::Span::current())]
pub async fn child_callback() -> std::io::Result<Box<dyn AsyncReadWrite>> {
    let pid = std::process::id();
    let cb_env = std::env::var("KAMEO_CALLBACK_SOCKET");
    let req_env = std::env::var("KAMEO_REQUEST_SOCKET");
    let actor_env = std::env::var("KAMEO_CHILD_ACTOR");
    debug!(pid = pid, request_socket = ?req_env, callback_socket = ?cb_env, actor_name = ?actor_env, "Child handshake env vars");
    if cb_env.is_err() || req_env.is_err() || actor_env.is_err() {
        error!(pid = pid, request_socket = ?req_env, callback_socket = ?cb_env, actor_name = ?actor_env, "Missing required handshake env var(s), aborting child early");
        return Err(std::io::Error::new(std::io::ErrorKind::Other, format!(
            "Missing required handshake env var(s): KAMEO_REQUEST_SOCKET={:?}, KAMEO_CALLBACK_SOCKET={:?}, KAMEO_CHILD_ACTOR={:?}",
            req_env, cb_env, actor_env
        )));
    }
    let socket_path = cb_env.unwrap();
    debug!(pid = pid, which = "callback", socket_path = %socket_path, "Child got KAMEO_CALLBACK_SOCKET env var");
    debug!(pid = pid, which = "callback", socket_path = %socket_path, "Child attempting to connect to callback socket");
    let stream = parity_tokio_ipc::Endpoint::connect(&socket_path).await?;
    Ok(Box::new(stream) as Box<dyn AsyncReadWrite>)
}
