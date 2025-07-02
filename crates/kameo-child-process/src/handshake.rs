use super::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::process::Command;
use tracing::{debug, error, instrument};
use uuid::Uuid;
use tokio::net::{UnixListener, UnixStream};
use std::any::Any;
use crate::AsyncReadWrite;
use tokio::io::{AsyncRead, AsyncWrite};

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
) -> std::io::Result<(Box<UnixStream>, tokio::process::Child, PathBuf)>
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

    let listener = UnixListener::bind(&socket_path)?;
    debug!(status = "spawning", actor_type = actor_name);
    let child = cmd.spawn()?;
    debug!(status = "waiting", actor_type = actor_name);
    let (stream, _addr) = listener.accept().await?;
    debug!(status = "completed", actor_type = actor_name);
    Ok((Box::new(stream), child, socket_path))
}

#[instrument(fields(pid= std::process::id(), actor_name = ?std::env::var("KAMEO_CHILD_ACTOR").ok()), parent = tracing::Span::current())]
pub async fn child_request() -> std::io::Result<Box<UnixStream>> {
    let req_env = std::env::var("KAMEO_REQUEST_SOCKET");
    let socket_path = req_env.expect("KAMEO_REQUEST_SOCKET must be set");
    let stream = UnixStream::connect(socket_path).await?;
    Ok(Box::new(stream))
}

#[instrument(fields(actor_name = ?std::env::var("KAMEO_CHILD_ACTOR").ok()), parent = tracing::Span::current())]
pub async fn child_callback() -> std::io::Result<Box<UnixStream>> {
    let cb_env = std::env::var("KAMEO_CALLBACK_SOCKET");
    let socket_path = cb_env.expect("KAMEO_CALLBACK_SOCKET must be set");
    let stream = UnixStream::connect(socket_path).await?;
    Ok(Box::new(stream))
}
