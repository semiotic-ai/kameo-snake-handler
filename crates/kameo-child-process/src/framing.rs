use bincode::{Decode, Encode};
use std::io;
use std::marker::Unpin;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::trace;

pub struct LengthPrefixedRead<R> {
    inner: R,
}

impl<R> LengthPrefixedRead<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncRead + Unpin> LengthPrefixedRead<R> {
    pub async fn read_msg<T: Decode<()>>(&mut self) -> io::Result<T> {
        let mut len_buf = [0u8; 4];
        self.inner.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut msg_buf = vec![0u8; len];
        self.inner.read_exact(&mut msg_buf).await?;
        let (msg, _): (T, _) = bincode::decode_from_slice(&msg_buf, bincode::config::standard())
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("bincode decode error: {e}"))
            })?;
        trace!(event = "framing_read", len, "Read length-prefixed message");
        Ok(msg)
    }
}

pub struct LengthPrefixedWrite<W> {
    inner: W,
}

impl<W> LengthPrefixedWrite<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: AsyncWrite + Unpin> LengthPrefixedWrite<W> {
    pub async fn write_msg<T: Encode>(&mut self, msg: &T) -> io::Result<()> {
        let bytes = bincode::encode_to_vec(msg, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let len = bytes.len() as u32;
        self.inner.write_all(&len.to_le_bytes()).await?;
        self.inner.write_all(&bytes).await?;
        Ok(())
    }
}

pub type LengthPrefixedStream<S> = (LengthPrefixedRead<S>, LengthPrefixedWrite<S>);
