//! Asynchronous ubus client based on tokio (requires `async` feature).
//!
//! tokio's `UnixStream` does not support `try_clone` (unlike std). For read/write separation
//! in async contexts, use [`UbusClientAsync::into_split`] which consumes the client and returns
//! [`UbusReader`] + [`UbusWriter`].

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::blob::BlobBuf;
use crate::error::{Result, UbusError};
use crate::{
    parse_attr_u32, MsgType, UbusMessage, ATTR_DATA, ATTR_METHOD, ATTR_NO_REPLY, ATTR_OBJID,
    ATTR_OBJPATH, ATTR_SIGNATURE, ATTR_STATUS, MSG_ADD_OBJECT, MSG_DATA, MSG_HELLO, MSG_NOTIFY,
    MSG_REMOVE_OBJECT, MSG_STATUS,
};

/// Asynchronous ubus client.
///
/// API mirrors [`crate::UbusClient`] with `async fn` methods.
/// Suitable for integration into tokio-based runtimes.
///
/// For concurrent read/write, use [`UbusClientAsync::into_split`] to consume the client
/// and obtain independent reader/writer halves that can be moved to different tasks.
pub struct UbusClientAsync {
    stream: UnixStream,
    seq: u16,
    client_id: u32,
}

impl UbusClientAsync {
    /// Connect to ubusd and complete the HELLO handshake.
    pub async fn connect(path: &str) -> Result<Self> {
        let mut stream = UnixStream::connect(path).await?;
        let (mtype, _seq, peer, _body) = recv_msg(&mut stream).await?;
        if mtype != MSG_HELLO {
            return Err(UbusError::Protocol("expected HELLO"));
        }
        Ok(Self { stream, seq: 0, client_id: peer })
    }

    /// Returns the client_id assigned by ubusd.
    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    /// Register an object with ubusd (automatically attaches an empty SIGNATURE).
    pub async fn add_object(&mut self, path: &str) -> Result<u32> {
        let mut bb = BlobBuf::new();
        bb.put_string(ATTR_OBJPATH, path);
        let sig_ofs = bb.nest_start(ATTR_SIGNATURE);
        bb.nest_end(sig_ofs);
        let body = bb.bytes();
        let seq = self.next_seq();
        send_msg(&mut self.stream, MSG_ADD_OBJECT, seq, 0, &body).await?;

        let mut objid: Option<u32> = None;
        loop {
            let (mtype, _seq, _peer, body) = recv_msg(&mut self.stream).await?;
            match mtype {
                MSG_DATA => {
                    if let Some(v) = parse_attr_u32(&body, ATTR_OBJID) {
                        objid = Some(v);
                    }
                }
                MSG_STATUS => match parse_attr_u32(&body, ATTR_STATUS) {
                    Some(0) => break,
                    Some(s) => return Err(UbusError::Status(s)),
                    None => break,
                },
                _ => {}
            }
        }
        objid.ok_or(UbusError::Protocol("no OBJID in ADD_OBJECT resp"))
    }

    /// Remove a previously registered object.
    ///
    /// # Errors
    ///
    /// Returns [`crate::UbusError::Status`] if ubusd rejects the removal,
    /// for example when the object does not exist.
    pub async fn remove_object(&mut self, objid: u32) -> Result<()> {
        let mut bb = BlobBuf::new();
        bb.put_u32(ATTR_OBJID, objid);
        let body = bb.bytes();
        let seq = self.next_seq();
        send_msg(&mut self.stream, MSG_REMOVE_OBJECT, seq, 0, &body).await?;

        loop {
            let (mtype, _seq, _peer, body) = recv_msg(&mut self.stream).await?;
            match mtype {
                MSG_DATA => continue,
                MSG_STATUS => {
                    return match parse_attr_u32(&body, ATTR_STATUS) {
                        Some(0) => Ok(()),
                        Some(s) => Err(UbusError::Status(s)),
                        None => Ok(()),
                    };
                }
                _ => continue,
            }
        }
    }

    /// Send a NOTIFY message (no_reply=1, no ack required).
    pub async fn notify<F>(&mut self, objid: u32, method: &str, data: F) -> Result<()>
    where
        F: FnOnce(&mut BlobBuf),
    {
        let mut bb = BlobBuf::new();
        bb.put_u32(ATTR_OBJID, objid);
        bb.put_string(ATTR_METHOD, method);
        let ofs = bb.nest_start(ATTR_DATA);
        data(&mut bb);
        bb.nest_end(ofs);
        bb.put_u8(ATTR_NO_REPLY, 1);
        let body = bb.bytes();
        let seq = self.next_seq();
        send_msg(&mut self.stream, MSG_NOTIFY, seq, 0, &body).await
    }

    /// Asynchronously read one inbound message.
    pub async fn recv(&mut self) -> Result<UbusMessage> {
        let (mtype, seq, peer, body) = recv_msg(&mut self.stream).await?;
        Ok(UbusMessage {
            msg_type: MsgType::from(mtype),
            seq,
            peer,
            body,
        })
    }

    /// Consume the client and split into read/write halves for concurrent operations.
    ///
    /// `add_object` / `remove_object` must be called before split (they require both read and write).
    /// After split, [`UbusWriter`] can only send `notify` (no_reply, no response), and [`UbusReader`] only reads.
    pub fn into_split(self) -> (UbusReader, UbusWriter) {
        let (read, write) = self.stream.into_split();
        (
            UbusReader { read, client_id: self.client_id },
            UbusWriter { write, seq: self.seq, client_id: self.client_id },
        )
    }

    /// Returns a mutable reference to the underlying stream.
    pub fn stream(&mut self) -> &mut UnixStream {
        &mut self.stream
    }

    fn next_seq(&mut self) -> u16 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }
}

/// Read half produced by [`UbusClientAsync::into_split`].
pub struct UbusReader {
    read: tokio::net::unix::OwnedReadHalf,
    client_id: u32,
}

impl UbusReader {
    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    /// Asynchronously read one inbound message.
    pub async fn recv(&mut self) -> Result<UbusMessage> {
        let (mtype, seq, peer, body) = recv_msg(&mut self.read).await?;
        Ok(UbusMessage {
            msg_type: MsgType::from(mtype),
            seq,
            peer,
            body,
        })
    }
}

/// Write half produced by [`UbusClientAsync::into_split`].
pub struct UbusWriter {
    write: tokio::net::unix::OwnedWriteHalf,
    seq: u16,
    client_id: u32,
}

impl UbusWriter {
    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    /// Send a NOTIFY message (no_reply=1, no ack required).
    pub async fn notify<F>(&mut self, objid: u32, method: &str, data: F) -> Result<()>
    where
        F: FnOnce(&mut BlobBuf),
    {
        let mut bb = BlobBuf::new();
        bb.put_u32(ATTR_OBJID, objid);
        bb.put_string(ATTR_METHOD, method);
        let ofs = bb.nest_start(ATTR_DATA);
        data(&mut bb);
        bb.nest_end(ofs);
        bb.put_u8(ATTR_NO_REPLY, 1);
        let body = bb.bytes();
        let seq = self.next_seq();
        send_msg(&mut self.write, MSG_NOTIFY, seq, 0, &body).await
    }

    fn next_seq(&mut self) -> u16 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }
}

async fn send_msg<W: AsyncWrite + Unpin>(
    stream: &mut W,
    mtype: u8,
    seq: u16,
    peer: u32,
    body: &[u8],
) -> Result<()> {
    let mut hdr = [0u8; 8];
    hdr[0] = 0;
    hdr[1] = mtype;
    hdr[2..4].copy_from_slice(&seq.to_be_bytes());
    hdr[4..8].copy_from_slice(&peer.to_be_bytes());
    stream.write_all(&hdr).await?;
    stream.write_all(body).await?;
    Ok(())
}

async fn recv_msg<R: AsyncRead + Unpin>(stream: &mut R) -> Result<(u8, u16, u32, Vec<u8>)> {
    let mut hdr = [0u8; 8];
    stream.read_exact(&mut hdr).await?;
    let mtype = hdr[1];
    let seq = u16::from_be_bytes([hdr[2], hdr[3]]);
    let peer = u32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    let mut blob_head = [0u8; 4];
    stream.read_exact(&mut blob_head).await?;
    let id_len = u32::from_be_bytes(blob_head);
    let total_len = (id_len & 0x00ff_ffff) as usize;
    if total_len < 4 {
        return Err(UbusError::Protocol("blob len < 4"));
    }
    let mut body_rest = vec![0u8; total_len - 4];
    stream.read_exact(&mut body_rest).await?;
    let mut body = Vec::with_capacity(total_len);
    body.extend_from_slice(&blob_head);
    body.extend_from_slice(&body_rest);
    Ok((mtype, seq, peer, body))
}
