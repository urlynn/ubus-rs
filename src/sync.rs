//! Synchronous ubus client using std only.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

use crate::blob::BlobBuf;
use crate::error::{Result, UbusError};
use crate::{
    parse_attr_u32, MsgType, UbusMessage, ATTR_DATA, ATTR_METHOD, ATTR_NO_REPLY, ATTR_OBJID,
    ATTR_OBJPATH, ATTR_SIGNATURE, ATTR_STATUS, MSG_ADD_OBJECT, MSG_DATA, MSG_HELLO, MSG_NOTIFY,
    MSG_REMOVE_OBJECT, MSG_STATUS,
};

/// Synchronous ubus client.
///
/// Each client instance maintains one connection to ubusd. Multiple objects can be registered
/// and multiple notifications can be sent. Inbound messages are consumed via [`recv`](Self::recv);
/// if not consumed, the socket buffer will eventually fill and ubusd will block.
pub struct UbusClient {
    stream: UnixStream,
    seq: u16,
    client_id: u32,
}

impl UbusClient {
    /// Connect to ubusd and complete the HELLO handshake.
    ///
    /// Default socket path is `/var/run/ubus/ubus.sock`; older OpenWrt systems may use `/var/run/ubus.sock`.
    pub fn connect(path: &str) -> Result<Self> {
        let mut stream = UnixStream::connect(path)?;
        let (mtype, _seq, peer, _body) = recv_msg(&mut stream)?;
        if mtype != MSG_HELLO {
            return Err(UbusError::Protocol("expected HELLO"));
        }
        Ok(Self { stream, seq: 0, client_id: peer })
    }

    /// Returns the client_id assigned by ubusd during HELLO.
    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    /// Clone the client (duplicates the underlying fd) for multi-threaded read/write separation.
    ///
    /// Note: The two clients have independent seq counters. Do not send requests from both
    /// simultaneously (this would cause seq conflicts). Typical usage: one client sends requests,
    /// the other only calls `recv`.
    pub fn try_clone(&self) -> Result<Self> {
        let stream = self.stream.try_clone()?;
        Ok(Self {
            stream,
            seq: self.seq,
            client_id: self.client_id,
        })
    }

    /// Register an object with ubusd (automatically attaches an empty SIGNATURE).
    ///
    /// Returns the objid assigned by ubusd.
    pub fn add_object(&mut self, path: &str) -> Result<u32> {
        let mut bb = BlobBuf::new();
        bb.put_string(ATTR_OBJPATH, path);
        let sig_ofs = bb.nest_start(ATTR_SIGNATURE);
        bb.nest_end(sig_ofs);
        let body = bb.bytes();
        let seq = self.next_seq();
        send_msg(&mut self.stream, MSG_ADD_OBJECT, seq, 0, &body)?;

        let mut objid: Option<u32> = None;
        loop {
            let (mtype, _seq, _peer, body) = recv_msg(&mut self.stream)?;
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
    pub fn remove_object(&mut self, objid: u32) -> Result<()> {
        let mut bb = BlobBuf::new();
        bb.put_u32(ATTR_OBJID, objid);
        let body = bb.bytes();
        let seq = self.next_seq();
        send_msg(&mut self.stream, MSG_REMOVE_OBJECT, seq, 0, &body)?;

        loop {
            let (mtype, _seq, _peer, _body) = recv_msg(&mut self.stream)?;
            match mtype {
                MSG_DATA => continue,
                MSG_STATUS => break,
                _ => continue,
            }
        }
        Ok(())
    }

    /// Send a NOTIFY message (no_reply=1, no ack required).
    ///
    /// The `data` closure is called after an internal `nest_start(ATTR_DATA)` to populate blobmsg fields.
    /// Pass `|bb| {}` if no data is needed (sends an empty DATA nested attribute).
    pub fn notify<F>(&mut self, objid: u32, method: &str, data: F) -> Result<()>
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
        send_msg(&mut self.stream, MSG_NOTIFY, seq, 0, &body)
    }

    /// Block and read one inbound message.
    pub fn recv(&mut self) -> Result<UbusMessage> {
        let (mtype, seq, peer, body) = recv_msg(&mut self.stream)?;
        Ok(UbusMessage {
            msg_type: MsgType::from(mtype),
            seq,
            peer,
            body,
        })
    }

    /// Returns a mutable reference to the underlying stream (for advanced usage like set_nonblocking).
    pub fn stream(&mut self) -> &mut UnixStream {
        &mut self.stream
    }

    fn next_seq(&mut self) -> u16 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }
}

fn send_msg(stream: &mut UnixStream, mtype: u8, seq: u16, peer: u32, body: &[u8]) -> Result<()> {
    let mut hdr = [0u8; 8];
    hdr[0] = 0;
    hdr[1] = mtype;
    hdr[2..4].copy_from_slice(&seq.to_be_bytes());
    hdr[4..8].copy_from_slice(&peer.to_be_bytes());
    stream.write_all(&hdr)?;
    stream.write_all(body)?;
    Ok(())
}

fn recv_msg(stream: &mut UnixStream) -> Result<(u8, u16, u32, Vec<u8>)> {
    let mut hdr = [0u8; 8];
    stream.read_exact(&mut hdr)?;
    let mtype = hdr[1];
    let seq = u16::from_be_bytes([hdr[2], hdr[3]]);
    let peer = u32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    let mut blob_head = [0u8; 4];
    stream.read_exact(&mut blob_head)?;
    let id_len = u32::from_be_bytes(blob_head);
    let total_len = (id_len & 0x00ff_ffff) as usize;
    if total_len < 4 {
        return Err(UbusError::Protocol("blob len < 4"));
    }
    let mut body_rest = vec![0u8; total_len - 4];
    stream.read_exact(&mut body_rest)?;
    let mut body = Vec::with_capacity(total_len);
    body.extend_from_slice(&blob_head);
    body.extend_from_slice(&body_rest);
    Ok((mtype, seq, peer, body))
}
