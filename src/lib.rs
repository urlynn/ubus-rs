//! Pure Rust implementation of the ubus protocol for OpenWrt.
//!
//! This crate provides a notify-only ubus client with both synchronous and asynchronous APIs,
//! enabling communication with the ubus daemon over Unix domain sockets.
//!
//! # Examples
//!
//! ## Synchronous Client
//!
//! ```no_run
//! use ubus_rs::UbusClient;
//!
//! fn main() -> Result<(), ubus_rs::UbusError> {
//!     let mut client = UbusClient::connect("/var/run/ubus/ubus.sock")?;
//!     let objid = client.add_object("example")?;
//!     client.notify(objid, "event.trigger", |bb| {
//!         bb.blobmsg_add_string("message", "hello");
//!     })?;
//!     Ok(())
//! }
//! ```
//!
//! ## Asynchronous Client
//!
//! Enable the `async` feature to use the tokio-based async client:
//!
//! ```ignore
//! use ubus_rs::UbusClientAsync;
//!
//! async fn example() -> Result<(), ubus_rs::UbusError> {
//!     let mut client = UbusClientAsync::connect("/var/run/ubus/ubus.sock").await?;
//!     let objid = client.add_object("example").await?;
//!     client.notify(objid, "event.trigger", |bb| {
//!         bb.blobmsg_add_string("message", "hello");
//!     }).await?;
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod blob;
pub mod sync;
#[cfg(feature = "async")]
pub mod async_io;

pub use error::{UbusError, Result};
pub use blob::BlobBuf;
pub use sync::UbusClient;
#[cfg(feature = "async")]
pub use async_io::{UbusClientAsync, UbusReader, UbusWriter};

// ── UBUS_ATTR_* (ubusmsg.h) ──

pub const ATTR_STATUS: u8 = 1;
pub const ATTR_OBJPATH: u8 = 2;
pub const ATTR_OBJID: u8 = 3;
pub const ATTR_METHOD: u8 = 4;
pub const ATTR_OBJTYPE: u8 = 5;
pub const ATTR_SIGNATURE: u8 = 6;
pub const ATTR_DATA: u8 = 7;
pub const ATTR_NO_REPLY: u8 = 10;

// ── UBUS_MSG_* (ubusmsg.h) ──

pub const MSG_HELLO: u8 = 0;
pub const MSG_STATUS: u8 = 1;
pub const MSG_DATA: u8 = 2;
pub const MSG_INVOKE: u8 = 5;
pub const MSG_ADD_OBJECT: u8 = 6;
pub const MSG_REMOVE_OBJECT: u8 = 7;
pub const MSG_NOTIFY: u8 = 10;

// ── blobmsg_type (blobmsg.h) ──

pub const BLOBMSG_STRING: u8 = 3;
pub const BLOBMSG_INT32: u8 = 5;

pub const EXTENDED: u32 = 0x8000_0000;

/// Inbound message type from ubusd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgType {
    Hello,
    Status,
    Data,
    Ping,
    Lookup,
    Invoke,
    AddObject,
    RemoveObject,
    Subscribe,
    Unsubscribe,
    Notify,
    Monitor,
    Unknown(u8),
}

impl From<u8> for MsgType {
    fn from(v: u8) -> Self {
        match v {
            0 => MsgType::Hello,
            1 => MsgType::Status,
            2 => MsgType::Data,
            3 => MsgType::Ping,
            4 => MsgType::Lookup,
            5 => MsgType::Invoke,
            6 => MsgType::AddObject,
            7 => MsgType::RemoveObject,
            8 => MsgType::Subscribe,
            9 => MsgType::Unsubscribe,
            10 => MsgType::Notify,
            11 => MsgType::Monitor,
            other => MsgType::Unknown(other),
        }
    }
}

impl MsgType {
    pub fn name(self) -> &'static str {
        match self {
            MsgType::Hello => "HELLO",
            MsgType::Status => "STATUS",
            MsgType::Data => "DATA",
            MsgType::Ping => "PING",
            MsgType::Lookup => "LOOKUP",
            MsgType::Invoke => "INVOKE",
            MsgType::AddObject => "ADD_OBJECT",
            MsgType::RemoveObject => "REMOVE_OBJECT",
            MsgType::Subscribe => "SUBSCRIBE",
            MsgType::Unsubscribe => "UNSUBSCRIBE",
            MsgType::Notify => "NOTIFY",
            MsgType::Monitor => "MONITOR",
            MsgType::Unknown(_) => "UNKNOWN",
        }
    }
}

/// Inbound message from ubusd.
#[derive(Debug, Clone)]
pub struct UbusMessage {
    pub msg_type: MsgType,
    pub seq: u16,
    pub peer: u32,
    pub body: Vec<u8>,
}

/// Parse INT32 attribute value from ubus message body.
pub fn parse_attr_u32(body: &[u8], target_id: u8) -> Option<u32> {
    if body.len() < 4 {
        return None;
    }
    let top_id_len = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let top_len = (top_id_len & 0x00ff_ffff) as usize;
    let end = top_len.min(body.len());
    let mut pos = 4;
    while pos + 4 <= end {
        let attr_id_len = u32::from_be_bytes([
            body[pos],
            body[pos + 1],
            body[pos + 2],
            body[pos + 3],
        ]);
        let attr_id = ((attr_id_len >> 24) & 0x7f) as u8;
        let attr_len = (attr_id_len & 0x00ff_ffff) as usize;
        if attr_len < 4 || pos + attr_len > end {
            break;
        }
        let pad_len = (attr_len + 3) & !3;
        if attr_id == target_id && attr_len >= 8 {
            return Some(u32::from_be_bytes([
                body[pos + 4],
                body[pos + 5],
                body[pos + 6],
                body[pos + 7],
            ]));
        }
        pos += pad_len;
    }
    None
}
