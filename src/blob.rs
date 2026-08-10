//! blob / blobmsg binary builder replicating libubox `blob_buf` and `blobmsg`.

use crate::{BLOBMSG_INT32, BLOBMSG_STRING, EXTENDED};

/// Top-level blob buffer.
///
/// The top-level blob_attr has `id=0`; `len` is backfilled in [`bytes()`](Self::bytes).
pub struct BlobBuf {
    buf: Vec<u8>,
}

impl BlobBuf {
    /// Create an empty blob buffer with top-level blob_attr `id=0 len=4` (header only).
    pub fn new() -> Self {
        BlobBuf { buf: vec![0, 0, 0, 4] }
    }

    /// Append an INT32 attribute (`len=8`: 4-byte header + 4-byte data).
    pub fn put_u32(&mut self, id: u8, val: u32) {
        let id_len = ((id as u32) << 24) | 8;
        self.buf.extend_from_slice(&id_len.to_be_bytes());
        self.buf.extend_from_slice(&val.to_be_bytes());
    }

    /// Append an INT8 attribute (`len=5`: 4-byte header + 1-byte data, padded to 8).
    pub fn put_u8(&mut self, id: u8, val: u8) {
        let id_len = ((id as u32) << 24) | 5;
        self.buf.extend_from_slice(&id_len.to_be_bytes());
        self.buf.push(val);
        self.buf.extend_from_slice(&[0, 0, 0]);
    }

    /// Append a STRING attribute (`len=4+n` including `\0`, 4-byte aligned).
    pub fn put_string(&mut self, id: u8, s: &str) {
        let n = s.len() + 1;
        let raw_len = 4 + n;
        let pad_len = (raw_len + 3) & !3;
        let id_len = ((id as u32) << 24) | (raw_len as u32);
        self.buf.extend_from_slice(&id_len.to_be_bytes());
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
        for _ in raw_len..pad_len {
            self.buf.push(0);
        }
    }

    /// Begin a nested attribute, returns the start offset for [`nest_end`](Self::nest_end).
    pub fn nest_start(&mut self, id: u8) -> usize {
        let ofs = self.buf.len();
        let id_len = ((id as u32) << 24) | 4;
        self.buf.extend_from_slice(&id_len.to_be_bytes());
        ofs
    }

    /// Backfill the nested attribute total length.
    ///
    /// # Panics
    /// Panics if `ofs` is beyond the buffer or leaves fewer than 4 bytes.
    pub fn nest_end(&mut self, ofs: usize) {
        assert!(
            ofs + 4 <= self.buf.len(),
            "nest_end: invalid offset {} (buf len {})",
            ofs,
            self.buf.len()
        );
        let total = (self.buf.len() - ofs) as u32;
        let old = u32::from_be_bytes(self.buf[ofs..ofs + 4].try_into().unwrap());
        let new_id_len = (old & 0xff00_0000) | total;
        self.buf[ofs..ofs + 4].copy_from_slice(&new_id_len.to_be_bytes());
    }

    /// Add a blobmsg named STRING field (`EXTENDED | STRING`).
    pub fn blobmsg_add_string(&mut self, name: &str, val: &str) {
        self.blobmsg_add(name, BLOBMSG_STRING, val.as_bytes(), true);
    }

    /// Add a blobmsg named INT32 field (`EXTENDED | INT32`).
    pub fn blobmsg_add_u32(&mut self, name: &str, val: u32) {
        self.blobmsg_add(name, BLOBMSG_INT32, &val.to_be_bytes(), false);
    }

    fn blobmsg_add(&mut self, name: &str, btype: u8, val: &[u8], val_is_string: bool) {
        let namelen = name.len();
        let hdr_len = (namelen + 6) & !3;
        let val_n = if val_is_string { val.len() + 1 } else { val.len() };
        let raw_len = 4 + hdr_len + val_n;
        let pad_len = (raw_len + 3) & !3;
        let id_len = EXTENDED | ((btype as u32) << 24) | (raw_len as u32);
        self.buf.extend_from_slice(&id_len.to_be_bytes());
        self.buf.extend_from_slice(&(namelen as u16).to_be_bytes());
        self.buf.extend_from_slice(name.as_bytes());
        self.buf.push(0);
        let written = 2 + namelen + 1;
        for _ in written..hdr_len {
            self.buf.push(0);
        }
        self.buf.extend_from_slice(val);
        if val_is_string {
            self.buf.push(0);
        }
        for _ in raw_len..pad_len {
            self.buf.push(0);
        }
    }

    /// Backfill the top-level length and return the complete buffer.
    pub fn bytes(mut self) -> Vec<u8> {
        let total = self.buf.len() as u32;
        self.buf[0..4].copy_from_slice(&total.to_be_bytes());
        self.buf
    }
}

impl Default for BlobBuf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_blob() {
        let bb = BlobBuf::new();
        assert_eq!(bb.buf, vec![0, 0, 0, 4]);
    }

    #[test]
    fn test_put_u32() {
        let mut bb = BlobBuf::new();
        bb.put_u32(3, 0x12345678);
        assert_eq!(bb.buf.len(), 12);
        assert_eq!(&bb.buf[4..8], &[0x03, 0x00, 0x00, 0x08]);
        assert_eq!(&bb.buf[8..12], &[0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_put_u8() {
        let mut bb = BlobBuf::new();
        bb.put_u8(10, 1);
        assert_eq!(bb.buf.len(), 12);
        assert_eq!(&bb.buf[4..8], &[0x0a, 0x00, 0x00, 0x05]);
        assert_eq!(&bb.buf[8..12], &[0x01, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_put_string() {
        let mut bb = BlobBuf::new();
        bb.put_string(2, "hi");
        assert_eq!(bb.buf.len(), 12);
        assert_eq!(&bb.buf[4..8], &[0x02, 0x00, 0x00, 0x07]);
        assert_eq!(&bb.buf[8..12], &[b'h', b'i', 0x00, 0x00]);
    }

    #[test]
    fn test_nest_start_end() {
        let mut bb = BlobBuf::new();
        let ofs = bb.nest_start(7);
        assert_eq!(ofs, 4);
        bb.put_u32(1, 42);
        bb.nest_end(ofs);
        assert_eq!(bb.buf.len(), 16);
        assert_eq!(&bb.buf[4..8], &[0x07, 0x00, 0x00, 0x0c]);
    }

    #[test]
    fn test_bytes_backfills_top_level() {
        let mut bb = BlobBuf::new();
        bb.put_u32(3, 1);
        let out = bb.bytes();
        assert_eq!(out.len(), 12);
        assert_eq!(&out[0..4], &[0x00, 0x00, 0x00, 0x0c]);
    }

    #[test]
    fn test_blobmsg_add_string() {
        let mut bb = BlobBuf::new();
        bb.blobmsg_add_string("msg", "hi");
        let out = bb.bytes();
        // namelen=3, hdr_len=(3+6)&!3=8, val_n=2+1=3, raw_len=4+8+3=15, pad_len=16
        // Total: 4 (top) + 16 = 20
        assert_eq!(out.len(), 20);
        let id_len = EXTENDED | ((BLOBMSG_STRING as u32) << 24) | 15;
        assert_eq!(&out[4..8], &id_len.to_be_bytes());
        // namelen field (2 bytes) + name "msg" (3 bytes) + null (1 byte) + padding (2 bytes) = 8 bytes
        assert_eq!(&out[8..10], &[0x00, 0x03]);
        assert_eq!(&out[10..16], &[b'm', b's', b'g', 0, 0, 0]);
        // Value starts at offset 16: "hi" (2 bytes) + null (1 byte) + padding (1 byte)
        assert_eq!(&out[16..18], &[b'h', b'i']);
    }

    #[test]
    fn test_blobmsg_add_u32() {
        let mut bb = BlobBuf::new();
        bb.blobmsg_add_u32("n", 0xAABBCCDD);
        let out = bb.bytes();
        // namelen=1, hdr_len=(1+6)&!3=4, val_n=4, raw_len=4+4+4=12, pad_len=12
        // Total: 4 (top) + 12 = 16
        assert_eq!(out.len(), 16);
        let id_len = EXTENDED | ((BLOBMSG_INT32 as u32) << 24) | 12;
        assert_eq!(&out[4..8], &id_len.to_be_bytes());
        // Value starts at offset 12: 4 (top) + 4 (attr) + 4 (blobmsg hdr)
        assert_eq!(&out[12..16], &[0xAA, 0xBB, 0xCC, 0xDD]);
    }
}
