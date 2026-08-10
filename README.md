# ubus-rs

Pure Rust implementation of the ubus protocol for OpenWrt, providing notify-only client functionality with both sync and async APIs.

## Features

- **Zero unsafe code** - Pure safe Rust implementation
- **Dual API** - Synchronous client (std-only) and asynchronous client (tokio-based)
- **Lightweight** - Minimal dependencies, only tokio when async feature is enabled
- **Protocol compliant** - Implements the ubus notify protocol for OpenWrt IPC

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ubus-rs = "0.1.0"
```

For async support:

```toml
[dependencies]
ubus-rs = { version = "0.1.0", features = ["async"] }
```

## Usage

### Synchronous Client

```rust
use ubus_rs::UbusClient;

fn main() -> Result<(), ubus_rs::UbusError> {
    let mut client = UbusClient::connect("/var/run/ubus/ubus.sock")?;
    let objid = client.add_object("example")?;
    
    client.notify(objid, "event.trigger", |bb| {
        bb.blobmsg_add_string("message", "hello");
        bb.blobmsg_add_u32("timestamp", 1234567890);
    })?;
    
    Ok(())
}
```

### Asynchronous Client

```rust
use ubus_rs::UbusClientAsync;

#[tokio::main]
async fn main() -> Result<(), ubus_rs::UbusError> {
    let mut client = UbusClientAsync::connect("/var/run/ubus/ubus.sock").await?;
    let objid = client.add_object("example").await?;
    
    client.notify(objid, "event.trigger", |bb| {
        bb.blobmsg_add_string("message", "hello");
    }).await?;
    
    Ok(())
}
```

## Protocol Overview

This crate implements the ubus notify protocol, which allows applications to:

1. Connect to the ubus daemon via Unix domain socket
2. Register objects with the ubus service
3. Send notifications to subscribers
4. Receive and parse inbound messages

The protocol uses a binary format based on libubox blob attributes, with big-endian encoding and 4-byte alignment.

## Requirements

Unix-like system (Linux/OpenWrt) with ubus daemon.

## License

MIT OR Apache-2.0

## Contributing

Contributions are welcome! Please feel free to [Submit a Pull Request](https://github.com/urlynn/ubus-rs/pulls).