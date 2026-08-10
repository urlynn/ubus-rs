//! Synchronous notify demo — sends ubus NOTIFY every N seconds with a background read loop.
//!
//! Usage: notify_demo [--socket PATH] [--object NAME] [--interval SECS] [--once]
//!   --once  Send one NOTIFY and exit

use std::env;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ubus_rs::UbusClient;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut socket_path = "/var/run/ubus/ubus.sock".to_string();
    let mut object_name = "example".to_string();
    let mut interval = 2u64;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--socket" if i + 1 < args.len() => {
                socket_path = args[i + 1].clone();
                i += 2;
            }
            "--object" if i + 1 < args.len() => {
                object_name = args[i + 1].clone();
                i += 2;
            }
            "--interval" if i + 1 < args.len() => {
                interval = args[i + 1].parse().unwrap_or(2);
                i += 2;
            }
            "--once" => {
                interval = 0;
                i += 1;
            }
            _ => {
                eprintln!("Usage: notify_demo [--socket PATH] [--object NAME] [--interval SECS] [--once]");
                eprintln!("  --once  Send one NOTIFY and exit");
                return;
            }
        }
    }

    eprintln!("[demo] connect {}", socket_path);
    let mut client = match UbusClient::connect(&socket_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[demo] connect failed: {}", e);
            std::process::exit(1);
        }
    };
    eprintln!("[demo] HELLO client_id={:#010x}", client.client_id());

    let objid = match client.add_object(&object_name) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[demo] add_object failed: {}", e);
            std::process::exit(1);
        }
    };
    eprintln!(
        "[demo] registered object '{}' objid={:#010x}",
        object_name, objid
    );

    let mut reader = match client.try_clone() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[demo] clone failed: {}", e);
            std::process::exit(1);
        }
    };
    thread::spawn(move || loop {
        match reader.recv() {
            Ok(msg) => eprintln!(
                "[read] {} seq={} peer={:#010x} body_len={}",
                msg.msg_type.name(),
                msg.seq,
                msg.peer,
                msg.body.len()
            ),
            Err(e) => {
                eprintln!("[read] exit: {}", e);
                break;
            }
        }
    });

    let mut counter = 0u64;
    loop {
        counter += 1;
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if let Err(e) = client.notify(objid, "event.trigger", |bb| {
            bb.blobmsg_add_string("message", "hello");
            bb.blobmsg_add_u32("timestamp", ts as u32);
            bb.blobmsg_add_u32("count", counter as u32);
        }) {
            eprintln!("[demo] send NOTIFY failed: {}", e);
            break;
        }
        eprintln!(
            "[demo] NOTIFY #{} sent (objid={:#010x}, ts={})",
            counter, objid, ts
        );

        if interval == 0 {
            thread::sleep(Duration::from_millis(200));
            break;
        }
        thread::sleep(Duration::from_secs(interval));
    }

    eprintln!("[demo] exit");
}
