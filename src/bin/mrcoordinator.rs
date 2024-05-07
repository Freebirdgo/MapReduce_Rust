use std::{env, sync::{Arc, Mutex}, time::Duration, net::SocketAddr};

use anyhow::Ok;
use futures::StreamExt;
use map_reduce::mr::coordinator::*;
use tarpc::{server::incoming::Incoming, tokio_serde::formats::Json};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<String>>();
    if args.len() != 4 {
        println!("Usage: cargo run --bin mrcoordinator -- <input files number> <reduce task number> <worker number>");
        return Ok(());
    }

    let (map_n, reduce_n, worker_n) = (args[1].parse::<i32>()?, args[2].parse::<i32>()?, args[3].parse::<i32>()?);

    println!(
        "[Coordinator Configuration] #{} Map Tasks | #{} Reduce Tasks | #{} Worker Processes",
        map_n,
        reduce_n,
        worker_n
    );

    let coordinator = Arc::new(Mutex::new(Coordinator::new(map_n, reduce_n, worker_n)));


    let coordinator_clone = Arc::clone(&coordinator);

    let server_address = "127.0.0.1:1040".parse::<SocketAddr>().unwrap();
    let server_transport = tarpc::serde_transport::tcp::listen(
        server_address, 
        Json::default
    ).await?;

    tokio::spawn(
        server_transport
            // Accepts if this is a valid connection, otherwise ignores this connection
            .filter_map(|r| async { r.ok() })
            .map(tarpc::server::BaseChannel::with_defaults)
            .execute(coordinator_clone.lock().unwrap().clone().serve())
    );

    println!("[Preparation] The Coordinator RPC server has launched and is currently serving, please launch #{} worker process(es) to begin MapReduce", worker_n);

    let lease_period = 5;
    let mut lease_time_counter = 0;
    while !coordinator.lock().unwrap().done() {
        sleep(Duration::from_secs(1)).await;
        lease_time_counter += 1;
        if lease_time_counter == lease_period {
            println!("[Check Lease] Check the current lease to see if any worker is offline");
            assert!(coordinator.lock().unwrap().check_lease());
            lease_time_counter = 0;
        }
    }

    println!(
        "\nThe MapReduce process has finished, please check the results at `mr-*.txt`");

    Ok(())
}