use std::{net::SocketAddr, env, time::Duration, sync::{Arc, Mutex}};

use map_reduce::mr::{coordinator::ServerClient, worker::Worker};
use tarpc::{tokio_serde::formats::Json, client, context};
use tokio::time::sleep;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<String>>();
    if args.len() != 3 {
        // The input file will start from `gut-0.txt` to `gut-{0 + map_n - 1}.txt`
        println!("Usage: cargo run --bin mrworker -- <input files number> <reduce task number>");
        return Ok(());
    }

    let (map_n, reduce_n) = (args[1].parse::<i32>()?, args[2].parse::<i32>()?);

    println!("[Worker Configuration] #{} Map Tasks | #{} Reduce Tasks", map_n, reduce_n);

    let server_address = "127.0.0.1:1040".parse::<SocketAddr>().unwrap();

    // Connect to the server
    let client_transport = match tarpc::serde_transport::tcp::connect(server_address, Json::default).await {
        Ok(t) => t,
        Err(e) => {
            println!(
                "[Preparation] Worker failed to connect to the RPC server, please check the Coordinator status!\n{}{}",
                "Error Message: ",
                e
            );
            return Ok(());
        }
    };

    let client = ServerClient::new(client::Config::default(), client_transport).spawn();

    let worker_id = client.get_worker_id(context::current()).await?;
    println!("[Preparation] Get worker id #{} from server", worker_id);

    let mut worker = Worker::new(map_n, reduce_n);

    loop {
        let cur_state = worker.get_state();
        match cur_state {
            false => {
                // In map phase
                assert!(worker.get_map_id() == -1);
                // Ask the coordinator for a new map task id
                let map_task_id = client.get_map_task(context::current()).await?;
                if map_task_id == -3 {
                    println!("[Map] The lease is not empty, go to sleep and recheck later");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                if map_task_id == -2 {
                    println!("[Preparation] There is no enough worker process to start the MapReduce, go to sleep");
                    // Sleep for a while
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                if map_task_id == -1 {
                    println!("[Map] No available map tasks at present, change the state to reduce and go to sleep");
                    worker.change_state();
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                // Otherwise, let's do the map job!
                worker.set_map_id(map_task_id);
                let lease_flag = Arc::new(Mutex::new(false));
                let lease_flag_clone = Arc::clone(&lease_flag);
                tokio::spawn(
                    async move {
                        // Connect to the server
                        let thread_client_transport = match tarpc::serde_transport::tcp::connect(server_address, Json::default).await {
                            Ok(t) => t,
                            Err(e) => {
                                println!(
                                    "[Map Lease] Worker failed to connect to the RPC server, please check the Coordinator status!\n{}{}",
                                    "Error Message: ",
                                    e
                                );
                                return;
                            }
                        };
                        let thread_client = ServerClient::new(client::Config::default(), thread_client_transport).spawn();
                        loop {
                            if *lease_flag_clone.lock().unwrap() {
                                return;
                            }
                            thread_client.renew_map_lease(context::current(), map_task_id).await.unwrap();
                        }
                    }
                );
                assert!(worker.map().await?);
                assert!(client.report_map_task_finish(context::current(), map_task_id).await?);
                assert!(!*lease_flag.lock().unwrap());
                *lease_flag.lock().unwrap() = true;
            }
            true => {
                // In reduce phase
                assert!(worker.get_map_id() == -1 && worker.get_reduce_id() == -1);
                // Ask the coordinator for a new reduce task id
                let reduce_task_id = client.get_reduce_task(context::current()).await?;
                if reduce_task_id == -3 {
                    println!("[Reduce] The lease is not empty, go to sleep and recheck later");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                if reduce_task_id == -2 {
                    println!("[Reduce] The reduce phase has not yet started due to unfinished map tasks, go to sleep");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                if reduce_task_id == -1 {
                    println!("[Reduce] No available reduce tasks at present, this worker process will thus terminate\nWish you a good day :)");
                    return Ok(());
                }
                worker.set_reduce_id(reduce_task_id);
                let lease_flag = Arc::new(Mutex::new(false));
                let lease_flag_clone = Arc::clone(&lease_flag);
                tokio::spawn(
                    async move {
                        let thread_client_transport = match tarpc::serde_transport::tcp::connect(server_address, Json::default).await {
                            Ok(t) => t,
                            Err(e) => {
                                println!(
                                    "[Reduce Lease] Worker failed to connect to the RPC server, please check the Coordinator status!\n{}{}",
                                    "Error Message: ",
                                    e
                                );
                                return;
                            }
                        };
                        let thread_client = ServerClient::new(client::Config::default(), thread_client_transport).spawn();
                        loop {
                            if *lease_flag_clone.lock().unwrap() {
                                return;
                            }
                            thread_client.renew_reduce_lease(context::current(), reduce_task_id).await.unwrap();
                            sleep(Duration::from_secs(1)).await;
                        }
                    }
                );
                assert!(worker.reduce().await?);
                assert!(client.report_reduce_task_finish(context::current(), reduce_task_id).await?);
                assert!(!*lease_flag.lock().unwrap());
                *lease_flag.lock().unwrap() = true;
            }
        }
    }
}