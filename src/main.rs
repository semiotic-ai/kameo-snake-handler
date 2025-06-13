use ipc_channel::ipc::{self};
use serde::{Serialize, Deserialize};
use std::env;
use kameo::{Actor, message::{Context, Message as KameoMessage}};
use kameo_child_process::{Control, spawn_subprocess, register_subprocess_actors};
use tracing_subscriber;
use tracing::{debug, info, trace, Level};

// Define a Counter actor
#[derive(Default)]
struct Counter {
    count: i64,
}

// Implement kameo::Actor for Counter
impl Actor for Counter {
    type Error = std::convert::Infallible;
}

// Define an increment message
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Inc {
    amount: i64,
}

// Implement Kameo's Message trait for Counter
impl KameoMessage<Inc> for Counter {
    type Reply = i64;
    async fn handle(&mut self, msg: Inc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.count += msg.amount;
        self.count
    }
}

impl kameo_child_process::Handler<Inc, i64> for Counter {
    fn handle(&mut self, msg: Inc) -> i64 {
        self.count += msg.amount;
        self.count
    }
}

register_subprocess_actors! {
    (Counter, Inc) => {
        debug!("[subprocess] Running handler for Counter/Inc, will exit after loop");
        kameo_child_process::default_actor_loop::<Counter, Inc, i64>();
        debug!("[subprocess] Exiting after handler loop");
    }
}

fn main() {
    // Initialize tracing as early as possible, explicitly to stderr
    tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
    // Check subprocess first
    let is_child = std::env::var("KAMEO_CHILD_ACTOR").is_ok();
    let process_role = if is_child { "child" } else { "parent" };
    let span = tracing::span!(Level::INFO, "process", process_role);
    let _enter = span.enter();
    if maybe_run_subprocess_registry_and_exit() {
        unreachable!("Subprocess should have exited");
    }
    info!("Parent process starting");
    eprintln!("Parent process starting");
    let actor_env = std::env::var("KAMEO_CHILD_ACTOR").ok();
    debug!("KAMEO_CHILD_ACTOR={:?}", actor_env);
    info!("Entered main()");
    eprintln!("Entered main()");
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run_main());
    info!("Exiting main()");
    eprintln!("Exiting main()");
}

async fn run_main() {
    info!("[run_main] Parent process");
    // Spawn and interact with multiple Counter actors in subprocesses
    let mut actors = Vec::new();
    for i in 0..5 {
        let (tx, rx) = spawn_subprocess::<Counter, Inc, i64>();
        debug!("[parent] Spawned subprocess Counter actor #{}", i);
        actors.push((tx, rx));
    }
    // Send each actor a message and log the reply
    for (i, (tx, rx)) in actors.into_iter().enumerate() {
        let amount = (i as i64 + 1) * 10;
        debug!("[parent] Sending Inc {{ amount: {} }} to actor #{}", amount, i);
        tx.send(Control::Real(Inc { amount })).unwrap();
        debug!("[parent] Waiting for reply from actor #{}", i);
        let reply = rx.recv().unwrap();
        debug!("[parent] Received reply from actor #{}: {:?}", i, reply);
        info!("[parent] Final reply from actor #{}: {:?}", i, reply);
    }
    info!("[run_main] All subprocess actors tested");
    eprintln!("[run_main] Exiting run_main() (parent)");
    info!("[run_main] Exiting run_main() (parent)");
} 