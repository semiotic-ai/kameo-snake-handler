use serde::{Serialize, Deserialize};
use kameo::{Actor, message::{Context, Message}};
use kameo_child_process::{
    KameoChildProcessMessage, Handler, register_subprocess_actors_async,
    SubprocessActor, handshake, Control
};
use tracing::{debug, info, Level};
use tracing_subscriber;
use std::collections::HashMap;
use bincode::{Encode, Decode};

// Ork types and behaviors
#[derive(Debug, Default)]
pub struct OrkMob {
    boyz: HashMap<String, i32>,  // Name -> WAAAGH! power
    total_waaagh: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone, Encode, Decode)]
pub enum OrkMessage {
    RecruitBoy { name: String, power: i32 },
    StartWaaagh { target: String },
    CheckPower { boy_name: String },
}

// Implement KameoChildProcessMessage for our message type
impl KameoChildProcessMessage for OrkMessage {}

#[derive(Debug, Serialize, Deserialize, Clone, Encode, Decode)]
pub enum OrkResponse {
    Recruited { name: String, power: i32 },
    WaaaghStarted { target: String, total_power: i32 },
    BoyPower { name: String, power: Option<i32> },
}

// Local actor implementation
impl Actor for OrkMob {
    type Error = std::convert::Infallible;
}

impl Message<OrkMessage> for OrkMob {
    type Reply = OrkResponse;

    async fn handle(&mut self, msg: OrkMessage, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match msg {
            OrkMessage::RecruitBoy { name, power } => {
                debug!(event = "ork_action", action = "recruit", ?name, ?power, "RECRUITIN' A NEW BOY!");
                self.boyz.insert(name.clone(), power);
                self.total_waaagh += power;
                OrkResponse::Recruited { name, power }
            }
            OrkMessage::StartWaaagh { target } => {
                info!(event = "ork_action", action = "waaagh", ?target, total_power = self.total_waaagh, "WAAAAAAAAAAAAAAAGH!");
                OrkResponse::WaaaghStarted { 
                    target,
                    total_power: self.total_waaagh 
                }
            }
            OrkMessage::CheckPower { boy_name } => {
                debug!(event = "ork_action", action = "check_power", ?boy_name, "CHECKIN' DA BOY'Z POWER!");
                OrkResponse::BoyPower { 
                    name: boy_name.clone(),
                    power: self.boyz.get(&boy_name).copied()
                }
            }
        }
    }
}

// Subprocess handler implementation
impl Handler<OrkMessage> for OrkMob {
    type Output = OrkResponse;

    async fn handle(&mut self, msg: OrkMessage) -> Self::Output {
        match msg {
            OrkMessage::RecruitBoy { name, power } => {
                debug!(event = "ork_action", action = "recruit", ?name, ?power, "RECRUITIN' A NEW BOY!");
                self.boyz.insert(name.clone(), power);
                self.total_waaagh += power;
                OrkResponse::Recruited { name, power }
            }
            OrkMessage::StartWaaagh { target } => {
                info!(event = "ork_action", action = "waaagh", ?target, total_power = self.total_waaagh, "WAAAAAAAAAAAAAAAGH!");
                OrkResponse::WaaaghStarted { 
                    target,
                    total_power: self.total_waaagh 
                }
            }
            OrkMessage::CheckPower { boy_name } => {
                debug!(event = "ork_action", action = "check_power", ?boy_name, "CHECKIN' DA BOY'Z POWER!");
                OrkResponse::BoyPower { 
                    name: boy_name.clone(),
                    power: self.boyz.get(&boy_name).copied()
                }
            }
        }
    }
}

// Register our Ork actor for subprocess handling
register_subprocess_actors_async!((OrkMob, OrkMessage));

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
    
    // Check if we're a subprocess and handle that case
    if maybe_run_subprocess_registry_and_exit_async().await {
        unreachable!("Subprocess should have exited");
    }

    info!("STARTIN' DA WAAAGH!");

    // Create a local Ork Mob
    let local_mob = kameo::spawn(OrkMob::default());
    
    // Create a subprocess Ork Mob using our SubprocessActor
    let (stream, child, socket_path) = handshake::host::<OrkMessage, OrkResponse>("OrkMob", std::env::current_exe().unwrap().to_str().unwrap())
        .await
        .expect("Failed to spawn subprocess");
    
    let subprocess_mob = SubprocessActor::<OrkMessage>::new(stream, child, socket_path);
    let subprocess_mob = kameo::spawn(subprocess_mob);

    // Test both mobs
    let mobs = vec![&local_mob, &subprocess_mob];
    
    for (i, mob) in mobs.iter().enumerate() {
        // Recruit some boyz
        let reply = mob.send(OrkMessage::RecruitBoy { 
            name: format!("GrimSkull_{}", i), 
            power: 100 * (i as i32 + 1)
        }).await;
        info!(mob_index = i, ?reply, "RECRUITED A BOY!");

        // Start a WAAAGH!
        let reply = mob.send(OrkMessage::StartWaaagh { 
            target: format!("Humie Base {}", i)
        }).await;
        info!(mob_index = i, ?reply, "STARTED A WAAAGH!");

        // Check a boy's power
        let reply = mob.send(OrkMessage::CheckPower { 
            boy_name: format!("GrimSkull_{}", i)
        }).await;
        info!(mob_index = i, ?reply, "CHECKED BOY'Z POWER!");
    }

    info!("WAAAGH COMPLETE! BACK TO DA BASE!");
} 