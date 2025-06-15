use bincode::{Encode, Decode};
use kameo::prelude::*;
use kameo_child_process::{KameoChildProcessMessage, prelude::*};
use std::error::Error;
use std::io;
use tracing::{debug, info, warn, error, Level, instrument};
use tracing_subscriber;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use tokio::time::sleep;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Mutex;

// Ork types and behaviors
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, Hash, Eq, PartialEq)]
pub enum OrkKlan {
    Goffs,      // Da best at krumpin'
    EvilSunz,   // Speed freeks
    BadMoons,   // Flash gitz with teef
    DeathSkulls, // Lucky blue gitz
    BloodAxes,  // Taktikul but still orky
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct OrkBoy {
    pub name: String,
    pub power: u32,
    pub klan: OrkKlan,
    pub waaagh_momentum: u32,
    pub victories: u32,
    pub teef: u32,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct OrkMob {
    pub waaagh_power: u32,
    pub boyz: HashMap<String, OrkBoy>,
    pub total_waaagh: u32,
    pub mob_momentum: f32,
    pub biggest_boss: Option<String>,
    pub klan_reputation: HashMap<OrkKlan, i32>,
}

impl Default for OrkMob {
    fn default() -> Self {
        Self {
            waaagh_power: 100,
            boyz: HashMap::new(),
            total_waaagh: 150,
            mob_momentum: 1.5,
            biggest_boss: Some("Ghazghkull".to_string()),
            klan_reputation: HashMap::new(),
        }
    }
}

impl OrkMob {
    fn get_boy_power(&self, name: &str) -> Option<(u32, u32)> {
        self.boyz.get(name).map(|boy| (boy.power, boy.waaagh_momentum))
    }

    fn update_klan_reputation(&mut self, klan: &OrkKlan, amount: i32) {
        *self.klan_reputation.entry(klan.clone()).or_insert(0) += amount;
    }

    fn handle_boy_action(&mut self, boy_name: &str, klan: &OrkKlan, power_increase: u32) -> Option<(String, u32, OrkKlan)> {
        if let Some(boy) = self.boyz.get_mut(boy_name) {
            boy.power = boy.power.saturating_add(power_increase);
            boy.victories += 1;
            boy.teef += 5;
            Some((boy_name.to_string(), boy.power, klan.clone()))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct OrkError {
    pub message: String,
}

impl From<io::Error> for OrkError {
    fn from(err: io::Error) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

impl std::fmt::Display for OrkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OrkError: {}", self.message)
    }
}

impl std::error::Error for OrkError {}

#[async_trait]
impl Actor for OrkMob {
    type Error = OrkError;

    fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            info!("WAAAGH! DA BOYZ ARE READY!");
            Ok(())
        }
    }

    fn on_stop(&mut self, _actor_ref: WeakActorRef<Self>, _reason: ActorStopReason) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            info!("WAAAGH! DA BOYZ ARE DONE!");
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum OrkMessage {
    RecruitBoy { name: String, power: u32, klan: OrkKlan },
    StartWaaagh { target: String },
    CheckPower { boy_name: String },
    PromoteToBoss { boy_name: String },
    KlanScrap { boy1: String, boy2: String },  // Let da boyz fight!
    LootTeef { boy_name: String, amount: u32 }, // Get more teef!
    CheckKlanStrength { klan: OrkKlan },       // How strong is dis klan?
    FightOtherBoy { boy_name: String, target_name: String },
    WinFight { winner: String, loser: String },
    PanicTest { reason: String }, // New message type for testing panics
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum OrkResponse {
    Recruited { name: String, power: u32, klan: OrkKlan },
    WaaghStarted { target: String, total_power: u32, momentum: u32 },
    PowerLevel { name: String, boy: Option<OrkBoy> },
    BossPromoted { name: String, power: u32 },
    ScrapResult { winner: String, new_power: u32, loser: String },
    TeefLooted { boy_name: String, new_teef: u32 },
    KlanStrength { klan: OrkKlan, strength: i32 },
    Fought { name: String, power: u32, klan: OrkKlan },
    WonFight { winner: String, loser: String, new_power: u32, klan: OrkKlan },
    Error(String),
}

impl Reply for OrkResponse {
    type Ok = OrkResponse;
    type Error = OrkError;
    type Value = OrkResponse;

    fn to_result(self) -> Result<OrkResponse, OrkError> {
        match self {
            OrkResponse::Error(e) => Err(OrkError { message: e }),
            _ => Ok(self),
        }
    }

    fn into_value(self) -> OrkResponse {
        self
    }

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        match self {
            OrkResponse::Error(e) => Some(Box::new(OrkError { message: e })),
            _ => None,
        }
    }
}

impl KameoChildProcessMessage for OrkMessage {
    type Reply = OrkResponse;
}

#[async_trait]
impl Message<OrkMessage> for OrkMob {
    type Reply = OrkResponse;

    fn handle(&mut self, msg: OrkMessage, _ctx: &mut Context<Self, Self::Reply>) -> impl std::future::Future<Output = Self::Reply> + Send {
        async move {
            match msg {
                OrkMessage::RecruitBoy { name, power, klan } => {
                    debug!(event = "ork_action", action = "recruit", name = ?name, power = power, klan = ?klan);
                    
                    // Check if name already exists
                    if self.boyz.contains_key(&name) {
                        return OrkResponse::Error(format!("Boy already exists: {}", name));
                    }

                    let boy = OrkBoy {
                        name: name.clone(),
                        power,
                        klan: klan.clone(),
                        waaagh_momentum: 0,
                        victories: 0,
                        teef: 10, // Start with some teef
                    };
                    
                    let old_boss_power = self.biggest_boss.as_ref()
                        .and_then(|boss| self.boyz.get(boss))
                        .map(|boss| boss.power)
                        .unwrap_or(0);

                    self.boyz.insert(name.clone(), boy);
                    self.total_waaagh = self.total_waaagh.saturating_add(power);
                    self.update_klan_reputation(&klan, power as i32);
                    
                    // Update biggest boss if needed
                    if self.biggest_boss.is_none() || power > old_boss_power {
                        self.biggest_boss = Some(name.clone());
                    }
                    
                    OrkResponse::Recruited { name, power, klan }
                }
                OrkMessage::StartWaaagh { target } => {
                    debug!(event = "ork_action", action = "waaagh", target = ?target);
                    
                    if self.boyz.is_empty() {
                        return OrkResponse::Error("Can't start WAAAGH! with no boyz!".to_string());
                    }
                    
                    let total_power = self.boyz.values().map(|boy| boy.power).sum::<u32>();
                    let momentum = self.mob_momentum as u32;
                    
                    OrkResponse::WaaghStarted {
                        target,
                        total_power,
                        momentum,
                    }
                }
                OrkMessage::CheckPower { boy_name } => {
                    debug!(event = "ork_action", action = "check_power", boy = ?boy_name);
                    
                    let boy = self.boyz.get(&boy_name).cloned();
                    OrkResponse::PowerLevel { name: boy_name, boy }
                }
                OrkMessage::PromoteToBoss { boy_name } => {
                    debug!(event = "ork_action", action = "promote", boy = ?boy_name);
                    
                    if let Some(boy) = self.boyz.get_mut(&boy_name) {
                        boy.power += 50;
                        boy.teef += 20;
                        self.biggest_boss = Some(boy_name.clone());
                        OrkResponse::BossPromoted { name: boy_name, power: boy.power }
                    } else {
                        OrkResponse::Error("Boy not found!".to_string())
                    }
                }
                OrkMessage::KlanScrap { boy1, boy2 } => {
                    debug!(event = "ork_action", action = "scrap", boy1 = ?boy1, boy2 = ?boy2);
                    
                    let (boy1_power, boy2_power) = match (self.get_boy_power(&boy1), self.get_boy_power(&boy2)) {
                        (Some((p1, _)), Some((p2, _))) => (p1, p2),
                        _ => return OrkResponse::Error("One or both boyz not found!".to_string()),
                    };

                    let (winner, loser) = if boy1_power > boy2_power { 
                        (boy1, boy2) 
                    } else { 
                        (boy2, boy1) 
                    };
                    
                    if let Some(boy) = self.boyz.get_mut(&winner) {
                        boy.power += 10;
                        boy.victories += 1;
                        boy.teef += 5;
                        OrkResponse::ScrapResult { 
                            winner: winner.clone(), 
                            new_power: boy.power, 
                            loser: loser.clone() 
                        }
                    } else {
                        OrkResponse::Error("Winner not found after initial check!".to_string())
                    }
                }
                OrkMessage::LootTeef { boy_name, amount } => {
                    debug!(event = "ork_action", action = "loot", boy = ?boy_name, amount = amount);
                    
                    if let Some(boy) = self.boyz.get_mut(&boy_name) {
                        boy.teef = boy.teef.saturating_add(amount);
                        OrkResponse::TeefLooted { boy_name, new_teef: boy.teef }
                    } else {
                        OrkResponse::Error("Boy not found!".to_string())
                    }
                }
                OrkMessage::CheckKlanStrength { klan } => {
                    debug!(event = "ork_action", action = "check_klan", klan = ?klan);
                    
                    let strength = *self.klan_reputation.get(&klan).unwrap_or(&0);
                    OrkResponse::KlanStrength { klan, strength }
                }
                OrkMessage::FightOtherBoy { boy_name, target_name } => {
                    debug!(event = "ork_action", action = "fight", boy = ?boy_name, target = ?target_name);
                    
                    let (boy_klan, target_klan) = match (self.boyz.get(&boy_name), self.boyz.get(&target_name)) {
                        (Some(boy), Some(target)) => (boy.klan.clone(), target.klan.clone()),
                        _ => return OrkResponse::Error("One or both boyz not found!".to_string()),
                    };

                    // Handle the fighting boy first
                    if let Some((name, power, klan)) = self.handle_boy_action(&boy_name, &boy_klan, 50) {
                        self.update_klan_reputation(&klan, 50);
                        OrkResponse::Fought { name, power, klan }
                    } else {
                        OrkResponse::Error("Boy not found after initial check!".to_string())
                    }
                }
                OrkMessage::WinFight { winner, loser } => {
                    debug!(event = "ork_action", action = "win_fight", winner = ?winner, loser = ?loser);
                    
                    let winner_klan = match self.boyz.get(&winner) {
                        Some(boy) => boy.klan.clone(),
                        None => return OrkResponse::Error("Winner boy not found!".to_string()),
                    };

                    // Handle the winning boy
                    if let Some((name, power, klan)) = self.handle_boy_action(&winner, &winner_klan, 10) {
                        self.update_klan_reputation(&klan, 10);
                        OrkResponse::WonFight { 
                            winner: name,
                            loser,
                            new_power: power,
                            klan,
                        }
                    } else {
                        OrkResponse::Error("Winner not found after initial check!".to_string())
                    }
                }
                OrkMessage::PanicTest { reason } => {
                    debug!(event = "ork_action", action = "panic_test", reason = ?reason);
                    panic!("WAAAGH! DELIBERATE PANIC: {}", reason);
                }
            }
        }
    }
}

// Use our macro to generate the main function
kameo_main! {
    actors = {
        (OrkMob, OrkMessage),
    },
    parent_runtime = { worker_threads = 4 },
    parent_main = parent_process_main()
}

// Define our parent process main function
async fn parent_process_main() -> Result<(), Box<dyn std::error::Error>> {
    info!(event = "startup", "Starting parent process");
    
    let mut warbosses = Vec::new();
    
    // Create multiple OrkMob actors
    for i in 0..3 {
        info!(event = "setup", warboss_index = i, "Creating warboss");
        let actor_ref = ChildProcessBuilder::<OrkMob, OrkMessage>::new()
            .log_level(Level::DEBUG)
            .spawn()
            .await?;
        warbosses.push(actor_ref);
    }

    let mut handles = Vec::new();
    
    // Test each warboss
    for (i, boss) in warbosses.iter().enumerate() {
        let boss = boss.clone();
        let handle = tokio::spawn(async move {
            // First test normal operation
            let boy1_name = format!("Warboss_{}_Boy_1", i);
            
            // Recruit one boy
            let response = boss.ask(OrkMessage::RecruitBoy {
                name: boy1_name.clone(),
                power: 100 + i as u32,
                klan: OrkKlan::Goffs,
            }).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            info!(event = "test", response = ?response, "Boy recruited!");

            // Start a WAAAGH!
            let response = boss.ask(OrkMessage::StartWaaagh {
                target: format!("Enemy_Warboss_{}", i),
            }).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            info!(event = "test", response = ?response, "WAAAGH! started!");

            // Now test panic handling
            info!(event = "test", warboss = i, "Testing panic handling...");
            match boss.ask(OrkMessage::PanicTest {
                reason: format!("Warboss {} testing panic handling!", i),
            }).await {
                Ok(_) => {
                    error!(event = "test", warboss = i, "Expected panic but got success!");
                    return Err(io::Error::new(io::ErrorKind::Other, "Expected panic but got success"));
                }
                Err(e) => {
                    info!(event = "test", warboss = i, error = ?e, "Successfully caught panic!");
                }
            }

            // Try to send another message after panic to verify connection handling
            info!(event = "test", warboss = i, "Testing post-panic message handling...");
            match boss.ask(OrkMessage::CheckPower {
                boy_name: boy1_name.clone(),
            }).await {
                Ok(_) => {
                    error!(event = "test", warboss = i, "Expected failure but got success!");
                    return Err(io::Error::new(io::ErrorKind::Other, "Expected failure but got success"));
                }
                Err(e) => {
                    info!(event = "test", warboss = i, error = ?e, "Successfully detected dead actor!");
                }
            }

            Ok::<_, io::Error>(())
        });
        handles.push(handle);
    }

    // Wait for all tests to complete
    for handle in handles {
        handle.await.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Task join error: {}", e)))??;
    }

    info!(event = "shutdown", "Parent process completed successfully");
    Ok(())
}