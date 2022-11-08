#![allow(warnings, unused)]
extern crate tokio;

//new
pub mod serialization;
pub mod consumer;
pub mod manager;
pub mod producer;
pub mod channel;
pub mod shared;
pub mod consumer_producer;
use serde::{Deserialize, Serialize};


async fn boot_up_func() {
    console_subscriber::init();
    //let mut set = JoinSet::new();

    let manager = manager::manager();

    loop{//System dies without this loop

    }


}

#[tokio::main]
//#[async_std::main]
async fn main() {
    //async_std::task::block_on(boot_up_func());
    boot_up_func().await;
    //manager::temp_spawn_operators().await;
}