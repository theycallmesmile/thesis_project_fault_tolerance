#![allow(warnings, unused)]
extern crate tokio;


//use std::hash::Hash;
//use std::os::raw;
//use std::ptr::eq;
//use std::str;
//use std::sync::Arc;

//use async_std::stream::Sum;
//use rskafka::time;
//use tokio::fs::File;
//use tokio::fs::OpenOptions;
//use tokio::io::AsyncReadExt;
//use tokio::io::AsyncWriteExt;

//use serde::{Deserializer, Serializer};
//use serde_json;

//use tokio::sync::oneshot;
//use tokio::task::JoinSet;
//use std::collections::VecDeque;





//new
pub mod serialize;
pub mod consumer;
pub mod rest;
pub mod manager;
pub mod producer;
pub mod channel;
use serde::{Deserialize, Serialize};




//static mut glob_snapshot_hashmap:HashMap<*const (), TaskToManagerMessage> = HashMap::new();



#[derive(Serialize, Deserialize, Debug, Clone)]
enum PersistentProducerState {
    S0 {
        output: PersistentPushChan<()>,
        count: i32,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PersistentPushChan<T> {
    uid: u64,
    buffer: Option<Vec<T>>
}

/*fn producer(manager_push: PushChan<Event<()>>) -> PullChan<Event<()>> {
    let (push, pull) = channel::<Event<()>>();
    let state = ProducerState::S0 {
        output: push,
        manager_output: manager_push,
        count: 0,
    };
    async_std::task::spawn(state.execute());
    println!("producer operator spawned!");
    println!("The producer channels: {:?}",pull);
    pull
}*/

async fn boot_up_func() {
    console_subscriber::init();
    //let mut set = JoinSet::new();

    let manager = manager::manager();

    //let stream = producer(manager);
    //consumer(stream);

    loop{//System dies without this loop

    }


}

#[tokio::main]
//#[async_std::main]
async fn main() {
    //async_std::task::block_on(boot_up_func());
    boot_up_func().await;
}