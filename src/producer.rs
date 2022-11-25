use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

use std::collections::HashSet;

use std::hash::Hash;
use std::time::Duration;
use tokio::time;

use async_std::task;

use tokio::sync::oneshot;

use std::sync::Arc;

//Consumer-producer module

//Serialization
use crate::serialization::PersistentTask;
use crate::serialization::PartialPersistentTask;

//Manager module
use crate::manager::{Context, self};
use crate::manager::Task;
use crate::manager::PersistentTaskToManagerMessage;

//Channel module
use crate::channel::PullChan;
use crate::channel::PushChan;

//Shared module
use crate::shared::SharedState;
use crate::shared::Shared;
use crate::shared::Event;

#[derive(Debug, Clone)]
pub enum ProducerState {
    S0 {
        out0: PushChan<Event<(u64, u64)>>,
        count: i32,
    },
}

impl ProducerState {
    pub async fn execute_unoptimized(mut self, ctx: Context) {
        println!("producer ON!");
        loop {
            self = match self {
                ProducerState::S0 { out0, count } => {
                    let manager_event = ctx.marker_manager_recv.as_ref().unwrap().pull().await;
                    match manager_event {
                        Event::Data(_) => {
                            panic!();
                        }
                        Event::Marker(marker_id) => {
                            let snapshot_state = ProducerState::S0 {
                                out0: out0.clone(),
                                count,
                            };
                            println!("start producer snapshotting");
                            let persistent_state = Task::Producer(snapshot_state.clone()).to_partial_persistent_task().await;

                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;
                            out0.push(Event::Marker(marker_id)).await;

                            snapshot_state
                        }
                        Event::MessageAmount((message_type, amount)) => {
                            let mut loc_count = count; 
                            if(message_type == "taxi_customer".to_string()) {
                                send_message(&out0, amount, &mut loc_count, 10).await;
                            }
                            else if (message_type == "taxi_driver".to_string()) {
                                send_message(&out0, amount, &mut loc_count, 10).await;
                            }
                            else if (message_type == "bus".to_string()) {
                                println!("sending bus");
                                send_message(&out0, amount, &mut loc_count, 60).await;
                            }
                            else if (message_type == "end_of_stream") {
                                out0.push(Event::Data((0, 0))).await;
                                println!("PRODUCER DONE--------------------");
                            }
                            
                            ProducerState::S0 {
                                out0,
                                count: loc_count,
                            }
                        }
                    }
                }
            }
        }
    }
}

pub async fn send_message(out: &PushChan<Event<(u64, u64)>>, amount: i32, count: &mut i32, sleep_amount: u64){ 
    let mut loc_count = *count;
    let mut data_counter:u64 = 1;
    for _ in 0..amount {
        let data = Event::Data((data_counter, data_counter + 1));
        task::sleep(Duration::from_millis(sleep_amount)).await;
        out.push(data).await;
        loc_count += 1; 
        if (data_counter == 130){
            data_counter = 1;
        }
        else {
            data_counter += 1;
        }
    }
    *count = loc_count; 
}