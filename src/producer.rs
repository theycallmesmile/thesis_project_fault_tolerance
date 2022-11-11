use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

use std::collections::HashSet;

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
        out0: PushChan<Event<i32>>,
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
                        Event::MessageAmount(amount) => {
                            let mut loc_count = count; 
                            let mut delay_time = 0;
                            if (amount == 0) {
                                out0.push(Event::Data(0)).await;
                                println!("PRODUCER DONE--------------------");
                            }
                            else {
                                if(amount == 200){
                                    delay_time = 10;
                                }
                                else{
                                    delay_time = 20;
                                }
                                for x in 0..amount {
                                    //println!("sending data");
                                    task::sleep(Duration::from_millis(delay_time)).await;
                                    out0.push(Event::Data(2)).await;
                                    loc_count += 1;
                                    
                                }
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
