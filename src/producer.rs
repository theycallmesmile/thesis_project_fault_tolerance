use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

use std::time::Duration;
use tokio::time;

use tokio::sync::oneshot;

use std::sync::Arc;

//Manager module
use crate::manager::{Context, self};
use crate::manager::Task;
use crate::manager::TaskToManagerMessage;

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
                    println!("manager_event: {:?}", manager_event);
                    match manager_event {
                        Event::Data(_) => {panic!()}
                        Event::Marker => {
                            //snapshoting
                            let snapshot_state = ProducerState::S0 {
                                out0: out0.clone(),
                                count,
                            };

                            println!("start producer snapshotting");
                            Shared::<()>::store(SharedState::Producer(snapshot_state), &ctx).await;
                            println!("Done with producer snapshotting");
                            //forward the marker to consumers
                            println!("Producer - SENDING MARKER!");
                            out0.push(Event::Marker).await;

                            ProducerState::S0 {
                                out0,
                                count,
                            }
                        }
                        Event::MessageAmount(amount) => {
                            println!("Received request of amount: {}", amount);
                            let mut loc_count = count; 
                            for x in 0..amount {
                                out0.push(Event::Data(2)).await;
                                loc_count += 1;
                            }
                            ProducerState::S0 {
                                out0,
                                count,
                            }
                        }
                    }
                }
            }
        }
    }

    
}
