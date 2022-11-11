use tokio::sync::oneshot;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use async_std::task;

use serde::Deserialize;
use serde::Serialize;

//Consumer-producer module


//Serialization module
use crate::serialization::PersistentTask;

//Manager module
use crate::manager::Context;
use crate::manager::Task;
use crate::manager::PersistentTaskToManagerMessage;

//Channel module
use crate::channel::channel;
use crate::channel::PullChan;
use crate::channel::PushChan;

//Shared module
use crate::shared::Event;
use crate::shared::Shared;
use crate::shared::SharedState;

#[derive(Debug, Clone)]
pub enum ConsumerState {
    S0 {
        stream0: PullChan<Event<i32>>,
        count: i32,
    },
}

impl ConsumerState {
    pub async fn execute_unoptimized(mut self, ctx: Context) {
        
        let mut benchmark_token_count = 0;
        println!("consumer ON!");
        loop {
            self = match self {
                ConsumerState::S0 { stream0, count } => {
                    let in_event0 = stream0.pull().await;

                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            if(event_data_s0 == 0){
                                println!("CONSUMER DONE!");
                                Shared::<()>::end_message(&ctx).await;
                            }
                            let loc_count = count + 1;
                            ConsumerState::S0 { stream0, count: loc_count }
                        }
                        Event::Marker(marker_id) => {
                            let snapshot_state = ConsumerState::S0 {
                                stream0, 
                                count,
                            };

                            println!("start Consumer snapshotting");
                            let persistent_state = Task::Consumer(snapshot_state.clone()).to_partial_persistent_task().await;
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;

                            snapshot_state
                        }
                        Event::MessageAmount(b_count) => {
                            panic!();                            
                        }
                    }
                }
            };
        }
    }
}
