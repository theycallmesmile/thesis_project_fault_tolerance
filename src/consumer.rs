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
        stream0: PullChan<Event<(String, String)>>,
        count: i32,
    },
}

impl ConsumerState {
    pub async fn execute_unoptimized(mut self, ctx: Context) {
        let mut benchmark_token_count = 0;
        println!("consumer {} ON!", ctx.operator_id);
        loop {
            self = match self {
                ConsumerState::S0 { stream0, count } => {
                    let in_event0 = stream0.pull().await;

                    match in_event0 {
                        Event::Data((event_data_0, event_data_1)) => {
                            if(event_data_0 == "end_of_stream".to_string()){
                                println!("CONSUMER {} DONE!", ctx.operator_id);
                                Shared::<()>::end_message(&ctx).await;
                            }
                            else {
                                if (ctx.operator_id == 0) { //taxi
                                    println!("The location of the taxi: {:?} and the location of the customer: {:?}.", event_data_0, event_data_1);
                                }
                                else {
                                    if(ctx.operator_id == 1) { //bus
                                        println!("The bus departure location: {:?} and the bus destination location {:?}.", event_data_0, event_data_1);
                                    }
                                }
                            }
                            task::sleep(Duration::from_millis(20)).await;
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
