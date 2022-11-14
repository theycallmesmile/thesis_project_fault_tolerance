use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

use core::panic;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time;
use tokio::time::Instant;

use async_std::task;

use tokio::sync::oneshot;

use std::sync::Arc;

use futures::join;

//Manager module
use crate::manager::Context;
use crate::manager::Task;
use crate::manager::PersistentTaskToManagerMessage;

//Channel module
use crate::channel::PullChan;
use crate::channel::PushChan;

//Shared module
use crate::shared::Event;
use crate::shared::Shared;
use crate::shared::SharedState;

#[derive(Debug, Clone)]
pub enum ConsumerProducerState {
    S0 {
        stream0: PullChan<Event<i32>>,
        stream1: PullChan<Event<i32>>,
        stream2: PullChan<Event<i32>>,
        out0: PushChan<Event<i32>>,
        out1: PushChan<Event<i32>>,
        count: i32,
    },
    S1 {
        stream0: PullChan<Event<i32>>,
        stream1: PullChan<Event<i32>>,
        stream2: PullChan<Event<i32>>,
        out0: PushChan<Event<i32>>,
        out1: PushChan<Event<i32>>,
        count: i32,
        data: i32,
    },
    S2 {
        stream0: PullChan<Event<i32>>,
        stream1: PullChan<Event<i32>>,
        stream2: PullChan<Event<i32>>,
        out0: PushChan<Event<i32>>,
        out1: PushChan<Event<i32>>,
        count: i32,
    },
}

#[derive(Debug, Clone)]
pub enum PartialConsumerProducerState {
    S0 {
        stream0: PullChan<Event<i32>>,
        stream1: PullChan<Event<i32>>,
        out0: PushChan<Event<i32>>,
    },
    S1 {
        stream0: PullChan<Event<i32>>,
        stream1: PullChan<Event<i32>>,
        out0: PushChan<Event<i32>>,
        data: i32,
    },
    S2 {
        stream2: PullChan<Event<i32>>,
        out1: PushChan<Event<i32>>,
    },
}


impl ConsumerProducerState {
     //deadlock can occur on restricted versions
    pub async fn execute_optimized_restricted(mut self, ctx: Context) {
        println!("ConsumerProducer optimized ON!");
        loop {
            //println!("SELF: {:?}", self);
            self = match self {
                ConsumerProducerState::S0 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                } => {
                    let in_event0 = stream0.pull_buff_log().await;
                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            ConsumerProducerState::S1 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                                data: event_data_s0,
                            }
                        }
                        Event::Marker(marker_id) => {
                            let loc_stream1 = drain_buffers(&stream1).await;

                            let partial_snapshot_state = PartialConsumerProducerState::S0 {
                                stream0: stream0.clone(), 
                                stream1: loc_stream1.clone(), 
                                out0: out0.clone(),
                            };
                            let persistent_state = Task::PartialConsumerProducer(partial_snapshot_state).to_partial_persistent_task().await;

                            println!("start ConsumerProducer Stream0 Partial snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;
                            out0.push(Event::Marker(marker_id)).await;

                            ConsumerProducerState::S2 {
                                stream0,
                                stream1: loc_stream1, 
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        },
                    }
                }
                ConsumerProducerState::S1 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                    data,
                } => {
                    let in_event1 = stream1.pull_buff_log().await;

                    match in_event1 {
                        Event::Data(event_data_s1) => {
                            if(event_data_s1 == 0){
                                out0.push(Event::Data(0)).await;
                                println!("CONSUMER_PRODUCER STREAM0&1 DONE!");
                            }
                            else {
                                out0.push(Event::Data(data.clone() + event_data_s1)).await;
                            }
                            ConsumerProducerState::S2 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::Marker(marker_id) => {
                            println!("------------------------------------------------------------------------------------------------------------------");
                            panic!();
                            let loc_stream0 = drain_buffers(&stream0).await;

                            let partial_snapshot_state = PartialConsumerProducerState::S1 {
                                stream0: loc_stream0.clone(), 
                                stream1: stream1.clone(), 
                                out0: out0.clone(),
                                data,
                            };
                            let persistent_state = Task::PartialConsumerProducer(partial_snapshot_state).to_partial_persistent_task().await;
                            println!("start ConsumerProducer Stream 1 Partial snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;
                            out0.push(Event::Marker(marker_id)).await;
                            

                            ConsumerProducerState::S1 {
                                stream0: loc_stream0,
                                stream1, 
                                stream2,
                                out0,
                                out1,
                                count,
                                data,
                            }
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        },
                    }
                }
                ConsumerProducerState::S2 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                } => {
                    let in_event2 = stream2.pull_buff_log().await;
                    match in_event2 {
                        Event::Data(event_data_s2) => {
                            if(event_data_s2 == 0){
                                out1.push(Event::Data(0)).await;
                                println!("CONSUMER_PRODUCER STREAM2 DONE!");
                            }
                            else {
                                out1.push(Event::Data(event_data_s2)).await;
                            }
                            ConsumerProducerState::S0 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::Marker(marker_id) => {
                            let partial_snapshot_state = PartialConsumerProducerState::S2 {
                                stream2: stream2.clone(),
                                out1: out1.clone(),
                            };
                            let persistent_state = Task::PartialConsumerProducer(partial_snapshot_state).to_partial_persistent_task().await;
                            println!("start ConsumerProducer Partial Stream 2 snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;
                            out1.push(Event::Marker(marker_id)).await;

                            ConsumerProducerState::S0 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }

                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        },
                    }
                }
            };
        }
    }

    pub async fn execute_unoptimized_unrestricted(mut self, ctx: Context) {
        println!("ConsumerProducer Unoptimized unrestricted ON!");
        loop {
            self = match self {
                ConsumerProducerState::S0 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                } => {
                    let in_event0 = stream0.pull_buff_log().await;
                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            ConsumerProducerState::S1 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                                data: event_data_s0,
                            }
                        }
                        Event::Marker(marker_id) => {
                            let loc_stream1 = drain_buffers(&stream1).await;
                            let loc_stream2 = drain_buffers(&stream2).await;

                            let snapshot_state = ConsumerProducerState::S0 {
                                stream0,
                                stream1: loc_stream1, 
                                stream2: loc_stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                            };

                            let persistent_state = Task::ConsumerProducer(snapshot_state.clone()).to_partial_persistent_task().await;
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;
                            
                            join!(out0.push(Event::Marker(marker_id)), out1.push(Event::Marker(marker_id)));

                            snapshot_state
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        }
                    }
                }
                ConsumerProducerState::S1 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                    data,
                } => {
                    let in_event1 = stream1.pull_buff_log().await;
                    match in_event1 {
                        Event::Data(event_data_s1) => {
                            if(event_data_s1 == 0){
                                println!("CONSUMER_PRODUCER STREAM0&1 DONE!");
                                
                                let next_state =join!(race_next_state(stream0, stream1, stream2, out0.clone(), out1, count), out0.push(Event::Data(0)));
                                next_state.0
                            }
                            else {
                                let next_state =join!(race_next_state(stream0, stream1, stream2, out0.clone(), out1, count), out0.push(Event::Data(data.clone() + event_data_s1)));
                                next_state.0
                            } 
                        }
                        Event::Marker(marker_id) => {
                            let loc_stream0 = drain_buffers(&stream0).await;
                            let loc_stream2 = drain_buffers(&stream2).await;

                            let snapshot_state = ConsumerProducerState::S1 {
                                stream0: loc_stream0,
                                stream1,
                                stream2: loc_stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                                data,
                            };
                            let persistent_state = Task::ConsumerProducer(snapshot_state.clone()).to_partial_persistent_task().await;
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;

                            join!(out0.push(Event::Marker(marker_id)), out1.push(Event::Marker(marker_id)));

                            snapshot_state
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        }
                    }
                }
                ConsumerProducerState::S2 { 
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                } => {
                    let in_event2 = stream2.pull_buff_log().await;
                    match in_event2 {
                        Event::Data(event_data_s2) => {
                            if(event_data_s2 == 0){
                                println!("CONSUMER_PRODUCER STREAM2 DONE!");
                                
                                let next_state =join!(race_next_state(stream0, stream1, stream2, out0, out1.clone(), count), out1.push(Event::Data(0)));
                                next_state.0
                            }
                            else {
                                let next_state =join!(race_next_state(stream0, stream1, stream2, out0, out1.clone(), count), out1.push(Event::Data(event_data_s2)));
                                next_state.0
                            } 
                            
                        }
                        Event::Marker(marker_id) => {
                            
                            let loc_stream0 = drain_buffers(&stream0).await;
                            let loc_stream1 = drain_buffers(&stream1).await;

                            let snapshot_state = ConsumerProducerState::S2 {
                                stream0: loc_stream0, 
                                stream1: loc_stream1,
                                stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                            };
                            
                            let persistent_state = Task::ConsumerProducer(snapshot_state.clone()).to_partial_persistent_task().await;
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;

  
                            join!(out0.push(Event::Marker(marker_id)), out1.push(Event::Marker(marker_id)));

                            snapshot_state
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        }
                    }
                }
            };
        }
    }

    pub async fn execute_unoptimized_restricted(mut self, ctx: Context) {
        println!("ConsumerProducer modified Unoptimized ON!");
        loop {
            self = match self {
                ConsumerProducerState::S0 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                } => {
                    let in_event0 = stream0.pull_buff_log().await;
                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            ConsumerProducerState::S1 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                                data: event_data_s0,
                            }
                        }
                        Event::Marker(marker_id) => {
                            let loc_stream1 = drain_buffers(&stream1).await;
                            let loc_stream2 = drain_buffers(&stream2).await;

                            let snapshot_state = ConsumerProducerState::S0 {
                                stream0,
                                stream1: loc_stream1, 
                                stream2: loc_stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                            };

                            let persistent_state = Task::ConsumerProducer(snapshot_state.clone()).to_partial_persistent_task().await;
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;
                            
                            join!(out0.push(Event::Marker(marker_id)), out1.push(Event::Marker(marker_id)));

                            snapshot_state
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        }
                    }
                }
                ConsumerProducerState::S1 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                    data,
                } => {
                    let in_event1 = stream1.pull_buff_log().await;
                    match in_event1 {
                        Event::Data(event_data_s1) => {
                            if(event_data_s1 == 0){
                                out0.push(Event::Data(0)).await;
                                println!("CONSUMER_PRODUCER STREAM0&1 DONE!");
                            }
                            else {
                                out0.push(Event::Data(data + event_data_s1)).await;
                            }
                            ConsumerProducerState::S2 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::Marker(marker_id) => {
                            let loc_stream0 = drain_buffers(&stream0).await;
                            let loc_stream2 = drain_buffers(&stream2).await;

                            let snapshot_state = ConsumerProducerState::S1 {
                                stream0: loc_stream0,
                                stream1,
                                stream2: loc_stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                                data,
                            };
                            let persistent_state = Task::ConsumerProducer(snapshot_state.clone()).to_partial_persistent_task().await;
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;

                            join!(out0.push(Event::Marker(marker_id)), out1.push(Event::Marker(marker_id)));

                            snapshot_state
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        }
                    }
                }
                ConsumerProducerState::S2 { 
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                } => {
                    let in_event2 = stream2.pull_buff_log().await;
                    match in_event2 {
                        Event::Data(event_data_s2) => {
                            if(event_data_s2 == 0){
                                out1.push(Event::Data(0)).await;
                                println!("CONSUMER_PRODUCER STREAM2 DONE!");
                            }
                            else {
                                out1.push(Event::Data(event_data_s2)).await;
                            }
                            ConsumerProducerState::S0 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::Marker(marker_id) => {
                            
                            let loc_stream0 = drain_buffers(&stream0).await;
                            let loc_stream1 = drain_buffers(&stream1).await;

                            let snapshot_state = ConsumerProducerState::S2 {
                                stream0: loc_stream0, 
                                stream1: loc_stream1,
                                stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                            };
                            
                            let persistent_state = Task::ConsumerProducer(snapshot_state.clone()).to_partial_persistent_task().await;
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;

  
                            join!(out0.push(Event::Marker(marker_id)), out1.push(Event::Marker(marker_id)));

                            snapshot_state
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        }
                    }
                }
            };
        }
    }

    pub async fn execute_optimized_unrestricted(mut self, ctx: Context) {
        let mut timer_now = Instant::now();
        println!("ConsumerProducer MODIFIED optimized ON!");
        loop {
            //println!("self: {:?}", &self);
            self = match self {
                ConsumerProducerState::S0 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                } => {
                    let in_event0 = stream0.pull_buff_log().await;
                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            ConsumerProducerState::S1 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                                data: event_data_s0,
                            }
                        }
                        Event::Marker(marker_id) => {
                            let loc_stream1 = drain_buffers(&stream1).await;

                            let partial_snapshot_state = PartialConsumerProducerState::S0 {
                                stream0: stream0.clone(), 
                                stream1: loc_stream1.clone(), 
                                out0: out0.clone(),
                            };
                            let persistent_state = Task::PartialConsumerProducer(partial_snapshot_state).to_partial_persistent_task().await;

                            println!("start ConsumerProducer Stream0 Partial snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;
                            out0.push(Event::Marker(marker_id)).await;
                            //println!("DONE WITH SNAPSHOTTING TESTING!");
                            //Race for next state!
                            //timer_now = Instant::now();
                            let stream_selection = tokio::select! {
                                _ = stream0.check_pull_length() => {
                                    0
                                },
                                _ = stream2.check_pull_length() => {
                                    2
                                },
                            };
                            //println!("WINNER AFTER MARKER: {}", timer_now.elapsed().as_millis());
                            if (stream_selection == 0){
                                ConsumerProducerState::S0 {
                                    stream0,
                                    stream1: loc_stream1, 
                                    stream2,
                                    out0,
                                    out1,
                                    count,
                                }
                            }
                            else {
                                ConsumerProducerState::S2 {
                                    stream0,
                                    stream1: loc_stream1, 
                                    stream2,
                                    out0,
                                    out1,
                                    count,
                                }
                            }                           
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        },
                    }
                }
                ConsumerProducerState::S1 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                    data,
                } => {
                    let in_event1 = stream1.pull_buff_log().await;

                    match in_event1 {
                        Event::Data(event_data_s1) => {                          
                            if(event_data_s1 == 0){
                                println!("CONSUMER_PRODUCER STREAM2 DONE!");
                                
                                let next_state =join!(race_next_state(stream0, stream1, stream2, out0.clone(), out1, count), out0.push(Event::Data(0)));
                                next_state.0
                            }
                            else {
                                let next_state =join!(race_next_state(stream0, stream1, stream2, out0.clone(), out1, count), out0.push(Event::Data(data.clone() + event_data_s1)));
                                next_state.0
                            }     
                        }
                        Event::Marker(marker_id) => {

                            let loc_stream0 = drain_buffers(&stream0).await;

                            let partial_snapshot_state = PartialConsumerProducerState::S1 {
                                stream0: loc_stream0.clone(), 
                                stream1: stream1.clone(), 
                                out0: out0.clone(),
                                data,
                            };
                            let persistent_state = Task::PartialConsumerProducer(partial_snapshot_state).to_partial_persistent_task().await;
                            println!("start ConsumerProducer Stream 1 Partial snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;
                            out0.push(Event::Marker(marker_id)).await;
                            

                            //Race for next state!
                            let stream_selection = tokio::select! {
                                _ = stream0.check_pull_length() => {
                                    0
                                },
                                _ = stream2.check_pull_length() => {
                                    2
                                },
                            };
                            if (stream_selection == 0){
                                ConsumerProducerState::S0 {
                                    stream0: loc_stream0,
                                    stream1, 
                                    stream2,
                                    out0,
                                    out1,
                                    count,
                                }
                            }
                            else {
                                ConsumerProducerState::S2 {
                                    stream0: loc_stream0,
                                    stream1,
                                    stream2,
                                    out0,
                                    out1,
                                    count,
                                }
                            }
                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        },
                    }
                }
                ConsumerProducerState::S2 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                } => {
                    let in_event2 = stream2.pull_buff_log().await;
                    
                    match in_event2 {
                        Event::Data(event_data_s2) => {
                            if(event_data_s2 == 0){
                                println!("CONSUMER_PRODUCER STREAM2 DONE!");
                                
                                let next_state =join!(race_next_state(stream0, stream1, stream2, out0, out1.clone(), count), out1.push(Event::Data(0)));
                                next_state.0
                            }
                            else {
                                let next_state =join!(race_next_state(stream0, stream1, stream2, out0, out1.clone(), count), out1.push(Event::Data(event_data_s2)));
                                next_state.0
                            }
                        }
                        Event::Marker(marker_id) => {
                            let partial_snapshot_state = PartialConsumerProducerState::S2 {
                                stream2: stream2.clone(),
                                out1: out1.clone(),
                            };
                            let persistent_state = Task::PartialConsumerProducer(partial_snapshot_state).to_partial_persistent_task().await;
                            println!("start ConsumerProducer Partial Stream 2 snapshotting");
                            Shared::<()>::persistent_store(persistent_state, marker_id, &ctx).await;
                            out1.push(Event::Marker(marker_id)).await;

                            //Race for next state!
                            let stream_selection = tokio::select! {
                                _ = stream0.check_pull_length() => {
                                    0
                                },
                                _ = stream2.check_pull_length() => {
                                    2
                                },
                            };
                            //println!("WINNER: {}", stream_selection);
                            if (stream_selection == 0){
                                ConsumerProducerState::S0 {
                                    stream0,
                                    stream1, 
                                    stream2,
                                    out0,
                                    out1,
                                    count,
                                }
                            }
                            else {
                                ConsumerProducerState::S2 {
                                    stream0,
                                    stream1,
                                    stream2,
                                    out0,
                                    out1,
                                    count,
                                }
                            }

                        }
                        Event::MessageAmount(_) => {
                            panic!();
                        },
                    }
                }
            };
        }
    }
}

pub async fn race_next_state(stream0: PullChan<Event<i32>>, stream1: PullChan<Event<i32>>, stream2: PullChan<Event<i32>>, out0: PushChan<Event<i32>>, out1: PushChan<Event<i32>>, count: i32 ) -> ConsumerProducerState{
    tokio::select! {
        _ = stream0.check_pull_length() => {
            ConsumerProducerState::S0 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
            }
        },
        _ = stream2.check_pull_length() => {
            ConsumerProducerState::S2 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
            }
        },
    }
    //println!("WINNER AFTER MARKER: {}", timer_now.elapsed().as_millis());
}

pub async fn drain_buffers(stream: &PullChan<Event<i32>>) -> PullChan<Event<i32>> {
    loop {
        let in_event = stream.pull().await;
        match in_event {
            Event::Data(event_data) => {
                stream.push_log(Event::Data(event_data)).await;
            }
            Event::Marker(marker_id) => {
                break;
            }
            Event::MessageAmount(_) => {
                panic!()},
        }
    }
    stream.to_owned()
}
