use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time;

use async_std::task;

use tokio::sync::oneshot;

use std::sync::Arc;

use futures::join;

//Manager module
use crate::manager::Context;
use crate::manager::Task;
use crate::manager::TaskToManagerMessage;

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

impl ConsumerProducerState {
    pub async fn execute_unoptimized(mut self, ctx: Context) {
        println!("ConsumerProducer Unoptimized ON!");
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
                        Event::Marker => {
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
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;

                            join!(out0.push(Event::Marker), out1.push(Event::Marker));

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
                            out0.push(Event::Data(data + event_data_s1)).await;
                            ConsumerProducerState::S2 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::Marker => {
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
                            
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;

                            join!(out0.push(Event::Marker), out1.push(Event::Marker));

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
                            out1.push(Event::Data(event_data_s2)).await;
                            ConsumerProducerState::S0 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::Marker => {
                            
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
                            
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;
  
                            join!(out0.push(Event::Marker), out1.push(Event::Marker));

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

    pub async fn execute_optimized(mut self, ctx: Context) {
        println!("ConsumerProducer optimized ON!");
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
                        Event::Marker => {
                            let stream1_future = drain_buffers(&stream1);
                            let stream2_future = process_until_marker(&stream2, &out1, &count);

                            let (loc_stream1, (loc_stream2, loc_count)) = join!(stream1_future, stream2_future);

                            let snapshot_state = ConsumerProducerState::S0 {
                                stream0, 
                                stream1: loc_stream1, 
                                stream2: loc_stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: loc_count,
                            };
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;

                            join!(out0.push(Event::Marker), out1.push(Event::Marker));

                            snapshot_state
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
                            out0.push(Event::Data(data.clone() + event_data_s1)).await;
                            ConsumerProducerState::S2 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::Marker => {
                            let stream0_future = drain_buffers(&stream0);
                            let stream2_future = process_until_marker(&stream2, &out1, &count);

                            //join allows both of the functions to run concurrently and awaits until both are done.
                            let (loc_stream0, (loc_stream2, loc_count)) = join!(stream0_future, stream2_future);
                            
                            let snapshot_state = ConsumerProducerState::S1 {
                                stream0: loc_stream0,
                                stream1, 
                                stream2: loc_stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: loc_count,
                                data,
                            };
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;

                            join!(out0.push(Event::Marker), out1.push(Event::Marker));

                            snapshot_state
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
                            out1.push(Event::Data(event_data_s2)).await;
                            ConsumerProducerState::S0 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::Marker => {
                            let snapshot_state = process_until_marker_multiple(&stream0, &stream1, &stream2, &out0, &out1, &count).await;

                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;

                            join!(out0.push(Event::Marker), out1.push(Event::Marker));
                            snapshot_state
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

pub async fn drain_buffers(stream: &PullChan<Event<i32>>) -> PullChan<Event<i32>> {
    loop {
        let in_event = stream.pull().await;
        match in_event {
            Event::Data(event_data) => {
                stream.push_log(Event::Data(event_data)).await;
            }
            Event::Marker => {
                break;
            }
            Event::MessageAmount(_) => {
                panic!()},
        }
    }
    stream.to_owned()
}

pub async fn process_until_marker(stream: &PullChan<Event<i32>>, out: &PushChan<Event<i32>>, count: &i32) -> (PullChan<Event<i32>>, i32){
    let mut loc_count = *count; 
    loop {
        let in_event = stream.pull_buff_log().await;

        match in_event {
            Event::Data(event_data) => {
                out.push(Event::Data(event_data)).await;
                loc_count += 1;
            }
            Event::Marker => {
                break;
            },
            Event::MessageAmount(_) => panic!(),
        }
    }
    (stream.to_owned(), loc_count)
}


pub async fn process_until_marker_multiple(stream0: &PullChan<Event<i32>>, stream1: &PullChan<Event<i32>>, stream2: &PullChan<Event<i32>>, out0: &PushChan<Event<i32>>, out1: &PushChan<Event<i32>>, count: &i32) -> ConsumerProducerState{
    let mut snapshot_state: ConsumerProducerState;
    let mut loc_count = count.clone();
    loop{
        let in_event0 = stream0.pull_buff_log().await;

        match in_event0 {
            Event::Data(event_data_0) => {
                loc_count += 1;

                let in_event1 = stream1.pull_buff_log().await;

                match in_event1 {
                    Event::Data(event_data_1) => {
                        out0.push(Event::Data(event_data_0.clone() + event_data_1)).await;
                        loc_count += 1;
                    }
                    Event::Marker => {
                        let loc_stream0 = drain_buffers(&stream0).await;
                        
                        snapshot_state = ConsumerProducerState::S2 {
                            stream0: loc_stream0,
                            stream1: stream1.to_owned(),
                            stream2: stream2.to_owned(),
                            out0: out0.clone(),
                            out1: out1.clone(),
                            count: count.clone(),
                        };
                        break;
                    },
                    Event::MessageAmount(_) => panic!(),
                }
            }
            Event::Marker => { 
                let loc_stream1 = drain_buffers(&stream1).await;

                snapshot_state = ConsumerProducerState::S2 {
                    stream0: stream0.to_owned(), 
                    stream1: loc_stream1,
                    stream2: stream2.to_owned(),
                    out0: out0.clone(),
                    out1: out1.clone(),
                    count: count.clone(),
                };
                break;
            },
            Event::MessageAmount(_) => panic!(),
        }                            
    }
    snapshot_state
}