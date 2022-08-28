use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time;

use tokio::sync::oneshot;

use std::sync::Arc;

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
        before_marker_queue: Vec<VecDeque<Event<i32>>>,
    },
    S1 {
        stream0: PullChan<Event<i32>>,
        stream1: PullChan<Event<i32>>,
        stream2: PullChan<Event<i32>>,
        out0: PushChan<Event<i32>>,
        out1: PushChan<Event<i32>>,
        count: i32,
        data: i32,
        before_marker_queue: Vec<VecDeque<Event<i32>>>,
    },
    S2 {
        stream0: PullChan<Event<i32>>,
        stream1: PullChan<Event<i32>>,
        stream2: PullChan<Event<i32>>,
        out0: PushChan<Event<i32>>,
        out1: PushChan<Event<i32>>,
        count: i32,
        before_marker_queue: Vec<VecDeque<Event<i32>>>,
    },
}

impl ConsumerProducerState {
    pub async fn execute_unoptimized(mut self, ctx: Context) {
        println!("ConsumerProducer Unoptimized ON!");

        let mut return_state = match &self {
            ConsumerProducerState::S0 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
                before_marker_queue,
            } => ConsumerProducerState::S0 {
                stream0: stream0.to_owned(),
                stream1: stream1.to_owned(),
                stream2: stream2.to_owned(),
                out0: out0.to_owned(),
                out1: out1.to_owned(),
                count: count.to_owned(),
                before_marker_queue: before_marker_queue.to_owned(),
            },
            ConsumerProducerState::S1 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
                data,
                before_marker_queue,
            } => ConsumerProducerState::S1 {
                stream0: stream0.to_owned(),
                stream1: stream1.to_owned(),
                stream2: stream2.to_owned(),
                out0: out0.to_owned(),
                out1: out1.to_owned(),
                count: count.to_owned(),
                data: count.to_owned(),
                before_marker_queue: before_marker_queue.to_owned(),
            },
            ConsumerProducerState::S2 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
                before_marker_queue,
            } => ConsumerProducerState::S2 {
                stream0: stream0.to_owned(),
                stream1: stream1.to_owned(),
                stream2: stream2.to_owned(),
                out0: out0.to_owned(),
                out1: out1.to_owned(),
                count: count.to_owned(),
                before_marker_queue: before_marker_queue.to_owned(),
            },
        };

        loop {
            self = match &self {
                ConsumerProducerState::S0 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                    before_marker_queue,
                } => {
                    //state -> s0
                    let mut loc_before_marker_queue = before_marker_queue.clone();
                    let in_event0 = if !loc_before_marker_queue[0].len().eq(&0){
                        loc_before_marker_queue[0].pop_back().unwrap()
                    } else {
                        stream0.pull().await 
                    };
                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            //state -> s1
                            ConsumerProducerState::S1 {
                                stream0: stream0.to_owned(),
                                stream1: stream1.to_owned(),
                                stream2: stream2.to_owned(),
                                out0: out0.to_owned(),
                                out1: out1.to_owned(),
                                count: count.clone(),
                                data: event_data_s0,
                                before_marker_queue: loc_before_marker_queue,
                            }
                        }
                        Event::Marker => {
                            //Only the pull channel buffer will be saved, the push will be reset and relinked by the connected operator down the dataflow graph
                            //Draining from the remaining input streams until a marker is received.
                            //The drained messages are stored for snapshotting.

                            let loc_stream1 = drain_buffers(stream1, &mut loc_before_marker_queue).await;
                            let loc_stream2 = drain_buffers(stream2, &mut loc_before_marker_queue).await;
                            //snapshot state:
                            let snapshot_state = ConsumerProducerState::S0 {
                                stream0: stream0.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                stream1: loc_stream1
                                    .clone()
                                    .replace_buffer(&mut loc_before_marker_queue[1])
                                    .await, //is it necessary to clean if before_marker_queue exsists ? Should the messages sent after marker be saved?
                                stream2: loc_stream2
                                    .clone()
                                    .replace_buffer(&mut loc_before_marker_queue[2])
                                    .await,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: count.to_owned(),
                                before_marker_queue: loc_before_marker_queue,
                            };
                            println!("start producer snapshotting");
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;
                            println!("done with producer snapshotting");

                            //forward the marker to consumers
                            println!("SENDING MARKER!");
                            out0.push(Event::Marker).await;
                            out1.push(Event::Marker).await;

                            //s0
                            ConsumerProducerState::S0 { //what happens if streams receive new messages leading loc_stream being old and deprecated?
                                stream0: stream0.to_owned(),
                                stream1: stream1.to_owned(),
                                stream2: stream2.to_owned(),
                                out0: out0.to_owned(),
                                out1: out1.to_owned(),
                                count: count.to_owned(),
                                before_marker_queue: before_marker_queue.clone(),
                            }
                        }
                        Event::MessageAmount(_) => panic!(),
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
                    before_marker_queue,
                } => {
                    //state -> s1
                    let mut loc_before_marker_queue = before_marker_queue.clone();
                    let in_event1 = if !loc_before_marker_queue[1].len().eq(&0){
                        loc_before_marker_queue[1].pop_back().unwrap()
                    } else {
                        stream1.pull().await 
                    };

                    match in_event1 {
                        Event::Data(event_data_s1) => {
                            out0.push(Event::Data(data.clone() + event_data_s1)).await;
                            ConsumerProducerState::S2 {
                                stream0: stream0.clone(),
                                stream1: stream1.clone(),
                                stream2: stream2.clone(),
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: count.clone(),
                                before_marker_queue: before_marker_queue.to_owned(),
                            }
                        }
                        Event::Marker => {
                            //Only the pull channel buffer will be saved, the push will be reset and relinked by the connected operator down the dataflow graph
                            //Draining from the remaining input streams until a marker is received.
                            //The drained messages are stored for snapshotting.

                            let loc_stream0 = drain_buffers(stream0, &mut loc_before_marker_queue).await;
                            let loc_stream2 = drain_buffers(stream2, &mut loc_before_marker_queue).await;
                            //snapshot state:
                            let snapshot_state = ConsumerProducerState::S0 {
                                stream0: loc_stream0
                                .clone()
                                .replace_buffer(&mut loc_before_marker_queue[0])
                                .await, //is it necessary to clean if before_marker_queue exsists ? Should the messages sent after marker be saved?
                                stream1: stream1.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                stream2: loc_stream2
                                    .clone()
                                    .replace_buffer(&mut loc_before_marker_queue[2])
                                    .await,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: count.to_owned(),
                                before_marker_queue: loc_before_marker_queue,
                            };
                            println!("start producer snapshotting");
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;
                            println!("done with producer snapshotting");

                            //forward the marker to consumers
                            println!("SENDING MARKER!");
                            out0.push(Event::Marker).await;
                            out1.push(Event::Marker).await;

                             //s1
                            ConsumerProducerState::S1 {
                                stream0: stream0.to_owned(),
                                stream1: stream1.to_owned(),
                                stream2: stream2.to_owned(),
                                out0: out0.to_owned(),
                                out1: out1.to_owned(),
                                count: count.to_owned(),
                                data: data.clone(),
                                before_marker_queue: before_marker_queue.clone(),
                            }
                        }
                        Event::MessageAmount(_) => panic!(),
                    }
                }
                ConsumerProducerState::S2 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                    before_marker_queue,
                } => {
                    //state -> s2
                    let mut loc_before_marker_queue = before_marker_queue.clone();
                    let in_event2 = if !loc_before_marker_queue[2].len().eq(&0){
                        loc_before_marker_queue[2].pop_back().unwrap()
                    } else {
                        stream2.pull().await 
                    };
                    match in_event2 {
                        Event::Data(event_data_s2) => {
                            out1.push(Event::Data(event_data_s2)).await;
                            ConsumerProducerState::S0 {
                                stream0: stream0.clone(),
                                stream1: stream1.clone(),
                                stream2: stream2.clone(),
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: count.clone(),
                                before_marker_queue: before_marker_queue.to_owned(),
                            }
                        }
                        Event::Marker => {
                            //Only the pull channel buffer will be saved, the push will be reset and relinked by the connected operator down the dataflow graph
                            //Draining from the remaining input streams until a marker is received.
                            //The drained messages are stored for snapshotting.

                            let loc_stream0 = drain_buffers(stream0, &mut loc_before_marker_queue).await;
                            let loc_stream1 = drain_buffers(stream1, &mut loc_before_marker_queue).await;
                            //snapshot state:
                            let snapshot_state = ConsumerProducerState::S0 {
                                stream0: loc_stream0
                                .clone()
                                .replace_buffer(&mut loc_before_marker_queue[0])
                                .await, //is it necessary to clean if before_marker_queue exsists ? Should the messages sent after marker be saved?
                                stream1: loc_stream1
                                .clone()
                                .replace_buffer(&mut loc_before_marker_queue[1])
                                .await,
                                stream2: stream2.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: count.to_owned(),
                                before_marker_queue: loc_before_marker_queue,
                            };
                            println!("start producer snapshotting");
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;
                            println!("done with producer snapshotting");

                            //forward the marker to consumers
                            println!("SENDING MARKER!");
                            out0.push(Event::Marker).await;
                            out1.push(Event::Marker).await;

                             //s2
                            ConsumerProducerState::S2 {
                                stream0: stream0.to_owned(),
                                stream1: stream1.to_owned(),
                                stream2: stream2.to_owned(),
                                out0: out0.to_owned(),
                                out1: out1.to_owned(),
                                count: count.to_owned(),
                                before_marker_queue: before_marker_queue.clone(),
                            }
                        },
                        Event::MessageAmount(_) => panic!(),
                    }
                }
            };
        }
    }

    pub async fn execute_optimized(mut self, ctx: Context) {
        println!("ConsumerProducer optimized ON!");

        let mut return_state = match &self {
            ConsumerProducerState::S0 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
                before_marker_queue,
            } => ConsumerProducerState::S0 {
                stream0: stream0.to_owned(),
                stream1: stream1.to_owned(),
                stream2: stream2.to_owned(),
                out0: out0.to_owned(),
                out1: out1.to_owned(),
                count: count.to_owned(),
                before_marker_queue: before_marker_queue.to_owned(),
            },
            ConsumerProducerState::S1 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
                data,
                before_marker_queue,
            } => ConsumerProducerState::S1 {
                stream0: stream0.to_owned(),
                stream1: stream1.to_owned(),
                stream2: stream2.to_owned(),
                out0: out0.to_owned(),
                out1: out1.to_owned(),
                count: count.to_owned(),
                data: count.to_owned(),
                before_marker_queue: before_marker_queue.to_owned(),
            },
            ConsumerProducerState::S2 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
                before_marker_queue,
            } => ConsumerProducerState::S2 {
                stream0: stream0.to_owned(),
                stream1: stream1.to_owned(),
                stream2: stream2.to_owned(),
                out0: out0.to_owned(),
                out1: out1.to_owned(),
                count: count.to_owned(),
                before_marker_queue: before_marker_queue.to_owned(),
            },
        };

        loop {
            self = match &self {
                ConsumerProducerState::S0 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                    before_marker_queue,
                } => {
                    //state -> s0
                    let in_event0 = stream0.pull().await;
                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            //state -> s1
                            ConsumerProducerState::S1 {
                                stream0: stream0.to_owned(),
                                stream1: stream1.to_owned(),
                                stream2: stream2.to_owned(),
                                out0: out0.to_owned(),
                                out1: out1.to_owned(),
                                count: count.clone(),
                                data: event_data_s0,
                                before_marker_queue: before_marker_queue.to_owned(),
                            }
                        }
                        Event::Marker => {
                            //Fix:marker should drain the other stream, but it will block stream 2 while draining
                            //implement draining
                            //implement snapshotting
                            //s0
                            ConsumerProducerState::S0 {
                                stream0: stream0.clone(),
                                stream1: stream1.clone(),
                                stream2: stream2.clone(),
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: count.clone(),
                                before_marker_queue: before_marker_queue.to_owned(),
                            }
                        }
                        Event::MessageAmount(_) => panic!(),
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
                    before_marker_queue,
                } => {
                    let in_event1 = stream1.pull().await;
                    match in_event1 {
                        Event::Data(event_data_s1) => {
                            out0.push(Event::Data(data.clone() + event_data_s1)).await;
                            ConsumerProducerState::S2 {
                                stream0: stream0.clone(),
                                stream1: stream1.clone(),
                                stream2: stream2.clone(),
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: count.clone(),
                                before_marker_queue: before_marker_queue.to_owned(),
                            }
                        }
                        Event::Marker => {
                            //s1
                            ConsumerProducerState::S1 {
                                stream0: stream0.clone(),
                                stream1: stream1.clone(),
                                stream2: stream2.clone(),
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: count.clone(),
                                data: data.clone(),
                                before_marker_queue: before_marker_queue.to_owned(),
                            }
                        }
                        Event::MessageAmount(_) => panic!(),
                    }
                }
                ConsumerProducerState::S2 {
                    stream0,
                    stream1,
                    stream2,
                    out0,
                    out1,
                    count,
                    before_marker_queue,
                } => {
                    //s2 Nothing to do with the first and second stream, should not block them during snapshotting
                    let in_event2 = stream2.pull().await;
                    match in_event2 {
                        Event::Data(event_data_s2) => {
                            out1.push(Event::Data(event_data_s2)).await;
                            ConsumerProducerState::S0 {
                                stream0: stream0.clone(),
                                stream1: stream1.clone(),
                                stream2: stream2.clone(),
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count: count.clone(),
                                before_marker_queue: before_marker_queue.to_owned(),
                            }
                        }
                        Event::Marker => ConsumerProducerState::S2 {
                            stream0: stream0.clone(),
                            stream1: stream1.clone(),
                            stream2: stream2.clone(),
                            out0: out0.clone(),
                            out1: out1.clone(),
                            count: count.clone(),
                            before_marker_queue: before_marker_queue.to_owned(),
                        },
                        Event::MessageAmount(_) => panic!(),
                    }
                }
            };
        }
    }
}

pub async fn drain_buffers(stream: &PullChan<Event<i32>>, before_marker_queue: &mut Vec<VecDeque<Event<i32>>>) -> PullChan<Event<i32>>{
    loop {
        let in_event = stream.pull().await;
        match in_event {
            Event::Data(event_data) => {
                before_marker_queue[0]
                    .push_front(Event::Data(event_data));
            }
            Event::Marker => {
                break;
            }
            Event::MessageAmount(_) => panic!(),
        }
    }
    stream.clone()
}

/*pub async fn merge(
    in_vec: &Vec<PullChan<Event<i32>>>,
    out_vec: &Vec<PushChan<Event<i32>>>,
    in_count: &i32,
    ctx: Context,
) {
    loop {
        loop {
            //s0
            let event0 = match in_vec[0].pull().await {
                Event::Data(event_data) => Some(event_data),
                Event::Marker => {
                    let mut new_state = ConsumerProducerState::S0 {
                        input_vec: in_vec.clone(),
                        output_vec: out_vec.clone(),
                        count: in_count.clone(),
                    };
                    Shared::<()>::store(SharedState::ConsumerProducer(new_state), &ctx).await;
                    break;
                }
                Event::MessageAmount(_) => panic!(),
            }.unwrap();
            //s1
            let event1 = match in_vec[1].pull().await {
                Event::Data(event_data) => Some(event_data),
                Event::Marker => {
                    let new_state = ConsumerProducerState::S1 {
                        input_vec: in_vec.clone(),
                        output_vec: out_vec.clone(),
                        count: in_count.clone(),
                        data: event0,
                    };
                    Shared::<()>::store(SharedState::ConsumerProducer(new_state), &ctx).await;
                    break;
                }
                Event::MessageAmount(_) => panic!(),
            }.unwrap();
            out_vec[0].push(Event::Data((event0 + event1))).await;
        }
    }
} */
