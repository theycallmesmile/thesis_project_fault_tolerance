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
                    //state -> s0
                    let in_event0 = if !stream0.clone().log_length_check().await {
                        stream0.pull_log().await
                    } else {
                        stream0.pull().await
                    };
                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            //state -> s1
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
                            //Only the pull channel buffer will be saved, the push will be reset and relinked by the connected operator down the dataflow graph
                            //Draining from the remaining input streams until a marker is received.
                            //The drained messages are stored for snapshotting.

                            println!("Consumer-producer: starting draining!");
                            let loc_stream1 = drain_buffers(&stream1).await;
                            let loc_stream2 = drain_buffers(&stream2).await;
                            println!("Consumer-Producer: Done with draining");
                            println!("LOG CHECK0: {:?}",loc_stream2);
                            //snapshot state:
                            let snapshot_state = ConsumerProducerState::S0 {
                                stream0: stream0.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                stream1: loc_stream1, //Should the messages sent after marker be saved?
                                stream2: loc_stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                            };
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;
                            println!("done with ConsumerProducer snapshotting");

                            //forward the marker to consumers
                            println!("SENDING MARKER!");
                            out0.push(Event::Marker).await;
                            out1.push(Event::Marker).await;

                            //s0
                            ConsumerProducerState::S0 {
                                //what happens if streams receive new messages leading loc_stream being old and deprecated?
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
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
                } => {
                    //state -> s1
                    let in_event1 = if !stream1.clone().log_length_check().await {
                        stream1.pull_log().await
                    } else {
                        stream1.pull().await
                    };
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
                            //Only the pull channel buffer will be saved, the push will be reset and relinked by the connected operator down the dataflow graph
                            //Draining from the remaining input streams until a marker is received.
                            //The drained messages are stored for snapshotting.

                            let loc_stream0 = drain_buffers(&stream0).await;
                            let loc_stream2 = drain_buffers(&stream2).await;
                            println!("LOG CHECK0: {:?}",loc_stream2);
                            //snapshot state:
                            let snapshot_state = ConsumerProducerState::S1 {
                                stream0: loc_stream0, //is it necessary to clean if log exsists ? Should the messages sent after marker be saved?
                                stream1: stream1.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                stream2: loc_stream2,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                                data,
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
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                                data,
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
                } => {
                    //state -> s2
                    let in_event2 = if !stream2.clone().log_length_check().await {
                        stream2.pull_log().await
                    } else {
                        stream2.pull().await
                    };
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
                            //Only the pull channel buffer will be saved, the push will be reset and relinked by the connected operator down the dataflow graph
                            //Draining from the remaining input streams until a marker is received.
                            //The drained messages are stored for snapshotting.

                            let loc_stream0 = drain_buffers(&stream0).await;
                            let loc_stream1 = drain_buffers(&stream1).await;
                            //snapshot state:
                            let snapshot_state = ConsumerProducerState::S2 {
                                stream0: loc_stream0, //is it necessary to clean if log exsists ? Should the messages sent after marker be saved?
                                stream1: loc_stream1,
                                stream2: stream2.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
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
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
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
            } => ConsumerProducerState::S0 {
                stream0: stream0.to_owned(),
                stream1: stream1.to_owned(),
                stream2: stream2.to_owned(),
                out0: out0.to_owned(),
                out1: out1.to_owned(),
                count: count.to_owned(),
            },
            ConsumerProducerState::S1 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
                data,
            } => ConsumerProducerState::S1 {
                stream0: stream0.to_owned(),
                stream1: stream1.to_owned(),
                stream2: stream2.to_owned(),
                out0: out0.to_owned(),
                out1: out1.to_owned(),
                count: count.to_owned(),
                data: count.to_owned(),
            },
            ConsumerProducerState::S2 {
                stream0,
                stream1,
                stream2,
                out0,
                out1,
                count,
            } => ConsumerProducerState::S2 {
                stream0: stream0.to_owned(),
                stream1: stream1.to_owned(),
                stream2: stream2.to_owned(),
                out0: out0.to_owned(),
                out1: out1.to_owned(),
                count: count.to_owned(),
            },
        };

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
                    //state -> s0
                    let in_event0 = if !stream0.clone().log_length_check().await {
                        stream0.pull_log().await
                    } else {
                        stream0.pull().await
                    };
                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            //state -> s1
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
                            //Only the pull channel buffer will be saved, the push will be reset and relinked by the connected operator down the dataflow graph
                            //Draining from the remaining input streams until a marker is received.
                            //The drained messages are stored for snapshotting.
                            let loc_stream1 = drain_buffers(&stream1).await;
                            
                            /*loop {
                                match stream2.pull().await {
                                    Event::Data(event_data_s2) => {
                                        out1.push(Event::Data(event_data_s2)).await;
                                    }
                                    Event::Marker => {
                                        break;
                                    },
                                    Event::MessageAmount(_) => panic!(),
                                }
                            }*/


                            //snapshot state:
                            let snapshot_state = ConsumerProducerState::S0 {//todo: dont need to clear buffer, the manager will only save the logs in the channels. 
                                stream0: stream0.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                stream1: loc_stream1, //Should the messages sent after marker be saved?
                                stream2: stream2.clone().clear_buffer().await,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                            };
                            println!("start ConsumerProducer snapshotting");
                            Shared::<()>::store(
                                SharedState::ConsumerProducer(snapshot_state.clone()),
                                &ctx,
                            )
                            .await;
                            println!("done with ConsumerProducer snapshotting");

                            //forward the marker to consumers
                            println!("SENDING MARKER!");
                            out0.push(Event::Marker).await;
                            out1.push(Event::Marker).await;

                            //s0
                            ConsumerProducerState::S0 {
                                //what happens if streams receive new messages leading loc_stream being old and deprecated?
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
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
                } => {
                    //state -> s1
                    let in_event1 = if !stream1.clone().log_length_check().await {
                        stream1.pull_log().await
                    } else {
                        stream1.pull().await
                    };
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
                            //Only the pull channel buffer will be saved, the push will be reset and relinked by the connected operator down the dataflow graph
                            //Draining from the remaining input streams until a marker is received.
                            //The drained messages are stored for snapshotting.

                            let loc_stream0 = drain_buffers(&stream0).await;

                            loop {
                                match stream2.pull().await {
                                    Event::Data(event_data_s2) => {
                                        out1.push(Event::Data(event_data_s2)).await;
                                    }
                                    Event::Marker => {
                                        break;
                                    },
                                    Event::MessageAmount(_) => panic!(),
                                }
                            }

                            //snapshot state:
                            let snapshot_state = ConsumerProducerState::S1 {
                                stream0: loc_stream0, //is it necessary to clean if log exsists ? Should the messages sent after marker be saved?
                                stream1: stream1.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                stream2: stream2.clone().clear_buffer().await,
                                out0: out0.clone(),
                                out1: out1.clone(),
                                count,
                                data,
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
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                                data,
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
                } => {
                    //state -> s2
                    let in_event2 = stream2.pull().await;
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
                            //Only the pull channel buffer will be saved, the push will be reset and relinked by the connected operator down the dataflow graph
                            //Draining from the remaining input streams until a marker is received.
                            //The drained messages are stored for snapshotting.


                            loop {
                                let event_data = match stream0.pull().await {
                                    Event::Data(event_data_0) => {
                                        event_data_0
                                    }
                                    Event::Marker => { 
                                        let loc_stream1 = drain_buffers(&stream1).await;

                                        //snapshot state:
                                        let snapshot_state = ConsumerProducerState::S2 {
                                            stream0: stream0.clone().clear_buffer().await, 
                                            stream1: loc_stream1,
                                            stream2: stream2.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                            out0: out0.clone(),
                                            out1: out1.clone(),
                                            count,
                                        };
                                        println!("start producer snapshotting");
                                        Shared::<()>::store(
                                            SharedState::ConsumerProducer(snapshot_state.clone()),
                                            &ctx,
                                        )
                                        .await;
                                        println!("done with producer snapshotting");

                                        break;
                                    },
                                    Event::MessageAmount(_) => panic!(),
                                };
                                match stream1.pull().await {
                                    Event::Data(event_data_1) => {
                                        out0.push(Event::Data(event_data.clone() + event_data_1)).await;
                                    }
                                    Event::Marker => {
                                        let loc_stream0 = drain_buffers(&stream0).await;

                                        //snapshot state: //maybe ::S1 instead?
                                        let snapshot_state = ConsumerProducerState::S2 {//TODO: BUG IN THE STATE, NEED TO SAVE event_data incase it has value in it
                                            stream0: loc_stream0,
                                            stream1: stream1.clone().clear_buffer().await,
                                            stream2: stream2.clone().clear_buffer().await, //data after marker should not be saved. thus, it is cleaned
                                            out0: out0.clone(),
                                            out1: out1.clone(),
                                            count,
                                            //might need event_data: event_data.clone(), here 
                                        };
                                        println!("start producer snapshotting");
                                        Shared::<()>::store(
                                            SharedState::ConsumerProducer(snapshot_state.clone()),
                                            &ctx,
                                        )
                                        .await;
                                        println!("done with producer snapshotting");

                                        break;
                                    },
                                    Event::MessageAmount(_) => panic!(),
                                }
                            }
                            //forward the marker to consumers
                            println!("SENDING MARKER!");
                            out0.push(Event::Marker).await;
                            out1.push(Event::Marker).await;

                            //s2
                            ConsumerProducerState::S2 {
                                stream0,
                                stream1,
                                stream2,
                                out0,
                                out1,
                                count,
                            }
                        }
                        Event::MessageAmount(_) => panic!(),
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
            Event::MessageAmount(_) => panic!(),
        }
    }
    stream.clone()
}
