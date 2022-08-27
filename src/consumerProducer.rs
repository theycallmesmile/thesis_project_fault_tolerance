use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

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
        //stream 1,
        //stream 2,
        //stream 3,
        //out1,
        //out2,
        input_vec: Vec<PullChan<Event<i32>>>,
        output_vec: Vec<PushChan<Event<i32>>>,
        //in_out_map: HashMap<Vec<PullChan<Event<i32>>>, Vec<PushChan<Event<i32>>>>,
        count: i32,
    },
    S1 {
        input_vec: Vec<PullChan<Event<i32>>>, //gör till array..?
        output_vec: Vec<PushChan<Event<i32>>>,
        count: i32,
        data: i32,
    },
    S2 {
        input_vec: Vec<PullChan<Event<i32>>>,
        output_vec: Vec<PushChan<Event<i32>>>,
        count: i32,
    },
}

impl ConsumerProducerState {
    pub async fn execute_unoptimized(mut self, ctx: Context) {
        println!("ConsumerProducer unoptimized ON!");
        let mut interval = time::interval(time::Duration::from_millis(200));

        let mut return_state = match &self {
            ConsumerProducerState::S0 {
                input_vec,
                output_vec,
                //in_out_map,
                count,
            } => ConsumerProducerState::S0 {
                input_vec: input_vec.to_owned(),
                output_vec: output_vec.to_owned(),
                //in_out_map: in_out_map.to_owned(),
                count: count.to_owned(),
            },
            ConsumerProducerState::S1 {
                input_vec,
                output_vec,
                count,
                data,
            } => ConsumerProducerState::S1 {
                input_vec: input_vec.to_owned(),
                output_vec: output_vec.to_owned(),
                count: count.to_owned(),
                data: count.to_owned(),
            },
            ConsumerProducerState::S2 {
                input_vec,
                output_vec,
                count,
            } => ConsumerProducerState::S2 {
                input_vec: input_vec.to_owned(),
                output_vec: output_vec.to_owned(),
                count: count.to_owned(),
            },
        };

        loop {
            self = match &self {
                ConsumerProducerState::S0 {
                    input_vec,
                    output_vec,
                    count,
                } => {
                    let mut loc_input_vec = input_vec;
                    let mut loc_count = count.clone();
                    let mut loc_out_vec = output_vec;
                    let mut snapshot_counter: HashSet<usize> = HashSet::new();
                    println!("ConsumerProducer count: {}", count);
                    for n in 0..output_vec.len() {
                        println!("ConsumerProducer queue: {:?}", &loc_out_vec[n].0.queue);
                    }
                    for n in 0..input_vec.len() {
                        if !snapshot_counter.contains(&n)
                            && !snapshot_counter.len().eq(&input_vec.len())
                        {
                            tokio::select! {
                                _ = interval.tick() => {
                                    println!("ConsumerProducer count is: {}, Buffer empty for: {:?}", count, &input_vec[n]);
                                },
                                event = input_vec[n].pull() => {
                                    match event {
                                        Event::Data(event_data) => {
                                            let loc_count = count + 1;
                                            println!("ConsumerProducer count is: {}", loc_count);
                                            println!("The consumerProducer buffer: {:?}", &input_vec[n].0.queue);

                                            for output in output_vec{ //pushes received message to every consumer (or consumerProducer) operator
                                                output.push(Event::Data(event_data)).await;
                                            }
                                            return_state = ConsumerProducerState::S0 {
                                                input_vec: input_vec.to_owned(),
                                                output_vec: output_vec.to_owned(),
                                                //in_out_map: in_out_map.to_owned(),
                                                count: loc_count,
                                            };
                                        },
                                        Event::Marker => {
                                            if snapshot_counter.is_empty(){
                                                for output in output_vec{ //pushes received marker to every consumer (or consumerProducer) operator
                                                    output.push(Event::Marker).await;
                                                }
                                            }
                                            //add to the snapshot_count
                                            snapshot_counter.insert(n);

                                            return_state = ConsumerProducerState::S0 {
                                                input_vec: input_vec.to_owned(),
                                                output_vec: output_vec.to_owned(),
                                                //in_out_map: in_out_map.to_owned(),
                                                count: count.to_owned(),
                                            };
                                        },
                                        Event::MessageAmount(amount) => {},
                                    }
                                },
                            }
                        } else if snapshot_counter.len().eq(&input_vec.len()) {
                            //snapshoting
                            println!("Start consumer snapshotting");
                            Shared::<()>::store(SharedState::ConsumerProducer(self.clone()), &ctx)
                                .await;
                            println!("Done with consumer snapshotting");
                            snapshot_counter.clear();
                        }
                    }
                    return_state.clone()
                }
                ConsumerProducerState::S1 {
                    input_vec,
                    output_vec,
                    count,
                    data,
                } => ConsumerProducerState::S1 {
                    input_vec: input_vec.to_owned(),
                    output_vec: output_vec.to_owned(),
                    count: count.to_owned(),
                    data: data.to_owned(),
                },
                ConsumerProducerState::S2 {
                    input_vec,
                    output_vec,
                    count,
                } => todo!(),
            }
        }
    }

    pub async fn execute_optimized(mut self, ctx: Context) {
        println!("ConsumerProducer optimized ON!");
        let mut interval = time::interval(time::Duration::from_millis(200));

        let mut return_state = match &self {
            ConsumerProducerState::S0 {
                input_vec,
                output_vec,
                //in_out_map,
                count,
            } => ConsumerProducerState::S0 {
                input_vec: input_vec.to_owned(),
                output_vec: output_vec.to_owned(),
                //in_out_map: in_out_map.to_owned(),
                count: count.to_owned(),
            },
            ConsumerProducerState::S1 {
                input_vec,
                output_vec,
                count,
                data,
            } => ConsumerProducerState::S1 {
                input_vec: input_vec.to_owned(),
                output_vec: output_vec.to_owned(),
                count: count.to_owned(),
                data: count.to_owned(),
            },
            ConsumerProducerState::S2 {
                input_vec,
                output_vec,
                count,
            } => ConsumerProducerState::S2 {
                input_vec: input_vec.to_owned(),
                output_vec: output_vec.to_owned(),
                count: count.to_owned(),
            },
        };

        loop {
            self = match &self {
                ConsumerProducerState::S0 {
                    input_vec,
                    output_vec,
                    //in_out_map,
                    count,
                } => {
                    //state -> s0
                    let in_event0 = input_vec[0].pull().await;
                    match in_event0 {
                        Event::Data(event_data_s0) => {
                            //state -> s1
                            ConsumerProducerState::S1 {
                                input_vec: input_vec.clone(),
                                output_vec: output_vec.clone(),
                                count: count.clone(),
                                data: event_data_s0,
                            }
                        }
                        Event::Marker => {
                            //s0
                            ConsumerProducerState::S0 {
                                input_vec: input_vec.clone(),
                                output_vec: output_vec.clone(),
                                count: count.clone(),
                            }
                        }
                        Event::MessageAmount(_) => panic!(),
                    }
                }
                ConsumerProducerState::S1 {
                    input_vec,
                    output_vec,
                    count,
                    data,
                } => {
                    let in_event1 = input_vec[1].pull().await;
                    match in_event1 {
                        Event::Data(event_data_s1) => {
                            output_vec[0]
                                .push(Event::Data(data.clone() + event_data_s1))
                                .await;
                            ConsumerProducerState::S2 {
                                input_vec: input_vec.clone(),
                                output_vec: output_vec.clone(),
                                count: count.clone(),
                            }
                        }
                        Event::Marker => {
                            //s1
                            ConsumerProducerState::S1 {
                                input_vec: input_vec.clone(),
                                output_vec: output_vec.clone(),
                                count: count.clone(),
                                data: data.clone(),
                            }
                        }
                        Event::MessageAmount(_) => panic!(),
                    }
                }
                ConsumerProducerState::S2 {
                    input_vec,
                    output_vec,
                    count,
                } => {
                    //s2 Nothing to do with the first and second stream, should not block them during snapshotting
                    let in_event2 = input_vec[2].pull().await;
                    match in_event2 {
                        Event::Data(event_data_s2) => {
                            output_vec[1].push(Event::Data(event_data_s2)).await;
                            ConsumerProducerState::S0 {
                                input_vec: input_vec.clone(),
                                output_vec: output_vec.clone(),
                                count: count.clone(),
                            }
                        }
                        Event::Marker => {
                            ConsumerProducerState::S2 {
                                input_vec: input_vec.clone(),
                                output_vec: output_vec.clone(),
                                count: count.clone(),
                            }
                        }
                        Event::MessageAmount(_) => panic!(),
                    }
                }
            };
        }
    }
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
