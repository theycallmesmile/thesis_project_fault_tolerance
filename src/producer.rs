use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

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
use crate::shared::SharedState;
use crate::shared::Shared;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Eq, Hash)]
pub enum Event<T> {
    Data(T),
    Marker,
    MessageAmount(i32),
}

#[derive(Debug, Clone)]
pub enum ProducerState {
    S0 {
        output_vec: Vec<PushChan<Event<()>>>,
        count: i32,
    },
}

impl ProducerState {
    pub async fn execute(mut self, ctx: Context) {
        println!("producer ON!");
        let mut interval = time::interval(time::Duration::from_millis(200));

        let mut return_state = match &self {
            ProducerState::S0 { output_vec, count } => ProducerState::S0 {
                output_vec: output_vec.to_owned(),
                count: count.to_owned(),
            },
        };

        loop {
            self = match &self {
                ProducerState::S0 { output_vec, count } => {
                    let mut loc_count = count.clone();
                    let mut loc_out = output_vec;
                    println!("producer count: {}", count);
                    for n in 0..output_vec.len() {
                        println!("producer queue: {:?}", &loc_out[n].0.queue);
                    }
                    //snapshot and send marker to consumer
                    let msg = ctx.marker_manager_recv.as_ref().unwrap().pull().await;
                    match msg {
                        Event::Data(_) => {}
                        Event::Marker => {
                            //snapshoting
                            println!("start producer snapshotting");
                            //self.store(&ctx).await;
                            Shared::<()>::store(SharedState::Producer(self.clone()), &ctx).await;
                            println!("done with producer snapshotting");
                            for n in 0..output_vec.len() {
                                //forward the marker to consumers
                                println!("SENDING MARKER!");
                                loc_out[n].push(Event::Marker).await;
                            }
                        }
                        Event::MessageAmount(amount) => {
                            for x in 0..amount {
                                for output in output_vec {
                                    output.push(Event::Data(())).await;
                                    loc_count = count + 1;
                                }
                                for n in 0..output_vec.len() {
                                    tokio::select! {
                                        //send data to consumer
                                        _ = interval.tick() => {
                                            println!("buffer might be full, going trying with next consumer instead.");
                                            println!("amount of elements in buffer: {:?}, out of 15.", &output_vec[n].0.queue);
                                        },
                                        event = loc_out[n].push(Event::Data(())) => {
                                            loc_count = count + 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    ProducerState::S0 {
                        output_vec: output_vec.to_owned(),
                        count: loc_count,
                    }
                }
            }
        }
    }
}
