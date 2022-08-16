use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

use std::collections::HashSet;
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

//Producer module
use crate::producer::Event;

#[derive(Debug, Clone)]
pub enum ConsumerProducerState {
    S0 {
        input_vec: Vec<PullChan<Event<()>>>,
        output_vec: Vec<PushChan<Event<()>>>,
        count: i32,
    },
}

impl ConsumerProducerState {
    pub async fn execute(mut self, ctx: Context) {
        println!("ConsumerProducer ON!");
        let mut interval = time::interval(time::Duration::from_millis(200));

        let mut return_state = match &self {
            ConsumerProducerState::S0 { input_vec, output_vec, count } => ConsumerProducerState::S0 {
                input_vec: input_vec.to_owned(),
                output_vec: output_vec.to_owned(),
                count: count.to_owned(),
            },
        };

        loop {
            self = match &self {
                
                ConsumerProducerState::S0 { input_vec, output_vec, count } => {
                    let mut loc_input_vec = input_vec;
                    let mut loc_count = count.clone();
                    let mut loc_out_vec = output_vec;
                    let mut snapshot_counter:HashSet<usize> = HashSet::new();
                    println!("ConsumerProducer count: {}", count);
                    for n in 0..output_vec.len() {
                        println!("ConsumerProducer queue: {:?}", &loc_out_vec[n].0.queue);
                    }
                    for n in 0..input_vec.len(){
                        if !snapshot_counter.contains(&n) && !snapshot_counter.len().eq(&input_vec.len()){
                            tokio::select! {
                                _ = interval.tick() => {
                                    println!("ConsumerProducer count is: {}, Buffer empty for: {:?}", count, &input_vec[n]);
                                    
                                },
                                event = input_vec[n].pull() => {
                                    match event {
                                        Event::Data(data) => {
                                            let loc_count = count + 1;
                                            println!("ConsumerProducer count is: {}", loc_count);
                                            println!("The consumerProducer buffer: {:?}", &input_vec[n].0.queue);
                                            
                                            for output in output_vec{ //pushes received message to every consumer (or consumerProducer) operator
                                                output.push(Event::Data(data)).await;
                                            }

                                            return_state = ConsumerProducerState::S0 {
                                                input_vec: input_vec.to_owned(),
                                                output_vec: output_vec.to_owned(),
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
                                                count: count.to_owned(),
                                            };
                                        },
                                        Event::MessageAmount(amount) => {},
                                    }
                                },
                            }
                        } else if snapshot_counter.len().eq(&input_vec.len()){
                            //snapshoting
                            println!("Start consumer snapshotting");
                            self.store(&ctx).await;
                            println!("Done with consumer snapshotting");
                            snapshot_counter.clear();
                        }
                    }
                    return_state.clone()
                }
            }
        }
    }

    pub async fn store(&self, ctx: &Context) {
        let mut interval = time::interval(time::Duration::from_millis(100));
        let slf = Arc::new(self.clone().to_owned());

        let (send, mut recv) = oneshot::channel();
        let evnt = TaskToManagerMessage::Serialise(Task::ConsumerProducer(self.clone()), send);

        println!("pushed state snapshot to manager");
        ctx.state_manager_send.push(evnt).await;
        println!("waiting for promise");

        loop {
            tokio::select! {
                _ = interval.tick() => println!("ConsumerProducer - Another 100ms"),
                msg = &mut recv => {
                    println!("Got message: {}", msg.unwrap());
                    break;
                }
            }
        }
        println!("got the promise!");
    }
}
