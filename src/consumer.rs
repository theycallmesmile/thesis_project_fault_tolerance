use tokio::sync::oneshot;

use std::sync::Arc;

use std::time::Duration;
use tokio::time;

use async_std::task;

use serde::Deserialize;
use serde::Serialize;

//Manager module
use crate::manager::Context;
use crate::manager::Task;
use crate::manager::TaskToManagerMessage;

//Channel module
use crate::channel::channel;
use crate::channel::PullChan;
use crate::channel::PushChan;

//Producer
use crate::producer::Event;

#[derive(Debug, Clone)]
pub enum ConsumerState {
    S0 {
        input_vec: Vec<PullChan<Event<()>>>,
        count: i32,
    },
}

impl ConsumerState {
    pub async fn execute(mut self, ctx: Context) {
        println!("consumer ON!");
        let mut interval = time::interval(time::Duration::from_millis(200));
        task::sleep(Duration::from_secs(2)).await;
        let mut return_state = match &self{
            ConsumerState::S0 { input_vec, count } => {
                ConsumerState::S0 {
                    input_vec: input_vec.to_owned(),
                    count: count.to_owned(),
                }
            },
        };

        loop {
            self = match &self {
                ConsumerState::S0 { input_vec, count } => {
                    //for input in input_vec {
                    for n in 0..input_vec.len(){
                        tokio::select! {
                            _ = interval.tick() => {
                                println!("Consumer count is: {},Buffer empty for: {:?}", count, &input_vec[n]);
                                
                            },
                            event = input_vec[n].pull() => {
                                match event {
                                    Event::Data(data) => {
                                        let loc_count = count + 1;
                                        println!("Consumer count is: {}", count);
                                        println!("The consumer buffer: {:?}", &input_vec[n].0.queue);

                                        return_state = ConsumerState::S0 {
                                            input_vec: input_vec.to_owned(),
                                            count: loc_count,
                                        };
                                },
                                    Event::Marker => {
                                        //snapshoting
                                        println!("Start consumer snapshotting");
                                        self.store(&ctx).await;
                                        println!("Done with consumer snapshotting");
                    
                                        return_state = ConsumerState::S0 {
                                            input_vec: input_vec.to_owned(),
                                            count: count.to_owned(),
                                        };
                                        break; //TODO; NEED TO WAIT FOR ALL OF THE MARKERS BEFORE PROCEEDING PROCESSING MORE MESSAGES FROM THE SAME CHANNEL!
                                    }
                                }
                            },
                        }
                    }
                    return_state.clone()

                }
            }
        }
    }

    /*    pub async fn temp(){
        for input in input {
            match input.pull().await {
                Event::Data(data) => {
                    let loc_count = count + 1;
                    println!("Consumer count is: {}", count);
                    println!("The consumer buffer: {:?}", &input.0.queue);
                    ConsumerState::S0 {
                        input: input.to_owned(),
                        count: loc_count,
                    }
                    break;
                },
                Event::Marker => {
                    //snapshoting
                    println!("Start consumer snapshotting");
                    self.store(&ctx).await;
                    println!("Done with consumer snapshotting");

                    ConsumerState::S0 {
                        input: input.to_owned(),
                        count: count.to_owned(),
                    }
                    break;
                }
            }
        }
    }*/

    pub async fn store(&self, ctx: &Context) {
        let mut interval = time::interval(time::Duration::from_millis(100));
        let slf = Arc::new(self.clone().to_owned());

        let (send, mut recv) = oneshot::channel();
        let evnt = TaskToManagerMessage::Serialise(Task::Consumer(self.clone()), send);

        println!("pushed state snapshot to manager");
        ctx.state_manager_send.push(evnt).await;
        println!("waiting for promise");

        loop {
            tokio::select! {
                _ = interval.tick() => println!("Consumer - Another 100ms"),
                msg = &mut recv => {
                    println!("Got message: {}", msg.unwrap());
                    break;
                }
            }
        }
        println!("got the promise!");
    }
}
