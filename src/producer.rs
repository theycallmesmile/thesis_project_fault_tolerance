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

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Eq, Hash)]
pub enum Event<i32> {
    Data(i32),
    Marker,
}

#[derive(Debug, Clone)]
pub enum ProducerState {
    S0 {
        output: PushChan<Event<()>>,
        count: i32,
    },
}

impl ProducerState {
    pub async fn execute(mut self, ctx: Context) {
        println!("producer ON!");
        let mut interval = time::interval(time::Duration::from_millis(100));
        loop {
            self = match &self {
                ProducerState::S0 { output, count } => {
                    let mut loc_count = count.clone();
                    let mut loc_out = output;
                    println!("producer count: {}", count);
                    println!("producer queue: {:?}", &loc_out.0.queue);

                    loop {
                        tokio::select! {
                            //send data to consumer
                            _ = interval.tick() => {
                                loc_count = count + 1;
                                loc_out.push(Event::Data(())).await;
                                break;
                        },
                            //snapshot and send marker to consumer
                            msg = ctx.marker_manager_recv.as_ref().unwrap().pull() => {
                                //snapshoting
                                println!("start producer snapshotting");
                                self.store(&ctx).await;
                                println!("done with producer snapshotting");

                                //forward the marker to consumers
                                println!("SENDING MARKER!");
                                loc_out.push(Event::Marker).await; 
                                break;
                            }
                        }
                    }
                    ProducerState::S0 {
                        output: output.to_owned(),
                        //marker_rec: marker_rec.to_owned(),
                        count: loc_count,
                    }
                }
            }
        }
    }

    pub async fn store(&self, ctx: &Context) {
        let mut interval = time::interval(time::Duration::from_millis(100));
        let slf = Arc::new(self.clone().to_owned());

        let (send, mut recv) = oneshot::channel();
        let evnt = TaskToManagerMessage::Serialise(Task::Producer(self.clone()), send);

        println!("pushed state snapshot to manager");
        ctx.state_manager_send.push(evnt).await;
        println!("waiting for promise");

        loop {
            tokio::select! {
                _ = interval.tick() => println!("Producer - Another 100ms"),
                msg = &mut recv => {
                    println!("Got message: {}", msg.unwrap());
                    break;
                }
            }
        }
        println!("got the promise!");
    }
}
