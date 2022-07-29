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

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Event<i32> {
    Data(i32),
    Marker,
}

#[derive(Debug, Clone)]
pub enum ProducerState {
    S0 {
        output: PushChan<Event<()>>,
        marker_rec: PullChan<Event<()>>, //ta bort från producer state, ha det i context istället.
        count: i32,
    },
}

impl ProducerState {
    pub async fn execute(mut self, ctx: Context) {
        println!("producer ON!");
        let mut interval = time::interval(time::Duration::from_millis(100));
        loop {
            self = match &self {
                ProducerState::S0 {
                    output,
                    marker_rec,
                    count,
                } => {
                    let mut loc_count = count.clone();
                    let mut loc_out = output;
                    println!("producer count: {}", count);

                    loop {
                        tokio::select! {
                            //send data to consumer
                            _ = interval.tick() => {
                                loc_count = count + 1;
                                loc_out.push(Event::Data(())).await;
                                break;
                        },
                                //snapshot and send marker to consumer
                                msg = marker_rec.pull() => {
                                    
                                //snapshoting
                                self.store(&ctx).await;

                                //forward the marker to consumers
                                loc_out.push(Event::Marker).await;
                                break;
                            }
                        }
                    }
                    ProducerState::S0 {
                        output: output.to_owned(),
                        marker_rec: marker_rec.to_owned(),
                        count: loc_count,
                    }
                }
            }
        }
    }

    pub async fn store(&self, ctx: &Context) {
        let mut interval = time::interval(time::Duration::from_millis(100));
        let slf = Arc::new(self.clone().to_owned());
        //Rawpointers of self
        //let raw_pointer: *const () = Arc::into_raw(slf) as *const ();
        //let new_chan = unsafe {&*raw_pointer}.clone();

        let (send, mut recv) = oneshot::channel();
        let evnt = TaskToManagerMessage::Serialise(Task::Producer(self.clone()), send);

        println!("pushed state snapshot to manager");
        ctx.state_manager_send.push(evnt).await;
        println!("waiting for promise");

        loop {
            tokio::select! {
                _ = interval.tick() => println!("Another 100ms"),
                msg = &mut recv => {
                    println!("Got message: {}", msg.unwrap());
                    break;
                }
            }
        }
        println!("got the promise!");
        //unsafe{glob_snapshot_hashmap.insert(raw_pointer, evnt3);}

        //ProducerState::store_hashmap(chan_id, raw_pointer);
    }

    pub async fn restore() -> Self {
        //take from hashmap
        //..how to know which key..?
        //or maybe it doesnt matter which key, the id should be updated because of the output

        //the value of the hashmap should give the output and the count will be extracted throught the output aswell.
        //let mut snapshot_hashmap: HashMap<u64,*const ()> = HashMap::new();

        //ProducerState::S0 { output: , count: }
        todo!()
    }

    pub fn store_hashmap(id: u64, p: *const ()) {
        //let mut snapshot_hashmap: HashMap<u64,*const ()> = HashMap::new();
        //if statement where it checks if the id excists in the hash, then skip

        //snapshot_hashmap.insert(id, p);
        println!("snapshot done");
    }
}

/*fn producer(manager_push: PushChan<Event<()>>) -> PullChan<Event<()>> {
    let (push, pull) = channel::<Event<()>>();
    let state = ProducerState::S0 {
        output: push,
        manager_output: manager_push,
        count: 0,
    };
    async_std::task::spawn(state.execute());
    println!("producer operator spawned!");
    println!("The producer channels: {:?}",pull);
    pull
}*/
