use tokio::sync::oneshot;

use std::sync::Arc;

use std::time::Duration;
use tokio::time;

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
        input: PullChan<Event<()>>,
        count: i32,
    },
}

impl ConsumerState {
    pub async fn execute(mut self, ctx: Context) {
        println!("consumer ON!");
        loop {
            self = match &self {
                ConsumerState::S0 { input, count } => {
                    match input.pull().await {
                        Event::Data(data) => {
                            let loc_count = count + 1;
                            println!("Consumer count is: {}", count);
                            ConsumerState::S0 {
                                input: input.to_owned(),
                                count: loc_count,
                            }
                        }
                        Event::Marker => {
                            //snapshoting
                            self.store(&ctx).await;
                            println!("Done with snapshotting");
                            
                            ConsumerState::S0 {
                                input: input.to_owned(),
                                count: count.to_owned(),
                            }
                        }
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
        let evnt = TaskToManagerMessage::Serialise(Task::Consumer(self.clone()), send);

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

    /*async fn store(&self) {
        let persistent = self.to_persistent().await;
        let serialized = serde_json::to_string(&persistent).unwrap();
        todo!()
    }

    async fn restore() -> Self {
        let persistent = serde_json::from_str(todo!()).unwrap();
        Self::from_persistent(persistent).await
    }

    async fn to_persistent(&self) -> PersistentProducerState {
        todo!()
    }

    async fn from_persistent(persistent: PersistentProducerState) -> Self {
        todo!()
    }*/
}

/*pub fn consumer (input: PullChan<Event<()>>){
    let state = ConsumerState::S0 { input, count: 0 };
    async_std::task::spawn(state.execute(todo!()));
    println!("sum_impl operator spawned!");
}*/

//fn consumer(input : PersistentPullChan<()> ) {
