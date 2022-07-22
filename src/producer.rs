use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

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
        count: i32,
    },
}



impl ProducerState {
    pub async fn execute(mut self, ctx: Context) {
        println!("producer ON!");
        loop {
            self = match &self {
                ProducerState::S0 { output, count } => {
                    let mut loc_count = count.clone();
                    let mut loc_out = output;
                    println!("count: {}",count);
                    if (count.eq(&10)){
                        //snapshot func
                        self.store(&ctx).await;
                        
                        //forward the marker to consumers
                        loc_out.push(Event::Marker).await;

                        loc_count = 0;
                    }
                    else{
                        loc_count = count + 1;
                        loc_out.push(Event::Data(())).await;
                    }
                    ProducerState::S0 { output:loc_out.clone(), count:loc_count }
                }
            }
        }
    }

    pub async fn store(&self, ctx: &Context) {

        let slf = Arc::new(self.clone().to_owned());

        //Rawpointers of self
        //let raw_pointer: *const () = Arc::into_raw(slf) as *const ();
        //let new_chan = unsafe {&*raw_pointer}.clone();


        let (send, recv) = oneshot::channel();
        let evnt = TaskToManagerMessage::Serialise(Task::Producer(self.clone()), send);
        
        println!("pushing to manager!");
        ctx.manager_send.push(evnt).await;
        println!("pushed to manager!");
        recv.await;
        //unsafe{glob_snapshot_hashmap.insert(raw_pointer, evnt3);}

        //ProducerState::store_hashmap(chan_id, raw_pointer);
        


        println!("!!!SELF: {:#?}.",self.clone());

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
    
    pub fn store_hashmap(id : u64,p : *const ()){
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