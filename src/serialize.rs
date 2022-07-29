use std::collections::HashMap;
use std::collections::HashSet;
use std::os::raw;

use async_std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

//Manager module
use crate::manager::Task;

//Channel module
use crate::channel::PullChan;
use crate::channel::PushChan;

//Producer module
use crate::producer::Event;
use crate::producer::ProducerState;

//Consumer module
use crate::consumer::ConsumerState;

unsafe impl Send for SerdeState {}
unsafe impl Sync for SerdeState {}

#[derive(Default, Clone)]
pub struct SerdeState {
    //for deserialization?
    pub serialised: HashSet<*const ()>, //raw pointer adress av arc chan
    pub deserialised: HashMap<*const (), *const ()>, //adress av  |
}

pub enum PersistentTask {
    Consumer(PersistentConsumerState),
    Producer(PersistentProducerState),
}

pub async fn serialize_func_old(s: Vec<Event<()>>) -> String {
    let serialized = serde_json::to_string(&s).unwrap();
    return serialized;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PersistentProducerState {
    S0 {
        output: PersistentPushChan<Event<()>>,
        count: i32,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PersistentConsumerState {
    S0 {
        input: PersistentPullChan<Event<()>>,
        count: i32,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistentPushChan<T> {
    uid: u64,
    buffer: Option<Vec<T>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistentPullChan<T> {
    uid: u64,
    buffer: Option<Vec<T>>,
}

// let data = Arc::new("foo");
// let ptr = Arc::into_raw(data); => 12345
// ...
// let json0 = data.serialize() => { old_ptr: 12345, data: Some("foo") }
// let json1 = data.serialize() => { old_ptr: 12345, data: None }

// match json0.deserialize() { varning, tänk på ordning
//     (old_ptr, Some(data)) => {
//         let arc = Arc::new(data);
//         let new_ptr = Arc::into_raw(arc.clone());
//         hashmap.insert(old_ptr, new_ptr);
//         arc
//     }
//     (old_ptr, None) => {
//         let new_ptr = hashmap.get(old_ptr).unwrap();
//         let arc = Arc::from_raw(new_ptr);
//         arc
//     }
// }
//
// let data = ser0.deserialize() => Arc::new("foo");
// data.serialize() => { old_ptr: 12345, data: Some("foo") }

impl<T: Clone + std::fmt::Debug> PullChan<T> {
    //USE THIS TO SERIALIZE THE STATES!
    pub async fn to_persistent_chan(&self, serde_state: &mut SerdeState) -> PersistentPullChan<T> {
        let queue = self.0.queue.lock().await;
        let ptr = std::sync::Arc::into_raw(self.0.clone()) as *const ();
        let buffer = if serde_state.serialised.contains(&ptr) {
            None
        } else {
            serde_state.serialised.insert(ptr);
            Some(queue.iter().cloned().collect())
        };
        PersistentPullChan {
            uid: ptr as u64,
            buffer,
        }
    }
}

impl<T: Clone + std::fmt::Debug> PushChan<T> {
    //USE THIS TO SERIALIZE THE STATES!
    pub async fn to_persistent_chan(&self, serde_state: &mut SerdeState) -> PersistentPushChan<T> {
        let queue = self.0.queue.lock().await;
        let ptr = std::sync::Arc::into_raw(self.0.clone()) as *const ();
        let buffer = if serde_state.serialised.contains(&ptr) {
            None
        } else {
            serde_state.serialised.insert(ptr);
            Some(queue.iter().cloned().collect())
        };
        PersistentPushChan {
            uid: ptr as u64,
            buffer,
        }
    }
}

//Rawpointers of self
//let raw_pointer: *const () = Arc::into_raw(slf) as *const ();
//let new_chan = unsafe {&*raw_pointer}.clone();

impl Task {
    pub async fn to_persistent_task(self, serde_state: &mut SerdeState) -> PersistentTask {
        match self {
            Task::Producer(state) => match state {
                ProducerState::S0 {
                    output,
                    marker_rec,
                    count,
                } => {
                    let output = output.to_persistent_chan(serde_state).await;
                    PersistentTask::Producer(PersistentProducerState::S0 { output, count })
                }
            },
            Task::Consumer(state) => match state {
                ConsumerState::S0 { input, count } => {
                    let input = input.to_persistent_chan(serde_state).await;
                    PersistentTask::Consumer(PersistentConsumerState::S0 { input, count })
                }
            },
        }
    }

    pub async fn serialize(self, mut serde_state: SerdeState) {
        //create raw pointer
        //check if raw pointer excists in the hashset
        //if not, inset it into the hashset
        //serialize the state and insert it into the hashmap
        //Save the hashmap persistenly

        /*println!("THE RAW POINTER: {:?}",raw_pointer);

               if !serde_state.serialised.contains(&raw_pointer){
                   serde_state.serialised.insert(raw_pointer);
               }

        */
        println!("Hashset: {:#?}", serde_state.serialised);
    }
}

/*pub struct SerdeContext {
    serialised: HashSet<u64>,
    deserialised: HashMap<u64, (PullChan<>, PushChan<>)>,
}*/
