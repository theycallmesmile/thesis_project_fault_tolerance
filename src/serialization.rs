use std::collections::HashMap;
use std::collections::HashSet;
use std::os::raw;

use async_std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

//use serde::ser::{Serializer, SerializeSeq};

//Manager module
use crate::manager::SerializeTaskVec;
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
#[derive(Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Clone)]
pub enum PersistentTask {
    Consumer(PersistentConsumerState),
    Producer(PersistentProducerState),
}

/*pub async fn serialize_func_old(s: Vec<Event<()>>) -> String {
    let serialized = serde_json::to_string(&s).unwrap();
    return serialized;
}*/

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PersistentProducerState {
    S0 {
        output: PersistentPushChan<Event<()>>,
        count: i32,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PersistentConsumerState {
    S0 {
        input: PersistentPullChan<Event<()>>,
        count: i32,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct PersistentPushChan<T> {
    uid: u64,
    buffer: Option<Vec<T>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
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
    //consumer: used to check if the buffer excists in the hashset, otherwise insert it into the hashset
    pub async fn to_persistent_chan_cons(
        &self,
        serde_state: &mut SerdeState,
    ) -> PersistentPullChan<T> {
        let queue = self.0.queue.lock().await;
        let ptr = std::sync::Arc::into_raw(self.0.clone()) as *const ();
        let buffer = if serde_state.serialised.contains(&ptr) {
            println!("SOMETHING IS WRONG IN THE SERIALIZATION!");
            None //this should never happen
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
    //producer: used to check if the buffer excists in the hashset, otherwise insert it into the hashset
    pub async fn to_persistent_chan_prod(
        &self,
        serde_state: &mut SerdeState,
    ) -> PersistentPushChan<T> {
        let queue = self.0.queue.lock().await;
        let ptr = std::sync::Arc::into_raw(self.0.clone()) as *const ();
        let buffer = if serde_state.serialised.contains(&ptr) {
            None
        } else {
            None
        };
        PersistentPushChan {
            uid: ptr as u64,
            buffer, //The buffer should be only stored by consumer, thus empty buffer given to producer
        } ////////////^This is done due of consistency of the snapshot and the placement of marker message in the buffer
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
                    //marker_rec,
                    count,
                } => {
                    let output = output.to_persistent_chan_prod(serde_state).await;
                    PersistentTask::Producer(PersistentProducerState::S0 { output, count })
                }
            },
            Task::Consumer(state) => match state {
                ConsumerState::S0 { input, count } => {
                    let input = input.to_persistent_chan_cons(serde_state).await;
                    PersistentTask::Consumer(PersistentConsumerState::S0 { input, count })
                }
            },
        }
    }
}

impl PersistentTask {
    pub async fn push_to_vec(self, serialize_task_vec: &mut SerializeTaskVec) {
        //push task into a vector which will later on be serialized
        //the snapshots/states will be stored on memory until the whole checkpointing is done
        serialize_task_vec.task_vec.push(self);
    }
}

impl SerializeTaskVec {
    pub async fn serialize_state(self) {
        //serializing the vector with all of its snapshot elements
        let serialized_vec = serde_json::to_string(&self).unwrap();
        println!("Serialized vec: {:?}", serialized_vec);

        self.save_persistent(serialized_vec).await;
    }
    pub async fn save_persistent(self, serialized_vec: String) {
        //create or open file
        //save to persistent disc
        //Saving checkpoint to file (appends atm)
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open("serialized_checkpoints.txt")
            .await;
        file.unwrap()
            .write_all(serialized_vec.as_bytes())
            .await
            .unwrap();

        println!("DONE!{}", serialized_vec);

        println!("saving checkpoint to persistent disc");
    }

    pub async fn deserialize(self, serialized_vec: String) {
        //1:read from file
        //2:deserialize

        let deserialized_vec: SerializeTaskVec = serde_json::from_str(&serialized_vec).unwrap();
        println!("Deserialized vec: {:?}", deserialized_vec.task_vec);
    }
}
