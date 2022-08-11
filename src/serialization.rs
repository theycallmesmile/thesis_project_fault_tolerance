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
    //for deserialization
    pub serialised: HashSet<*const ()>, //raw pointer adress av arc chan
    //pub deserialised: HashMap<*const (), *const ()>, //adress av  |
    pub deserialised: HashMap<u64, u64>,
    pub persistent_task_vec: Vec<PersistentTask>, 
}
#[derive(Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Clone)]
pub enum PersistentTask {
    Consumer(PersistentConsumerState),
    Producer(PersistentProducerState),
}

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
    pub uid: u64,
    pub buffer: Option<Vec<T>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct PersistentPullChan<T> {
    pub uid: u64,
    pub buffer: Option<Vec<T>>,
}

impl Task {
    pub async fn to_persistent_task(self, serde_state: &mut SerdeState) -> PersistentTask {
        match self {
            Task::Producer(state) => match state {
                ProducerState::S0 {
                    output,
                    //marker_rec,
                    count,
                } => {
                    let output = output.to_persistent(serde_state).await;
                    PersistentTask::Producer(PersistentProducerState::S0 { output, count })
                }
            },
            Task::Consumer(state) => match state {
                ConsumerState::S0 { input_vec, count } => todo!()/*ConsumerState::S0 { input, count } => {
                    let input = input.to_persistent(serde_state).await;
                    PersistentTask::Consumer(PersistentConsumerState::S0 { input, count })
                }*/
            },
        }
    }
    /*pub async fn from_persistent_task(self, serde_state: &mut SerdeState) -> Task { //Not used
        todo!()
    }*/
}

impl PersistentTask {
    pub async fn push_to_vec(self, serialize_task_vec: &mut SerializeTaskVec) {
        //push task into a vector which will later on be serialized
        //the snapshots/states will be stored on memory until the whole checkpointing is done
        serialize_task_vec.task_vec.push(self);
    }
}


pub async fn serialize_state(serde_state: &mut SerdeState) {
    //serializing the vector with all of its snapshot elements
    
    let bytes = serde_json::to_string(&serde_state.persistent_task_vec).unwrap();
    println!("Serialized vec: {:?}", bytes);

    save_persistent(bytes).await;
}
pub async fn save_persistent(serialized_vec: String) {
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

    println!("DONE!, {}", serialized_vec);

    println!("saving checkpoint to persistent disc");
}

pub async fn load_persistent() -> String {
    //open file
    //load the checkpoint json string
    //return the string
    let mut serialized_vec: String = String::from("");
    let file = OpenOptions::new()
        .read(true)
        .create(false)
        .open("serialized_checkpoints.txt")
        .await;
    file.unwrap()
        .read_to_string(&mut serialized_vec)
        //file.unwrap().read_to_string()
        .await
        .unwrap();

    println!("LOADED Persistent!, {}", serialized_vec);
    serialized_vec
}

pub async fn load_deserialize(serialized_vec: String, ptr_vec_hashmap: &mut HashMap<u64, u64>) -> Vec<Task> {
    //1:deserialize
    //2:Link the buffers to the producer
    //2:return the vec

    println!("Start deserialize!");
    //deserialize
    let mut tasks: Vec<Task> = Vec::new();
    println!("Serialized vec: {}", &serialized_vec);
    let mut persistent_tasks: Vec<PersistentTask> = serde_json::from_str(&serialized_vec).unwrap();
    //let mut ptr_vec_hashmap: HashMap<u64, u64> = HashMap::new();

    //reversing the persistent_task vector for deserialization, the consumers will have buffers, while the producers will have empty buffers.
    persistent_tasks.reverse();

    for persistent_task in persistent_tasks {
        match persistent_task {
            PersistentTask::Consumer(state) => match state {
                PersistentConsumerState::S0 { input, count } => {
                    //look up in the hashtable to find value of ptr and check if the value is empty vec
                    let input = PullChan::from_persistent(input, ptr_vec_hashmap);
                    todo!()//tasks.push(Task::Consumer(ConsumerState::S0 { input, count }));
                }
            },
            PersistentTask::Producer(state) => match state {
                PersistentProducerState::S0 { output, count } => {
                    let output = PushChan::from_persistent(output, ptr_vec_hashmap);
                    tasks.push(Task::Producer(ProducerState::S0 { output, count }));
                }
            },
        }
    }
    tasks
}

impl<T: Clone> PushChan<T> {
    //producer: used to check if the buffer excists in the hashset, otherwise insert it into the hashset
    pub async fn to_persistent(&self, serde_state: &mut SerdeState) -> PersistentPushChan<T> {
        let queue = self.0.queue.lock().await;
        let ptr = std::sync::Arc::into_raw(self.0.clone()) as *const ();
        let buffer = if serde_state.serialised.contains(&ptr) {
            //Some(vec![])
            None
        } else {
            //Some(vec![])
            None
        };
        PersistentPushChan {
            uid: ptr as u64,
            buffer, //The buffer should be only stored by consumer, thus empty buffer given to producer
        } ////////////^This is done due of consistency of the snapshot and the placement of marker message in the buffer
    }
    pub fn from_persistent(
        input: PersistentPushChan<T>,
        ptr_vec_hashmap: &mut HashMap<u64, u64>, // TODO: Should be &mut SerdeState
    ) -> Self {
        let input = if let Some(new_uid) = ptr_vec_hashmap.get(&input.uid) {
            PushChan::from_uid(*new_uid)
        } else {
            //insert into the hashtable,
            let new_input = PushChan::from_vec(input.buffer.unwrap());
            ptr_vec_hashmap.insert(input.uid, new_input.get_uid());
            new_input
        };
        input
    }
}

impl<T: Clone> PullChan<T> {
    //consumer: used to check if the buffer excists in the hashset, otherwise insert it into the hashset
    pub async fn to_persistent(&self, serde_state: &mut SerdeState) -> PersistentPullChan<T> {
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

    pub fn from_persistent(
        input: PersistentPullChan<T>,
        ptr_vec_hashmap: &mut HashMap<u64, u64>, // TODO: Should be &mut SerdeState
    ) -> Self {
        let input = if let Some(new_uid) = ptr_vec_hashmap.get(&input.uid) {
            PullChan::from_uid(*new_uid)
        } else {
            //insert into the hashtable,
            let new_input = PullChan::from_vec(input.buffer.unwrap());
            ptr_vec_hashmap.insert(input.uid, new_input.get_uid());
            new_input
        };
        input
    }
}

//insert to hashmap anyway
//replace if the value is empty [] using len prev_val.len < curr_val.len ->replace
