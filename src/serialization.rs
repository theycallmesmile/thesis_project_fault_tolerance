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
//ConsumerProducer Module
use crate::consumer_producer::ConsumerProducerState;
use crate::consumer_producer::PartialConsumerProducerState;

//Manager module
use crate::manager::SerializeTaskVec;
use crate::manager::Task;

//Channel module
use crate::channel::PullChan;
use crate::channel::PushChan;

//Producer module
use crate::producer::ProducerState;

//Consumer module
use crate::consumer::ConsumerState;

//Shared module
use crate::shared::Event;

unsafe impl Send for SerdeState {}
unsafe impl Sync for SerdeState {}

#[derive(Default, Clone)]
pub struct SerdeState {
    //for deserialization
    pub serialised: HashSet<*const ()>, //raw pointer adress av arc chan
    //pub deserialised: HashMap<*const (), *const ()>, //adress av  |
    pub deserialised: HashMap<u64, u64>,
    pub persistent_task_map: HashMap<i32, Vec<PersistentTask>>, 
}
#[derive(Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Clone)]
pub enum PersistentTask {
    Consumer(PersistentConsumerState),
    Producer(PersistentProducerState),
    ConsumerProducer(PersistentConsumerProducerState),
}

#[derive(Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Clone)]
pub enum PartialPersistentTask {
    Consumer(PersistentConsumerState),
    Producer(PersistentProducerState),
    ConsumerProducer(PersistentConsumerProducerState),
    PartialConsumerProducer(PartialPersistentConsumerProducerState),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PersistentProducerState {
    S0 {
        output0: PersistentPushChan<Event<(u64, u64)>>,
        count: i32,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PersistentConsumerState {
    S0 {
        input0: PersistentPullChan<Event<(String, String)>>,
        count: i32,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PersistentConsumerProducerState {
    S0 {
        stream0: PersistentPullChan<Event<(u64, u64)>>,
        stream1: PersistentPullChan<Event<(u64, u64)>>,
        stream2: PersistentPullChan<Event<(u64, u64)>>,
        out0: PersistentPushChan<Event<(String, String)>>,
        out1: PersistentPushChan<Event<(String, String)>>,
        count: i32,
    },
    S1 {
        stream0: PersistentPullChan<Event<(u64, u64)>>,
        stream1: PersistentPullChan<Event<(u64, u64)>>,
        stream2: PersistentPullChan<Event<(u64, u64)>>,
        out0: PersistentPushChan<Event<(String, String)>>,
        out1: PersistentPushChan<Event<(String, String)>>,
        count: i32,
        data: (u64, u64),
    },
    S2 {
        stream0: PersistentPullChan<Event<(u64, u64)>>,
        stream1: PersistentPullChan<Event<(u64, u64)>>,
        stream2: PersistentPullChan<Event<(u64, u64)>>,
        out0: PersistentPushChan<Event<(String, String)>>,
        out1: PersistentPushChan<Event<(String, String)>>,
        count: i32,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PartialPersistentConsumerProducerState {
    S0 {
        stream0: PersistentPullChan<Event<(u64, u64)>>,
        stream1: PersistentPullChan<Event<(u64, u64)>>,
        out0: PersistentPushChan<Event<(String, String)>>,
    },
    S1 {
        stream0: PersistentPullChan<Event<(u64, u64)>>,
        stream1: PersistentPullChan<Event<(u64, u64)>>,
        out0: PersistentPushChan<Event<(String, String)>>,
        data: (u64, u64),
    },
    S2 {
        stream2: PersistentPullChan<Event<(u64, u64)>>,
        out1: PersistentPushChan<Event<(String, String)>>,
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
    pub log: Option<Vec<T>>,
}

impl Task {
    pub async fn to_partial_persistent_task(self) -> PartialPersistentTask {
        let mut serialised: HashSet<u64> = HashSet::new();
        match self {
            Task::Producer(state) => match state {
                ProducerState::S0 {
                    out0,
                    count,
                } => {
                    let loc_output0 = out0.to_persistent(&mut serialised).await;
                    PartialPersistentTask::Producer(PersistentProducerState::S0 { output0: loc_output0, count })
                }
            },
            Task::Consumer(state) => match state { //never needed?
                ConsumerState::S0 { stream0, count } => {
                    let loc_input0 = stream0.to_persistent(&mut serialised).await;
                    PartialPersistentTask::Consumer(PersistentConsumerState::S0 { input0: loc_input0, count })
                }
            },
            Task::PartialConsumerProducer(state) => match state {
                PartialConsumerProducerState::S0 { stream0, stream1, out0 } => {
                    let loc_input0 = stream0.to_persistent(&mut serialised).await;
                    let loc_input1 = stream1.to_persistent(&mut serialised).await;
                    let loc_out0 = out0.to_persistent(&mut serialised).await;
                    
                    PartialPersistentTask::PartialConsumerProducer(PartialPersistentConsumerProducerState::S0 { stream0: loc_input0, stream1: loc_input1, out0: loc_out0 } )
                },
                PartialConsumerProducerState::S1 { stream0, stream1, out0, data } => {
                    let loc_input0 = stream0.to_persistent(&mut serialised).await;
                    let loc_input1 = stream1.to_persistent(&mut serialised).await;
                    let loc_out0 = out0.to_persistent(&mut serialised).await;
                    
                    PartialPersistentTask::PartialConsumerProducer(PartialPersistentConsumerProducerState::S1 { stream0: loc_input0, stream1: loc_input1, out0: loc_out0, data } )
                },
                PartialConsumerProducerState::S2 { stream2, out1 } => {
                    let loc_input2 = stream2.to_persistent(&mut serialised).await;
                    let loc_out1 = out1.to_persistent(&mut serialised).await;
                    
                    PartialPersistentTask::PartialConsumerProducer(PartialPersistentConsumerProducerState::S2 { stream2: loc_input2, out1: loc_out1 } )
                },
            },
            Task::ConsumerProducer(state) =>  match state {
                ConsumerProducerState::S0 { stream0, stream1, stream2, out0, out1, count } => {
                    let loc_input0 = stream0.to_persistent(&mut serialised).await;
                    let loc_input1 = stream1.to_persistent(&mut serialised).await;
                    let loc_input2 = stream2.to_persistent(&mut serialised).await;
                    let loc_out0 = out0.to_persistent(&mut serialised).await;
                    let loc_out1 = out1.to_persistent(&mut serialised).await;

                    PartialPersistentTask::ConsumerProducer(PersistentConsumerProducerState::S0 { stream0: loc_input0, stream1: loc_input1, stream2: loc_input2, out0: loc_out0, out1: loc_out1, count } )
                },
                ConsumerProducerState::S1 { stream0, stream1, stream2, out0, out1, count, data } => {
                    let loc_input0 = stream0.to_persistent(&mut serialised).await;
                    let loc_input1 = stream1.to_persistent(&mut serialised).await;
                    let loc_input2 = stream2.to_persistent(&mut serialised).await;
                    let loc_out0 = out0.to_persistent(&mut serialised).await;
                    let loc_out1 = out1.to_persistent(&mut serialised).await;
                    
                    PartialPersistentTask::ConsumerProducer(PersistentConsumerProducerState::S1 { stream0: loc_input0, stream1: loc_input1, stream2: loc_input2, out0: loc_out0, out1: loc_out1, count, data } )
                },
                ConsumerProducerState::S2 { stream0, stream1, stream2, out0, out1, count } => {
                    let loc_input0 = stream0.to_persistent(&mut serialised).await;
                    let loc_input1 = stream1.to_persistent(&mut serialised).await;
                    let loc_input2 = stream2.to_persistent(&mut serialised).await;
                    let loc_out0 = out0.to_persistent(&mut serialised).await;
                    let loc_out1 = out1.to_persistent(&mut serialised).await;
                    
                    PartialPersistentTask::ConsumerProducer(PersistentConsumerProducerState::S2 { stream0: loc_input0, stream1: loc_input1, stream2: loc_input2, out0: loc_out0, out1: loc_out1, count } )
                },
           
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


pub async fn serialize_state(state: &Vec<PersistentTask>){//(serde_state: &mut SerdeState) {
    //serializing the vector with all of its snapshot elements
    //let bytes = serde_json::to_string(&serde_state.persistent_task_vec).unwrap();
    let bytes = serde_json::to_string(&state).unwrap();
    //println!("Serialized vec: {:?}", bytes);

    save_persistent(bytes).await;
}
pub async fn save_persistent(serialized_vec: String) {
    //create or open file
    //save to persistent disc
    //Saving checkpoint to file (appends atm)
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(true)
        .create(true)
        .open("serialized_checkpoints.txt")
        .await;
    file.unwrap()
        .write_all(serialized_vec.as_bytes())
        .await
        .unwrap();
    println!("Successfully saved checkpoint to persistent disc");
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
    //2:Link the buffers
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
                PersistentConsumerState::S0 { input0, count } => {
                    //look up in the hashtable to find value of ptr and check if the value is empty vec
                    let loc_input0 = PullChan::from_persistent(input0, ptr_vec_hashmap);
                    tasks.push(Task::Consumer(ConsumerState::S0 { stream0: loc_input0, count })); //todo: match the state!
                }
            },
            PersistentTask::Producer(state) => match state {
                PersistentProducerState::S0 { output0, count } => {
                    let loc_output0 = PushChan::from_persistent(output0, ptr_vec_hashmap);
                    tasks.push(Task::Producer(ProducerState::S0 { out0: loc_output0, count })); //todo: match the state!
                }
            },
            PersistentTask::ConsumerProducer(state) => match state {
                PersistentConsumerProducerState::S0 { stream0, stream1, stream2, out0, out1, count } => {
                    let loc_input0 = PullChan::from_persistent(stream0, ptr_vec_hashmap);
                    let loc_input1 = PullChan::from_persistent(stream1, ptr_vec_hashmap);
                    let loc_input2 = PullChan::from_persistent(stream2, ptr_vec_hashmap);
                    let loc_output0 = PushChan::from_persistent(out0, ptr_vec_hashmap);
                    let loc_output1 = PushChan::from_persistent(out1, ptr_vec_hashmap);
                    tasks.push(Task::ConsumerProducer(ConsumerProducerState::S0 { stream0: loc_input0, stream1: loc_input1, stream2: loc_input2, out0: loc_output0, out1: loc_output1, count } ));
                },
                PersistentConsumerProducerState::S1 { stream0, stream1, stream2, out0, out1, count, data } => {
                    let loc_input0 = PullChan::from_persistent(stream0, ptr_vec_hashmap);
                    let loc_input1 = PullChan::from_persistent(stream1, ptr_vec_hashmap);
                    let loc_input2 = PullChan::from_persistent(stream2, ptr_vec_hashmap);
                    let loc_output0 = PushChan::from_persistent(out0, ptr_vec_hashmap);
                    let loc_output1 = PushChan::from_persistent(out1, ptr_vec_hashmap);
                    tasks.push(Task::ConsumerProducer(ConsumerProducerState::S1 { stream0: loc_input0, stream1: loc_input1, stream2: loc_input2, out0: loc_output0, out1: loc_output1, count, data } ));
                },
                PersistentConsumerProducerState::S2 { stream0, stream1, stream2, out0, out1, count } => {
                    let loc_input0 = PullChan::from_persistent(stream0, ptr_vec_hashmap);
                    let loc_input1 = PullChan::from_persistent(stream1, ptr_vec_hashmap);
                    let loc_input2 = PullChan::from_persistent(stream2, ptr_vec_hashmap);
                    let loc_output0 = PushChan::from_persistent(out0, ptr_vec_hashmap);
                    let loc_output1 = PushChan::from_persistent(out1, ptr_vec_hashmap);
                    tasks.push(Task::ConsumerProducer(ConsumerProducerState::S2 { stream0: loc_input0, stream1: loc_input1, stream2: loc_input2, out0: loc_output0, out1: loc_output1, count } ));
                },
            },
        }
    }
    tasks
}

impl<T: Clone> PushChan<T> {
    //producer: used to check if the buffer excists in the hashset, otherwise insert it into the hashset
    pub async fn to_persistent(&self, serialised: &mut HashSet<u64>) -> PersistentPushChan<T> {
        let queue = self.0.queue.lock().await;
        let ptr = std::sync::Arc::into_raw(self.0.clone()) as *const () as u64;
        let buffer = if serialised.contains(&ptr) {
            None
        } else {
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
            //let new_input = PushChan::from_vec(input.buffer.unwrap());
            let new_input = PushChan::new();
            ptr_vec_hashmap.insert(input.uid, new_input.get_uid());
            new_input
        };
        input
    }
}

impl<T: Clone> PullChan<T> {
    //consumer: used to check if the buffer excists in the hashset, otherwise insert it into the hashset
    pub async fn to_persistent(&self, serialised: &mut HashSet<u64>) -> PersistentPullChan<T> { //TODO: REMOVE BUFFER, DONT NEED TO SAVE BUFFER SINCE IT DOESNT MATTER. IT WILL BE EMPTY DURING RECOVERY
        let loc_log = &self.get_log().await;
        let (loc_buffer,loc_ptr) = &self.get_buffer(serialised).await;
        PersistentPullChan {
            uid: loc_ptr.clone(),
            buffer: loc_buffer.clone(),
            log: loc_log.clone(),
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
            //let tez = input.log.unwrap();
            
            let new_input = PullChan::from_vec_with_log(input.log.unwrap());
            ptr_vec_hashmap.insert(input.uid, new_input.get_uid());
            //println!("TESTING_LOG1: {:?} {:?}", tez.is_empty(), ptr_vec_hashmap.get(&input.uid));
            new_input
        };
        input
    }

    pub async fn get_log(&self) -> Option<Vec<T>>{
        let log = self.0.log.lock().await;
        Some(log.iter().cloned().collect())
    }

    pub async fn get_buffer(&self, serialised: &mut HashSet<u64>) -> (Option<Vec<T>>, u64) {
        let queue = self.0.queue.lock().await;
        let ptr = std::sync::Arc::into_raw(self.0.clone()) as *const () as u64;
        let buffer = if serialised.contains(&ptr) {
            None
        } else {
            serialised.insert(ptr);
            Some(queue.iter().cloned().collect())
        };
        drop(queue);
        (buffer, ptr as u64)
    }
}