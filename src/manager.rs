use tokio::sync::oneshot;

use std::collections::HashMap;
use std::collections::HashSet;

use std::time::Duration;

use async_std::task;

//Serialize module
use crate::serialize::serialize_func_old;
use crate::serialize::SerdeState;

//Consumer module
use crate::consumer::ConsumerState;

//Producer module
use crate::producer::Event;
use crate::producer::ProducerState;

//Channel module
use crate::channel::channel;
use crate::channel::channel_manager;
use crate::channel::PullChan;
use crate::channel::PushChan;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ManagerToTaskMessage {
    Snapshot,
    Serialised,
}

//Manager state with channels used for commincation between the operators and manager operator
struct Manager {
    state_chan_push: PushChan<TaskToManagerMessage>,
    state_chan_pull: PullChan<TaskToManagerMessage>,
    marker_chan_push: PushChan<Event<()>>,
    marker_chan_pull: PullChan<Event<()>>,
    marker_chan_vec: Vec<PushChan<Event<()>>>,
    serde_state: SerdeState,
}

#[derive(Debug)]
pub enum TaskToManagerMessage {
    Serialise(Task, oneshot::Sender<u64>),
}

#[derive(Debug, Clone)]
pub enum Task {
    Consumer(ConsumerState),
    Producer(ProducerState),
}

#[derive(Debug)]
pub struct ProducerContext {
    pub marker_manager_recv: PullChan<Event<()>>,
    pub state_manager_send: PushChan<TaskToManagerMessage>,
}
#[derive(Debug)]
pub struct ConsumerContext {
    pub state_manager_send: PushChan<TaskToManagerMessage>,
}


impl Task {
    fn spawn_prod(self, ctx: ProducerContext) {
        match self {
            Task::Consumer(state) => {},
            Task::Producer(state) => {async_std::task::spawn(state.execute(ctx));},
        };
    }

    fn spawn_con(self, ctx: ConsumerContext) {
        match self {
            Task::Consumer(state) => {async_std::task::spawn(state.execute(ctx));},
            Task::Producer(state) => {},
        };
    }

}

impl Manager {
    async fn run(mut self) {
        //let mut snapshot_hashmap:HashMap<*const (), TaskToManagerMessage> = HashMap::new();
        //let mut raw_pointer: *const ();

        //creating hashmap and hashset
        let mut serde_state = SerdeState {
            serialised: HashSet::new(),
            deserialised: HashMap::new(),
        };

        //init the operators
        spawn_operators(&mut self).await;

        //Sleeping before sending a marker to the source-producers
        task::sleep(Duration::from_secs(2)).await;
        //loop to send markers to source-producers
        for marker_chan in self.marker_chan_vec {
            marker_chan.push(Event::Marker).await;
        }

        loop {
            match self.state_chan_pull.pull_manager().await {
                TaskToManagerMessage::Serialise(state, promise) => {



                    state.to_persistent_task(&mut serde_state).await;

                    task::sleep(Duration::from_secs(2)).await;
                    promise.send(1);
                }
            };
        }
    }
}
/*
#[derive(Debug)]
enum ProducerState {
    S0 {
        output: PushChan<Event<()>>,
        count: i32,
    },
}

#[derive(Serialize, Deserialize, Debug)]
enum PersistentProducerState {
    S0 {
        output: PersistentPushChan<()>,
        count: i32,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct PersistentPushChan<T> {
    uid: u64,
    buffer: Option<Vec<T>>
}


#[derive(Serialize, Deserialize, Debug)]
enum ConsumerState {
    S0 {
        input: PersistentPullChan<()>,
        count: i32,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct PersistentPullChan<T> {
    uid: u64,
    buffer: Option<Vec<T>>
}

#[derive(Serialize, Deserialize, Debug)]
enum PersistentConsumerState {
    S0 {
        input: Vec<Event<()>>,
        count: i32,
    },
}*/

//Rawpointers of self
//let raw_pointer: *const () = Arc::into_raw(slf) as *const ();
//let new_chan = unsafe {&*raw_pointer}.clone();

async fn spawn_operators(self_manager: &mut Manager) {
    //creating channel for communication between a producer and consumer
    let (prod_push, prod_pull) = channel::<Event<()>>();


    //vector with all source producers marker channels
    self_manager.marker_chan_vec = vec![self_manager.marker_chan_push.clone()];

    //creating the states for producer and consumer operators
    let prod_state = ProducerState::S0 {
        output: prod_push, //data to buffer
        //marker_rec: marker_rec,
        count: 0,
    };
    let cons_state = ConsumerState::S0 {
        input: prod_pull, //taking data from buffer
        count: 0,
    };

    //Contexts will be used by producers and consumers to send the state to manager and receive and ACK to unblock
    //marker_managers will be used for communication about marker. state_manager will be used for communication about state snapshot¨
    let prod_ctx = ProducerContext{
        marker_manager_recv: self_manager.marker_chan_pull.clone(),
        state_manager_send: self_manager.state_chan_push.clone(),
    };

    let cons_ctx = ConsumerContext{
        state_manager_send: self_manager.state_chan_push.clone(),
    };


    let prod_task = Task::Producer(prod_state); //producer operator
    let cons_task = Task::Consumer(cons_state); //consumer operator

    prod_task.spawn_prod(prod_ctx); //spawning producer operator
    cons_task.spawn_con(cons_ctx); //spawning consumer operator
}

pub fn manager() {
    //push: from the operator to the manager(fe, state), filling the buffer
    //pull: manager can pull from the buffer
    let (state_push, state_pull) = channel::<TaskToManagerMessage>();

    let(marker_push, marker_pull) = channel::<Event<()>>();

    let manager_state = Manager {
        state_chan_push: state_push, // channel for operator state, operator -> buffer
        state_chan_pull: state_pull, // channel for operator state, buffer -> manager
        marker_chan_push: marker_push, //source producer marker channel, manager -> buffer
        marker_chan_pull: marker_pull, //source producer marker channel, buffer -> producer
        marker_chan_vec: Vec::new(), //all source producer marker channels
        serde_state: SerdeState::default(), //for serialization and deserialization
    };

    async_std::task::spawn(manager_state.run());
    println!("manager operator spawned!");
}
