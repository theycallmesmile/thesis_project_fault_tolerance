use tokio::sync::oneshot;

use std::collections::HashMap;
use std::collections::HashSet;

use std::time::Duration;

use async_std::task;

//Serialize module
use crate::serialize::serialize_func;
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
    chan_push: PushChan<TaskToManagerMessage>,
    chan_pull: PullChan<TaskToManagerMessage>,
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
pub struct Context {
    pub manager_send: PushChan<TaskToManagerMessage>,
    pub manager_recv: PullChan<ManagerToTaskMessage>,
}

impl Task {
    fn spawn(self, ctx: Context) {
        match self {
            Task::Consumer(state) => async_std::task::spawn(state.execute(ctx)),
            Task::Producer(state) => async_std::task::spawn(state.execute(ctx)),
        };
    }
}

impl Manager {
    async fn run(mut self) {
        //let mut snapshot_hashmap:HashMap<*const (), TaskToManagerMessage> = HashMap::new();
        //let mut raw_pointer: *const ();

        //list of the operators
        let prod_operators = spawn_operators(&mut self).await;

        //Sleeping before sending a marker to the source-producers
        //task::sleep(Duration::from_secs(4)).await;

        //loop to send markers to source-producers
        /*for prod_op in prod_operators {
            //prod_op

        }*/

        task::sleep(Duration::from_secs(2)).await;

        loop {
            println!("WAITING FOR TASK TO RETURN");
            //let task = self.chan_pull.pullTEST().await;
            let task = self.chan_pull.pull_manager().await;
            println!("Done with return, the task val: {:#?}", task);

            match task {
                TaskToManagerMessage::Serialise(state, promise) => {
                    // state.serialise();
                    println!("will send promise!");
                    task::sleep(Duration::from_secs(2)).await;
                    promise.send(1);
                    println!("Sent promise!!");
                }
            };
            //println!("the task: {:?}",task);
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

async fn spawn_operators(self_manager: &mut Manager) -> Vec<Task> {
    //creating channel for communication between a producer and consumer
    let (prod_push, prod_pull) = channel::<Event<()>>();
    //creating the states for producer and consumer operators
    let prod_state = ProducerState::S0 {
        output: prod_push, //data to buffer
        count: 0,
    };
    let cons_state = ConsumerState::S0 {
        input: prod_pull, //taking data from buffer
        count: 0,
    };

    //creating channels for communication with and from manager, messages will be: markers, snapshot state and acks
    //Every operator needs its own communication channel for manager
    let (manager_push1, manager_pull1) =
        channel_manager::<TaskToManagerMessage, ManagerToTaskMessage>();
    let (manager_push2, manager_pull2) =
        channel_manager::<TaskToManagerMessage, ManagerToTaskMessage>();

    //Contexts will be used by producers and consumers to send the state to manager and receive and ACK to unblock
    let ctx1 = Context {
        manager_recv: manager_pull1,
        manager_send: self_manager.chan_push.clone(),
    };
    let ctx2 = Context {
        manager_recv: manager_pull2,
        manager_send: self_manager.chan_push.clone(),
    };

    let prod_task = Task::Producer(prod_state); //producer operator
    let cons_task = Task::Consumer(cons_state); //consumer operator

    let source_producer_op_vec = vec![prod_task.clone()];

    prod_task.spawn(ctx1); //spawning producer operator
    cons_task.spawn(ctx2);

    source_producer_op_vec
}

pub fn manager() {
    //let mut snapshot_hashmap: HashMap<u64,*const ()> = HashMap::new();

    //push: from the operator to the manager(fe, state), filling the buffer
    //pull: manager can pull from the buffer
    let (push, pull) = channel::<TaskToManagerMessage>();

    let manager_state = Manager {
        chan_push: push,
        chan_pull: pull,
        serde_state: SerdeState::default(),
    };

    async_std::task::spawn(manager_state.run());
    println!("manager operator spawned!");
}
