use tokio::sync::oneshot;

use std::collections::HashMap;
use std::collections::HashSet;

use std::time::Duration;
use std::vec;
use tokio::time;

use async_std::sync::Mutex;
use async_std::task;

use serde::Deserialize;
use serde::Serialize;

use crate::channel::channel_vec;
use crate::channel::CAPACITY;
use crate::consumerProducer::ConsumerProducerState;

//Serialize module
use crate::serialization::load_deserialize;
use crate::serialization::load_persistent;
use crate::serialization::save_persistent;
use crate::serialization::serialize_state;
use crate::serialization::PersistentConsumerState;
use crate::serialization::PersistentProducerState;
use crate::serialization::PersistentTask;
use crate::serialization::SerdeState;

//Consumer module
use crate::consumer::ConsumerState;

//Producer module
use crate::producer::ProducerState;

//Channel module
use crate::channel::channel;
use crate::channel::channel_load;
use crate::channel::channel_manager;
use crate::channel::PullChan;
use crate::channel::PushChan;

//Shared module
use crate::shared::Event;

use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ManagerToTaskMessage {
    Snapshot,
    Serialised,
}

//Manager state with channels used for commincation between the operators and manager operator
pub struct Manager {
    state_chan_push: PushChan<TaskToManagerMessage>,
    state_chan_pull: PullChan<TaskToManagerMessage>,
    marker_chan_vec: Vec<(PushChan<Event<()>>, PullChan<Event<()>>)>,
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
    ConsumerProducer(ConsumerProducerState),
}

#[derive(Debug)]
pub struct Context {
    pub marker_manager_recv: Option<PullChan<Event<()>>>,
    pub state_manager_send: PushChan<TaskToManagerMessage>,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct SerializeTaskVec {
    pub task_vec: Vec<PersistentTask>,
}

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub enum Operators {
    SourceProducer(i32),
    Consumer(i32),
    ConsumerProducer(i32),
}

#[derive(Debug)]
pub enum OperatorChannels {
    Push(PushChan<Event<i32>>),
    Pull(PullChan<Event<i32>>),
}

impl Task {
    fn spawn(self, ctx: Context) -> async_std::task::JoinHandle<()> {
        match self {
            Task::Consumer(state) => async_std::task::spawn(state.execute_unoptimized(ctx)),
            Task::Producer(state) => async_std::task::spawn(state.execute_unoptimized(ctx)),
            Task::ConsumerProducer(state) => async_std::task::spawn(state.execute_unoptimized(ctx)),
        }
    }
}

impl Manager {
    async fn run(mut self, operator_connections: HashMap<Operators, Vec<Operators>>) {
        let mut interval = time::interval(time::Duration::from_millis(100));
        let mut snapshot_timeout_counter = 0;
        let mut snapshot_resend_chance = true;

        //creating hashmap and hashset
        let mut serde_state = SerdeState {
            serialised: HashSet::new(),
            deserialised: HashMap::new(),
            persistent_task_vec: Vec::new(),
        };

        let mut serialize_task_vec: Vec<Task> = Vec::new(); //given to the serialize function

        //init the operators and returns amount of spawned operato&mut rs
        let mut operator_spawn_vec = spawn_operators(&mut self, operator_connections).await;
        let mut operator_amount = operator_spawn_vec.len();
        let mut operator_counter = 0;

        //Giving permission to sourceProducers to create and send X amount of messages to connected operators.
        for prod_chan in &self.marker_chan_vec {
            prod_chan.0.push(Event::MessageAmount(20)).await;
        }
        //Sleeping before sending a marker to the source-producers
        task::sleep(Duration::from_secs(2)).await;
        //loop to send markers to source-producers
        //self.send_markers().await;
        println!("Resuming!");
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if snapshot_timeout_counter == 3000 && snapshot_resend_chance == true {
                        println!("One or more operator does not respond, rollbacking to the latest checkpoint!");
                        while let Some(operator) = operator_spawn_vec.pop() {
                            operator.cancel().await;
                        }

                        task::sleep(Duration::from_secs(2)).await;
                        serde_state.persistent_task_vec.clear(); //clearing the vector
                        let loaded_checkpoint = load_persistent().await; //the json string
                        serialize_task_vec = load_deserialize(loaded_checkpoint, &mut serde_state.deserialised).await; //deserialized checkpoint vec

                        operator_spawn_vec = respawn_operator(&mut self, serialize_task_vec).await; //respawning task operators

                        //resetting the values
                        operator_amount = operator_spawn_vec.len();
                        operator_counter = 0;

                        //self.send_markers().await; //sending new markers
                        break;

                    } else if snapshot_timeout_counter == 3000 && snapshot_resend_chance == false {
                        println!("At least of the operators does not respond, resending markers!");
                        snapshot_timeout_counter = 0;
                        snapshot_resend_chance = true;
                        serde_state.persistent_task_vec.clear();
                        operator_counter = 0;
                        //self.send_markers().await;
                    }
                    else {
                        snapshot_timeout_counter += 1;
                    }
                },
                msg = self.state_chan_pull.pull_manager() => {
                    match msg {
                        TaskToManagerMessage::Serialise(state, promise) => {
                            let persistent_task = state.to_persistent_task(&mut serde_state).await;

                            serde_state.persistent_task_vec.push(persistent_task);
                            //persistent_task.push_to_vec(&mut serialize_task_vec).await;// <-- remove and change to persistent_task_vec?
                            //task::sleep(Duration::from_secs(2)).await;
                            promise.send(1);
                            snapshot_timeout_counter = 0;
                            snapshot_resend_chance = false;
                            //operator_counter +=1;
                            println!("operator_amount: {}, operator_counter: {}",&operator_amount,&operator_counter);
                            if (operator_amount == operator_counter){
                                println!("CHECKPOINTING!!");
                                serialize_state(&mut serde_state).await;
                                break;
                            }
                        },
                    };
                },
            }
        }
        println!("Dobby is a free elf now!");
    }
    async fn send_markers(&self) {
        //sending markers to the source-producers
        for marker_chan in &self.marker_chan_vec {
            marker_chan.0.push(Event::Marker).await;
        }
        println!("Done sending the markers to source-producers.");
    }
}

pub async fn spawn_operators(
    self_manager: &mut Manager,
    operator_connections: HashMap<Operators, Vec<Operators>>,
) -> Vec<async_std::task::JoinHandle<()>> {
    let mut operator_channel: HashMap<
        Operators,
        Vec<(PushChan<Event<i32>>, PullChan<Event<i32>>)>,
    > = HashMap::new(); //store operator in/out channels
    let mut operator_state_chan: HashMap<Operators, Vec<OperatorChannels>> = HashMap::new();

    let mut task_op_spawn_vec = Vec::new();
    let mut marker_vec_counter = 0;

    //creating push and pull channels. Assigning push channels to producer and consumerProducer operators
    init_channels(
        &operator_connections,
        &mut operator_channel,
        &mut operator_state_chan,
    )
    .await;

    //Assigning pull channels to consumer and consumerProducer operators.
    init_pull_channels(
        &operator_connections,
        &mut operator_channel,
        &mut operator_state_chan,
    )
    .await;

    //spawning the tasks
    for operator in &operator_state_chan {
        match operator.0 {
            Operators::SourceProducer(_) => {
                let chan =
                    operator_channel_to_push_vec(operator_state_chan.get(operator.0).unwrap())
                        .await;
                let prod_state = ProducerState::S0 {
                    out0: chan[0].to_owned(),
                    count: 0,
                };
                let prod_ctx = Context {
                    marker_manager_recv: Some(
                        self_manager.marker_chan_vec[marker_vec_counter].1.clone(),
                    ),
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                let prod_task = Task::Producer(prod_state);
                task_op_spawn_vec.push(prod_task.spawn(prod_ctx));
                marker_vec_counter += 1;
            }
            Operators::Consumer(_) => {
                let in_chan =  operator_channel_to_pull_vec(operator_state_chan.get(operator.0).unwrap()).await;
                let cons_state = ConsumerState::S0 {
                    stream0: in_chan[0].to_owned(),
                    count: 0,
                };
                let cons_ctx = Context {
                    marker_manager_recv: None,
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                let cons_task = Task::Consumer(cons_state);
                task_op_spawn_vec.push(cons_task.spawn(cons_ctx));
            }
            Operators::ConsumerProducer(_) => {
                let in_chan =
                    operator_channel_to_pull_vec(operator_state_chan.get(operator.0).unwrap())
                        .await;
                let out_chan =
                    operator_channel_to_push_vec(operator_state_chan.get(operator.0).unwrap())
                        .await;
                let cons_prod_state = ConsumerProducerState::S0 {
                    stream0: in_chan[0].to_owned(),
                    stream1: in_chan[1].to_owned(),
                    stream2: in_chan[2].to_owned(),
                    out0: out_chan[0].to_owned(),
                    out1: out_chan[1].to_owned(),
                    count: 0,
                };
                let cons_prod_ctx = Context {
                    marker_manager_recv: None,
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                let cons_prod_task = Task::ConsumerProducer(cons_prod_state);
                task_op_spawn_vec.push(cons_prod_task.spawn(cons_prod_ctx));
            }
        }
    }
    task_op_spawn_vec
}

async fn init_channels(
    operator_connections: &HashMap<Operators, Vec<Operators>>,
    operator_channel: &mut HashMap<Operators, Vec<(PushChan<Event<i32>>, PullChan<Event<i32>>)>>,
    operator_state_chan: &mut HashMap<Operators, Vec<OperatorChannels>>,
) {
    //creating channels for source producers and consumer_producers. Every prod/con_prod will have a separate channel with connected operator
    //create as a new func?
    for connection in operator_connections {
        let chan_vec = channel_vec::<Event<i32>>(connection.1.clone().len());
        operator_channel.insert(connection.0.to_owned(), chan_vec);
    }
    init_operator_push_channels(operator_connections, operator_channel, operator_state_chan).await;
}

async fn init_operator_push_channels(
    operator_connections: &HashMap<Operators, Vec<Operators>>,
    operator_channel: &mut HashMap<Operators, Vec<(PushChan<Event<i32>>, PullChan<Event<i32>>)>>,
    operator_state_chan: &mut HashMap<Operators, Vec<OperatorChannels>>,
) {
    println!("operator_connections: {:#?}", operator_connections);
    println!("operator_channel: {:#?}",operator_channel);
    println!("operator_state_chan: {:#?}",operator_state_chan);
    for connection in operator_connections {
        //Producer/ConsumerProducer operator_state_chan is given X amount of push for each connected channel in graph.
        let key_chan = operator_channel.get(connection.0).unwrap().clone();
        let mut operator_prod_push_vec: Vec<OperatorChannels> = Vec::new();
        for chan in key_chan {
            operator_prod_push_vec.push(OperatorChannels::Push(chan.0));
        }
        operator_state_chan.insert(connection.0.to_owned(), operator_prod_push_vec);
    }
}

async fn init_pull_channels(
    operator_connections: &HashMap<Operators, Vec<Operators>>,
    operator_channel: &mut HashMap<Operators, Vec<(PushChan<Event<i32>>, PullChan<Event<i32>>)>>,
    operator_state_chan: &mut HashMap<Operators, Vec<OperatorChannels>>,
) {
    //Giving pull channels to the consumer and consumerProducer operators
    for connection in operator_connections {
        println!("operator_state_chan: {:?}", operator_state_chan);

        //going through the vector in the value of hashmap
        let mut count = 0;
        for connection_val in connection.1 {
            match connection_val {
                Operators::SourceProducer(_) => {
                    println!("This should not happen, ERROR!");
                    panic!();
                }
                Operators::Consumer(_) => {
                    if operator_state_chan.contains_key(&connection_val) {
                        //add a new chan to the vector and update the vector
                        let val_chan = operator_channel.get(connection.0).unwrap()[count].1.clone();
                        operator_state_chan
                            .entry(connection_val.to_owned())
                            .and_modify(|e| e.push(OperatorChannels::Pull(val_chan)));
                    } else {
                        let val_chan = operator_channel.get(connection.0).unwrap()[count].1.clone();
                        operator_state_chan.insert(
                            connection_val.to_owned(),
                            vec![OperatorChannels::Pull(val_chan)],
                        );
                    }
                }
                Operators::ConsumerProducer(_) => {
                    if operator_state_chan.contains_key(&connection_val) {
                        //add a new chan to the vector and update the vector
                        let val_chan = operator_channel.get(connection.0).unwrap()[count].1.clone();
                        operator_state_chan
                            .entry(connection_val.to_owned())
                            .and_modify(|e| e.push(OperatorChannels::Pull(val_chan)));
                    } else {
                        //There should always be an entry in the hashmap due of init_operator_push_channels
                        println!("This should not happen, ERROR!");
                        panic!();
                    }
                }
            }
            count += 1;
        }
    }
}

async fn operator_channel_to_pull_vec(
    operator_chan: &Vec<OperatorChannels>,
) -> Vec<PullChan<Event<i32>>> {
    let mut pull_vec = Vec::new();
    for chan in operator_chan {
        match chan {
            OperatorChannels::Push(chan) => {}
            OperatorChannels::Pull(chan) => pull_vec.push(chan.clone()),
        }
    }
    pull_vec
}

async fn operator_channel_to_push_vec(
    operator_chan: &Vec<OperatorChannels>,
) -> Vec<PushChan<Event<i32>>> {
    let mut push_vec = Vec::new();
    for chan in operator_chan {
        match chan {
            OperatorChannels::Push(chan) => push_vec.push(chan.clone()),
            OperatorChannels::Pull(chan) => {}
        }
    }
    push_vec
}

async fn respawn_operator(
    self_manager: &mut Manager,
    task_vec: Vec<Task>,
) -> Vec<async_std::task::JoinHandle<()>> {
    let mut handle_vec: Vec<async_std::task::JoinHandle<()>> = Vec::new();
    let mut marker_vec_counter = 0;
    for task in task_vec {
        let handle = match task {
            Task::Consumer(_) => {
                let cons_ctx = Context {
                    marker_manager_recv: None,
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                task.spawn(cons_ctx)
            }
            Task::Producer(_) => {
                let prod_ctx = Context {
                    marker_manager_recv: Some(
                        self_manager.marker_chan_vec[marker_vec_counter].1.clone(),
                    ),
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                marker_vec_counter += 1;
                task.spawn(prod_ctx)
            }
            Task::ConsumerProducer(_) => {
                let cons_prod_ctx = Context {
                    marker_manager_recv: None,
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                task.spawn(cons_prod_ctx)
            }
        };
        handle_vec.push(handle);
    }
    handle_vec
}

fn create_marker_chan_vec(
    operator_connections: &HashMap<Operators, Vec<Operators>>,
) -> Vec<(PushChan<Event<()>>, PullChan<Event<()>>)> {
    let mut counter = 0;
    for operator in operator_connections {
        match operator.0 {
            Operators::SourceProducer(_) => counter += 1,
            Operators::Consumer(_) => {}
            Operators::ConsumerProducer(_) => {}
        }
    }

    channel_vec::<Event<()>>(counter)
}

pub fn manager() {
    let mut operator_connections: HashMap<Operators, Vec<Operators>> = HashMap::new(); //init dataflow graph

    //creating the dataflow graph
    operator_connections.insert(
        Operators::SourceProducer(1),
        vec![Operators::ConsumerProducer(1)],
    );
    operator_connections.insert(
        Operators::SourceProducer(2),
        vec![Operators::ConsumerProducer(1)],
    );
    operator_connections.insert(
        Operators::SourceProducer(3),
        vec![Operators::ConsumerProducer(1)],
    );
    operator_connections.insert(Operators::ConsumerProducer(1), vec![Operators::Consumer(1)]);
    operator_connections.entry(Operators::ConsumerProducer(1)).and_modify(|e| { e.push(Operators::Consumer(2)) }).or_insert(vec![Operators::Consumer(2)]);


    println!("TEST OPERATOR_CONNECTIONS: {:?}", operator_connections);

    //push: from the operator to the manager(fe, state), filling the buffer
    //pull: manager can pull from the buffer
    let (state_push, state_pull) = channel::<TaskToManagerMessage>();

    let (marker_push, marker_pull) = channel::<Event<()>>();

    let marker_vec = create_marker_chan_vec(&operator_connections);

    let manager_state = Manager {
        state_chan_push: state_push, //channel for operator state, operator -> buffer
        state_chan_pull: state_pull, //channel for operator state, buffer -> manager
        marker_chan_vec: marker_vec, //all source producer marker channels
        serde_state: SerdeState::default(), //for serialization and deserialization
    };
    async_std::task::spawn(manager_state.run(operator_connections));
    println!("manager operator spawned!");
}
