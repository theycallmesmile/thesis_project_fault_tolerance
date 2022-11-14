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
use crate::serialization::PersistentConsumerProducerState;


//Time
use tokio::time::Instant;

//Serialize module
use crate::serialization::load_deserialize;
use crate::serialization::load_persistent;
use crate::serialization::save_persistent;
use crate::serialization::serialize_state;
use crate::serialization::PersistentConsumerState;
use crate::serialization::PersistentProducerState;
use crate::serialization::PartialPersistentConsumerProducerState;
use crate::serialization::PersistentTask;
use crate::serialization::SerdeState;
use crate::serialization::PartialPersistentTask;
use crate::serialization::PersistentPushChan;
use crate::serialization::PersistentPullChan;

//Consumer module
use crate::consumer::ConsumerState;

//Producer module
use crate::producer::ProducerState;

//Consumer_producer module
use crate::consumer_producer::ConsumerProducerState;
use crate::consumer_producer::PartialConsumerProducerState;

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
    state_chan_push: PushChan<PersistentTaskToManagerMessage>,
    state_chan_pull: PullChan<PersistentTaskToManagerMessage>,
    marker_chan_hash: HashMap<i32, (PushChan<Event<()>>, PullChan<Event<()>>)>,//Vec<(PushChan<Event<()>>, PullChan<Event<()>>)>,
    serde_state: SerdeState,
}

#[derive(Debug)]
pub enum PersistentTaskToManagerMessage {
    Serialise(PartialPersistentTask, i32, oneshot::Sender<u64>),
    Benchmarking(oneshot::Sender<u64>),
}

#[derive(Debug, Clone)]
pub enum Task {
    Consumer(ConsumerState),
    Producer(ProducerState),
    ConsumerProducer(ConsumerProducerState),
    PartialConsumerProducer(PartialConsumerProducerState),
}

#[derive(Debug)]
pub struct Context {
    pub marker_manager_recv: Option<PullChan<Event<()>>>,
    pub state_manager_send: PushChan<PersistentTaskToManagerMessage>,
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
            Task::ConsumerProducer(state) => async_std::task::spawn(state.execute_unoptimized_unrestricted(ctx)),
            Task::PartialConsumerProducer(_) => todo!(),
        }
    }
}

impl Manager {
    async fn run(mut self, operator_connections: HashMap<Operators, Vec<Operators>>) {
        let mut interval = time::interval(time::Duration::from_millis(600));
        let mut snapshot_timeout_counter = 0;

        //creating hashmap and hashset
        let mut serde_state = SerdeState {
            serialised: HashSet::new(),
            deserialised: HashMap::new(),
            persistent_task_map: HashMap::new(), 
        };

        let mut serialize_task_vec: Vec<Task> = Vec::new(); //given to the serialize function

        //init the operators and returns amount of spawned operato&mut rs
        let mut operator_spawn_vec = spawn_operators(&mut self, operator_connections).await;
        let mut operator_amount = operator_spawn_vec.len();
        let mut operator_counter = 0;
        let mut marker_id = 1;
        let mut partial_snapshot_hashset: HashMap<i32, Vec<PartialPersistentConsumerProducerState>> = HashMap::new();

        
        let mut timer_now = Instant::now();
        let mut total_time = Instant::now();
        let mut benchmarking_timer_iteration: VecDeque<f64> = VecDeque::new();
        let mut benchmarking_counter = 0;
        let mut old_highest_marker = 0;
        let mut sink_node_done = 0;
        loop {
            if(benchmarking_timer_iteration.len() == 10){
                benchmarking_timer_iteration.pop_front();
                let timer_avg = average(benchmarking_timer_iteration.clone());
                println!("Whole vector without the first element: {:?}, the average: {:?}", benchmarking_timer_iteration, timer_avg);
                break;
            }
            timer_now = Instant::now();
            println!("Sending new messages with marker.");
            if(benchmarking_timer_iteration.len() < 5){
                //self.send_messages_markers(100).await;
                self.send_message_both_ways(200, &mut marker_id).await;         
                //self.send_message_both_ways(1000, &mut marker_id).await;
                //self.send_message_one_way(300,&mut marker_id).await;
                //self.send_message_test(500, &mut marker_id).await;
            }
            else {
                //self.send_messages_markers_inverted(30000).await;
                //self.send_messages_markers(100).await;
                self.send_last_message().await;
                
            }
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if snapshot_timeout_counter >= 300000 { //no reason to checkpoint after a rollback, it will be similar to the rollbacked checkpoint.
                            println!("One or more operator does not respond, rollbacking to the latest checkpoint!");
                            while let Some(operator) = operator_spawn_vec.pop() {
                                operator.cancel().await;
                            }

                            //resetting the values
                            self.reset_values(&mut operator_amount, operator_spawn_vec.len(), &mut operator_counter, &mut snapshot_timeout_counter, &mut serde_state).await;
                            let loaded_checkpoint = load_persistent().await; //the json string
                            println!("Loaded checkpoint: {:?}",loaded_checkpoint);
                            serialize_task_vec = load_deserialize(loaded_checkpoint, &mut serde_state.deserialised).await; //deserialized checkpoint vec
                            println!("Loaded serialize_task_vec: {:?}",serialize_task_vec);
                            operator_spawn_vec = respawn_operator(&mut self, serialize_task_vec).await; //respawning task operators
                            break;
                        }
                        else {
                            snapshot_timeout_counter += 1;
                        }
                    },
                    msg = self.state_chan_pull.pull_manager() => {
                        match msg {
                            PersistentTaskToManagerMessage::Serialise(state, m_id, promise) => {
                                snapshot_timeout_counter = 0;
                                operator_counter +=1;
                                promise.send(8);
                                populate_persistent_task_map(state.clone(), m_id, &mut serde_state.persistent_task_map, &mut partial_snapshot_hashset).await;
                                
                                if (serde_state.persistent_task_map.get(&m_id).unwrap().len() == operator_amount){
                                    benchmarking_counter += 1;
                                    if (m_id > old_highest_marker){
                                        serialize_state(serde_state.persistent_task_map.get(&m_id).unwrap()).await;
                                        //self.reset_values(...);
                                        println!("Whole vector element without the last yet: {:?}, elapsed time: {:?}", benchmarking_timer_iteration, timer_now.elapsed().as_millis());

                                        old_highest_marker = m_id;
                                    }

                                    //removing the saved checkpoint from the hashmap!
                                    serde_state.persistent_task_map.remove_entry(&m_id);
                                    partial_snapshot_hashset.remove_entry(&m_id);

                                    if(benchmarking_counter == 8){
                                        let timer_now = timer_now.elapsed().as_millis();
                                        benchmarking_timer_iteration.push_back(timer_now as f64);
                                        benchmarking_counter = 0;
                                        let timer_avg = average(benchmarking_timer_iteration.clone());
                                        println!("Time for all of the iterations: {:?}, the average: {:?}", benchmarking_timer_iteration, timer_avg);
                                        break;
                                    }
                                }                                
                        }
                        PersistentTaskToManagerMessage::Benchmarking(promise) => {
                            sink_node_done += 1;
                            if (sink_node_done == 2){
                                println!("Total runtime: {:?}.\nBenchmarking times in each iteration: {:?}", total_time.elapsed().as_millis(), benchmarking_timer_iteration);
                                promise.send(sink_node_done-1);
                                task::sleep(Duration::from_secs(1000)).await;
                            }
                            else{
                                promise.send(sink_node_done-1);
                            }
                            
                        },
                    };
                },
            }
        } 
    }   
    }
    
    async fn send_message_test(&self, amount: i32, marker_id: &mut i32){
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(amount)).await;
        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id)).await;

        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(amount)).await;
        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id)).await;

        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(amount*2)).await;
        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id)).await;

        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(amount*2)).await;
        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id+1)).await;

        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(amount*2)).await;
        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id+1)).await;

        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(amount)).await;
        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id+1)).await;
        println!("DONE SENDING LAST MESSAGE!");
    }

    async fn send_message_one_way(&self, amount: i32, marker_id: &mut i32) {
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        println!("MARKER_KEYS: {:?}", marker_keys);
        for n in 0..4{

            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(amount)).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(amount)).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(amount*2)).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
        }
        *marker_id += 4;
    }

    async fn send_message_both_ways(&self, amount: i32, marker_id: &mut i32) {
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        println!("MARKER_KEYS: {:?}", marker_keys);
        for n in 0..4{

            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(amount)).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(amount)).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(amount*2)).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            *marker_id += 1;
        
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(amount*2)).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(amount*2)).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(amount)).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
        }
        *marker_id += 4;
    }

    async fn send_test(&self, amount: i32, marker_id: &mut i32){
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        println!("MARKER_KEYS: {:?}", marker_keys);
        
        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(amount)).await;
        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id)).await;
        
        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(amount)).await;
        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id)).await;
        
        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(amount+3)).await;
        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id)).await;
        println!("DONE SENDING MESSAGES AND MARKERS!");
    }

    async fn send_last_message(&self){
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(0)).await;
        
        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(0)).await;
        
        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(0)).await;
        println!("DONE SENDING LAST MESSAGE!");
    }

    async fn reset_values(&self, operator_amount: &mut usize, operator_spawn_vec_len: usize, operator_counter: &mut usize, snapshot_timeout_counter: &mut i32, serde_state: &mut SerdeState) {
        *operator_amount = operator_spawn_vec_len;
        *operator_counter = 0;
        *snapshot_timeout_counter = 0;
        //self.state_chan_pull.clear_pull_chan().await; //enable after benchmarking
        //serde_state.persistent_task_vec.clear();
        serde_state.deserialised.clear();
        serde_state.serialised.clear();
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

    init_channels_modified(
        &operator_connections,
        &mut operator_channel,
        &mut operator_state_chan,
    ).await;
    let mut operator_pullpush: HashMap<(Operators,Operators),(PushChan<Event<i32>>, PullChan<Event<i32>>)> = init_channels_modified(
        &operator_connections,
        &mut operator_channel,
        &mut operator_state_chan,
    ).await;
    let mut all_operators: HashSet<Operators> = all_graph_operators(operator_connections).await;

    //spawning the tasks
    for operator in all_operators {
        match operator { //SourceProducer(0)
            Operators::SourceProducer(operator_id) => {
                let prod_state = ProducerState::S0 {
                    out0: operator_pullpush.get(&(operator, Operators::ConsumerProducer(0))).unwrap().0.to_owned(),
                    count: 0,
                };
                let prod_ctx = Context {
                    marker_manager_recv: Some(
                        self_manager.marker_chan_hash.get(&operator_id).unwrap().1.clone(),
                    ),
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                let prod_task = Task::Producer(prod_state);
                task_op_spawn_vec.push(prod_task.spawn(prod_ctx));
                marker_vec_counter += 1;
            }
            Operators::Consumer(_) => {
                let cons_state = ConsumerState::S0 {
                    stream0: operator_pullpush.get(&(Operators::ConsumerProducer(0),operator)).unwrap().1.to_owned(),
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
                let cons_prod_state = ConsumerProducerState::S0 {
                    stream0: operator_pullpush.get(&(Operators::SourceProducer(0),operator.clone())).unwrap().1.to_owned(),
                    stream1: operator_pullpush.get(&(Operators::SourceProducer(1),operator.clone())).unwrap().1.to_owned(),
                    stream2: operator_pullpush.get(&(Operators::SourceProducer(2),operator.clone())).unwrap().1.to_owned(),
                    out0: operator_pullpush.get(&(operator.clone(), Operators::Consumer(0))).unwrap().0.to_owned(),
                    out1: operator_pullpush.get(&(operator.clone(), Operators::Consumer(1))).unwrap().0.to_owned(),
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

async fn init_channels_modified(
    operator_connections: &HashMap<Operators, Vec<Operators>>,
    operator_channel: &mut HashMap<Operators, Vec<(PushChan<Event<i32>>, PullChan<Event<i32>>)>>,
    operator_state_chan: &mut HashMap<Operators, Vec<OperatorChannels>>,
) -> HashMap<(Operators,Operators),(PushChan<Event<i32>>, PullChan<Event<i32>>)>{
    //creating channels for source producers and consumer_producers. Every prod/con_prod will have a separate channel with connected operator
    //create as a new func?
    let mut latest_operator_pullpush: HashMap<(Operators,Operators),(PushChan<Event<i32>>, PullChan<Event<i32>>)> = HashMap::new();

    for connection in operator_connections {
        let mut count = 0;
        let chan_vec = channel_vec::<Event<i32>>(connection.1.clone().len());
        for operator_connection_value in connection.1{
            latest_operator_pullpush.insert((connection.0.clone(), operator_connection_value.clone()), chan_vec[count].to_owned());
            count += 1;
        }
    }
    latest_operator_pullpush
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
            Task::Producer(operator_id) => {
                let prod_ctx = Context {
                    marker_manager_recv: Some(
                        panic!(), // FIX THE OPERATOR_ID !!!!!!!!!!!!!!!!!!!!!!!!! should be similar to how its during initial creation/spawning
                        //self_manager.marker_chan_hash.get(todo!()).unwrap().1.clone(), //todo!() should be replaced with operator_id
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
            Task::PartialConsumerProducer(_) => todo!(),
        };
        handle_vec.push(handle);
    }
    handle_vec
}

fn create_marker_chan_vec(
    operator_connections: &HashMap<Operators, Vec<Operators>>,
) -> HashMap<i32, (PushChan<Event<()>>, PullChan<Event<()>>)>{//Vec<(PushChan<Event<()>>, PullChan<Event<()>>)> {
    let mut marker_hash:HashMap<i32, (PushChan<Event<()>>, PullChan<Event<()>>)> = HashMap::new();
    for operator in operator_connections {
        match operator.0 {
            Operators::SourceProducer(n) => {
                let temp_chan_vec = channel_vec::<Event<()>>(1).pop().unwrap();
                marker_hash.insert(*n, temp_chan_vec);
            }, 
            Operators::Consumer(_) => {}
            Operators::ConsumerProducer(_) => {}
        }
    }
    marker_hash
}

pub fn manager() {
    let mut operator_connections: HashMap<Operators, Vec<Operators>> = HashMap::new(); //init dataflow graph

    //creating the dataflow graph
    operator_connections.insert(
        Operators::SourceProducer(0), 
        vec![Operators::ConsumerProducer(0)],
    );
    operator_connections.insert(
        Operators::SourceProducer(1), 
        vec![Operators::ConsumerProducer(0)],
    );
    operator_connections.insert(
        Operators::SourceProducer(2), 
        vec![Operators::ConsumerProducer(0)],
    );
    operator_connections.insert(
        Operators::ConsumerProducer(0), 
        vec![Operators::Consumer(0)]
    );
    operator_connections.entry(
        Operators::ConsumerProducer(0))
        .and_modify(|e| 
            { e.push(Operators::Consumer(1)) })
        .or_insert(vec![Operators::Consumer(1)]
    );


    println!("TEST OPERATOR_CONNECTIONS: {:?}", operator_connections);

    //push: from the operator to the manager(fe, state), filling the buffer
    //pull: manager can pull from the buffer
    let (state_push, state_pull) = channel::<PersistentTaskToManagerMessage>();

    //let (marker_push, marker_pull) = channel::<Event<()>>();

    let marker_hash:HashMap<i32, (PushChan<Event<()>>, PullChan<Event<()>>)> = create_marker_chan_vec(&operator_connections); //HASHMAP TEX: {operator_nummer, (push/pull)} . {nummer, tupple}

    let manager_state = Manager {
        state_chan_push: state_push, //channel for operator state, operator -> buffer
        state_chan_pull: state_pull, //channel for operator state, buffer -> manager
        marker_chan_hash: marker_hash, //all source producer marker channels
        serde_state: SerdeState::default(), //for serialization and deserialization
    };
    async_std::task::spawn(manager_state.run(operator_connections));
    println!("manager operator spawned!");
}

fn average(numbers: VecDeque<f64>) -> f64 {
    let nnumbers = numbers.len() as f64;
    let mut sum = 0.0;
    for n in numbers {
        sum += n;
    }
    let avrg = sum / nnumbers;
    avrg
}


pub fn partial_to_persistent(p_0:PartialPersistentConsumerProducerState, p_1:PartialPersistentConsumerProducerState) -> PersistentConsumerProducerState{
    let mut p_state:PersistentConsumerProducerState;
    let loc_p_0 = p_0.clone();
    let loc_p_1 = p_1.clone();
    match p_0 {
        PartialPersistentConsumerProducerState::S0 { stream0, stream1, out0 } => {
            match p_1 {
                PartialPersistentConsumerProducerState::S0 { stream0, stream1, out0 } => panic!(),
                PartialPersistentConsumerProducerState::S1 { stream0, stream1, out0, data } => panic!(),
                PartialPersistentConsumerProducerState::S2 { stream2, out1 } => {
                    p_state = PersistentConsumerProducerState::S0 { stream0, stream1, stream2, out0, out1, count: 0 };//remove count
                },
            }
        },
        PartialPersistentConsumerProducerState::S1 { stream0, stream1, out0, data } => {
            match p_1 {
                PartialPersistentConsumerProducerState::S0 { stream0, stream1, out0 } => panic!(),
                PartialPersistentConsumerProducerState::S1 { stream0, stream1, out0, data } => panic!(),
                PartialPersistentConsumerProducerState::S2 { stream2, out1 } => {
                    p_state = PersistentConsumerProducerState::S1 { stream0, stream1, stream2, out0, out1, count: 0, data };//remove count
                },
            }
        },
        PartialPersistentConsumerProducerState::S2 { stream2, out1 } => {
            p_state = partial_to_persistent(loc_p_1, loc_p_0);
        },
    }
    p_state
}

pub async fn  populate_persistent_task_map(state: PartialPersistentTask, m_id: i32, persistent_task_map: &mut HashMap<i32, Vec<PersistentTask>>, partial_snapshot_hashset: &mut HashMap<i32, Vec<PartialPersistentConsumerProducerState>>) {
    match state {
        PartialPersistentTask::Consumer(p_state) => {
            match persistent_task_map.get_mut(&m_id) {
                Some(persistent_vec) => persistent_vec.push(PersistentTask::Consumer(p_state)),
                None => {
                    persistent_task_map.insert(m_id, vec![PersistentTask::Consumer(p_state)]);
                },
            }
        },
        PartialPersistentTask::Producer(p_state) => 
        match persistent_task_map.get_mut(&m_id) {
            Some(persistent_vec) => persistent_vec.push(PersistentTask::Producer(p_state)),
            None => {
                persistent_task_map.insert(m_id, vec![PersistentTask::Producer(p_state)]);
            },
        },
        PartialPersistentTask::ConsumerProducer(p_state) => 
            match persistent_task_map.get_mut(&m_id) {
                Some(persistent_vec) => persistent_vec.push(PersistentTask::ConsumerProducer(p_state)),
                None => {
                    persistent_task_map.insert(m_id, vec![PersistentTask::ConsumerProducer(p_state)]);
                },
            },
        PartialPersistentTask::PartialConsumerProducer(p_state) => 
        match partial_snapshot_hashset.get_mut(&m_id) { //check if a partial snapshot exists
            Some(p_vec) => {//Insert the second value and combine them.
                p_vec.push(p_state); 

                let persistent_consumer_producer_state = partial_to_persistent(p_vec[0].clone(), p_vec[1].clone());

                match persistent_task_map.get_mut(&m_id) {//check if the marker column exists in persistent_task_map.
                    Some(persistent_vec) => persistent_vec.push(PersistentTask::ConsumerProducer(persistent_consumer_producer_state)), //it does, and simply pushed the state into it
                    None => {persistent_task_map.insert(m_id, vec![PersistentTask::ConsumerProducer(persistent_consumer_producer_state)]);}, //it does not, create a vec and push the state
                }
            },
            None => {//hashset is empty, insert the first partial snapshot
                partial_snapshot_hashset.insert(m_id, vec![p_state]);
            },
        },
    }
}

pub async fn all_graph_operators (operator_connections:HashMap<Operators, Vec<Operators>>) -> HashSet<Operators>{
    let mut all_operators: HashSet<Operators> = HashSet::new();
    for operators in operator_connections{
        all_operators.insert(operators.0);
        for operator_in_val in operators.1 {
            all_operators.insert(operator_in_val);
        }
    }
    all_operators
}