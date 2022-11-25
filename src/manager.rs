use tokio::sync::oneshot;

use core::panic;
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
    areas: HashMap<u64, String>,
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
    pub operator_id: i32,
    pub areas: HashMap<u64, String>,
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
            Task::ConsumerProducer(state) => async_std::task::spawn(state.execute_optimized_unrestricted(ctx)),
            Task::PartialConsumerProducer(_) => todo!(),
        }
    }
}

impl Manager {
    async fn run(mut self, operator_connections: HashMap<Operators, Vec<Operators>>) {
        let mut interval = time::interval(time::Duration::from_millis(400));
        let mut snapshot_timeout_counter = 0;

        let mut log_size:HashSet<i32> = HashSet::new();

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
        let mut marker_id = 1;
        let mut partial_snapshot_hashset: HashMap<i32, Vec<PartialPersistentConsumerProducerState>> = HashMap::new();
        
        let mut timer_now = Instant::now();
        let mut total_time = Instant::now();
        let mut benchmarking_timer_iteration: VecDeque<f64> = VecDeque::new();
        let mut benchmarking_counter = 0;
        let mut old_highest_marker = 0;
        let mut sink_node_done = 0;
        
        let mut test_total_messages = 0; //remove

        //warmup
        let mut warmup_bool = true;
        self.warmup_messages(10).await;

        loop {
            if (!warmup_bool){
                if(benchmarking_timer_iteration.len() < 3) { //one way: 3*4 * amount*2 (or amount*4), both way: 3*4 * (amount + amount *2)  
                    timer_now = Instant::now();
                    //self.send_message_one_way(50, &mut marker_id, &mut test_total_messages).await;
                    self.send_message_both_ways(200, &mut marker_id, &mut test_total_messages).await;
                    //self.send_message_both_ways_even(50, &mut marker_id, &mut test_total_messages).await;
                }
                else {
                    if(benchmarking_timer_iteration.len() == 3){
                        println!("Total runtime before last message: {:?}.", total_time.elapsed().as_millis());
                        self.send_last_message().await;
                    }
                    else {
                        println!("Total runtime: {:?}. amount:{:?}", total_time.elapsed().as_millis(), test_total_messages);
                        break;
                    }
                }
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
                            //self.reset_values(&mut operator_amount, operator_spawn_vec.len(), &mut operator_counter, &mut snapshot_timeout_counter, &mut serde_state).await;
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
                                promise.send(m_id as u64);
                                populate_persistent_task_map(state.clone(), m_id, &mut serde_state.persistent_task_map, &mut partial_snapshot_hashset).await;
                                
                                if (serde_state.persistent_task_map.get(&m_id).unwrap().len() == operator_amount){
                                    benchmarking_counter += 1;
                                    if (m_id > old_highest_marker){
                                        serialize_state(serde_state.persistent_task_map.get(&m_id).unwrap()).await;
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
                        },
                        PersistentTaskToManagerMessage::Benchmarking(promise) => {
                            sink_node_done += 1;
                            if (sink_node_done == 2){
                                println!("Total runtime: {:?}.\nBenchmarking times in each iteration: {:?}. tot amount: {}", total_time.elapsed().as_millis(), benchmarking_timer_iteration,test_total_messages);
                                promise.send(sink_node_done-1);
                                if(warmup_bool){
                                    println!("DONE WITH WARMUP!");
                                    warmup_bool = false;
                                    old_highest_marker = 0;
                                    benchmarking_counter = 0;
                                    sink_node_done = 0;
                                    total_time = Instant::now();
                                    break;
                                }
                                else {
                                    println!("Sleeping now!");
                                    task::sleep(Duration::from_secs(1000)).await;
                                }
                            }
                            else {
                                promise.send(sink_node_done-1);
                            }  
                        },
                    };
                    },
                }
            } 
        }   
    }

    async fn send_message_both_ways_even(&self, amount: i32, marker_id: &mut i32, nn: &mut i32) {
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        println!("MARKER_KEYS: {:?}", marker_keys);
        for n in 0..4{
            *nn += amount;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(("taxi_customer".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(("taxi_driver".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(("bus".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            *marker_id += 1;
            
            *nn += (amount *2);
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(("taxi_customer".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(("taxi_driver".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(("bus".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
        }
        *marker_id += 4;
    }

    async fn send_message_one_way(&self, amount: i32, marker_id: &mut i32, nn: &mut i32) {
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        println!("MARKER_KEYS: {:?}", marker_keys);
        for n in 0..4{
            *nn += amount*2;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(("taxi_customer".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(("taxi_driver".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(("bus".to_string(), amount*2))).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            *marker_id += 1;
            *nn += amount*2;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(("taxi_customer".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(("taxi_driver".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(("bus".to_string(), amount*2))).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
        }
        *marker_id += 4;
    }

    async fn send_message_both_ways(&self, amount: i32, marker_id: &mut i32, nn: &mut i32) {
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        println!("MARKER_KEYS: {:?}", marker_keys);
        for n in 0..4{
            *nn += amount;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(("taxi_customer".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(("taxi_driver".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(("bus".to_string(), amount*2))).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            *marker_id += 1;
            
            *nn += (amount *2);
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(("taxi_customer".to_string(), amount*2))).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(("taxi_driver".to_string(), amount*2))).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(("bus".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id + n)).await;
        }
        *marker_id += 4;
    }

    async fn warmup_messages(&self, amount: i32) {
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();

        for n in 0..4{
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(("taxi_customer".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(0 + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(("taxi_driver".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(0 + n)).await;
            
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(("bus".to_string(), amount))).await;
            self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(0 + n)).await;
        }
        self.send_last_message().await;
    }

    async fn send_test(&self, amount: i32, marker_id: &mut i32){
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        println!("MARKER_KEYS: {:?}", marker_keys);
        
        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(("taxi_customer".to_string(), amount))).await;
        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::Marker(*marker_id)).await;
        
        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(("taxi_driver".to_string(), amount))).await;
        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::Marker(*marker_id)).await;
        
        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(("bus".to_string(), amount+3))).await;
        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::Marker(*marker_id)).await;
        println!("DONE SENDING MESSAGES AND MARKERS!");
    }

    async fn send_last_message(&self){
        let mut marker_keys:Vec<i32> = self.marker_chan_hash.to_owned().into_keys().collect();
        marker_keys.sort();
        self.marker_chan_hash.get(&marker_keys[0]).unwrap().0.push(Event::MessageAmount(("end_of_stream".to_string(), 0))).await;
        
        self.marker_chan_hash.get(&marker_keys[1]).unwrap().0.push(Event::MessageAmount(("end_of_stream".to_string(), 0))).await;
        
        self.marker_chan_hash.get(&marker_keys[2]).unwrap().0.push(Event::MessageAmount(("end_of_stream".to_string(), 0))).await;
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

    

    /*let mut operator_pullpush: HashMap<(Operators,Operators),(PushChan<Event<i32>>, PullChan<Event<i32>>)> = init_channels(
        &operator_connections,
        &mut operator_channel,
        &mut operator_state_chan,
    ).await;*/

    let (producer_channels, consumer_producer_channels) = init_channels(
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
                    out0: producer_channels.get(&(operator, Operators::ConsumerProducer(0))).unwrap().0.to_owned(),
                    count: 0,
                };
                let prod_ctx = Context {
                    operator_id,
                    areas: self_manager.areas.clone(),
                    marker_manager_recv: Some(
                        self_manager.marker_chan_hash.get(&operator_id).unwrap().1.clone(),
                    ),
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                let prod_task = Task::Producer(prod_state);
                task_op_spawn_vec.push(prod_task.spawn(prod_ctx));
                marker_vec_counter += 1;
            }
            Operators::Consumer(operator_id) => {
                let cons_state = ConsumerState::S0 {
                    stream0: consumer_producer_channels.get(&(Operators::ConsumerProducer(0),operator)).unwrap().1.to_owned(),
                    count: 0,
                };
                let cons_ctx = Context {
                    operator_id,
                    areas: self_manager.areas.clone(),
                    marker_manager_recv: None,
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                let cons_task = Task::Consumer(cons_state);
                task_op_spawn_vec.push(cons_task.spawn(cons_ctx));
            }
            Operators::ConsumerProducer(operator_id) => {
                let cons_prod_state = ConsumerProducerState::S0 {
                    stream0: producer_channels.get(&(Operators::SourceProducer(0),operator.clone())).unwrap().1.to_owned(),
                    stream1: producer_channels.get(&(Operators::SourceProducer(1),operator.clone())).unwrap().1.to_owned(),
                    stream2: producer_channels.get(&(Operators::SourceProducer(2),operator.clone())).unwrap().1.to_owned(),
                    out0: consumer_producer_channels.get(&(operator.clone(), Operators::Consumer(0))).unwrap().0.to_owned(),
                    out1: consumer_producer_channels.get(&(operator.clone(), Operators::Consumer(1))).unwrap().0.to_owned(),
                    count: 0,
                };
                let cons_prod_ctx = Context {
                    operator_id,
                    areas: self_manager.areas.clone(),
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
) -> (
    HashMap<(Operators,Operators),(PushChan<Event<(u64, u64)>>, PullChan<Event<(u64, u64)>>)>,
    HashMap<(Operators,Operators),(PushChan<Event<(String, String)>>, PullChan<Event<(String, String)>>)>){
    //creating channels for source producers and consumer_producers. Every prod/con_prod will have a separate channel with connected operator
    let mut producer_operator_channel: HashMap<(Operators,Operators),(PushChan<Event<(u64, u64)>>, PullChan<Event<(u64, u64)>>)> = HashMap::new();
    let mut consumer_producer_operator_channel: HashMap<(Operators,Operators),(PushChan<Event<(String, String)>>, PullChan<Event<(String, String)>>)> = HashMap::new(); 

    for connection in operator_connections {
        match connection.0 {
            Operators::SourceProducer(_) => {
                let mut count = 0;
                let chan_vec = channel_vec::<Event<(u64, u64)>>(connection.1.clone().len());
                for operator_connection_value in connection.1 {
                    producer_operator_channel.insert((connection.0.clone(), operator_connection_value.clone()), chan_vec[count].to_owned());
                    count += 1;
                }
            },
            Operators::Consumer(_) => panic!(),
            Operators::ConsumerProducer(_) => {
                let mut count = 0;
                let chan_vec = channel_vec::<Event<(String, String)>>(connection.1.clone().len());
                for operator_connection_value in connection.1 {
                    consumer_producer_operator_channel.insert((connection.0.clone(), operator_connection_value.clone()), chan_vec[count].to_owned());
                    count += 1;
                }
            },
        }
    }
    (producer_operator_channel, consumer_producer_operator_channel)
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
                    operator_id: todo!(), 
                    areas: self_manager.areas.clone(),
                    marker_manager_recv: None,
                    state_manager_send: self_manager.state_chan_push.clone(),
                };
                task.spawn(cons_ctx)
            }
            Task::Producer(operator_id) => {
                let prod_ctx = Context {
                    operator_id: todo!(),
                    areas: self_manager.areas.clone(),
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
                    operator_id: todo!(),
                    areas: self_manager.areas.clone(),
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

    let swedish_locality: HashMap<u64, String> = HashMap::from([(1, "Stockholm".to_string()),
    (2, "Upplands Väsby".to_string()),
    (3, "Södertälje".to_string()),
    (4, "Lidingö".to_string()),
    (5, "Tumba".to_string()),
    (6, "Åkersberga".to_string()),
    (7, "Vallentuna".to_string()),
    (8, "Märsta".to_string()),
    (9, "Gustavsberg".to_string()),
    (10, "Norrtälje".to_string()),
    (11, "Västerhaninge".to_string()),
    (12, "Nynäshamn".to_string()),
    (13, "Ekerö".to_string()),
    (14, "Jordbro".to_string()),
    (15, "Kungsängen".to_string()),
    (16, "Saltsjöbaden".to_string()),
    (17, "Sigtuna".to_string()),
    (18, "Bro".to_string()),
    (19, "Fisksätra".to_string()),
    (20, "Järna".to_string()),
    (21, "Vaxholm".to_string()),
    (22, "Hallstavik".to_string()),
    (23, "Brunna".to_string()),
    (24, "Ösmo".to_string()),
    (25, "Stenhamra".to_string()),
    (26, "Resarö".to_string()),
    (27, "Sticklinge".to_string()),
    (28, "Vårsta".to_string()),
    (29, "Norra_Riksten".to_string()),
    (30, "Svinninge".to_string()),
    (31, "Fågelvikshöjden".to_string()),
    (32, "Kopparmora".to_string()),
    (33, "Pershagen".to_string()),
    (34, "Stavsnäs".to_string()),
    (35, "Rosersberg".to_string()),
    (36, "Dalarö".to_string()),
    (37, "Djurö".to_string()),
    (38, "Älmsta".to_string()),
    (39, "Sorunda".to_string()),
    (40, "Brunn".to_string()),
    (41, "Steningehöjden".to_string()),
    (42, "Långvik".to_string()),
    (43, "Lindholmen".to_string()),
    (44, "Rindö".to_string()),
    (45, "Mölnbo".to_string()),
    (46, "Älgö".to_string()),
    (47, "Kil".to_string()),
    (48, "Parksidan".to_string()),
    (49, "Ekeby".to_string()),
    (50, "Kullö".to_string()),
    (51, "Viksäter".to_string()),
    (52, "Ingaröstrand".to_string()),
    (53, "Älvnäs".to_string()),
    (54, "Vidja".to_string()),
    (55, "Lugnet".to_string()),
    (56, "Segersäng".to_string()),
    (57, "Stora_Vika".to_string()),
    (58, "Kungsberga".to_string()),
    (59, "Hästängen".to_string()),
    (60, "Håbo-Tibble_kyrkby".to_string()),
    (61, "Gladö_Kvarn".to_string()),
    (62, "Ölsta".to_string()),
    (63, "Bammarboda".to_string()),
    (64, "Löwenströmska_Lasarettet".to_string()),
    (65, "Nyhagen".to_string()),
    (66, "Älvsala".to_string()),
    (67, "Ekerö_Sommarstad".to_string()),
    (68, "Bergshamra".to_string()),
    (69, "Arninge".to_string()),
    (70, "Gräddö".to_string()),
    (71, "Rydbo".to_string()),
    (72, "Edsbro".to_string()),
    (73, "Grödby".to_string()),
    (74, "Kallfors".to_string()),
    (75, "Hästhagen".to_string()),
    (76, "Väländan".to_string()),
    (77, "Svanberga".to_string()),
    (78, "Kårsta".to_string()),
    (79, "Östra Kallfors".to_string()),
    (80, "Spånlöt".to_string()),
    (81, "Spillersboda".to_string()),
    (82, "Ängsvik".to_string()),
    (83, "Solberga".to_string()),
    (84, "Täljö".to_string()),
    (85, "Årsta_Havsbad".to_string()),
    (86, "Sandviken".to_string()),
    (87, "Rånäs".to_string()),
    (88, "Grisslehamn".to_string()),
    (89, "Tranholmen".to_string()),
    (90, "Tuna".to_string()),
    (91, "Sundby".to_string()),
    (92, "Vattubrinken".to_string()),
    (93, "Herräng".to_string()),
    (94, "Stava".to_string()),
    (95, "Lidatorp".to_string()),
    (96, "Sibble".to_string()),
    (97, "Tynningö".to_string()),
    (98, "Drottningholm".to_string()),
    (99, "Nolsjö".to_string()),
    (100, "Norra_Lagnö".to_string()),
    (101, "Söderby".to_string()),
    (102, "Södersvik".to_string()),
    (103, "Ekskogen_Älgeby".to_string()),
    (104, "Riala".to_string()),
    (105, "Rättarboda".to_string()),
    (106, "Sättra".to_string()),
    (107, "Koviksudde".to_string()),
    (108, "Betsede".to_string()),
    (109, "Oxnö".to_string()),
    (110, "Laggarsvik".to_string()),
    (111, "Söderby".to_string()),
    (112, "Ekskogen".to_string()),
    (113, "Granby".to_string()),
    (114, "Johannesudd".to_string()),
    (115, "Skarpö".to_string()),
    (116, "Nysättra".to_string()),
    (117, "Baldersnäs".to_string()),
    (118, "Finsta".to_string()),
    (119, "Muskö".to_string()),
    (120, "Norra_Vindö".to_string()),
    (121, "Söderby-Karl".to_string()),
    (122, "Östorp".to_string()),
    (123, "Landfjärden".to_string()),
    (124, "Nibble".to_string()),
    (125, "Brottby".to_string()),
    (126, "Stensättra".to_string()),
    (127, "Blidö".to_string()),
    (128, "Skebobruk".to_string()),
    (129, "Lurudden".to_string()),
    (130, "Northern_Muskö".to_string()),
    (131, "Hilleshög".to_string()),
    (132, "Lurudden".to_string())
    ]);

    let manager_state = Manager {
        areas: swedish_locality,
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