#![allow(warnings, unused)]
extern crate tokio;

use std::ptr::eq;
use std::str;

use async_std::stream::Sum;
use rskafka::time;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};
use serde_json;

mod channel;
use channel::channel;
use channel::PullChan;
use channel::PushChan;

use std::collections::VecDeque;
use std::sync::Arc;

async fn map_operator(input: PullChan<i32>, f: fn(i32) -> i32, output: PushChan<i32>) {
    loop {
        let x = input.pull().await;
        let y = f(x);
        output.push(y).await;
    }
}
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
enum Event<i32> {
    Data(i32),
    Marker,
}
#[derive(Serialize, Deserialize, Debug)]
enum SumState<i32> {
    S0 {
        input: PullChan<Event<i32>>,
        output: PushChan<Event<i32>>,
        sum: i32,
    },
    S1 {
        output: PushChan<Event<i32>>,
    },
}

async fn sum_impl(mut state: SumState<i32>) {
    loop {
        println!("inside loop!");
        state = match state {
            SumState::S1 { output } => {
                println!("Something went wrong in sum_impl!");
                SumState::S1 { output }
            }
            SumState::S0 { input, output, sum } => {
                match input.pull().await {
                    Event::Data(data) => {
                        let sum = sum + data;
                        //output.push(Event::Data(sum)).await;
                        println!("sum is: {}", sum);
                        SumState::S0 { input, output, sum }
                    }
                    Event::Marker => {
                        /* snapshop
                        1. pull the whole queue
                        2. serialize
                        3. save
                        */

                        let queue = input.get_buffer().await;
                        println!("The buffer to save to disk is: {:?}", queue);

                        output.push(Event::Data(1)).await;
                        SumState::S0 { input, output, sum }
                    }
                }
            }
        }
    }
}

async fn producer_op(mut state: SumState<i32>) {
    loop {
        println!("Inside producer loop!");
        state = match state {
            SumState::S0 { input, output, sum } => {
                println!("Something went wrong in producer_op!");
                SumState::S1 { output }
            }
            SumState::S1 { output } => {
                println!("output1: {:?}", output);
                let x = 10;
                output.push(Event::Data(x)).await;
                //println!("RES: {:?}", res);
                SumState::S1 { output}
                
            }
        }
    }
}

fn prod_op_spawn() -> PullChan<Event<i32>> {
    let (push, pull) = channel::<Event<i32>>();
    let state = SumState::S1 { output: push };
    tokio::task::spawn(producer_op(state));
    println!("producer operator spawned!");
    pull
}

fn sum(input: PullChan<Event<i32>>) -> PullChan<Event<i32>> {
    let (push, pull) = channel::<Event<i32>>();
    let state = SumState::S0 {
        input,
        output: push,
        sum: 0,
    };
    tokio::task::spawn(sum_impl(state));
    println!("sum_impl operator spawned!");
    pull
}

#[test]
fn test() {
    console_subscriber::init();
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let (w, r) = channel();
            let r2 = sum(r);
            w.push(Event::Data(2)).await;
            w.push(Event::Marker).await;
            w.push(Event::Data(3)).await;
            let x = r2.pull().await;
            println!("Result of x: {:?}", x);
            assert!(x == Event::Data(2));
            let y = r2.pull().await;
            println!("Result of y: {:?}", y);
            assert!(y == Event::Data(1));
        })
}

#[test]
fn test_queue_capacity() {
    //console_subscriber::init();
    tokio::runtime::Builder::new_multi_thread()
        .build()
        .unwrap()
        .block_on(async {
            //boot_up_func().await;
            let producer = prod_op_spawn();
            let consumer = sum(producer);

            loop{
                let ax = 2;
            }

            //let z = r2.pull().await;
            //println!("Result of z: {:?}", z);
            let z = Event::Data(2);
            assert!(z == Event::Data(2));
        })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum State1 {
    S1(String, String, i32),
    S2(String, String, String),
}

async fn execute_func(mut s: State1) {
    s = match s {
        State1::S1(_, _, _) => State1::S1("in1".to_string(), "out1".to_string(), 1),
        State1::S2(_, _, _) => State1::S2("in2".to_string(), "out2".to_string(), "f2".to_string()),
    };
    println!("{:?}", s);
    store_func(s).await;
}

async fn load_func() -> State1 {
    let mut contents = vec![];

    let file = OpenOptions::new().read(true).open("foo.txt").await;

    file.unwrap().read_to_end(&mut contents).await.unwrap();

    let checkpoint_serialized = std::str::from_utf8(&contents).unwrap().to_string();

    let checkpoint_deserialized = deserialize_func(checkpoint_serialized).await;

    return checkpoint_deserialized;
}

async fn recover_func() {
    execute_func(load_func().await).await;
}

async fn serialize_func(s: State1) -> String {
    let serialized = serde_json::to_string(&s).unwrap();
    return serialized;
}

async fn deserialize_func(s: String) -> State1 {
    let deserialized: State1 = serde_json::from_str(&s).unwrap();
    return deserialized;
}

async fn store_func(s: State1) {
    //Serialize the state
    let serialized_state = serialize_func(s).await;
    //Saving checkpoint to file (appends atm)
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open("foo.txt")
        .await;
    file.unwrap()
        .write_all(serialized_state.as_bytes())
        .await
        .unwrap();
}
impl<T: Serialize> Serialize for PushChan<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        todo!();
        //VecDeque::<T>::serialize(&self.0.as_ref().queue, serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for PushChan<T> {
    fn deserialize<D>(deserializer: D) -> Result<PushChan<T>, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!(); /*
                 let chan = Chan {
                     queue: VecDeque::<T>::deserialize(deserializer),
                     pullvar: Notify::new(),
                     pushvar: Notify::new(),
                 };
                 Ok(PullChan(Arc::new(chan)))
                 */
    }
}

impl<T: Serialize> Serialize for PullChan<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        todo!();
        //VecDeque::<T>::serialize(&self.0.as_ref().queue, serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for PullChan<T> {
    fn deserialize<D>(deserializer: D) -> Result<PullChan<T>, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!(); /*
                 let chan = Chan {
                     queue: VecDeque::<T>::deserialize(deserializer),
                     pullvar: Notify::new(),
                     pushvar: Notify::new(),
                 };
                 Ok(PullChan(Arc::new(chan)))
                 */
    }
}

async fn boot_up_func() {
    console_subscriber::init();
    //let (w, r) = channel();
    let producer = prod_op_spawn();
    let consumer = sum(producer);


    loop{
        
    }

    /*
    w.push(Event::Data(2)).await;
    w.push(Event::Marker).await;
    w.push(Event::Data(3)).await;*/

    /*
    let x = r2.pull().await;
    println!("Result of x: {:?}", x);
    let y = r2.pull().await;
    println!("Result of y: {:?}", y);*/
}

#[tokio::main]
async fn main() {
    //let mut stateTest = State1::S2("In".to_string(), "Out".to_string(), "f".to_string());
    //execute_func(stateTest).await;
    //recover_func();
    boot_up_func().await;
}
