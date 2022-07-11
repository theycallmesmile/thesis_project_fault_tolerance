#![allow(warnings, unused)]
extern crate tokio;

use std::str;
use std::ptr::eq;

use rskafka::time;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncReadExt;

use serde::{Serialize, Deserialize};
use serde::{Serializer, Deserializer};
use serde_json;

mod channel;
use channel::PullChan;
use channel::PushChan;
use channel::channel;

use std::collections::VecDeque;
use std::sync::Arc;


async fn map_operator(input: PullChan<i32>, f: fn(i32) -> i32, output:PushChan<i32>) {
    loop {
        let x = input.pull().await;
        let y = f(x);
        output.push(y).await;
    }
}

#[derive(Serialize, Deserialize)]
enum MapState {
    S0{input: PullChan<i32>, output:PushChan<i32>}
}

async fn map_operator_transition(mut state: MapState) {
    if let MapState::S0 {input, output} = state {
                let x = input.pull().await;
                let y = x + 1;
                output.push(y).await;
                MapState::S0 { input, output };
            }
}


fn map(input: PullChan<i32>) -> PullChan<i32> {
    let ( push, pull) = channel();
    let state = MapState::S0 { input, output: push };
    tokio::task::spawn(map_operator_transition(state));
    pull
}
#[test]
fn test() {
    console_subscriber::init();
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let (w, r) = channel();
        let r2 = map(r);
        w.push(1).await;
        w.push(2).await;
        let x = r2.pull().await;
        println!("Result of x: {}", x);
        assert!(x == 2);
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum State1 {
    S1(String,String,i32),
    S2(String,String,String),
}


async fn execute_func(mut s: State1){
    s = match s {
        State1::S1(_,_,_) => State1::S1("in1".to_string(),"out1".to_string(),1),
        State1::S2(_,_,_) => State1::S2("in2".to_string(),"out2".to_string(),"f2".to_string()),
    };
    println!("{:?}",s);
    store_func(s).await;

}

async fn load_func() -> State1{
    let mut contents = vec![];

    let file = OpenOptions::new()
        .read(true)
        .open("foo.txt")
        .await;

    file.unwrap()
    .read_to_end(&mut contents)
    .await
    .unwrap();

    let checkpoint_serialized = std::str::from_utf8(&contents).unwrap().to_string();

    let checkpoint_deserialized = deserialize_func(checkpoint_serialized).await;

    return checkpoint_deserialized;
}

async fn recover_func(){
    execute_func(load_func().await).await; 
}

async fn serialize_func(s: State1)->String{
    let serialized = serde_json::to_string(&s).unwrap();
    return serialized;
}

async fn deserialize_func(s: String)->State1{
    let deserialized: State1 = serde_json::from_str(&s).unwrap();
    return deserialized;
}

async fn store_func(s: State1){
    //Serialize the state
    let serialized_state = serialize_func(s).await;
    //Saving checkpoint to file (appends atm)
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open("foo.txt")
        .await;
    file.unwrap().write_all(serialized_state.as_bytes()).await.unwrap();
}
impl<T: Serialize> Serialize for PushChan<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {todo!();
        //VecDeque::<T>::serialize(&self.0.as_ref().queue, serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for PushChan<T> {
    fn deserialize<D>(deserializer: D) -> Result<PushChan<T>, D::Error>
    where
        D: Deserializer<'de>,
    {todo!();/*
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
    {todo!();
        //VecDeque::<T>::serialize(&self.0.as_ref().queue, serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for PullChan<T> {
    fn deserialize<D>(deserializer: D) -> Result<PullChan<T>, D::Error>
    where
        D: Deserializer<'de>,
    {todo!();/*
        let chan = Chan {
            queue: VecDeque::<T>::deserialize(deserializer),
            pullvar: Notify::new(),
            pushvar: Notify::new(),
        };
        Ok(PullChan(Arc::new(chan)))
        */
    }
}


#[tokio::main]
async fn main() {
    let mut stateTest = State1::S2("In".to_string(), "Out".to_string(), "f".to_string());
    execute_func(stateTest).await;
    //recover_func();
}