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
//use tokio::task::JoinSet;

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
enum ProducerState {
    S0 {
        output: PushChan<Event<()>>,
        count: i32,
    },
}

#[derive(Serialize, Deserialize, Debug)]
enum ConsumerState {
    S0 {
        input: PullChan<Event<()>>,
        count: i32,
    },
}

async fn consumer_impl(mut state: ConsumerState) {
    loop {
        state = match state {
            ConsumerState::S0 { input, count } => {
                match input.pull().await {
                    Event::Data(data) => {
                        let count = count + 1;
                        println!("Consumer count is: {}", count);
                        ConsumerState::S0 { input, count }
                    }
                    Event::Marker => {
                        /* snapshot
                        1. pull the whole queue
                        2. serialize
                        3. save
                        */

                        let queue = input.get_buffer().await;
                        println!("The buffer to save to disk is: {:?}", queue);

                        ConsumerState::S0 { input, count }
                    }
                }
            }
        }
    }
}

async fn producer_impl(mut state: ProducerState) {
    loop {
        println!("Inside producer loop!");
        state = match state {
            ProducerState::S0 { output, count } => {
                let count = count + 1;
                output.push(Event::Data(())).await;
                ProducerState::S0 { output, count }
            }
        }
    }
}

fn producer() -> PullChan<Event<()>> {
    let (push, pull) = channel::<Event<()>>();
    let state = ProducerState::S0 {
        output: push,
        count: 0,
    };
    async_std::task::spawn(producer_impl(state));
    println!("producer operator spawned!");
    pull
}

fn consumer(input: PullChan<Event<()>>) {
    let state = ConsumerState::S0 { input, count: 0 };
    async_std::task::spawn(consumer_impl(state));
    println!("sum_impl operator spawned!");
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
    //let mut set = JoinSet::new();

    let stream = producer();
    consumer(stream);

    loop{//System dies without this loop

    }


}

#[tokio::main]
//#[async_std::main]
async fn main() {
    //async_std::task::block_on(boot_up_func());
    boot_up_func().await;
}