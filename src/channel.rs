use tokio::sync::Notify;
//use tokio::sync::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

use async_std::sync::{Condvar, Mutex};

use serde::Deserialize;
use serde::Serialize;

//producer module
use crate::producer::Event;


pub const CAPACITY: usize = 15;

/// An async FIFO SPSC channel.
#[derive(Debug)]
pub struct Chan<T> {
    pub queue: Mutex<VecDeque<T>>,
    pullvar: Condvar,
    pushvar: Condvar,
}

impl<T> Chan<T> {
    fn new(cap: usize) -> Self {
        let chan = VecDeque::with_capacity(cap);
        //println!("Created channel with capacity {}", chan.capacity());
        Self {
            queue: Mutex::new(chan),
            pullvar: Condvar::new(),
            pushvar: Condvar::new(),
        }
    }
    fn load(cap: usize, ch: Chan<T>) -> Self { //To be used when loading the checkpoints
        println!("Loading channel with capacity {}", cap);
        Self {
            queue: ch.queue,
            pullvar: ch.pullvar,
            pushvar: ch.pushvar,
        }
    }
    fn from_vec(buf: Vec<T>) -> Self {
        Self { queue: Mutex::new(buf.into_iter().collect()), pullvar: Condvar::new(), pushvar: Condvar::new() }
    }
}

#[derive(Debug)]
pub struct PushChan<T>(pub Arc<Chan<T>>);

#[derive(Debug)]
pub struct PullChan<T>(pub Arc<Chan<T>>);

impl<T> Clone for PushChan<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Clone for PullChan<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> PullChan<T> {
    pub fn from_vec(buf: Vec<T>) -> Self {
        Self(Arc::new(Chan::from_vec(buf)))
    }
    pub fn get_uid(&self) -> u64 {
        Arc::into_raw(self.0.clone()) as *const () as u64
    }
    pub fn from_uid(uid: u64) -> Self {
        unsafe {
            Self(Arc::from_raw(uid as *const _))
        }
    }
}

impl<T> PushChan<T> {
    pub fn from_vec(buf: Vec<T>) -> Self {
        Self(Arc::new(Chan::from_vec(buf)))
    }
    pub fn get_uid(&self) -> u64 {
        Arc::into_raw(self.0.clone()) as *const () as u64
    }
    pub fn from_uid(uid: u64) -> Self {
        unsafe {
            Self(Arc::from_raw(uid as *const _))
        }
    }
}

pub fn channel<T>() -> (PushChan<T>, PullChan<T>) {
    let chan = Arc::new(Chan::new(CAPACITY));
    (PushChan(chan.clone()), PullChan(chan))
}

pub fn channel_vec<T>(amount: usize) -> Vec<(PushChan<T>, PullChan<T>)> {
    let mut chan_vec: Vec<(PushChan<T>, PullChan<T>)> = Vec::new();
    for n in 0..amount {
        let chan = Arc::new(Chan::new(CAPACITY));
        let a = PushChan(chan.clone());
        chan_vec.push((PushChan(chan.clone()), PullChan(chan)));
    }
    chan_vec
}

//channel for manager, the operators will be able to send and receive messages(marker, ) to manager
pub fn channel_manager<T,G>() -> (PushChan<T>, PullChan<G>) {
    let chan = Arc::new(Chan::new(CAPACITY));
    let chan2= Arc::new(Chan::new(CAPACITY));
    (PushChan(chan.clone()), PullChan(chan2.clone()))
}

pub fn channel_load(vec_d: VecDeque<Event<()>>) -> (PushChan<Event<()>>, PullChan<Event<()>>) { //To be used when loading the checkpoints to connect the operator channels.
    let mut chan = Chan::new(CAPACITY);
    let mtx_queue = Mutex::new(vec_d);
    chan.queue = mtx_queue;
    let arc_chan = Arc::new(chan);
    (PushChan(arc_chan.clone()), PullChan(arc_chan))
}

impl<T:  std::fmt::Debug> PushChan<T> {
    pub async fn push(&self, data: T) {
        //println!("Trying to acquire lock for push");
        let mut queue = self.0.queue.lock().await;
        /*println!(
            "Trying to push into queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );*/
        queue = self
            .0
            .pushvar
            .wait_until(queue, |queue| {
                //println!("Checking push condition for {:?}", queue);
                queue.len() < queue.capacity()
            })
            .await;
        /*println!(
            "Pushing into queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );*/
        queue.push_back(data);
        drop(queue);
        self.0.pullvar.notify_one();
        //println!("Pushing done");
    }
}

impl<T:  std::fmt::Debug> PullChan<T> {
    pub async fn pull(&self) -> T {
        //println!("Trying to acquire lock for pull");
        let mut queue = self.0.queue.lock().await;
        /*println!(
            "Trying to pull from queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );*/
        queue = self
            .0
            .pullvar
            .wait_until(queue, |queue| {
                //println!("Checking pull condition for {:?}", queue);
                !queue.is_empty()
            })
            .await;
        /*println!(
            "Pulling from queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );*/
        let data = queue.pop_front().unwrap();
        drop(queue);
        self.0.pushvar.notify_one();
        //println!("Pulling done");
        data
    }

    pub async fn pull_manager(&self) -> T {
        //println!("****Trying to acquire lock for pull");
        let mut queue = self.0.queue.lock().await;
        /*println!(
            "****Trying to pull from queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );*/
        queue = self
            .0
            .pullvar
            .wait_until(queue, |queue| {
                println!("****Checking pull condition for {:?}", queue);
                !queue.is_empty()
            })
            .await;
        /*println!(
            "****Pulling from queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );*/
        let data = queue.pop_front().unwrap();
        drop(queue);
        self.0.pushvar.notify_one();
        //println!("****Pulling done");
        data
    }

}

impl<T: Clone + std::fmt::Debug> PushChan<T> {
    
    pub async fn get_buffer(&self) -> Vec<T> {
        let queue = self.0.queue.lock().await;
        let buffer = queue.iter().cloned().collect();
        drop(queue);
        buffer
    }

    pub async fn get_chan(&self) -> Arc<Chan<T>>{
        self.0.clone()
    }

    /*pub async fn push_snapshot(&self) {
        println!("Sending snapshot");
        println!("Trying to acquire lock for snapshot-push");
        let mut queue = self.0.queue.lock().await;
        println!(
            "Trying to snapshot-push into queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );
        queue = self
            .0
            .pushvar
            .wait_until(queue, |queue| {
                println!("Checking snapshot-push condition for {:?}", queue);
                queue.len() < queue.capacity()
            })
            .await;
        println!(
            "Snapshot-Pushing into queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );
        //queue.push_back(data);
        drop(queue);
        self.0.pullvar.notify_one();
        println!("Snapshot-pushing done");
    }*/
}


