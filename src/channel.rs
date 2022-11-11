use futures::future::join;
use futures::join;
use tokio::sync::Notify;
//use tokio::sync::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

use async_std::task;
use std::time::Duration;
use async_std::sync::{Condvar, Mutex};

use serde::Deserialize;
use serde::Serialize;

//shared module
use crate::shared::Event;

pub const CAPACITY: usize = 15;

/// An async FIFO SPSC channel.
#[derive(Debug)]
pub struct Chan<T> {
    pub queue: Mutex<VecDeque<T>>,
    pullvar: Condvar,
    pushvar: Condvar,
    pub log: Mutex<VecDeque<T>>,
}


impl<T> Chan<T> {
    fn new(cap: usize) -> Self {
        let chan_buff = VecDeque::with_capacity(cap);
        let chan_log = VecDeque::with_capacity(cap);
        Self {
            queue: Mutex::new(chan_buff),
            pullvar: Condvar::new(),
            pushvar: Condvar::new(),
            log: Mutex::new(chan_log),
        }
    }
    fn load(cap: usize, ch: Chan<T>) -> Self {
        println!("Loading channel with capacity {}", cap);
        Self {
            queue: ch.queue,
            pullvar: ch.pullvar,
            pushvar: ch.pushvar,
            log: ch.log,
        }
    }
    fn from_vec(buf: Vec<T>) -> Self {
        let chan_log = VecDeque::with_capacity(CAPACITY);
        Self { queue: Mutex::new(buf.into_iter().collect()), pullvar: Condvar::new(), pushvar: Condvar::new(), log: Mutex::new(chan_log)}
    }
    fn from_vec_log(chan_log: Vec<T>) -> Self {
        let chan_buff = VecDeque::with_capacity(CAPACITY);
        Self { queue: Mutex::new(chan_buff), pullvar: Condvar::new(), pushvar: Condvar::new(), log: Mutex::new(chan_log.into_iter().collect())}
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
    //pub fn from_vec(buf: Vec<T>) -> Self {
    //    Self(Arc::new(Chan::from_vec(buf)))
    //}
    pub fn from_vec_with_log(chan_log: Vec<T>) -> Self {
        Self(Arc::new(Chan::from_vec_log(chan_log)))
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
    pub fn new() -> Self {
        Self(Arc::new(Chan::new(CAPACITY)))
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

pub fn channel_vec<T>(amount: usize) -> Vec<(PushChan<T>, PullChan<T>)> { //ORIGINAL -- REMOVE AND REPLACE WITH MODIFIED
    let mut chan_vec: Vec<(PushChan<T>, PullChan<T>)> = Vec::new();
    for n in 0..amount {
        let chan = Arc::new(Chan::new(CAPACITY));
        chan_vec.push((PushChan(chan.clone()), PullChan(chan)));
    }
    chan_vec
}
pub fn channel_vec_modified<T>(amount_vec: Vec<i32>) -> Vec<(PushChan<T>, PullChan<T>)> {
    let mut chan_vec: Vec<(PushChan<T>, PullChan<T>)> = Vec::new();
    for n in amount_vec {
        let chan = Arc::new(Chan::new(CAPACITY));
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
        let mut queue = self.0.queue.lock().await;
        queue = self
            .0
            .pushvar
            .wait_until(queue, |queue| {
                queue.len() < queue.capacity()
            })
            .await;
        queue.push_back(data);
        drop(queue);
        self.0.pullvar.notify_one();
    }
}

impl<T:  std::fmt::Debug> PullChan<T> {
    pub async fn pull(&self) -> T {
        let mut queue = self.0.queue.lock().await;

        queue = self
            .0
            .pullvar
            .wait_until(queue, |queue| {
                //println!("queue: {:?}, self: {:?}",queue, self);
                !queue.is_empty()
            })
            .await;
        let data = queue.pop_front().unwrap();
        drop(queue);
        //println!("the data: {:?}", data);
        self.0.pushvar.notify_one();
        data
    }

    pub async fn pull_manager(&self) -> T {
        let mut queue = self.0.queue.lock().await;
        queue = self
            .0
            .pullvar
            .wait_until(queue, |queue| {
                !queue.is_empty()
            })
            .await;
        let data = queue.pop_front().unwrap();
        drop(queue);
        self.0.pushvar.notify_one();
        data
    }

    pub async fn clear_pull_chan(&self) {
        let mut queue = self.0.queue.lock().await;
        queue.clear();
        drop(queue);
    }

    pub async fn pull_log(&self) -> T {
        let mut log = self.0.log.lock().await;
        let data = log.pop_front().unwrap();
        drop(log);
        data
    }
    
    pub async fn push_log(&self, data: T) {
        let mut log = self.0.log.lock().await;
        log.push_back(data);
        drop(log);
    }

    pub async fn log_length_check(&self) -> bool {
        self.0.log.lock().await.is_empty()
    }

    pub async fn buffer_length_check(&self) -> bool {
        self.0.queue.lock().await.is_empty()
    }

    pub async fn check_pull_length(&self) {//testing, remove when done
        let mut queue = self.0.queue.lock().await;
        let mut log = self.0.log.lock().await;
        self
            .0
            .pullvar
            .wait_until(queue, |queue| {
                (!queue.is_empty() || !log.is_empty())
            })
            .await;
        self.0.pushvar.notify_one();
    }

    pub async fn pull_buff_log(&self) -> T {
        let in_event0 = if !self.log_length_check().await {
            self.pull_log().await
        } else {
            let rtrn = self.pull().await;
            rtrn
        };
        in_event0
    }

    pub async fn check_pull_length_original(&self) {
        loop{
            let (log_check, buffer_check) = join!(self.log_length_check(), self.buffer_length_check());
            task::sleep(Duration::from_millis(2)).await; //change to some notification system
            if (!log_check || !buffer_check){
                break;
            }
        }
    }
    pub async fn check_pull_length_original2(&self) {
        //self.testing().await;
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
}


