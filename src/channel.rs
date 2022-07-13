use tokio::sync::Notify;
//use tokio::sync::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

use async_std::sync::{Condvar, Mutex};

const CAPACITY: usize = 15;

/// An async FIFO SPSC channel.
#[derive(Debug)]
struct Chan<T> {
    queue: Mutex<VecDeque<T>>,
    pullvar: Condvar,
    pushvar: Condvar,
}

impl<T> Chan<T> {
    fn new(cap: usize) -> Self {
        println!("Creating channel with capacity {}", cap);
        let chan = VecDeque::with_capacity(cap);
        println!("Created channel with capacity {}", chan.capacity());
        Self {
            queue: Mutex::new(chan),
            pullvar: Condvar::new(),
            pushvar: Condvar::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PushChan<T>(Arc<Chan<T>>);

#[derive(Clone, Debug)]
pub struct PullChan<T>(Arc<Chan<T>>);

pub fn channel<T>() -> (PushChan<T>, PullChan<T>) {
    let chan = Arc::new(Chan::new(CAPACITY));
    (PushChan(chan.clone()), PullChan(chan))
}

impl<T: Clone + std::fmt::Debug> PushChan<T> {
    pub async fn push(&self, data: T) {
        println!("Trying to acquire lock for push");
        let mut queue = self.0.queue.lock().await;
        println!(
            "Trying to push into queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );
        queue = self
            .0
            .pushvar
            .wait_until(queue, |queue| {
                println!("Checking push condition for {:?}", queue);
                queue.len() < queue.capacity()
            })
            .await;
        println!(
            "Pushing into queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );
        queue.push_back(data);
        drop(queue);
        self.0.pullvar.notify_one();
        println!("Pushing done");
    }
}

impl<T: Clone + std::fmt::Debug> PullChan<T> {
    pub async fn pull(&self) -> T {
        println!("Trying to acquire lock for pull");
        let mut queue = self.0.queue.lock().await;
        println!(
            "Trying to pull from queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );
        queue = self
            .0
            .pullvar
            .wait_until(queue, |queue| {
                println!("Checking pull condition for {:?}", queue);
                !queue.is_empty()
            })
            .await;
        println!(
            "Pulling from queue with length: {} / {}",
            queue.len(),
            queue.capacity()
        );
        let data = queue.pop_front().unwrap();
        drop(queue);
        self.0.pushvar.notify_one();
        println!("Pulling done");
        data
    }
    pub async fn get_buffer(&self) -> Vec<T> {
        let queue = self.0.queue.lock().await;
        queue.iter().cloned().collect()
    }
}
