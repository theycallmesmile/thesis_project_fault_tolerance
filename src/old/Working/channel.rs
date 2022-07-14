use tokio::sync::Notify;
//use tokio::sync::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

use async_std::sync::{Mutex, Condvar};
use async_std::task;

const CAPACITY: usize = 10;

/// An async FIFO SPSC channel.
#[derive(Debug)]
struct Chan<T> {
    queue: Mutex<VecDeque<T>>,
    pullvar: Condvar,
    pushvar: Condvar,
}

impl<T> Chan<T> {
    fn new(cap: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(cap)),
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

impl<T:Clone> PushChan<T>{
    pub async fn push(&self, data: T) -> Result<&'static str, T>{
        let mut queue = self.0.queue.lock().await;
        //let q = &queue.len();
        //println!("queue len: {}",q);
        while queue.len() == queue.capacity() {
            println!("while, queue len: {}",queue.len());
            queue = self.0.pushvar.wait(queue).await;
        }
        queue.push_back(data);
        self.0.pullvar.notify_one();
        Ok("ok")
    }
}

impl<T:Clone> PullChan<T> {
    pub async fn pull(&self) -> T {
        let mut queue = self.0.queue.lock().await;
        while queue.is_empty() {
            queue = self.0.pullvar.wait(queue).await;
        }
        let data = queue.pop_front().unwrap();
        self.0.pushvar.notify_one();
        data
    }
    pub async fn get_buffer(&self) -> Vec<T> {
        let queue = self.0.queue.lock().await;
        queue.iter().cloned().collect()
    }
}

#[test]
fn test() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let (w, r) = channel::<i32>();
        w.push(1).await;
        let x = r.pull().await;
        assert!(x == 1);
    }
);
}