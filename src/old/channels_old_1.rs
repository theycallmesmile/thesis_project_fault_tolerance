use tokio::sync::Notify;
use tokio::sync::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

const CAPACITY: usize = 10;

/// An async FIFO SPSC channel.
#[derive(Debug)]
struct Chan<T> {
    queue: Mutex<VecDeque<T>>,
    pullvar: Notify,
    pushvar: Notify,
}

impl<T> Chan<T> {
    fn new(cap: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(cap)),
            pullvar: Notify::new(),
            pushvar: Notify::new(),
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

impl<T> PushChan<T> {
    pub async fn push(&self, data: T) {
        println!("pushchan lock");
        let mut queue = self.0.queue.lock().await;
        println!("pushchan unlocked");
        println!("push: length: {}", queue.len());
        println!("queue capacity {}", queue.capacity());
        //testing

        while queue.len().eq(&2){  //while queue.len() == queue.capacity() {
            println!("baa");
            self.0.pushvar.notified().await;
            println!("baa2");
            //uu = uu +1;
        }
        println!("test1");
        queue.push_back(data);
        println!("test2");
        //drop(queue);
        self.0.pullvar.notify_one();
    }
}

impl<T> PullChan<T> {
    pub async fn pull(&self) -> T {
        println!("pullchan lock");
        let mut queue = self.0.queue.lock().await;
        println!("pullchan unlock");
        println!("pull: length: {}",queue.len());
        
        while queue.is_empty() {
            println!("paa");
            self.0.pullvar.notified().await;
            println!("paa2");
        }
        let data = queue.pop_front().unwrap();
        //drop(queue);
        self.0.pushvar.notify_one();
        println!("pull: length_after: {}",queue.len());
        data
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