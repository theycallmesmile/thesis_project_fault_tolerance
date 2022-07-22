
//Manager module
use crate::manager::Context;

//Channel module
use crate::channel::channel;
use crate::channel::PullChan;
use crate::channel::PushChan;

//Producer
use crate::producer::Event;


#[derive(Debug, Clone)]
pub enum ConsumerState {
    S0 {
        input: PullChan<Event<()>>,
        count: i32,
    },
}


impl ConsumerState {
    pub async fn execute(mut self, ctx: Context) {
        println!("consumer ON!");
        loop {
            self = match self {
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
    
                            //let queue = input.get_buffer().await;
                            //println!("The buffer to save to disk is: {:?}", queue);
                            
                            
                            //store_func(input.get_buffer().await).await;
                            self = ConsumerState::S0 { input, count };
                            //self.store().await;
                            self
                        }
                    }
                }
            }
        }
    }

    /*async fn store(&self) {
        let persistent = self.to_persistent().await;
        let serialized = serde_json::to_string(&persistent).unwrap();
        todo!()
    }

    async fn restore() -> Self {
        let persistent = serde_json::from_str(todo!()).unwrap();
        Self::from_persistent(persistent).await
    }

    async fn to_persistent(&self) -> PersistentProducerState {
        todo!()
    }

    async fn from_persistent(persistent: PersistentProducerState) -> Self {
        todo!()
    }*/

    
}

/*pub fn consumer (input: PullChan<Event<()>>){
    let state = ConsumerState::S0 { input, count: 0 };
    async_std::task::spawn(state.execute(todo!()));
    println!("sum_impl operator spawned!");
}*/

//fn consumer(input : PersistentPullChan<()> ) {