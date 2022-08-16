use tokio::sync::oneshot;

use std::sync::Arc;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time;

use async_std::task;

use serde::Deserialize;
use serde::Serialize;

//Manager module
use crate::manager::Context;
use crate::manager::Task;
use crate::manager::TaskToManagerMessage;

//Channel module
use crate::channel::channel;
use crate::channel::PullChan;
use crate::channel::PushChan;

//Producer
use crate::producer::Event;

//Shared module
use crate::shared::SharedState;
use crate::shared::Shared;

#[derive(Debug, Clone)]
pub enum ConsumerState {
    S0 {
        input_vec: Vec<PullChan<Event<()>>>,
        count: i32,
    },
}

impl ConsumerState {
    pub async fn execute(mut self, ctx: Context) {
        println!("consumer ON!");
        let mut interval = time::interval(time::Duration::from_millis(200));
        let mut snapshot_counter:HashSet<usize> = HashSet::new();

        task::sleep(Duration::from_secs(2)).await;
        let mut return_state = match &self {
            ConsumerState::S0 { input_vec, count } => ConsumerState::S0 {
                input_vec: input_vec.to_owned(),
                count: count.to_owned(),
            },
        };

        loop {
            self = match &self {
                ConsumerState::S0 { input_vec, count } => {
                    for n in 0..input_vec.len() {
                        if !snapshot_counter.contains(&n) && !snapshot_counter.len().eq(&input_vec.len()){
                            tokio::select! {
                                _ = interval.tick() => {
                                    println!("Consumer count is: {}, Buffer empty for: {:?}", count, &input_vec[n]);

                                },
                                event = input_vec[n].pull() => {
                                    match event {
                                        Event::Data(data) => {
                                            let loc_count = count + 1;
                                            println!("Consumer count is: {}", count);
                                            println!("The consumer buffer: {:?}", &input_vec[n].0.queue);

                                            return_state = ConsumerState::S0 {
                                                input_vec: input_vec.to_owned(),
                                                count: loc_count,
                                            };
                                        },
                                        Event::Marker => {
                                            //add to the snapshot_count
                                            snapshot_counter.insert(n);

                                            return_state = ConsumerState::S0 {
                                                input_vec: input_vec.to_owned(),
                                                count: count.to_owned(),
                                            };
                                        },
                                        Event::MessageAmount(amount) => {},
                                    }
                                },
                            }
                        } else if snapshot_counter.len().eq(&input_vec.len()){
                            //snapshoting
                            println!("Start consumer snapshotting");
                            //self.store(&ctx).await;
                            Shared::<()>::store(SharedState::Consumer(self.clone()), &ctx).await;
                            println!("Done with consumer snapshotting");
                            snapshot_counter.clear();
                        }
                    }
                    return_state.clone()
                }
            }
        }
    }
}
