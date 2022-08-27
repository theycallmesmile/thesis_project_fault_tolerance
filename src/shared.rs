use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::serialization::SerdeState;

use tokio::sync::oneshot;

//Consumer module
use crate::consumer::ConsumerState;

//Producer module
use crate::producer::ProducerState;

//Manager module
use crate::manager::Context;
use crate::manager::Task;
use crate::manager::TaskToManagerMessage;

//ConsumerProducer module
use crate::consumerProducer::ConsumerProducerState;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Eq, Hash)]
pub enum Event<T> {
    Data(T),
    Marker,
    MessageAmount(i32),
}

pub enum SharedState {
    Producer(ProducerState),
    ConsumerProducer(ConsumerProducerState),
    Consumer(ConsumerState),
}


#[derive(Clone)]
pub struct Shared<T>(Arc<T>);


impl<T> Shared<T> {
    fn serialize(self, state: &mut SerdeState) {
        
    }

    pub async fn store(shared_state: SharedState, ctx: &Context) {
        let (send, mut recv) = oneshot::channel();

        let evnt = match shared_state {
            SharedState::Producer(producer_state) => {
                TaskToManagerMessage::Serialise(Task::Producer(producer_state), send)
            },
            SharedState::ConsumerProducer(producer_consumer_state) => {
                TaskToManagerMessage::Serialise(Task::ConsumerProducer(producer_consumer_state), send)
            },
            SharedState::Consumer(consumer_state) => {
                TaskToManagerMessage::Serialise(Task::Consumer(consumer_state), send)
            },
        };

        println!("pushed state snapshot to manager");
        ctx.state_manager_send.push(evnt).await;
        println!("waiting for promise");

        let result = recv.await;

        println!("Got the promise: {}", result.unwrap());
    }

    pub async fn store_modified(shared_state: SharedState, ctx: &Context) {
        let (send, mut recv) = oneshot::channel();

        let evnt = match shared_state {
            SharedState::Producer(producer_state) => {
                TaskToManagerMessage::Serialise(Task::Producer(producer_state), send)
            },
            SharedState::ConsumerProducer(producer_consumer_state) => {
                TaskToManagerMessage::Serialise(Task::ConsumerProducer(producer_consumer_state), send)
            },
            SharedState::Consumer(consumer_state) => {
                TaskToManagerMessage::Serialise(Task::Consumer(consumer_state), send)
            },
        };

        println!("pushed state snapshot to manager");
        ctx.state_manager_send.push(evnt).await;
        println!("waiting for promise");

        let result = recv.await;

        println!("Got the promise: {}", result.unwrap());
    }

}