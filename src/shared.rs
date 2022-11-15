use core::panic;
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

//Serialization module
use crate::serialization::PersistentTask;
use crate::serialization::PartialPersistentTask;
use crate::serialization::SerdeState;

//Consumer module
use crate::consumer::ConsumerState;

//Producer module
use crate::producer::ProducerState;

//Manager module
use crate::manager::Context;
//use crate::manager::Task;
use crate::manager::PersistentTaskToManagerMessage;

//ConsumerProducer module
use crate::consumer_producer::ConsumerProducerState;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Eq, Hash)]
pub enum Event<T> {
    Data(T),
    Marker(i32),
    MessageAmount((String, i32)),
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

    pub async fn persistent_store(shared_state: PartialPersistentTask, m_id: i32, ctx: &Context) {
        let (send, mut recv) = oneshot::channel();

        let evnt = match shared_state {
            PartialPersistentTask::Consumer(consumer_state) => {
                PersistentTaskToManagerMessage::Serialise(PartialPersistentTask::Consumer(consumer_state), m_id, send)
            },
            PartialPersistentTask::Producer(producer_state) => {
                PersistentTaskToManagerMessage::Serialise(PartialPersistentTask::Producer(producer_state), m_id, send)
            },
            PartialPersistentTask::ConsumerProducer(consumer_producer_state) => {
                PersistentTaskToManagerMessage::Serialise(PartialPersistentTask::ConsumerProducer(consumer_producer_state), m_id, send)
            },
            PartialPersistentTask::PartialConsumerProducer(partial_consumer_producer_state) => {
                PersistentTaskToManagerMessage::Serialise(PartialPersistentTask::PartialConsumerProducer(partial_consumer_producer_state), m_id, send)
            },
        };

        println!("pushed state snapshot to manager");
        ctx.state_manager_send.push(evnt).await;
        println!("waiting for promise");

        let result = recv.await;

        println!("Got the promise: {}", result.unwrap());
    }

    pub async fn end_message(ctx: &Context) {
        let (send, mut recv) = oneshot::channel();
        ctx.state_manager_send.push(PersistentTaskToManagerMessage::Benchmarking(send)).await;
        let result = recv.await;
        println!("RECEIVED PROMISE: {}", result.unwrap());
    }
}