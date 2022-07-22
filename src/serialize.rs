

use std::collections::HashMap;
use std::collections::HashSet;

//Channel module
use crate::channel::PullChan;
use crate::channel::PushChan;

//Producer module
use crate::producer::Event;







unsafe impl Send for SerdeState {}
unsafe impl Sync for SerdeState {}
#[derive(Default)]
pub struct SerdeState {
    serialised: HashSet<*const ()>, //raw pointer adress av arc chan
    deserialised: HashMap<*const (), *const ()> //adress av  |
}



pub async fn serialize_func(s: Vec<Event<()>>) -> String {
    let serialized = serde_json::to_string(&s).unwrap();
    return serialized;
}


/*pub struct SerdeContext {
    serialised: HashSet<u64>,
    deserialised: HashMap<u64, (PullChan<>, PushChan<>)>,
}*/

/* 
impl<T: Serialize> Serialize for PushChan<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        todo!();
        //let se = &self.0.as_ref().queue;

        //VecDeque::<T>::serialize(se, serializer)
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
        // VecDeque::<T>::serialize(&self.0.as_ref().queue, serializer)
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

*/