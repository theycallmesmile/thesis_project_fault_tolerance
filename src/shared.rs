use std::sync::Arc;
use crate::serialization::SerdeState;

#[derive(Clone)]
struct Shared<T>(Arc<T>);


impl<T> Shared<T> {
    fn serialize(self, state: &mut SerdeState) {
        
    }
}