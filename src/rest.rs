
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

//Producer module
use crate::producer::Event;

//Serialize module
use crate::serialize::serialize_func_old;



async fn store_func(s: Vec<Event<()>>) {
    //Serialize the state
    println!("init");
    let serialized_state = serialize_func_old(s).await;
    //Saving checkpoint to file (appends atm)
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open("foo2.txt")
        .await;
    file.unwrap()
        .write_all(serialized_state.as_bytes())
        .await
        .unwrap();

    println!("DONE!{}",serialized_state)
}