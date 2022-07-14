use rskafka::time;





async fn test() {
    use rskafka::{
        client::{
            ClientBuilder,
            partition::Compression,
        },
        record::Record,
    };

    use time::OffsetDateTime;
    use std::collections::BTreeMap;
    
    // setup client
    let connection = "localhost:9092".to_owned();
    let client = ClientBuilder::new(vec![connection]).build().await.unwrap();

    // create a topic
    let topic = "RustTestTopicCode";
    /*let controller_client = client.controller_client().await.unwrap();
    controller_client.create_topic(
        topic,
        2,      // partitions
        1,      // replication factor
        5_000,  // timeout (ms)
    ).await.unwrap();*/
    
    // get a partition-bound client
    let partition_client = client
        .partition_client(
            topic.to_owned(),
            0,  // partition
         ).
        await.unwrap();
    
    // produce some data
    let record = Record {
        key: None,
        value: Some(b"hello kafka, this is Smile!".to_vec()),
        headers: BTreeMap::from([
            ("foo".to_owned(), b"bar".to_vec()),
        ]),
        timestamp: OffsetDateTime::now_utc(),
    };
    partition_client.produce(vec![record], Compression::default()).await.unwrap();
    
    // consume data
    let (records, high_watermark) = partition_client
        .fetch_records(
            0,  // offset
            1..1_000_000,  // min..max bytes
            1_000,  // max wait time
        )
       .await
       .unwrap();
    
    println!("{:?}",std::str::from_utf8(&records.last().unwrap().record.value.clone().unwrap()).unwrap().to_string());
}

async fn testing_func() {
    let te = Some(b"a1A".to_vec());
    let te2 = std::str::from_utf8(&te.clone().unwrap_or_default()).unwrap().to_string();
    println!("{:?}",te2);
    println!("{:?}",te);
}

async fn testing_record() {
    
    let record = rskafka::record::Record {
        key: None,
        value: Some(b"hello kafka, this is Smile!".to_vec()),
        headers: std::collections::BTreeMap::from([
            ("foo".to_owned(), b"bar".to_vec()),
        ]),
        timestamp: time::OffsetDateTime::now_utc(),
    };

    println!("TESTAR: {:?}",record);
    println!("");
    println!("TESTAR2: {:?}",record.value);
}


#[tokio::main]
async fn main() {
    test().await;
    //testing_func().await;
    //testing_record().await;
}