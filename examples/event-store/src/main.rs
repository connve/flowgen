use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::collect_sendable_stream;
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::{protocol::SaveMode, DeltaOps};
use flowgen::flow;
use flowgen_salesforce::pubsub::eventbus::v1::TopicInfo;
use flowgen_salesforce::pubsub::subscriber;
use futures::future::TryJoinAll;
use std::env;
use std::process;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::event;
use tracing::Level;

use std::sync::Arc;

fn get_table_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            String::from("int"),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            String::from("string"),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            String::from("timestamp"),
            DataType::Primitive(PrimitiveType::TimestampNtz),
            true,
        ),
    ]
}

fn get_table_batches() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("int", ArrowDataType::Int32, false),
        Field::new("string", ArrowDataType::Utf8, true),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let int_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let str_values = StringArray::from(vec!["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]);
    let ts_values = TimestampMicrosecondArray::from(vec![
        1000000012, 1000000012, 1000000012, 1000000012, 500012305, 500012305, 500012305, 500012305,
        500012305, 500012305, 500012305,
    ]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(int_values),
            Arc::new(str_values),
            Arc::new(ts_values),
        ],
    )
    .unwrap()
}

#[tokio::main]
async fn main() {
    // // Create a delta operations client pointing at an un-initialized location.
    // let ops = if let Ok(table_uri) = std::env::var("TABLE_URI") {
    //     DeltaOps::try_from_uri(table_uri).await.unwrap()
    // } else {
    //     DeltaOps::try_from_uri("/tmp/test").await.unwrap()
    // };

    // let table = ops
    //     .create()
    //     .with_columns(get_table_columns())
    //     .with_partition_columns(["timestamp"])
    //     .with_table_name("account")
    //     .with_comment("A table to show how delta-rs works")
    //     .await
    //     .unwrap();

    // let writer_properties = WriterProperties::builder()
    //     .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
    //     .build();

    // let batch = get_table_batches();
    // let table = DeltaOps(table)
    //     .write(vec![batch.clone()])
    //     .with_writer_properties(writer_properties)
    //     .await
    //     .unwrap();

    // // Install global log collector.
    // tracing_subscriber::fmt::init();

    // // Setup environment variables
    // let config_path = env::var("CONFIG_PATH").expect("env variable CONFIG_PATH should be set");

    // // Run flowgen service with a provided config.
    // let f = flowgen::flow::Builder::new(config_path.into())
    //     .build()
    //     .unwrap_or_else(|err| {
    //         error!("{:?}", err);
    //         process::exit(1);
    //     })
    //     .init()
    //     .await
    //     .unwrap_or_else(|err| {
    //         error!("{:?}", err);
    //         process::exit(1);
    //     });

    // let mut async_task_list: Vec<JoinHandle<Result<(), subscriber::Error>>> = Vec::new();
    // if let Some(source) = f.source {
    //     match source {
    //         flow::Source::salesforce_pubsub(source_subscriber) => {
    //             let mut subscriber_task_list = source_subscriber.init().unwrap();
    //             let mut rx = source_subscriber.rx;

    //             let mut topic_info_list: Vec<TopicInfo> = Vec::new();
    //             let receiver_task: JoinHandle<Result<(), subscriber::Error>> =
    //                 tokio::spawn(async move {
    //                     while let Some(cm) = rx.recv().await {
    //                         match cm {
    //                     flowgen_salesforce::pubsub::subscriber::ChannelMessage::FetchResponse(
    //                         fr,
    //                     ) => {
    //                         event!(name: "fetch_response", Level::INFO, rpc_id = fr.rpc_id);
    //                         for ce in fr.events {
    //                             if let Some(pe) = ce.event {
    //                                 event!(name: "event_consumed", Level::INFO, event_id = pe.id);
    //                                 // Get the relevant topic from the list.
    //                                 let topic: Vec<TopicInfo> = topic_info_list
    //                                     .iter()
    //                                     .filter(|t| t.schema_id == pe.schema_id)
    //                                     .cloned()
    //                                     .collect();

    //                                 // Setup nats subject based on the topic_name.
    //                                 let s = topic[0].topic_name.replace('/', ".").to_lowercase();
    //                                 let subject = &s[1..];

    //                                 // Publish an event.
    //                                 // let event: Vec<u8> = bincode::serialize(&pe).map_err(Error::Bincode)?;

    //                                 // nats_jetstream
    //                                 //     .send_publish(
    //                                 //         subject.to_string(),
    //                                 //         Publish::build().payload(event.into()),
    //                                 //     )
    //                                 //     .await
    //                                 //     .map_err(Error::NatsPublish)?;
    //                             }
    //                         }
    //                     }
    //                     flowgen_salesforce::pubsub::subscriber::ChannelMessage::TopicInfo(t) => {
    //                         event!(name: "topic_info", Level::INFO, rpc_id = t.rpc_id);
    //                         // let t_bytes: Vec<u8> = bincode::serialize(&t).unwrap();
    //                         // kv.put(t.topic_name.clone(), t_bytes.into())
    //                         //     .await
    //                         //     .map_err(Error::NatsPutKeyValue)?;
    //                         topic_info_list.push(t);
    //                     }
    //                 }
    //                     }
    //                     Ok(())
    //                 });
    //             async_task_list.append(&mut subscriber_task_list);
    //             async_task_list.push(receiver_task);
    //         }
    //     }
    // }
    // // Run all async tasks.
    // async_task_list
    //     .into_iter()
    //     .collect::<TryJoinAll<_>>()
    //     .await
    //     .map_err(subscriber::Error::TokioJoin)
    //     .unwrap();
}
