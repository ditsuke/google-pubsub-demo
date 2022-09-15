use anyhow::anyhow;
use std::collections::HashMap;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;

use google_cloud_pubsub::{
    client::Client, subscriber::ReceivedMessage, subscription::SubscriptionConfig,
};

const MAX_PULL: i32 = 10;
const CONSUMERS: u8 = 3;

const SUBSCRIPTION_NAME: &str = "some-subscription-2";
const TOPIC: &str = "something-i-really-like";

const ACK_DEADLINE: i32 = 10;
const ORDERING: bool = true;

struct TaggedMessageBatch {
    consumer_id: u8,
    message_batch: Vec<ReceivedMessage>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let client = Client::default().await?;
    let subscription = client.subscription(SUBSCRIPTION_NAME);

    let topic = client.topic(TOPIC);

    // subscription -- create if doesn't exist
    if !subscription.exists(None, None).await? {
        subscription
            .create(
                topic.fully_qualified_name(),
                SubscriptionConfig {
                    enable_message_ordering: ORDERING,
                    ack_deadline_seconds: ACK_DEADLINE,
                    ..SubscriptionConfig::default()
                },
                None,
                None,
            )
            .await?;
        println!("had to create subscription i did...");
    }

    let (sender, receiver) = channel::<TaggedMessageBatch>();

    for i in 0..CONSUMERS {
        let sender = sender.clone();
        spawn_consumer(i, &client, sender.clone());
    }

    let mut counters: HashMap<u8, i32> = HashMap::new();
    for batch in receiver {
        // println!(
        //     "Consumer {}. Messages ====> {:?}",
        //     batch.consumer_id,
        //     batch.message_batch.iter().map(|m| m.message.clone()).collect::<Vec<_>>(),
        // );
        let now = match counters.get(&batch.consumer_id) {
            Some(n) => *n,
            None => 0,
        };
        counters.insert(batch.consumer_id, now + batch.message_batch.len() as i32);
        println!("now: {:#?}", counters);
    }

    Ok(())
}

fn spawn_consumer(id: u8, client: &Client, batch_sender: Sender<TaggedMessageBatch>) {
    let sub = client.subscription(SUBSCRIPTION_NAME);
    tokio::spawn(async move {
        loop {
            let pull_result = sub.pull(MAX_PULL, None, None).await;
            match pull_result {
                Ok(messages) => {
                    // Ack each message
                    for message in &messages {
                        match message.ack().await {
                            Ok(_) => {}
                            Err(status) => println!("error in acking message; status={status}"),
                        }
                    }

                    batch_sender
                        .send(TaggedMessageBatch {
                            consumer_id: id,
                            message_batch: messages,
                        })
                        .unwrap();
                }
                Err(e) => println!("error in pulling messages. status={e}"),
            };
        }
    });
}
