use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::{client::Client, publisher::Awaiter};
use serde::Deserialize;
use std::error::Error;

#[derive(Deserialize, Debug)]
struct Config {
    pubsub_subscription: String,
    g_credentials: String,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let env: Config = match envy::from_env::<Config>() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("invalid env");
            return Err(std::io::Error::new(std::io::ErrorKind::Other, e).into());
        }
    };

    println!("env: {:?}", env);

    let client = Client::default().await?;

    let topic = client.topic("something-i-really-like");
    if !topic.exists(None, None).await? {
        topic.create(None, None, None).await?
    }

    let publisher = topic.new_publisher(None);

    let awaiters: Vec<tokio::task::JoinHandle<_>> = (1..10000)
        .map(|i| {
            let publisher = publisher.clone();
            let message = PubsubMessage {
                data: format!("some-{i}").into(),
                ..PubsubMessage::default()
            };
            tokio::spawn(async move {
                let awaiter: Awaiter = publisher.publish(message).await;
                awaiter.get(None).await
            })
        })
        .collect();

    for awaiter in awaiters {
        let result = awaiter.await?;
        match result {
            Ok(message_id) => println!("message published. id={message_id}"),
            Err(e) => println!("error publishing. status={e}"),
        }
    }
    Ok(())
}
