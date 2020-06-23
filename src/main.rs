use std::collections::HashMap;
use std::process::exit;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures::{FutureExt, StreamExt};
use lapin::message::DeliveryResult;
use lapin::options::*;
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties, ExchangeKind};
use tokio::sync::{mpsc, RwLock};
use tokio_amqp::*;
use warp;
use warp::ws::{Message, WebSocket};
use warp::Filter;

/// Global unique user id counter.
static NEXT_USER_ID: AtomicUsize = AtomicUsize::new(1);

/// State of connected users.
type Users = Arc<RwLock<HashMap<usize, mpsc::UnboundedSender<Result<Message, warp::Error>>>>>;

/// RabbitMQ fanout exchange name
const NOTIFICATIONS_EXCHANGE_NAME: &str = "wavy.user.notifications";

/// Builds the options for an exclusive AMQP `queue_declare`.
fn queue_declare_options_exclusive() -> QueueDeclareOptions {
    let mut queue_options = QueueDeclareOptions::default();
    queue_options.exclusive = true;
    return queue_options;
}

async fn rabbitmq_main(users: Users) {
    println!("Starting RabbitMQ client");

    let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://@127.0.0.1:5672/local".into());

    let conn = Connection::connect(&addr, ConnectionProperties::default().with_tokio())
        .await
        .unwrap();
    let channel = conn.create_channel().await.unwrap();

    // Declare the fanout exchange
    channel
        .exchange_declare(
            NOTIFICATIONS_EXCHANGE_NAME,
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // Create the queue (exclusive to this connection, and will self-delete)
    let queue = channel
        .queue_declare("", queue_declare_options_exclusive(), FieldTable::default())
        .await
        .unwrap();
    let queue_name = queue.name().as_str();

    // Bind the queue to the fanout exchange
    channel
        .queue_bind(
            queue_name,
            NOTIFICATIONS_EXCHANGE_NAME,
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // Create the consumer
    let mut consumer = channel
        .basic_consume(
            queue.name().as_str(),
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // The 'delegate' is the task being done by the executor on each new message
    consumer
        .set_delegate(move |delivery: DeliveryResult| async move {
            let delivery = delivery.unwrap_or_else(|e| {
                eprintln!("RabbitMQ consumer error: {:?} {}", e, e);
                exit(1);
            });

            if let Some((channel, delivery)) = delivery {
                println!("Data: {:?}", delivery.data);

                for (&user_id, tx) in users.read().await.iter() {
                    // TODO: closure is `FnOnce` because it moves the variable `users` out of its environment
                    println!("- Sending to: {}", user_id);
                    tx.send(Ok(Message::binary(delivery.data.clone())));
                }

                channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .expect("failed to ack");
            }
        })
        .unwrap();

    loop {
        // Block on getting next message
        let _ = consumer.next().await.unwrap_or_else(|| {
            eprintln!("RabbitMQ consumer failed to read a message, exiting");
            exit(1);
        });
    }
}

async fn warp_main(users: Users) {
    println!("Starting websocket service");

    let users = warp::any().map(move || users.clone());

    let routes = warp::path("notifications")
        // The `ws()` filter will prepare the Websocket handshake.
        .and(warp::ws())
        .and(users)
        .map(|ws: warp::ws::Ws, users| {
            ws.on_upgrade(move |websocket| user_connected(websocket, users))
        });

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await
}

async fn user_connected(websocket: WebSocket, users: Users) {
    let user_id = Arc::from(NEXT_USER_ID.fetch_add(1, Ordering::Relaxed));

    let (user_ws_tx, _) = websocket.split();
    let (tx, rx) = mpsc::unbounded_channel();

    // Forwards the channel to the websocket
    let users2 = users.clone();
    let uid2 = user_id.clone();
    tokio::task::spawn(rx.forward(user_ws_tx).map(|result| async move {
        if let Err(e) = result {
            eprintln!("websocket send error by {}: {}", &uid2, e);
            users2.write().await.remove(&uid2);
        }
    }));

    println!("user_id: {}", *user_id);
    users.write().await.insert(*user_id, tx);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let users = Users::default();

    loop {
        tokio::join!(warp_main(users.clone()), rabbitmq_main(users.clone()));
    }
}
