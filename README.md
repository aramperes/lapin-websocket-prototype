# lapin-websocket-prototype
A prototype to broadcast AMQP messages to websocket clients, written in stable Rust

## Objective

Publish a UTF-8 message on an AMQP (RabbitMQ) fanout exchange. Each instance of `lapin-websocket-prototype` will receive this payload,
and broadcast it to the websocket clients connected. Implemented using async to support a high volume of connections.

## Main crates used

* [lapin](https://crates.io/crates/lapin) and [tokio-amqp](https://crates.io/crates/tokio-amqp) for the async AMQP implementation
* [warp](https://crates.io/crates/warp) for the websocket service
* [tokio](https://crates.io/crates/tokio) for the async runtime

## Things I learned

* Using lapin and warp
* Async closures
* Lazy statics
* The difference between `Fn` and `FnOnce`
