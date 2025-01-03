use tokio::{select, signal};

#[tokio::main]
async fn main() {
    // TODO ADD REAL TESTS
    let ctrl_c = signal::ctrl_c();
    println!("Press Ctrl+C to exit...");
    println!("starting...");
    let (sled_event_queue, enqueuer) = sled_queue::SledEventQueue::init_actor_proxy("test", 128);
    println!("enqueueing...");

    let _loop = tokio::spawn(sled_event_queue.run());

    enqueuer
        .add(Vec::from(String::from("HELLO")))
        .await
        .unwrap()
        .unwrap();

    println!("Adding...");
    enqueuer
        .add(Vec::from(String::from("BYE")))
        .await
        .unwrap()
        .unwrap();
    println!("Adding...");
    {
        println!("Reading...");
        enqueuer
            .ready_to_read()
            .await
            .unwrap()
            .recv()
            .await
            .unwrap();

        let (value, key) = enqueuer.read().await.unwrap().unwrap();

        println!("Enqueued value: {}", String::from_utf8_lossy(&value));
        println!("Enqueued key: {}", String::from_utf8_lossy(&key));

        println!("Deleting...");
        enqueuer.delete(key).await.unwrap().unwrap();
    }
    {
        println!("Reading...");
        enqueuer
            .ready_to_read()
            .await
            .unwrap()
            .recv()
            .await
            .unwrap();

        let (value, key) = enqueuer.read().await.unwrap().unwrap();

        println!("Enqueued value: {}", String::from_utf8_lossy(&value));
        println!("Enqueued key: {}", String::from_utf8_lossy(&key));

        println!("Deleting...");
        enqueuer.delete(key).await.unwrap().unwrap();
    }
    enqueuer.shutdown().await.unwrap();
    _loop.await.unwrap();

    select! {
        _ = enqueuer.shutdown() => (),
        _ = ctrl_c => println!("Ctrl+C received. Exiting...")
    }
}
