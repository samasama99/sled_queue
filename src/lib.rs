use log::{debug, error, info};
use sled::Db;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{sleep, Duration};
use uuid_v7::gen_uuid_v7;

pub enum Command {
    Add {
        payload: Vec<u8>,
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    ReadyToRead(oneshot::Sender<Receiver<()>>),
    Read {
        result_sender: oneshot::Sender<Option<(Vec<u8>, Vec<u8>)>>,
    },
    Delete {
        key: Vec<u8>,
        result_sender: oneshot::Sender<Result<(), ()>>,
    },
    Shutdown(oneshot::Sender<()>),
}

pub struct SledEventQueue {
    pub sled_db: Db,
    pub receiver: mpsc::Receiver<Command>,
}

impl SledEventQueue {
    pub fn enqueue(&self, item: &[u8]) -> sled::Result<()> {
        let key = gen_uuid_v7().to_string();
        debug!("Enqueueing event with key {:?}", key);
        self.sled_db.insert(Vec::from(key), item)?;
        Ok(())
    }

    pub fn init_actor_proxy(queue_name: &str, size: usize) -> (SledEventQueue, SledQueueProxy) {
        let sled_db = sled::open(queue_name).expect("Failed to open database");
        let (sender, receiver) = mpsc::channel(size);
        let actor = SledEventQueue { sled_db, receiver };
        let proxy = SledQueueProxy { sender };
        (actor, proxy)
    }

    fn dequeue(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if let Some(Ok((key, value))) = self.sled_db.iter().next() {
            Some((key.to_vec(), value.to_vec()))
        } else {
            println!("empty queue");
            None
        }
    }
    pub async fn run(mut self) {
        let (channel, _) = broadcast::channel(1);
        let channel_clone = channel.clone();
        let sled_db = self.sled_db.clone();
        tokio::spawn(async move {
            loop {
                if channel_clone.receiver_count() == 0 {
                    sleep(Duration::from_secs(10)).await;
                    continue;
                }
                tokio::select! {
                   _ =   if sled_db.is_empty() {
                            sleep(Duration::from_secs(10))
                        } else {
                            match channel_clone.send(()) {
                                Ok(count) => {
                                        info!("sled len: {} receiver count: {} received count: {}", sled_db.len(), channel_clone.receiver_count(), count);
                                },
                                Err(err) if channel_clone.receiver_count() != 0 => {
                                        error!("Error sending message: {:?} sled len: {} receiver count: {}", err, sled_db.len(), channel_clone.receiver_count());
                                }
                                _ => ()
                            }
                            sleep(Duration::from_secs(1))
                        } => debug!("timeout"),
                  change = sled_db.watch_prefix("") => {
                      debug!("change in queue");
                      if let Some(change) =  change {
                         debug!("change {:?}", change);
                      }
                  }
                }
            }
        });

        while let Some(command) = self.receiver.recv().await {
            match command {
                Command::Add {
                    payload,
                    result_sender,
                } => {
                    // TODO: this will change to something more graceful
                    self.enqueue(&payload)
                        .expect("queueing data should not fail");
                    let _ = result_sender.send(Ok(()));
                }
                Command::Shutdown(result_sender) => {
                    println!("shutting down...");
                    let _ = result_sender.send(());
                    break;
                }
                Command::Read { result_sender } => {
                    result_sender
                        .send(self.dequeue())
                        .expect("dequeue should not fail");
                }
                Command::Delete { key, result_sender } => {
                    self.sled_db.remove(&key).unwrap_or_else(|_| {
                        panic!(
                            "failed to remove key {}",
                            String::from_utf8_lossy(key.as_slice())
                        )
                    });
                    result_sender.send(Ok(())).expect("dequeue should not fail");
                }
                Command::ReadyToRead(result_sender) => {
                    result_sender
                        .send(channel.subscribe())
                        .expect("failed to send");
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SledQueueProxy {
    sender: mpsc::Sender<Command>,
}

impl SledQueueProxy {
    pub async fn read(
        &self,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, mpsc::error::SendError<Command>> {
        let (result_sender, result_receiver) = oneshot::channel();
        self.sender.send(Command::Read { result_sender }).await?;
        Ok(result_receiver
            .await
            .unwrap_or_else(|_| panic!("Failed to receive result from actor")))
    }

    pub async fn delete(
        &self,
        key: Vec<u8>,
    ) -> Result<Result<(), ()>, mpsc::error::SendError<Command>> {
        let (result_sender, result_receiver) = oneshot::channel();
        self.sender
            .send(Command::Delete { key, result_sender })
            .await?;
        Ok(result_receiver
            .await
            .unwrap_or_else(|_| panic!("Failed to receive result from actor")))
    }

    pub async fn add(
        &self,
        payload: Vec<u8>,
    ) -> Result<Result<(), ()>, mpsc::error::SendError<Command>> {
        let (result_sender, result_receiver) = oneshot::channel();
        self.sender
            .send(Command::Add {
                payload,
                result_sender,
            })
            .await?;
        Ok(result_receiver
            .await
            .unwrap_or_else(|_| panic!("Failed to receive result from actor")))
    }

    pub async fn shutdown(&self) -> Result<(), mpsc::error::SendError<Command>> {
        let (result_sender, result_receiver) = oneshot::channel();
        self.sender.send(Command::Shutdown(result_sender)).await?;
        result_receiver
            .await
            .unwrap_or_else(|_| panic!("Failed to receive result from actor"));
        Ok(())
    }

    pub async fn ready_to_read(&self) -> Result<Receiver<()>, mpsc::error::SendError<Command>> {
        let (result_sender, result_receiver) = oneshot::channel();
        self.sender
            .send(Command::ReadyToRead(result_sender))
            .await?;

        Ok(result_receiver
            .await
            .unwrap_or_else(|_| panic!("Failed to receive result from actor")))
    }
}
