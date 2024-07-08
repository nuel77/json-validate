use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use tokio::sync::{mpsc, oneshot};

pub enum Message {
    //start and end indeces of the chunk
    Chunk(Vec<u8>),
}

pub struct WriteActorMessage {
    message: Message,
    respond_to: oneshot::Sender<u64>,
}
enum DataOut {
    File(File),
    Stdout(io::Stdout),
}

impl DataOut {
    fn write(&mut self, chunk: Vec<u8>) -> io::Result<usize> {
        match self {
            DataOut::File(file) => file.write(&chunk),
            DataOut::Stdout(stdout) => stdout.write(&chunk),
        }
    }
}

/// WriteActor: Runs in a separate thread and writes the processed chunks to the output file
pub struct WriteActor {
    out: DataOut,
    receiver: mpsc::Receiver<WriteActorMessage>,
}

impl WriteActor {
    /// create a new WriteActor
    fn new(
        out_dir: Option<&String>,
        receiver: mpsc::Receiver<WriteActorMessage>,
    ) -> anyhow::Result<WriteActor> {
        let out = match out_dir {
            Some(dir) => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(dir)?;
                DataOut::File(file)
            }
            None => DataOut::Stdout(io::stdout()),
        };
        Ok(WriteActor { receiver, out })
    }
    /// handle Write Request
    /// merges the chunk into the output file mmap
    fn handle_message(&mut self, msg: WriteActorMessage) {
        match msg.message {
            Message::Chunk(chunk) => {
                //slice off zero bytes
                let chunk = chunk.iter().cloned().filter(|&x| x != 0).collect();
                // merge the chunk into the appropriate position in the mmap
                self.out.write(chunk).unwrap();
                msg.respond_to.send(0).expect("Failed to send response");
            }
        }
    }
}

pub async fn run_actor(mut actor: WriteActor) {
    while let Some(msg) = actor.receiver.recv().await {
        actor.handle_message(msg);
    }
}

/// WriteActorHandle: A handle to the WriteActor
/// Used to send actions to a WriteActor running in background
#[derive(Clone)]
pub struct WriteActorHandle {
    sender: mpsc::Sender<WriteActorMessage>,
}

impl WriteActorHandle {
    pub fn new(out_dir: Option<&String>) -> anyhow::Result<Self> {
        // create a channel with a buffer of 100 messages (can be put in as argument)
        let (sender, receiver) = mpsc::channel(100);
        let actor = WriteActor::new(out_dir, receiver)?;
        tokio::task::spawn(run_actor(actor));
        Ok(Self { sender })
    }

    /// send a processed chunk to the write actor
    pub async fn send_chunk(&self, chunk: Vec<u8>) -> u64 {
        let (send, recv) = oneshot::channel::<u64>();
        let msg = WriteActorMessage {
            message: Message::Chunk(chunk),
            respond_to: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Failed to send message")
    }
}
