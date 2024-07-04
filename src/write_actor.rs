use memmap2::{Mmap, MmapMut};
use std::fs::{File};
use tokio::sync::{mpsc, oneshot};

pub struct ChunkMessage {
    //start and end indeces of the chunk
    pub indices: (u32, u32),
    pub chunk: Vec<u8>,
}

impl ChunkMessage {
    pub fn from_mmap(mmap: &Mmap, indices: (u32, u32)) -> Self {
        let chunk = mmap[indices.0 as usize..indices.1 as usize].to_vec();
        ChunkMessage { indices, chunk }
    }
}

pub struct WriteActorMessage {
    message: ChunkMessage,
    respond_to: oneshot::Sender<u64>,
}

/// WriteActor: Runs in a separate thread and writes the processed chunks to the output file
pub struct WriteActor {
    mmap: MmapMut,
    size: u64,
    receiver: mpsc::Receiver<WriteActorMessage>,
}

impl WriteActor {
    /// create a new WriteActor
    fn new(
        size: u64,
        out_dir: &str,
        receiver: mpsc::Receiver<WriteActorMessage>,
    ) -> anyhow::Result<WriteActor> {
        let file =File::open(out_dir)?;
        file.set_len(size)?;

        let mmap = unsafe { Mmap::map(&file)? };
        Ok(WriteActor {
            receiver,
            size,
            mmap: mmap.make_mut()?,
        })
    }
    /// handle Write Request
    /// merges the chunk into the output file mmap
    fn handle_message(&mut self, msg: WriteActorMessage) {
        // merge the chunk into the appropriate position in the mmap
        let (start, end) = msg.message.indices;
        let chunk = msg.message.chunk;
        // copy the chunk into the mmap: can cause a overhead in performance?
        self.mmap[start as usize..end as usize].copy_from_slice(&chunk);
        let index = (end as u64).div_floor(self.size);
        msg.respond_to
            .send(index)
            .expect("Failed to send response");
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
    pub fn new(size: u64, out_dir: &str) -> anyhow::Result<Self> {
        // create a channel with a buffer of 100 messages (can be put in as argument)
        let (sender, receiver) = mpsc::channel(100);
        let actor = WriteActor::new(size, out_dir, receiver)?;
        tokio::task::spawn(run_actor(actor));
        Ok(Self { sender })
    }

    /// send a processed chunk to the write actor
    pub async fn send_chunk(&self, msg: ChunkMessage) -> u64 {
        let (send, recv) = oneshot::channel::<u64>();
        let msg = WriteActorMessage {
            message: msg,
            respond_to: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Failed to send message")
    }
}
