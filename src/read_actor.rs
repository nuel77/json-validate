use crate::write_actor::{Message, WriteActorHandle};
use memmap2::{Mmap, MmapMut};
use std::fs::OpenOptions;
use std::io;
use std::io::{BufReader, Read};
use tokio::task::JoinSet;
use tokio_util::bytes::Buf;

enum DataInput {
    File(Mmap),
    Stdin(io::Stdin),
}

pub struct ReadActor {
    data: DataInput,
}
impl ReadActor {
    pub fn new(path: Option<&String>) -> Self {
        match path {
            Some(path) => {
                let file = OpenOptions::new().read(true).open(path).unwrap();
                let mmap = unsafe { Mmap::map(&file).unwrap() };
                Self {
                    data: DataInput::File(mmap),
                }
            }
            None => {
                let stdin = io::stdin();
                Self {
                    data: DataInput::Stdin(stdin),
                }
            }
        }
    }

    pub async fn process(&mut self, size: usize, handle: WriteActorHandle) {
        let mut buf = vec![0; size];
        let mut set = JoinSet::new();
        let mut index = 0;
        match &mut self.data {
            DataInput::File(mmap) => {
                let mut reader = mmap.reader();
                while let Ok(n) = reader.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    log::info!("sending index {:?}", index);
                    index+=1;
                    let handle = handle.clone();
                    set.spawn(tokio::task::spawn(process_chunk(buf.clone(), handle)));
                }
            }
            DataInput::Stdin(stdin) => {
                while let Ok(n) = stdin.read(&mut buf) {
                    println!("{:?}", n);
                    if n == 0 {
                        break;
                    }
                    let handle = handle.clone();
                    set.spawn(tokio::task::spawn(process_chunk(buf.clone(), handle)));
                }
            }
        }
        while let Some(res) = set.join_next().await {
            if let Err(e) = res {
                log::error!("Error processing chunk: {:?}", e);
            }
        }
    }
}

async fn process_chunk(mut data: Vec<u8>, handle: WriteActorHandle) -> u64 {
    // replace semicolons with colons in the chunk
    for item in data.iter_mut() {
        if *item == b';' {
            *item = b':';
        }
    }
    //send the chunk to the write actor to be written to the output file
    handle.send_chunk(data).await
}
