#![feature(int_roundings)]

#[cfg(test)]
mod test;
mod write_actor;

use crate::write_actor::{ChunkMessage, WriteActorHandle};
use clap::Parser;
use memmap2::Mmap;
use std::fs::OpenOptions;
use tokio::task::JoinSet;

//cmd arguments

#[derive(Debug, Parser)]
struct Args {
    #[arg(short, long, default_value = "data/unit_test1.json")]
    input: String,
    #[arg(short, long, default_value = "data/out_unit_test1.json")]
    output: String,
}
#[tokio::main]
async fn main() {
    let then = tokio::time::Instant::now();
    let args = Args::parse();
    env_logger::init();

    //read the file into memory-mapped file
    let mmap = read_file(&args.input);

    //create a write actor handle for the tasks
    let write_actor = WriteActorHandle::new(mmap.len() as u64, &args.output).unwrap();

    //divide mmap into equal chunks for length mmap.len() / max_cores
    let max_cores: usize = std::thread::available_parallelism().unwrap().into();
    let chunk_size = mmap.len() / max_cores;

    let mut start = 0;
    let mut tasks = JoinSet::new();
    //spawn tasks to process each chunk
    for _ in 0..max_cores {
        let end = (start + chunk_size).min(mmap.len());
        let chunk = ChunkMessage::from_mmap(&mmap, (start as u32, end as u32));
        let actor = write_actor.clone();
        tasks.spawn(tokio::spawn(async move {
            process_chunk(chunk, actor).await;
        }));
        start = end + 1;
    }

    //wait for all tasks to complete
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            log::error!("Error processing chunk: {:?}", e);
        }
    }
    log::info!("Time taken: {:?}", then.elapsed());
}

/// Process the chunk of the file, returns the index of the chunk processed
async fn process_chunk(mut data: ChunkMessage, handle: WriteActorHandle) -> u64 {
    log::info!("processing: {:?}", data.indices);
    // replace semicolons with colons in the chunk
    for item in data.chunk.iter_mut() {
        if *item == b';' {
            *item = b':';
        }
    }
    //send the chunk to the write actor to be written to the output file
    handle.send_chunk(data).await
}

pub fn read_file(directory: &str) -> Mmap {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory)
        .expect("File not found");
    unsafe { Mmap::map(&file).unwrap_or_else(|_| panic!("Error mapping file {}", directory))}
}
