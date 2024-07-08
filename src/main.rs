#![feature(int_roundings)]

mod read_actor;
#[cfg(test)]
mod test;
mod write_actor;

use crate::read_actor::ReadActor;
use crate::write_actor::{Message, WriteActorHandle};
use clap::Parser;
use memmap2::{Mmap, MmapMut};
use std::fs::OpenOptions;
use std::io;
use std::io::{BufRead, BufReader, Read, Write};
use tokio::task::JoinSet;
use tokio_util::bytes::Buf;
//cmd arguments

#[derive(Debug, Parser)]
struct Args {
    //optional argument to specify the input file
    #[structopt(short, long)]
    input: Option<String>,
    #[structopt(short, long)]
    output: Option<String>,
}
#[tokio::main]
async fn main() {
    let then = tokio::time::Instant::now();
    let args = Args::parse();
    env_logger::init();
    let mut input = ReadActor::new(args.input.as_ref());
    // //create a write actor handle for the tasks
    let write_actor = WriteActorHandle::new(args.output.as_ref()).unwrap();
    // process in chunks for 100MBs
    let one_mb = 1024 * 1024;
    input.process(one_mb, write_actor).await;
    log::info!("Time taken: {:?}", then.elapsed());
}
