use crate::write_actor::{ChunkMessage, WriteActorHandle};
use crate::{process_chunk, read_file};

#[tokio::test]
async fn test_replace() {
    let input = "data/unit_test1.json";
    let output = "data/out_unit_test1.json";
    let file = read_file(input);
    let actor = WriteActorHandle::new(file.len() as u64, output).unwrap();
    let chunk = ChunkMessage::from_mmap(&file, (0, file.len() as u32));
    process_chunk(chunk, actor.clone()).await;
    let out_file = read_file(output);
    // ensure no semicolons in the output file
    for item in out_file.iter() {
        assert_ne!(*item, b';');
    }
}
