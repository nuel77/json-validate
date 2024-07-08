use std::fs::{File};
use std::io::Read;
use crate::write_actor::{WriteActorHandle};
use crate::read_actor::ReadActor;

#[tokio::test]
async fn test_replace() {
    let input = String::from("data/unit_test1.json");
    let output = String::from("data/out_unit_test1.json");
    let mut reader = ReadActor::new(Some(&input));
    let actor = WriteActorHandle::new(Some(&output)).unwrap();
    reader.process(1024, actor).await;
    let mut out_file = read_file(output);
    // ensure no semicolons in the output file
    let mut out_str = String::new();
    out_file.read_to_string(&mut out_str).unwrap();
    assert!(!out_str.contains(";"));
}

fn read_file(directory: String) -> File {
    File::open(&directory).unwrap()
}
