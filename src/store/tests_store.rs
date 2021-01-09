use std::path::Path;

use crate::HasLen;
use crate::directory::{Directory, ManagedDirectory, MmapDirectory, RAMDirectory};
use crate::fastfield::DeleteBitSet;

use super::{StoreReader, StoreWriter};

#[test]
fn test_toto2() -> crate::Result<()> {
    let directory = ManagedDirectory::wrap(MmapDirectory::open("src/store/broken_seg")?)?;
    let path = Path::new("b6029ade1b954ea1acad15b432eaacb9.store");
    assert!(directory.validate_checksum(path)?);
    let store_file = directory.open_read(path)?;
    let store = StoreReader::open(store_file)?;
    let documents = store.documents();
    // for doc in documents {
    //     println!("{:?}", doc);
    // }
    let doc= store.get(15_086)?;
    Ok(())
}

#[test]
fn test_toto() -> crate::Result<()> {
    let directory = ManagedDirectory::wrap(MmapDirectory::open("src/store/broken_seg")?)?;
    assert!(directory.validate_checksum(Path::new("e6ece22e5bca4e0dbe7ce3e4dcbd5bbf.store"))?);
    let store_file = directory.open_read(Path::new("e6ece22e5bca4e0dbe7ce3e4dcbd5bbf.store.patched"))?;
    let store = StoreReader::open(store_file)?;
    let doc= store.get(53)?;
    println!("{:?}", doc);
    // let documents = store.documents();
    // let ram_directory = RAMDirectory::create();
    // let path = Path::new("store");

    // let store_wrt = ram_directory.open_write(path)?;
    // let mut store_writer = StoreWriter::new(store_wrt);
    // for doc in &documents {
    //     store_writer.store(doc)?;
    // }
    // store_writer.close()?;
    // let store_data = ram_directory.open_read(path)?;
    // let new_store = StoreReader::open(store_data)?;
    // for doc in 0..59 {
    //     println!("{}", doc);
    //     let doc = new_store.get(doc)?;
    //     println!("{:?}", doc);
    // }
    Ok(())
}
