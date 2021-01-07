use std::path::Path;

use crate::HasLen;
use crate::directory::{Directory, ManagedDirectory, MmapDirectory, RAMDirectory};
use crate::fastfield::DeleteBitSet;

use super::{StoreReader, StoreWriter};


#[test]
fn test_toto() -> crate::Result<()> {
    let directory = ManagedDirectory::wrap(MmapDirectory::open("src/store/broken_seg")?)?;
    assert!(directory.validate_checksum(Path::new("e6ece22e5bca4e0dbe7ce3e4dcbd5bbf.store"))?);
    let store_file = directory.open_read(Path::new("e6ece22e5bca4e0dbe7ce3e4dcbd5bbf.store"))?;
    let store = StoreReader::open(store_file)?;
    let documents = store.documents();
    let ram_directory = RAMDirectory::create();
    let path = Path::new("store");
    let store_wrt = ram_directory.open_write(path)?;
    let mut store_writer = StoreWriter::new(store_wrt);
    for doc in &documents {
        store_writer.store(doc)?;
    }
    store_writer.close()?;
    let store_data = ram_directory.open_read(path)?;
    let new_store = StoreReader::open(store_data)?;
    for doc in 0.. {
        println!("{}", doc);
        let doc = store.get(53)?;
        println!("{:?}", doc);
    }
    Ok(())
}
