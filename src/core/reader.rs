use core::directory::Directory;

pub struct IndexReader {
    directory: Directory,
}

impl IndexReader {

    pub fn open(directory: &Directory) -> IndexReader {
		IndexReader {
			directory: (*directory).clone(),
        }
    }

}
