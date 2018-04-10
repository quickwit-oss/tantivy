use std::collections::HashMap;
use Directory;
use std::path::PathBuf;
use directory::ReadOnlySource;
use std::io::BufWriter;
use directory::error::{DeleteError, OpenReadError, OpenWriteError};
use std::path::Path;
use std::fmt::{Formatter, Debug, self};
use Result as TantivyResult;
use directory::SeekableWrite;
use std::io;
use std::fs;
use common::Endianness;
use common::BinarySerializable;
use common::VInt;
use byteorder::ByteOrder;
use std::str;
use std::fs::File;
use std::io::{Read, Write};
use std::ffi::OsString;

#[derive(Clone)]
pub struct StaticDirectory {
    files: HashMap<PathBuf, &'static [u8]>,
}

impl Debug for StaticDirectory {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "StaticDirectory[{} files]", self.files.len())?;
        Ok(())
    }
}

impl StaticDirectory {
    pub fn open(mut data: &'static [u8]) -> TantivyResult<StaticDirectory> {
        assert!(data.len() > 8);
        let footer_len_offset = data.len() - 8;
        let body_len = Endianness::read_u64(&data[footer_len_offset..]) as usize;
        let mut body = &data[..body_len];
        let mut footer = &data[body_len..footer_len_offset];
        let num_files = VInt::deserialize(&mut footer)?.0 as usize;
        let mut files = HashMap::new();
        for _ in 0..num_files {
            let filename_len = VInt::deserialize(&mut footer)?.0 as usize;
            let filename = &footer[..filename_len];
            footer = &footer[filename_len..];
            let data_len = VInt::deserialize(&mut footer)?.0 as usize;
            let file_data = &body[..data_len];
            body = &body[data_len..];
            let filename_str = str::from_utf8(filename).expect("Invalid UTF8");
            let filename = PathBuf::from(filename_str);
            println!("{:?} {:?}", filename, data_len);
            files.insert(filename, file_data);
        }
        Ok(StaticDirectory {
            files
        })
    }
}

impl Directory for StaticDirectory {
    fn open_read(&self, path: &Path) -> Result<ReadOnlySource, OpenReadError> {
        if let Some(static_data) = self.files.get(path) {
            Ok(ReadOnlySource::from(*static_data))
        } else {
            Err(OpenReadError::FileDoesNotExist(path.to_owned()))
        }
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        unimplemented!("Static directory is read-only !")
    }

    fn exists(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }

    fn open_write(&mut self, path: &Path) -> Result<BufWriter<Box<SeekableWrite>>, OpenWriteError> {
        unimplemented!("Static directory is read-only !")
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if let Some(static_data) = self.files.get(path) {
            Ok(static_data.to_vec())
        } else {
            Err(OpenReadError::FileDoesNotExist(path.to_owned()))
        }
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> io::Result<()> {
        unimplemented!("Static directory is read-only !")
    }

    fn box_clone(&self) -> Box<Directory> {
        box self.clone()
    }
}

pub fn write_static_from_directory(directory_path: &Path) -> TantivyResult<Vec<u8>> {
    assert!(directory_path.is_dir());
    let mut file_data: Vec<(OsString, usize)> = Vec::new();
    let mut write: Vec<u8> = Vec::new();
    for entry in fs::read_dir(directory_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            info!("Appending {}", path.to_string_lossy());
            let mut open_file = File::open(&path)?;
            let file_len = open_file.read_to_end(&mut write)?;
            file_data.push((entry.file_name(), file_len));
        }
    }
    // write footer
    let body_len = write.len();
    VInt(file_data.len() as u64).serialize(&mut write)?;
    for (filename, filelen) in file_data {
        VInt(filename.len() as u64).serialize(&mut write)?;
        write.write_all(filename.to_string_lossy().as_bytes())?;
        VInt(filelen as u64).serialize(&mut write)?;
    }
    (body_len as u64).serialize(&mut write)?;
    Ok(write)
}