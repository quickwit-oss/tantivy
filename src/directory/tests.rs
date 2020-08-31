use super::*;
use futures::channel::oneshot;
use futures::executor::block_on;
use std::io::Write;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "mmap")]
mod mmap_directory_tests {
    use crate::directory::MmapDirectory;

    type DirectoryImpl = MmapDirectory;

    fn make_directory() -> DirectoryImpl {
        MmapDirectory::create_from_tempdir().unwrap()
    }

    #[test]
    fn test_simple() {
        let mut directory = make_directory();
        super::test_simple(&mut directory);
    }

    #[test]
    fn test_write_create_the_file() {
        let mut directory = make_directory();
        super::test_write_create_the_file(&mut directory);
    }

    #[test]
    fn test_rewrite_forbidden() {
        let mut directory = make_directory();
        super::test_rewrite_forbidden(&mut directory);
    }

    #[test]
    fn test_directory_delete() {
        let mut directory = make_directory();
        super::test_directory_delete(&mut directory);
    }

    #[test]
    fn test_lock_non_blocking() {
        let mut directory = make_directory();
        super::test_lock_non_blocking(&mut directory);
    }

    #[test]
    fn test_lock_blocking() {
        let mut directory = make_directory();
        super::test_lock_blocking(&mut directory);
    }

    #[test]
    fn test_watch() {
        let mut directory = make_directory();
        super::test_watch(&mut directory);
    }
}

mod ram_directory_tests {
    use crate::directory::RAMDirectory;

    type DirectoryImpl = RAMDirectory;

    fn make_directory() -> DirectoryImpl {
        RAMDirectory::default()
    }

    #[test]
    fn test_simple() {
        let mut directory = make_directory();
        super::test_simple(&mut directory);
    }

    #[test]
    fn test_write_create_the_file() {
        let mut directory = make_directory();
        super::test_write_create_the_file(&mut directory);
    }

    #[test]
    fn test_rewrite_forbidden() {
        let mut directory = make_directory();
        super::test_rewrite_forbidden(&mut directory);
    }

    #[test]
    fn test_directory_delete() {
        let mut directory = make_directory();
        super::test_directory_delete(&mut directory);
    }

    #[test]
    fn test_lock_non_blocking() {
        let mut directory = make_directory();
        super::test_lock_non_blocking(&mut directory);
    }

    #[test]
    fn test_lock_blocking() {
        let mut directory = make_directory();
        super::test_lock_blocking(&mut directory);
    }

    #[test]
    fn test_watch() {
        let mut directory = make_directory();
        super::test_watch(&mut directory);
    }
}

#[test]
#[should_panic]
fn ram_directory_panics_if_flush_forgotten() {
    let test_path: &'static Path = Path::new("some_path_for_test");
    let mut ram_directory = RAMDirectory::create();
    let mut write_file = ram_directory.open_write(test_path).unwrap();
    assert!(write_file.write_all(&[4]).is_ok());
}

fn test_simple(directory: &mut dyn Directory) {
    let test_path: &'static Path = Path::new("some_path_for_test");
    {
        let mut write_file = directory.open_write(test_path).unwrap();
        assert!(directory.exists(test_path));
        write_file.write_all(&[4]).unwrap();
        write_file.write_all(&[3]).unwrap();
        write_file.write_all(&[7, 3, 5]).unwrap();
        write_file.flush().unwrap();
    }
    {
        let read_file = directory.open_read(test_path).unwrap();
        let data: &[u8] = &*read_file;
        assert_eq!(data, &[4u8, 3u8, 7u8, 3u8, 5u8]);
    }
    assert!(directory.delete(test_path).is_ok());
    assert!(!directory.exists(test_path));
}

fn test_rewrite_forbidden(directory: &mut dyn Directory) {
    let test_path: &'static Path = Path::new("some_path_for_test");
    {
        directory.open_write(test_path).unwrap();
        assert!(directory.exists(test_path));
    }
    {
        assert!(directory.open_write(test_path).is_err());
    }
    assert!(directory.delete(test_path).is_ok());
}

fn test_write_create_the_file(directory: &mut dyn Directory) {
    let test_path: &'static Path = Path::new("some_path_for_test");
    {
        assert!(directory.open_read(test_path).is_err());
        let _w = directory.open_write(test_path).unwrap();
        assert!(directory.exists(test_path));
        assert!(directory.open_read(test_path).is_ok());
        assert!(directory.delete(test_path).is_ok());
    }
}

fn test_directory_delete(directory: &mut dyn Directory) {
    let test_path: &'static Path = Path::new("some_path_for_test");
    assert!(directory.open_read(test_path).is_err());
    let mut write_file = directory.open_write(&test_path).unwrap();
    write_file.write_all(&[1, 2, 3, 4]).unwrap();
    write_file.flush().unwrap();
    {
        let read_handle = directory.open_read(&test_path).unwrap();
        assert_eq!(&*read_handle, &[1u8, 2u8, 3u8, 4u8]);
        // Mapped files can't be deleted on Windows
        if !cfg!(windows) {
            assert!(directory.delete(&test_path).is_ok());
            assert_eq!(&*read_handle, &[1u8, 2u8, 3u8, 4u8]);
        }

        assert!(directory.delete(Path::new("SomeOtherPath")).is_err());
    }

    if cfg!(windows) {
        assert!(directory.delete(&test_path).is_ok());
    }

    assert!(directory.open_read(&test_path).is_err());
    assert!(directory.delete(&test_path).is_err());
}

fn test_watch(directory: &mut dyn Directory) {
    let num_progress: Arc<AtomicUsize> = Default::default();
    let counter: Arc<AtomicUsize> = Default::default();
    let counter_clone = counter.clone();
    let (sender, receiver) = crossbeam::channel::unbounded();
    let watch_callback = Box::new(move || {
        counter_clone.fetch_add(1, SeqCst);
    });
    // This callback is used to synchronize watching in our unit test.
    // We bind it to a variable because the callback is removed when that
    // handle is dropped.
    let watch_handle = directory.watch(watch_callback).unwrap();
    let _progress_listener = directory
        .watch(Box::new(move || {
            let val = num_progress.fetch_add(1, SeqCst);
            let _ = sender.send(val);
        }))
        .unwrap();

    for i in 0..10 {
        assert!(i <= counter.load(SeqCst));
        assert!(directory
            .atomic_write(Path::new("meta.json"), b"random_test_data_2")
            .is_ok());
        assert_eq!(receiver.recv_timeout(Duration::from_millis(500)), Ok(i));
        assert!(i + 1 <= counter.load(SeqCst)); // notify can trigger more than once.
    }
    mem::drop(watch_handle);
    assert!(directory
        .atomic_write(Path::new("meta.json"), b"random_test_data")
        .is_ok());
    assert!(receiver.recv_timeout(Duration::from_millis(500)).is_ok());
    assert!(10 <= counter.load(SeqCst));
}

fn test_lock_non_blocking(directory: &mut dyn Directory) {
    {
        let lock_a_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: false,
        });
        assert!(lock_a_res.is_ok());
        let lock_b_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("b.lock"),
            is_blocking: false,
        });
        assert!(lock_b_res.is_ok());
        let lock_a_res2 = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: false,
        });
        assert!(lock_a_res2.is_err());
    }
    let lock_a_res = directory.acquire_lock(&Lock {
        filepath: PathBuf::from("a.lock"),
        is_blocking: false,
    });
    assert!(lock_a_res.is_ok());
}

fn test_lock_blocking(directory: &mut dyn Directory) {
    let lock_a_res = directory.acquire_lock(&Lock {
        filepath: PathBuf::from("a.lock"),
        is_blocking: true,
    });
    assert!(lock_a_res.is_ok());
    let in_thread = Arc::new(AtomicBool::default());
    let in_thread_clone = in_thread.clone();
    let (sender, receiver) = oneshot::channel();
    std::thread::spawn(move || {
        //< lock_a_res is sent to the thread.
        in_thread_clone.store(true, SeqCst);
        let _just_sync = block_on(receiver);
        // explicitely droping lock_a_res. It would have been sufficient to just force it
        // to be part of the move, but the intent seems clearer that way.
        drop(lock_a_res);
    });
    {
        // A non-blocking call should fail, as the thread is running and holding the lock.
        let lock_a_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: false,
        });
        assert!(lock_a_res.is_err());
    }
    let directory_clone = directory.box_clone();
    let (sender2, receiver2) = oneshot::channel();
    let join_handle = std::thread::spawn(move || {
        assert!(sender2.send(()).is_ok());
        let lock_a_res = directory_clone.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: true,
        });
        assert!(in_thread.load(SeqCst));
        assert!(lock_a_res.is_ok());
    });
    assert!(block_on(receiver2).is_ok());
    assert!(sender.send(()).is_ok());
    assert!(join_handle.join().is_ok());
}
