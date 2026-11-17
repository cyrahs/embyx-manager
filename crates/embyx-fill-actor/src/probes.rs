//! API-coverage probe for the filesystem calls of the journaled moves
//! (docs/rewrite/API-COVERAGE.md, "rustix").
//!
//! `fill_actor/service.py` moves with `renameat2(RENAME_NOREPLACE)` through ctypes, falls
//! back to a no-follow hard link (`os.link(..., follow_symlinks=False)`) when the filesystem
//! refuses the flag (EINVAL, ENOSYS, EOPNOTSUPP: NFS), and compares inodes and link counts to
//! check what it moved. `locking.py` serializes runs with `flock(LOCK_EX | LOCK_NB)`. What
//! rustix and std give for each, on the local filesystem of the test machine. The NFS
//! fallback errnos cannot be produced here; the port maps them as listed in
//! `RENAME_NOREPLACE_UNSUPPORTED`. No application code is ported here.

use std::fs::{self, File};
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;

use rustix::fs::{AtFlags, CWD, FlockOperation, RenameFlags};
use rustix::io::Errno;

/// `service.py`: `_RENAME_NOREPLACE_UNSUPPORTED`.
const RENAME_NOREPLACE_UNSUPPORTED: [Errno; 3] = [Errno::INVAL, Errno::NOSYS, Errno::OPNOTSUPP];

fn rename_no_replace(source: &Path, destination: &Path) -> rustix::io::Result<()> {
    rustix::fs::renameat_with(CWD, source, CWD, destination, RenameFlags::NOREPLACE)
}

#[test]
fn rename_noreplace_moves_and_refuses_an_existing_destination() {
    let root = tempfile::tempdir().expect("tempdir");
    let source = root.path().join("ABC-123.mp4");
    let destination = root.path().join("ABC-123/ABC-123.mp4");
    fs::write(&source, b"video").expect("write");
    fs::create_dir(destination.parent().expect("parent")).expect("mkdir");
    let inode = fs::metadata(&source).expect("stat").ino();

    rename_no_replace(&source, &destination).expect("rename");
    assert!(!source.exists());
    assert_eq!(fs::metadata(&destination).expect("stat").ino(), inode);

    // An existing destination is EEXIST (Python raises FileExistsError), and both files stay.
    fs::write(&source, b"other").expect("write again");
    let error = rename_no_replace(&source, &destination).expect_err("destination exists");
    assert_eq!(error, Errno::EXIST);
    assert!(!RENAME_NOREPLACE_UNSUPPORTED.contains(&error));
    assert_eq!(fs::read(&destination).expect("read"), b"video");
    assert_eq!(fs::read(&source).expect("read"), b"other");

    // A missing source is ENOENT, which the service reports as `move_failed`.
    let error = rename_no_replace(&root.path().join("missing"), &root.path().join("x")).expect_err("missing");
    assert_eq!(error, Errno::NOENT);
}

#[test]
fn a_no_follow_hard_link_shares_the_inode_and_refuses_an_existing_name() {
    let root = tempfile::tempdir().expect("tempdir");
    let source = root.path().join("a.mp4");
    let destination = root.path().join("b.mp4");
    fs::write(&source, b"video").expect("write");

    // linkat without AT_SYMLINK_FOLLOW is os.link(..., follow_symlinks=False).
    rustix::fs::linkat(CWD, &source, CWD, &destination, AtFlags::empty()).expect("link");
    let (a, b) = (
        fs::metadata(&source).expect("stat a"),
        fs::metadata(&destination).expect("stat b"),
    );
    assert_eq!((a.dev(), a.ino()), (b.dev(), b.ino()));
    assert_eq!(a.nlink(), 2);
    let error = rustix::fs::linkat(CWD, &source, CWD, &destination, AtFlags::empty()).expect_err("exists");
    assert_eq!(error, Errno::EXIST);
    fs::remove_file(&source).expect("unlink");
    assert_eq!(fs::metadata(&destination).expect("stat").nlink(), 1);

    // Linking a symlink without following it links the symlink itself.
    let link = root.path().join("link.mp4");
    std::os::unix::fs::symlink(&destination, &link).expect("symlink");
    let linked = root.path().join("linked.mp4");
    rustix::fs::linkat(CWD, &link, CWD, &linked, AtFlags::empty()).expect("link symlink");
    assert!(fs::symlink_metadata(&linked).expect("lstat").file_type().is_symlink());

    // std's hard_link is the same call on Linux (no follow); either serves.
    let via_std = root.path().join("std.mp4");
    fs::hard_link(&link, &via_std).expect("std link");
    assert!(fs::symlink_metadata(&via_std).expect("lstat").file_type().is_symlink());
}

#[test]
fn flock_is_exclusive_between_open_file_descriptions() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("fill-actor.lock");
    let first = File::create(&path).expect("create");
    let second = File::open(&path).expect("open");

    rustix::fs::flock(&first, FlockOperation::NonBlockingLockExclusive).expect("lock");
    // Python sees BlockingIOError (EWOULDBLOCK) and sleeps before the next try.
    let error = rustix::fs::flock(&second, FlockOperation::NonBlockingLockExclusive).expect_err("held");
    assert_eq!(error, Errno::WOULDBLOCK);
    // std's File::try_lock is flock(LOCK_EX | LOCK_NB) on Linux and sees the same lock.
    assert!(matches!(second.try_lock(), Err(fs::TryLockError::WouldBlock)));

    rustix::fs::flock(&first, FlockOperation::Unlock).expect("unlock");
    second.try_lock().expect("free after unlock");
    second.unlock().expect("std unlock");
    // Closing the file releases the lock as well (Python closes the handle in `finally`).
    rustix::fs::flock(&first, FlockOperation::NonBlockingLockExclusive).expect("relock");
    drop(first);
    rustix::fs::flock(&second, FlockOperation::NonBlockingLockExclusive).expect("free after close");
}
