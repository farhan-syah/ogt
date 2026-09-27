//! Adapted from rtk's `core::tee::cleanup_old_files`.
//! Source: reference/rtk/src/core/tee.rs
//!
//! Deviation from rtk: `max_files` is floored to 1 before computing how
//! many files to remove, so a caller passing `max_files: 0` can never
//! delete the fold file that was just written — rotation must never
//! destroy the newest file.

use std::path::Path;

/// Rotate old fold files: keep every file named in `current`, plus only the
/// newest `max_files` of the rest, and delete what is left over. `current`
/// counts toward `max_files`.
///
/// `current` names the files the calling invocation just wrote. They are never
/// deleted, because the filename is not a reliable creation order: a name freed
/// by an earlier rotation is reused, so the file a run just wrote can sort among
/// the oldest. A non-text stream is read back from its fold file after rotation
/// runs, so deleting it would lose the output the caller is owed.
pub(crate) fn cleanup_old_files(dir: &Path, max_files: usize, current: &[&Path]) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "log"))
        .filter(|e| !current.iter().any(|c| e.path().as_path() == *c))
        .collect();

    // The caller's own files occupy part of the budget. At least one file always
    // survives, so a `max_files` of 0 cannot empty the directory.
    let budget = max_files.max(1).saturating_sub(current.len());
    if entries.len() <= budget {
        return;
    }

    // Sort by filename, which starts with the epoch timestamp. This is only a
    // coarse order — a reused name sorts by its old position — which is why the
    // caller's own files are excluded above rather than trusted to sort last.
    entries.sort_by_key(|e| e.file_name());

    let to_remove = entries.len() - budget;
    for entry in entries.iter().take(to_remove) {
        let _ = std::fs::remove_file(entry.path());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn keeps_newest_max_files() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = tmpdir.path();

        for i in 0..25 {
            let filename = format!("{:010}_test.log", 1_000_000 + i);
            fs::write(dir.join(&filename), "content").unwrap();
        }

        cleanup_old_files(dir, 20, &[]);

        let remaining: Vec<_> = fs::read_dir(dir).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(remaining.len(), 20);

        for i in 0..5 {
            let filename = format!("{:010}_test.log", 1_000_000 + i);
            assert!(!dir.join(&filename).exists());
        }
        for i in 5..25 {
            let filename = format!("{:010}_test.log", 1_000_000 + i);
            assert!(dir.join(&filename).exists());
        }
    }

    #[test]
    fn ignores_non_log_files() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = tmpdir.path();
        fs::write(dir.join("notes.txt"), "keep me").unwrap();
        for i in 0..5 {
            fs::write(dir.join(format!("{:010}_x.log", i)), "c").unwrap();
        }

        cleanup_old_files(dir, 1, &[]);

        assert!(dir.join("notes.txt").exists());
    }

    #[test]
    fn max_files_zero_never_deletes_the_newest_file() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = tmpdir.path();
        for i in 0..5 {
            fs::write(dir.join(format!("{:010}_x.log", i)), "c").unwrap();
        }

        cleanup_old_files(dir, 0, &[]);

        // Floored to keep(1): the newest file (highest epoch prefix) survives.
        assert!(dir.join("0000000004_x.log").exists());
        let remaining: Vec<_> = fs::read_dir(dir).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(remaining.len(), 1);
    }

    /// A name freed by an earlier rotation is reused, so a file written this
    /// moment can carry a name that sorts among the oldest. Rotation must keep
    /// it anyway: a non-text stream is read back out of its file afterwards, and
    /// deleting it there loses the caller's output.
    #[test]
    fn never_deletes_a_file_the_caller_still_holds() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = tmpdir.path();
        // `-1` sorts BELOW the unsuffixed name, so a name-only rule would delete
        // it first even though it is the file the caller just wrote.
        let current = dir.join("0000000000_sh.out-1.log");
        fs::write(&current, "the caller's own output").unwrap();
        fs::write(dir.join("0000000001_sh.out.log"), "an older file").unwrap();

        cleanup_old_files(dir, 1, &[current.as_path()]);

        assert!(current.exists(), "the caller's own file must survive");
        assert!(
            !dir.join("0000000001_sh.out.log").exists(),
            "the budget is spent on the caller's file, so the other is the one removed"
        );
        let remaining: Vec<_> = fs::read_dir(dir).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(remaining.len(), 1);
    }
}
