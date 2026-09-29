# Changelog

All notable changes to ogt are documented here.

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Add GitHub Actions workflows for release validation, preparation, and publication.
- Add version, provenance, archive, and checksum release helpers.

### Fixed

- Normalize a PATH shim's `OGT_SHIM_DIR` to the platform's native path form. The recursion
  guard in `cli::path_shim` compares `OGT_SHIM_DIR` against `PATH` entries by exact string,
  and a native Windows `ogt.exe` sees Windows entries (`C:\Users\...`), while the shim's
  `cd && pwd` yields a POSIX path (`/c/Users/...`). Under MSYS/Git Bash the two could never
  match, so the shim directory was never stripped, ogt resolved the target program back to
  its own shim, and every PATH shim failed with
  `%1 is not a valid Win32 application. (os error 193)`. The shim now rewrites
  `OGT_SHIM_DIR` through `cygpath -w` when `cygpath` is available, which is a no-op on Unix.
- Never fold a stream that is not text. A stream containing a NUL byte or invalid UTF-8 was
  decoded as UTF-8, so invalid bytes reached the caller as U+FFFD and any pipeline consuming
  the output broke silently. Such a stream now passes through unchanged.

## [0.1.0] - 2026-08-31

### Added

- Fold large output from `grep`, `cat`, `find`, `rg`, `cargo`, `git`, `ls`, and `diff`.
- Pass output through byte-for-byte below the estimated token threshold.
- Save complete folded output with an exact recovery command.
- Report full output, returned output, and estimated saved tokens for handled calls.
- Scope folding to project roots with `OGT_ROOTS` or a roots file.
- Install the built-in Claude Code adapter with `ogt init`.
- Use direct command prefixes with any shell or harness that supports them.
- Create Unix PATH shims with `ogt init --shims <dir>` for harnesses that start programs by name.
- Add interactive initialization for terminal input.

### Known limitations

- The folding threshold is an estimate.
- ogt buffers selected output until the child exits.
- Pipes can change terminal detection because ogt wraps child streams.
- ogt stores recovery files on disk and rotation removes older files.
- Commands outside the target list or configured roots pass through without folding.
- Re-read counts detect only reads that also go through ogt, so they are a lower bound.

[Unreleased]: https://github.com/farhan-syah/ogt/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/farhan-syah/ogt/releases/tag/v0.1.0
