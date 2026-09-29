//! Per-stream spill-to-disk state machine. New code — rtk read child output
//! line by line into an unbounded `String` behind a 10 MiB truncate-with-
//! warning cap (`RAW_CAP`), which is exactly the loss this design removes.
//!
//! Two states, one per stream:
//!
//! * **Buffering** — raw bytes accumulate in memory while the running total
//!   stays under the token gate. If the child ends here, those bytes are the
//!   passthrough output, byte for byte.
//! * **Spilling** — on crossing the gate the fold file is opened once, the
//!   buffered prefix is written, and every later chunk goes straight to disk.
//!   Memory stops growing: from then on only a byte count, a newline count
//!   and a bounded tail ring are kept.
//!
//! The head needs no tracking. `SLICE_MAX_BYTES` (8 KB) is far below the
//! default gate (~100 KB), so the head is still sitting in the pre-spill
//! buffer at the moment of crossing — it is sliced there, once.

use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::PathBuf;

use crate::fold::summary::{Fold, SLICE_MAX_BYTES, TAIL_LINES, total_lines_from};
use crate::fold::tokens::estimate_tokens_len;
use crate::fold::write::open_fold_file;

/// Read size per `read` call on a child pipe.
const READ_CHUNK: usize = 64 * 1024;

/// Upper bound on the tail ring before it is trimmed back. Trimming at twice
/// the slice cap keeps the work amortized O(1) per byte: each byte is copied
/// at most once more, no matter how long the stream runs.
const TAIL_RING_MAX: usize = 2 * SLICE_MAX_BYTES;

/// What one stream produced.
pub(crate) enum StreamCapture {
    /// Stayed under the gate: these are the child's bytes, to be written to
    /// the real fd untouched.
    Passthrough(Vec<u8>),
    /// Crossed the gate: full output is on disk, this is the summary of it.
    Folded(Fold),
    /// Crossed the gate but is NOT text, so no summary was made. Every byte is
    /// on disk at `Fold::path` and is written to the real fd unchanged.
    ///
    /// The gate is a SIZE gate, and a fold renders through a `String`, which
    /// decodes as UTF-8 and substitutes U+FFFD for each invalid byte. That is
    /// correct for text and destructive for anything else, so a non-text stream
    /// must never take the `Folded` path.
    BinaryPassthrough(Fold),
}

impl StreamCapture {
    /// Raw byte count the command actually produced on this stream. For a
    /// passthrough this is `kept_bytes` too — the tracking row's mandatory
    /// evidence that "no fold happened" is not the same as "zero bytes".
    pub(crate) fn raw_bytes(&self) -> usize {
        match self {
            StreamCapture::Passthrough(bytes) => bytes.len(),
            StreamCapture::Folded(fold) => fold.raw_bytes,
            StreamCapture::BinaryPassthrough(fold) => fold.raw_bytes,
        }
    }

    /// Bytes that actually reached the caller: all of them for a
    /// passthrough, only the compact summary for a fold.
    pub(crate) fn kept_bytes(&self) -> usize {
        match self {
            StreamCapture::Passthrough(bytes) => bytes.len(),
            StreamCapture::Folded(fold) => fold.kept_bytes,
            // Every byte reaches the caller; nothing was summarised.
            StreamCapture::BinaryPassthrough(fold) => fold.raw_bytes,
        }
    }

    pub(crate) fn is_folded(&self) -> bool {
        matches!(self, StreamCapture::Folded(_))
    }

    /// Path to the full output on disk, if this stream crossed the gate.
    pub(crate) fn fold_path(&self) -> Option<&std::path::Path> {
        match self {
            StreamCapture::Passthrough(_) => None,
            StreamCapture::Folded(fold) => Some(&fold.path),
            StreamCapture::BinaryPassthrough(fold) => Some(&fold.path),
        }
    }
}

/// Incremental check for a stream that cannot be rendered as text.
///
/// A fold's summary goes through `Fold::render`, which returns a `String`, so
/// the fold path lossily decodes its input: every invalid byte becomes U+FFFD.
/// A stream is safe to fold only while every byte of it is valid UTF-8.
///
/// The check is incremental because a code point can be split across two reads.
/// Only an incomplete sequence at the very end of the bytes seen so far is
/// deferred; an invalid sequence inside them is final, and once one is found the
/// stream is non-text for the rest of its life.
///
/// A NUL byte marks the stream non-text on its own. NUL is legal UTF-8, so this
/// is this tool's binary heuristic rather than a decoding rule: text output does
/// not carry one, and a leading NUL is the conventional signature of a binary
/// file.
#[derive(Default)]
struct BinaryScan {
    /// Trailing bytes of a code point the next read may complete. At most three.
    pending: Vec<u8>,
}

impl BinaryScan {
    /// Feed one chunk. Returns `true` once the stream is known not to be text.
    fn update(&mut self, chunk: &[u8]) -> bool {
        if chunk.contains(&0) {
            return true;
        }

        // Judge the carried bytes together with this chunk: the first bytes here
        // may complete a code point the previous read cut in half, and
        // validating them alone would call valid text invalid.
        let carried = !self.pending.is_empty();
        if carried {
            self.pending.extend_from_slice(chunk);
        }
        let subject: &[u8] = if carried { &self.pending } else { chunk };

        let decided = match std::str::from_utf8(subject) {
            Ok(_) => subject.len(),
            // An invalid sequence inside the bytes already seen is final.
            Err(err) if err.error_len().is_some() => return true,
            // Incomplete at the end: hold it back for the next read.
            Err(err) => err.valid_up_to(),
        };

        let unfinished = subject[decided..].to_vec();
        self.pending = unfinished;
        false
    }
}

enum State {
    Buffering(Vec<u8>),
    Spilling(BufWriter<File>),
}

/// Size gate and destination for one stream. stdout and stderr each own one:
/// separate threshold evaluation, separate fold file, separate destination
/// fd. They are never merged or interleaved.
pub(crate) struct StreamSink {
    /// Slug for the fold filename, already suffixed per stream by the caller
    /// so both streams crossing in the same second cannot collide.
    slug: String,
    /// `None` when no fold directory could be resolved. Only an error if the
    /// stream actually crosses the gate — a small run must still work on a
    /// platform with no data directory.
    dir: Option<PathBuf>,
    threshold_tokens: usize,
    enabled: bool,
    state: State,
    path: Option<PathBuf>,
    raw_bytes: usize,
    newlines: usize,
    last_byte: Option<u8>,
    /// First `SLICE_MAX_BYTES` bytes, captured at the moment of spilling.
    head: Vec<u8>,
    /// Trailing window, bounded by `TAIL_RING_MAX`.
    tail: Vec<u8>,
    /// Set once any byte seen is not text. The stream still spills, so memory
    /// stays bounded, but it is reported as `BinaryPassthrough` and never
    /// rendered as a summary.
    binary: bool,
    /// Text check carried across reads, so a code point split between two of
    /// them is not mistaken for invalid UTF-8.
    scan: BinaryScan,
}

impl StreamSink {
    pub(crate) fn new(
        slug: String,
        dir: Option<PathBuf>,
        threshold_tokens: usize,
        enabled: bool,
    ) -> Self {
        Self {
            slug,
            dir,
            threshold_tokens,
            enabled,
            state: State::Buffering(Vec::new()),
            path: None,
            raw_bytes: 0,
            newlines: 0,
            last_byte: None,
            head: Vec::new(),
            tail: Vec::new(),
            binary: false,
            scan: BinaryScan::default(),
        }
    }

    /// Feed one chunk of raw child bytes.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> io::Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        self.raw_bytes += chunk.len();
        // Count newlines instead of materializing lines: the line total must
        // stay exact for output that never fits in memory.
        self.newlines += chunk.iter().filter(|b| **b == b'\n').count();
        self.last_byte = chunk.last().copied();

        let crossed = match &mut self.state {
            State::Buffering(buf) => {
                buf.extend_from_slice(chunk);
                if self.enabled
                    && !self.binary
                    && estimate_tokens_len(self.raw_bytes) >= self.threshold_tokens
                {
                    // Crossing commits the stream to a fold, and a fold renders
                    // through a `String`. Inspect the buffered prefix HERE, the
                    // one moment it is still in memory: after spilling, only a
                    // byte count and a bounded tail ring survive.
                    self.binary = self.scan.update(buf);
                    true
                } else {
                    false
                }
            }
            State::Spilling(writer) => {
                writer.write_all(chunk)?;
                // Keep checking after the gate: a stream can start as text and
                // turn non-text later, and the decision holds only until the
                // last read is in.
                if !self.binary {
                    self.binary = self.scan.update(chunk);
                }
                self.tail.extend_from_slice(chunk);
                trim_tail(&mut self.tail);
                false
            }
        };
        if crossed {
            self.spill()?;
        }
        Ok(())
    }

    /// Cross into the spilling state: open the fold file, snapshot head and
    /// tail out of the buffer, flush the buffered prefix to disk, drop it.
    fn spill(&mut self) -> io::Result<()> {
        let State::Buffering(buf) = &self.state else {
            return Ok(());
        };
        let dir = self.dir.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "no fold directory available (no data_local_dir on this platform)",
            )
        })?;

        let (file, path) = open_fold_file(&self.slug, dir)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(buf)?;

        // Head and tail come out of the buffer here, the one moment both are
        // still in memory. After this the buffer is dropped and nothing
        // re-reads the file.
        self.head
            .extend_from_slice(&buf[..SLICE_MAX_BYTES.min(buf.len())]);
        self.tail
            .extend_from_slice(&buf[buf.len().saturating_sub(TAIL_RING_MAX)..]);
        trim_tail(&mut self.tail);

        self.path = Some(path);
        self.state = State::Spilling(writer);
        Ok(())
    }

    /// Drain `reader` to EOF into this sink, then finish.
    pub(crate) fn pump(mut self, mut reader: impl Read) -> io::Result<StreamCapture> {
        let mut chunk = vec![0u8; READ_CHUNK];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.feed(&chunk[..n])?,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        self.finish()
    }

    fn finish(self) -> io::Result<StreamCapture> {
        match self.state {
            State::Buffering(buf) => Ok(StreamCapture::Passthrough(buf)),
            State::Spilling(mut writer) => {
                writer.flush()?;
                let path = self
                    .path
                    .ok_or_else(|| io::Error::other("spilled stream has no fold file path"))?;
                let fold = Fold::from_regions(
                    &self.head,
                    &self.tail,
                    total_lines_from(self.newlines, self.last_byte),
                    self.raw_bytes,
                    path,
                );
                // A non-text stream is handed back whole rather than summarised.
                // The `Fold` still carries the path, so the bytes remain on disk
                // and `emit` can stream them to the real fd.
                if self.binary {
                    Ok(StreamCapture::BinaryPassthrough(fold))
                } else {
                    Ok(StreamCapture::Folded(fold))
                }
            }
        }
    }
}

/// Bound the tail ring: past `TAIL_RING_MAX` bytes, keep the last
/// `SLICE_MAX_BYTES` and then only the last `TAIL_LINES` lines of those.
/// Never drops anything the fold could display — the summary caps the tail
/// at the same byte and line limits.
///
/// Computes the single cut point up front and drains once — draining the
/// byte cap and then the line cap separately would memmove the retained
/// bytes twice per trim instead of once.
fn trim_tail(tail: &mut Vec<u8>) {
    if tail.len() <= TAIL_RING_MAX {
        return;
    }
    let byte_cut = tail.len() - SLICE_MAX_BYTES;

    // Keep one newline more than TAIL_LINES so the boundary line stays whole.
    let mut seen = 0;
    let mut cut = byte_cut;
    for i in (byte_cut..tail.len()).rev() {
        if tail[i] == b'\n' {
            seen += 1;
            if seen > TAIL_LINES {
                cut = i + 1;
                break;
            }
        }
    }
    tail.drain(..cut);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink(dir: &std::path::Path, threshold: usize) -> StreamSink {
        StreamSink::new("test".to_string(), Some(dir.to_path_buf()), threshold, true)
    }

    #[test]
    fn stays_buffered_below_the_gate() {
        let tmp = tempfile::tempdir().unwrap();
        let raw = b"small output\n";
        let capture = sink(tmp.path(), 1_000).pump(&raw[..]).unwrap();
        match capture {
            StreamCapture::Passthrough(bytes) => assert_eq!(bytes, raw),
            StreamCapture::Folded(_) => panic!("must not fold below the gate"),
            StreamCapture::BinaryPassthrough(_) => {
                panic!("must not cross the gate below it, text or not")
            }
        }
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
    }

    #[test]
    fn disabled_never_spills_however_large() {
        let tmp = tempfile::tempdir().unwrap();
        let raw = vec![b'x'; 200_000];
        let s = StreamSink::new("test".into(), Some(tmp.path().into()), 10, false);
        let capture = s.pump(&raw[..]).unwrap();
        assert!(matches!(capture, StreamCapture::Passthrough(_)));
    }

    #[test]
    fn spilled_file_is_byte_identical_to_the_input() {
        let tmp = tempfile::tempdir().unwrap();
        let raw: Vec<u8> = (0..40_000)
            .flat_map(|i| format!("line {i}\n").into_bytes())
            .collect();
        let capture = sink(tmp.path(), 100).pump(&raw[..]).unwrap();
        let StreamCapture::Folded(fold) = capture else {
            panic!("expected a fold");
        };
        assert_eq!(std::fs::read(&fold.path).unwrap(), raw);
        assert_eq!(fold.raw_bytes, raw.len());
        assert_eq!(fold.total_lines, 40_000);
    }

    #[test]
    fn head_and_tail_survive_a_stream_far_larger_than_memory_window() {
        let tmp = tempfile::tempdir().unwrap();
        let raw: Vec<u8> = (0..40_000)
            .flat_map(|i| format!("line {i}\n").into_bytes())
            .collect();
        let capture = sink(tmp.path(), 100).pump(&raw[..]).unwrap();
        let StreamCapture::Folded(fold) = capture else {
            panic!("expected a fold");
        };
        assert!(fold.head.starts_with("line 0\nline 1\n"));
        assert_eq!(fold.head.lines().count(), 50);
        assert!(fold.tail.ends_with("line 39999"));
        assert_eq!(fold.tail.lines().count(), 50);
    }

    #[test]
    fn folding_matches_the_in_memory_path_byte_for_byte() {
        // The single-constructor guarantee, end to end: streaming a buffer
        // through the sink must produce the same head/tail/counts as folding
        // the same buffer in memory.
        let tmp = tempfile::tempdir().unwrap();
        let raw: String = (0..5_000).map(|i| format!("line {i}\n")).collect();
        let capture = sink(tmp.path(), 100).pump(raw.as_bytes()).unwrap();
        let StreamCapture::Folded(streamed) = capture else {
            panic!("expected a fold");
        };

        let cfg = crate::fold::FoldConfig {
            threshold_tokens: 100,
            directory: Some(tmp.path().to_path_buf()),
            ..crate::fold::FoldConfig::default()
        };
        let crate::fold::FoldOutcome::Folded(in_memory) =
            crate::fold::fold_output(&raw, "test", &cfg).unwrap()
        else {
            panic!("expected a fold");
        };

        assert_eq!(streamed.head, in_memory.head);
        assert_eq!(streamed.tail, in_memory.tail);
        assert_eq!(streamed.total_lines, in_memory.total_lines);
        assert_eq!(streamed.raw_bytes, in_memory.raw_bytes);
        assert_eq!(streamed.kept_bytes, in_memory.kept_bytes);
    }

    #[test]
    fn single_huge_line_keeps_head_only() {
        let tmp = tempfile::tempdir().unwrap();
        let raw = vec![b'x'; 200_000];
        let capture = sink(tmp.path(), 100).pump(&raw[..]).unwrap();
        let StreamCapture::Folded(fold) = capture else {
            panic!("expected a fold");
        };
        assert_eq!(fold.total_lines, 1);
        assert!(fold.tail.is_empty(), "one line leaves nothing for the tail");
        assert!(fold.head.len() <= SLICE_MAX_BYTES);
    }

    #[test]
    fn invalid_utf8_is_not_folded_at_all() {
        let tmp = tempfile::tempdir().unwrap();
        let mut raw = Vec::new();
        for i in 0..20_000 {
            raw.extend_from_slice(&[0xff, 0xfe, b'a' + (i % 26) as u8, b'\n']);
        }
        let capture = sink(tmp.path(), 100).pump(&raw[..]).unwrap();

        // A non-text stream must not take the fold path: `render` returns a
        // `String`, so it would decode as UTF-8 and turn every 0xff/0xfe here
        // into U+FFFD. The bytes on disk were always exact, which is why only
        // the printed stream was ever wrong.
        assert!(
            matches!(capture, StreamCapture::BinaryPassthrough(_)),
            "invalid UTF-8 above the gate must not take the fold path"
        );
        assert!(
            !capture.is_folded(),
            "a binary stream must not be reported as folded"
        );
        assert_eq!(
            capture.kept_bytes(),
            capture.raw_bytes(),
            "every byte must reach the caller, so kept equals raw"
        );

        let StreamCapture::BinaryPassthrough(fold) = capture else {
            unreachable!("just asserted");
        };
        assert_eq!(
            std::fs::read(&fold.path).unwrap(),
            raw,
            "persisted bytes must be exactly what the stream carried"
        );
        assert!(fold.head.len() <= SLICE_MAX_BYTES);
        assert!(fold.tail.len() <= SLICE_MAX_BYTES);
    }

    /// A code point split between two reads must not be mistaken for invalid
    /// UTF-8. `é` is `0xC3 0xA9`; when the gate opens with only its lead byte
    /// received, `from_utf8` reports an incomplete sequence, and the whole text
    /// stream would be misclassified as non-text.
    #[test]
    fn a_code_point_split_at_the_gate_stays_text() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = sink(tmp.path(), 100);

        // 400 ASCII bytes put the running total exactly at the gate (100 tokens
        // = 400 bytes), so the crossing read ends on the split lead byte.
        let mut first = vec![b'a'; 400];
        first.push(0xC3);
        s.feed(&first).unwrap();
        assert!(matches!(s.state, State::Spilling(_)), "must have spilled");

        // The continuation byte arrives in the next read and completes the code
        // point; everything after it is plain ASCII.
        let mut second = vec![0xA9];
        second.extend(std::iter::repeat_n(b'b', 40_000));
        s.feed(&second).unwrap();

        let capture = s.finish().unwrap();
        assert!(
            matches!(capture, StreamCapture::Folded(_)),
            "a valid text stream must fold even when a code point is split across reads"
        );
    }

    /// The same split, once the stream is already spilling: the reads after the
    /// gate need the same carried-byte handling as the crossing read.
    #[test]
    fn a_code_point_split_while_spilling_stays_text() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = sink(tmp.path(), 100);
        s.feed(&vec![b'a'; 40_000]).unwrap();

        s.feed(&[0xC3]).unwrap();
        let mut rest = vec![0xA9];
        rest.extend(std::iter::repeat_n(b'b', 40_000));
        s.feed(&rest).unwrap();

        let capture = s.finish().unwrap();
        assert!(
            matches!(capture, StreamCapture::Folded(_)),
            "a split code point after the gate must not disable folding either"
        );
    }

    /// The gate decides on a prefix, but a stream can turn non-text later. The
    /// decision must be revisited on every read, or a text-then-binary stream is
    /// summarised through `render` and its tail is destroyed.
    #[test]
    fn binary_after_the_gate_is_still_caught() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = sink(tmp.path(), 100);

        // Text first: the gate opens and the stream commits to a fold.
        let text = vec![b'a'; 40_000];
        s.feed(&text).unwrap();
        assert!(!s.binary, "text alone must not mark the stream non-text");

        // Then bytes that are not text at all.
        s.feed(&[0xff, 0x00]).unwrap();

        let capture = s.finish().unwrap();
        assert!(
            matches!(capture, StreamCapture::BinaryPassthrough(_)),
            "a stream that turns non-text after the gate must not be rendered"
        );
        assert_eq!(
            capture.kept_bytes(),
            capture.raw_bytes(),
            "every byte must reach the caller, so kept equals raw"
        );

        let StreamCapture::BinaryPassthrough(fold) = capture else {
            unreachable!("just asserted");
        };
        let mut expected = text;
        expected.extend_from_slice(&[0xff, 0x00]);
        assert_eq!(
            std::fs::read(&fold.path).unwrap(),
            expected,
            "the spill file must hold the whole stream, text and trailing bytes alike"
        );
    }

    #[test]
    fn tail_ring_stays_bounded_across_many_chunks() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = sink(tmp.path(), 10);
        for i in 0..2_000 {
            s.feed(format!("chunk {i} padding padding padding\n").as_bytes())
                .unwrap();
        }
        assert!(
            s.tail.len() <= TAIL_RING_MAX,
            "tail ring grew to {}",
            s.tail.len()
        );
        assert!(matches!(s.state, State::Spilling(_)), "must have spilled");
    }

    #[test]
    fn accessors_report_passthrough_bytes_before_emit_consumes_the_capture() {
        let tmp = tempfile::tempdir().unwrap();
        let raw = b"small output\n";
        let capture = sink(tmp.path(), 1_000).pump(&raw[..]).unwrap();
        assert!(!capture.is_folded());
        assert_eq!(capture.raw_bytes(), raw.len());
        assert_eq!(capture.kept_bytes(), raw.len());
        assert!(capture.fold_path().is_none());
    }

    #[test]
    fn accessors_report_folded_raw_and_kept_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let raw: Vec<u8> = (0..40_000)
            .flat_map(|i| format!("line {i}\n").into_bytes())
            .collect();
        let capture = sink(tmp.path(), 100).pump(&raw[..]).unwrap();
        assert!(capture.is_folded());
        assert_eq!(capture.raw_bytes(), raw.len());
        assert!(capture.kept_bytes() < capture.raw_bytes());
        assert!(capture.fold_path().is_some());
    }

    #[test]
    fn empty_stream_is_empty_passthrough() {
        let tmp = tempfile::tempdir().unwrap();
        let capture = sink(tmp.path(), 10).pump(&b""[..]).unwrap();
        match capture {
            StreamCapture::Passthrough(bytes) => assert!(bytes.is_empty()),
            StreamCapture::Folded(_) => panic!("empty stream must not fold"),
            StreamCapture::BinaryPassthrough(_) => {
                panic!("an empty stream is text, and must not spill at all")
            }
        }
    }
}
