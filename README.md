# XZ Decompressor (`xz_decompressor`)
pure-python xz / lzma2 streaming decompressor with resumption

Target Environments: MicroPython, PyPy 3, CPython 3.8+

---

## 1. System Architecture & High-Performance Block Execution Model

This specification defines the architecture, wire protocols, storage model, fault tolerance, and event-driven lifecycle for a zero-dependency, pure-Python XZ (LZMA2) decompression engine.

Revision 4 incorporates comprehensive deadline, wall-clock timeout, and CPU timeout capabilities with dual-layer enforcement:
1. **Asynchronous Preemptive Signals**: Utilizing POSIX `SIGALRM` and `SIGVTALRM` (via `setitimer` or `alarm`) where supported.
2. **Cooperative Event-Loop Polling ("Slightly Early")**: Enforcing timeouts at event boundaries across all platforms with a small safety margin (~150–200 ms) prior to deadline expiry, ensuring clean state persistence and resumption command output before external supervisors trigger abrupt termination.
3. **Flexible ISO-8601 UTC Parser**: Supporting bare dates, compact notation, optional punctuation (`-`, `:`, `T`, `Z`), and variable time granularities (hours, minutes, seconds, subsecond fractions) across CPython, PyPy, and MicroPython.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        CORE PURE-PYTHON XZ / LZMA2 ENGINE                              │
│  • Pure computational range decoding, LZMA2 chunk parsing, probability arithmetic      │
│  • Decodes directly into internal active block buffer (4 KiB to 64 KiB bytearray)      │
│  • Local matches resolved via fast in-memory C slice copies (no method calls)          │
│  • Interacts with HistoryBackend via block-level slices (get_history_slice / append)   │
│  • Emits cooperative lifecycle events via block-level generator yielding               │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                     Yields Events:         ▼
            ┌───────────────────────────────┼───────────────────────────────┐
            │ [CHUNK_OUTPUT] (Block slices) │ [STREAM_BOUNDARY]             │
            │ [BLOCK_COMMITTED] (64 KiB)    │ [PROGRESS] (Block interval)   │
            └───────────────────────────────┴───────────────────────────────┘
                                            │
                   ┌────────────────────────┴────────────────────────┐
                   ▼                                                 ▼
┌──────────────────────────────────────┐          ┌──────────────────────────────────────┐
│        STANDALONE CLI DRIVER         │          │       EMBEDDED / CALLER DRIVER       │
│        (Target: CLI Utility)         │          │     (In-Memory / Streaming Tar)      │
├──────────────────────────────────────┤          ├──────────────────────────────────────┤
│ • 1025-block DirectoryStateStore     │          │ • Single-file FileHistory / MemHist  │
│ • Splits output_0.bin, output_1.bin  │          │ • Streams TAR chunks to unbundler    │
│ • Manages .part staging & --in-place │          │ • Transitions between VS streams 0/1 │
│ • Dual-Layer Watchdog:               │          │ • Services watchdog heartbeat        │
│   - --timeout (SIGALRM + coop)       │          │ • Zero disk hops                     │
│   - --cpu-timeout (SIGVTALRM + coop) │          │                                      │
│   - --deadline (ISO-8601 UTC)        │          │                                      │
│ • Prints stderr boundary telemetry   │          │                                      │
│ • Draws rotating ASCII indicator     │          │                                      │
└──────────────────────────────────────┘          └──────────────────────────────────────┘
```

---

## 2. Block-Level Decoding Execution Invariants

### 2.1 The Active Working Block Buffer (`cur_block`)
1. **Buffer Allocation**: The decompressor allocates an active decoding working buffer `cur_block = bytearray(block_size)`, where `block_size` is typically 64 KiB (65,536 bytes), or a caller-configured block size between 4 KiB and 64 KiB.
2. **Zero-Call Intra-Block Decoding**:
   - **Literals**: Directly assigned into `cur_block[pos] = symbol` at native C-extension speed.
   - **Local Matches (`distance <= pos`)**:
     - *RLE (`distance == 1`)*: Expanded via `cur_block[pos : pos + length] = bytes([cur_block[pos - 1]]) * length`.
     - *Non-Overlapping (`distance >= length`)*: Direct slice assignment `cur_block[pos : pos + length] = cur_block[pos - dist : pos - dist + length]`.
     - *Overlapping (`distance < length`)*: Fast unrolled loop or repeated slice multiplication within `cur_block`.
   - Over 95% of LZMA matches in typical streams are local (`distance <= pos`), completely eliminating backend lookup calls during active block execution.
3. **Inter-Block Matches (`distance > pos`)**:
   - When an LZMA match references history spanning prior blocks, the engine calculates the slice boundary and fetches the necessary byte slice from the pluggable backend:
     ```python
     needed_len = min(length, distance - pos)
     history_bytes = history.get_history_slice(distance - pos, needed_len)
     cur_block[pos : pos + len(history_bytes)] = history_bytes
     ```
   - If the match continues into the current block, subsequent bytes are copied from `cur_block` directly.
4. **Variable Quantum & Network Stream Support**:
   - If the input source is a network socket, pipe, or small payload yielding partial data, the decompressor does not stall waiting for a full 64 KiB buffer.
   - Slices as small as 1 byte are flushed and yielded cleanly when an explicit chunk boundary, end-of-stream, or caller flush is encountered.

---

## 3. Pluggable Block-Level Storage Protocol (`HistoryBackend`)

The decompressor is decoupled from storage through an abstract, block-level protocol:

### 3.1 Abstract Protocol Definition
```python
class HistoryBackend:
    def append_block(self, block_data: bytes, block_index: int = 0, input_hash: str = "") -> None:
        """Commit completed uncompressed block to history."""
        raise NotImplementedError

    def get_history_slice(self, distance: int, length: int) -> bytes:
        """Retrieve contiguous byte slice from past history."""
        raise NotImplementedError

    def checkpoint(self, block_index: int, state_record: dict, input_hash: str = "") -> None:
        """Persist decompression state at block boundary."""
        pass

    def restore(self, block_index: int) -> dict:
        """Load decompression state for block_index."""
        return None

    def evict_prior(self, min_retained_block: int) -> None:
        """Purge blocks older than min_retained_block from circular buffer."""
        pass

    def reset(self) -> None:
        """Reset history state between streams while keeping directories intact."""
        pass

    def cleanup(self) -> None:
        """Release backend resources upon successful completion."""
        pass
```

### 3.2 Concrete Backend Implementations

#### A. `DirectoryBlockHistoryStore` (Default Standalone CLI Backend)
- **Topology**: Maintains up to 1025 discrete 64 KiB files on disk in a dedicated working directory (1024 * 64 KiB = 64 MiB sliding history window + 1 active block).
- **FIFO Eviction**: When block index `k` is committed, if `k >= 1025`, all files associated with block `k - 1025` (`*.block`, `*.state`) are deleted.
- **Atomic File Writing**:
  1. Block data written to `.tmp_<hash>_<index:08d>.block` and companion state to `.tmp_<hash>_<index:08d>.state`.
  2. Flushed and atomically moved into place using `os.replace`.
- **LRU Block Cache**: In RAM, keeps an LRU cache of 1 to 2 recent 64 KiB history blocks to eliminate disk seeks for nearby inter-block lookbacks.
- **Crash Recovery & Verify-Output Mode**:
  - Direct resume when valid `.state` and history exist.
  - If state is missing, corrupted, or incomplete: restarts from block 0 in **Verify-Output Mode**, comparing each 64 KiB block against the existing partial output file. Upon the first mismatch, the output file is truncated immediately to that block boundary, transitioning to normal appended output.

#### B. `FileHistory` (Single-File Scratch CAS)
- Backed by a single scratch file (e.g. `.history.tmp`) with an internal LRU page cache.
- Avoids FAT directory exhaustion and flash write-wear on embedded MCUs while supporting full 64 MiB dictionaries.

#### C. `MemoryHistory` (In-Memory Ring Buffer)
- Backed by an in-memory `bytearray` ring buffer.
- Ideal for small dictionary streams, test harnesses, or memory-rich host environments.

---

## 4. Timeout, Deadline & Watchdog Specifications

### 4.1 CLI Flags & Parameters
1. `--timeout=[<N>s|<N>m|0]` (Default: `52s`):
   - Sets wall-clock timeout relative to process startup.
   - Values: integer or float followed by `s` (seconds) or `m` (minutes), or literal `0` (disables wall-clock timeout). Case-insensitive suffix.
   - At startup, initial `time.perf_counter()` is captured and subtracted from `--timeout`:
     `wall_remaining = timeout_sec - (time.perf_counter() - init_perf)`
2. `--deadline=<iso-8601-timestamp>Z`:
   - Sets an absolute UTC deadline instant.
   - Supported notations (case-insensitive for `T` and `Z`, all punctuation `-`, `:`, `T`, `Z` optional):
     - Bare dates: `YYYYMMDD` or `YYYY-MM-DD` (implicitly ending with `T00:00:00.000Z`).
     - Date and hours: `YYYYMMDDTHH`, `YYYY-MM-DD HH`.
     - Date, hours, minutes: `YYYYMMDDHHMM`, `YYYY-MM-DDTHH:MM`.
     - Date, hours, minutes, seconds: `YYYYMMDDHHMMSS`, `YYYY-MM-DDTHH:MM:SSZ`.
     - Date, hours, minutes, seconds, subsecond fractions: `YYYYMMDDHHMMSS.fff`, `YYYY-MM-DDTHH:MM:SS.fffZ`.
   - Resolves to:
     `deadline_remaining = max(0.0, deadline_epoch - now_epoch)`
   - If both `--timeout` and `--deadline` are supplied, the effective wall-clock duration resolves to the stricter (earlier) instant:
     `effective_timeout = min(timeout_sec, deadline_remaining)`
3. `--cpu-timeout=[<N>s|<N>m|0]` (Default: `22s`):
   - Sets process CPU time timeout.
   - Initial `time.process_time()` is captured and subtracted:
     `cpu_remaining = cpu_timeout_sec - (time.process_time() - init_cpu)`

### 4.2 Dual-Layer Enforcement Mechanism
- **Preemptive Asynchronous Signals**:
  - Wall-clock: Configures `SIGALRM` via `setitimer(ITIMER_REAL)` or `alarm` on POSIX systems.
  - CPU-time: Configures `SIGVTALRM` via `setitimer(ITIMER_VIRTUAL)` where supported.
  - Platforms without signals (Windows, MicroPython, non-main threads) gracefully bypass signal registration without failure.
- **Cooperative Clock-Checking ("Slightly Early")**:
  - Enforced on all platforms inside the CLI event loop on every decompressor event.
  - Uses a safety margin of ~150–200 ms:
    `wall_elapsed >= timeout_sec - 0.15`
    `cpu_elapsed >= cpu_timeout_sec - 0.15`
  - Exiting slightly early guarantees that the process commits active state, preserves `.part` files, emits the resumption message to `sys.stderr`, and exits before external supervisors issue hard termination.

### 4.3 Interruption Output & Exit Code
Upon wall-clock or CPU timeout:
1. Emits standard resumption message to `sys.stderr`:
   ```
   \nDecompression timed out [wall-clock deadline reached / CPU time limit reached].
   To resume, run with: --resume-dir=<dir> [--resume-from=<in_offset>] [--resume-at=<out_offset>]
   ```
2. Exits with standard status code **124** (`E_DEADLINE`).

---

## 5. Multi-Stream XZ Handling & Stream Boundary Semantics

- Concatenated XZ streams are permitted to have 0 to any multiple of 4 bytes null padding (`0x00`) per RFC §2.1.2.
- Stream 0 writes to `<base>`, stream `K >= 1` writes to `<base>_<K><ext>`.
- Boundary telemetry emitted to `sys.stderr`:
  - Normal files: `[xz:stream boundary] input_offset=...B stream_in=...B stream_out=...B output_offset=...B starting stream <K> -> <target>`
  - Non-files: `[xz:stream boundary] input_offset=...B stream_in=...B stream_out=...B output_offset=...B starting stream <K> -> <target>+<offset>B`

---

## 6. Staging, Filename Resolution & In-Place Directives

- Auto-naming: case-insensitive `<base>.xz`/`<base>.lzma` -> `<base>`, `-` -> `stdin.unxz`, fallback `<name>.unxz`.
- Staged as `<target>.part`, atomically renamed to `<target>` on stream completion. Unlinks stale `.part` on non-resuming runs.
- Direct `--in-place` mode writes directly without `.part` intermediaries.
- Resumption stream-0 invariant: always invoked with base stream-0 filename.

---

## 7. Command-Line Interface (CLI)

```
usage: xz_decompressor.py [-h] [-o OUTPUT] [--resume-dir RESUME_DIR]
                          [--resume-from RESUME_FROM] [--resume-at RESUME_AT]
                          [--in-place] [--timeout TIMEOUT]
                          [--cpu-timeout CPU_TIMEOUT] [--deadline DEADLINE]
                          [--test] [-v] [INPUT]

Universal Pure-Python Resumable XZ / LZMA2 Streaming Decompressor

positional arguments:
  INPUT                 Path to .xz file or '-' for stdin (default: '-')

optional arguments:
  -h, --help            Show this help message and exit
  -o, --output OUTPUT   Output file path or '-' for stdout (default: auto)
  --resume-dir DIR      Working directory for circular buffer checkpoints
  --resume-from OFFSET  Input offset for non-seekable resumption
  --resume-at OFFSET    Output offset for non-seekable resumption
  --in-place            Write directly to target files without .part staging
  --timeout TIMEOUT     Wall-clock timeout [<N>s|<N>m|0] (default: 52s)
  --cpu-timeout C_TO    CPU timeout [<N>s|<N>m|0] (default: 22s)
  --deadline DEADLINE   Absolute UTC deadline instant (ISO-8601)
  --test                Run self-contained unit test suite against lzma
  -v, --verbose         Enable diagnostic logging on stderr
```

---

## 8. Verification Suite Specification (`--test`)

Built-in unit tests verifying:
1. Flexible ISO-8601 parsing across bare dates, compact formats, and variable time precisions.
2. Duration parser (`<N>s`, `<N>m`, `0`).
3. Cooperative wall-clock timeout triggering slightly early.
4. Multi-stream concatenation with 0, 4, 8 null padding bytes.
5. Presets 0, 1, 6, and 9e (64 MiB dictionary).
6. DirectoryBlockHistoryStore circular eviction and Verify-Output mode recovery.
