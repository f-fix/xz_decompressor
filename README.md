# XZ Decompressor (`xz_decompressor`)
Pure-Python XZ / LZMA2 Streaming Decompressor with Resumption

Target Environments: MicroPython, CircuitPython, PyPy 3, CPython 3.8+, GraalPy, RustPython, PyEmscripten
Authoritative Standard: The .xz File Format Specification (v1.2.0 / RFC §2 & §3)
Revision: 11 (Three-tier block sizing [512 B, 8 KiB, 64 KiB] based purely on available RAM, zero-closure inner-loop execution, immutable _LIT_TRANS bytes literal, pure-RAM startup CRC64 determination, and standalone .inbuf/.hbuf generational sharing; see Appendix A)

---

## 1. System Architecture & Scope Definition

This specification defines the architecture, wire protocols, integrity verification, storage model, fault tolerance, and event-driven lifecycle for a zero-dependency, pure-Python XZ streaming decompression engine.

### 1.1 Wire Format Scope & Boundaries
- **Supported Primary Container**: The .xz File Format Specification (v1.2.0 / RFC §2 & §3), supporting multi-stream concatenation, variable null padding, and arbitrary multi-block structures. Each Stream is parsed independently (its Check type may differ from other Streams in the same file).
- **Supported Legacy Container (`.lzma`)**: The legacy 13-byte raw LZMA header format (LZMA_Alone) is fully supported, including independent and concatenated / intermixed XZ/LZMA streams (see §5.5). Non-XZ/non-LZMA input raises `E_FORMAT`.
- **Supported Filters**: LZMA2 (`Filter ID 0x21`) and raw LZMA1 (in `.lzma` containers) with arbitrary dictionary sizes up to 64 MiB of *usable* history.
- **Supported Check IDs**: `0x00` None, `0x01` CRC32, `0x04` CRC64, `0x0A` SHA-256. All other IDs raise `E_UNSUPPORTED`.
- **Unsupported Secondary Filters**: BCJ (`0x04`-`0x0B`) and Delta (`0x03`) are not included. Blocks with filter count > 1 or a non-LZMA2 filter ID are rejected with `E_UNSUPPORTED` before any output is produced for that Block.
- **Out of Scope**: random-access decoding via the Index, multithreaded decoding, and `.lz`/`.zst` containers.

### 1.2 Architectural Diagram

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        CORE PURE-PYTHON XZ / LZMA2 ENGINE                              │
│  • Pure computational range decoding, LZMA2 chunk parsing, probability arithmetic      │
│  • Normative Block Header parsing (Flags, VLI sizes, LZMA2 dictionary property)        │
│  • Mandatory Block Check computation (CRC32, CRC64, SHA-256) on uncompressed data      │
│  • Complete Index & Stream Footer verification (Backward Size, CRC32, Flag symmetry)   │
│  • Decodes directly into internal active block buffer (4 KiB to 64 KiB bytearray)      │
│  • Interacts with UnifiedHistory via tiered sliding window (RAM ring buffer + backing) │
│  • Emits cooperative lifecycle events via block-level generator yielding               │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                     Yields Events:         ▼
            ┌───────────────────────────────┼───────────────────────────────┐
            │ [CHUNK_OUTPUT] (Block slices) │ [STREAM_BOUNDARY]             │
            │ [BLOCK_COMMITTED] (offsets)   │ [PROGRESS] (Block interval)   │
            └───────────────────────────────┴───────────────────────────────┘
                                            │
                   ┌────────────────────────┴────────────────────────┐
                   ▼                                                 ▼
┌──────────────────────────────────────┐          ┌──────────────────────────────────────┐
│        STANDALONE CLI DRIVER         │          │       EMBEDDED / CALLER DRIVER       │
│        (Target: CLI Utility)         │          │     (In-Memory / Streaming Tar)      │
├──────────────────────────────────────┤          ├──────────────────────────────────────┤
│ • UnifiedHistory Engine              │          │ • UnifiedHistory Engine              │
│ • Splits output, output_1, output_2  │          │ • Streams TAR chunks to unbundler    │
│ • Manages .part staging & --in-place │          │ • Zero-allocation memoryview iter    │
│ • Binary Freeze/Thaw Resume Engine   │          │ • Pluggable MemoryStorage virtual VFS│
│ • Dual-Layer Watchdog:               │          │ • Zero disk hops when requested      │
│   - --timeout (SIGALRM + coop)       │          │                                      │
│   - --cpu-timeout (SIGVTALRM + coop) │          │                                      │
│   - --deadline (ISO-8601 UTC)        │          │                                      │
│ • Prints stderr boundary telemetry   │          │                                      │
│ • Rotating ASCII indicator (TTY only)│          │                                      │
└──────────────────────────────────────┘          └──────────────────────────────────────┘
```

---

## 2. Block-Level Decoding Execution Invariants

### 2.1 The Active Working Block Buffer (`cur_block`) & Ephemeral Views
1. **Buffer Allocation**: The decompressor allocates `cur_block = bytearray(block_size)`. `block_size` MUST be one of the three allowable sizes: **512 bytes**, **8192 bytes** (8 KiB), or **65536 bytes** (64 KiB). Sizing is determined automatically based purely on available system RAM without engine-sniffing:
   - **512 bytes**: When available RAM $< 64	ext{ KiB}$ (enabling postage-stamp microcontrollers to decompress extreme `lzma -9e` streams bit-perfectly).
   - **8192 bytes**: When available RAM $< 1	ext{ MiB}$.
   - **65536 bytes**: Standard operating environments with $\ge 1	ext{ MiB}$ RAM.
   Any override supplied via CLI (`--block-size`) or API MUST be exactly one of `{512, 8192, 65536}`; all other values are rejected (`E_FORMAT`, exit 1). Buffer allocation occurs strictly once at initialization; buffers are never reallocated or deallocated inside the decoding loop.
2. **Ephemeral Buffer View Lifetime Contract (Normative)**:
   - When `CHUNK_OUTPUT` is yielded by the iterator protocol, `event.data` exposes `memoryview(cur_block)[:pos]`.
   - The engine guarantees that `event.data` remains valid and intact **only until the caller requests the next item from the generator** (re-entry).
   - Upon generator re-entry, `pos` resets to 0 and `cur_block` is immediately overwritten by subsequent literals and match copies.
   - Streaming consumers (`file.write()`, network sockets, tar unpackers) consume `event.data` directly with zero intermediate heap allocations. Callers needing retention across iterations must explicitly invoke `bytes(event.data)`.
3. **Distance Convention**: `d` denotes the *zero-based* distance as held in `reps[]` and produced by the distance decoder (actual offset = `d + 1`). With `pos` = current index within `cur_block`:
   - A match is **local** iff `d < pos`; its source index is `pos - 1 - d`.
   - Otherwise it is **inter-block**, needing `h = (d + 1) - pos` bytes (`h >= 1`) from committed history.
4. **Zero-Allocation Intra-Block Decoding**:
   - **Literals**: assigned directly into `cur_block[pos]`.
   - **Local Run-Length (`d == 0`)**: expanded in-place inside `cur_block` using logarithmic doubling slice copies with zero heap allocations:
     ```python
     cur_block[pos] = b_val
     copied = 1
     while copied < copy_len:
         take = min(copied, copy_len - copied)
         cur_block[pos + copied : pos + copied + take] = cur_block[pos : pos + take]
         copied += take
     ```
   - **Local Matches (`d + 1 >= copy_len`)**: direct slice copy `cur_block[pos : pos + copy_len] = cur_block[src : src + copy_len]`.
   - **Overlapping Local Matches (`d + 1 < copy_len`)**: expanded in-place using logarithmic doubling slice copies with zero heap allocations.
5. **Inter-Block Matches**: The engine requests `m = min(n, h)` bytes via `history.get_history_slice(h, m)`. If `n > h`, the remaining `n - h` bytes are copied from `cur_block[0 : n - h]` (the match has re-entered the current block; source index for output byte `j >= h` is `j - h`).
6. **Block Commit Order**: When `pos == block_size` (or at end of an XZ Block / stream), the block is (a) added to the running Check, (b) appended to history, (c) counted in offsets, (d) yielded as events, in that strict order.

### 2.1.3 Resource-Adaptive Write-Back Cache
To eliminate per-block system call overhead (`f.write()`, `f.flush()`, event generator context switches, and terminal I/O) on fast runtimes while strictly preserving microcontroller memory bounds:
1. **Adaptive Sizing Matrix**:
   $$\text{wb\_blocks} = \max\left(1, \min\left(16, \text{int}\left(\frac{\text{avail\_mem} \times 0.20}{\text{block\_size}}\right)\right)\right)$$
   - Desktop / Server (RAM $\ge 32\text{ MiB}$): Allocates a preallocated write-back buffer of 16 blocks (1 MiB when `block_size = 64 KiB`).
   - Microcontrollers (RAM $\le 256\text{ KiB}$): Automatically sets `wb_blocks = 1` (immediate write-through mode, allocating **0 bytes** of write-back buffer).
2. **Four Deterministic Flush Triggers**:
   Accumulated blocks are flushed and yielded downstream if and only if:
   a. **Buffer Full**: Exactly `wb_blocks` blocks have accumulated (e.g. 1 MiB batch).
   b. **Stream End**: EOF or an explicit `STREAM_BOUNDARY` is reached.
   c. **Wall-Clock Inactivity**: $> 2.0\text{ seconds}$ have elapsed since the last flush.
   d. **Process Termination**: `atexit` hook, SIGINT (`Ctrl+C`), or timeout deadline trigger.
3. **Yielding & Terminal Telemetry**:
   In write-back mode, `CHUNK_OUTPUT`, `BLOCK_COMMITTED`, and `PROGRESS` events are emitted at write-back flush boundaries. Downstream file writing and terminal status indicators (braille spinner) advance in **1 MiB increments** (or once every 2s (aligned with FAT32 2-second timestamp resolution)), eliminating over 94% of system call overhead.

4. **Terminal Status Line Format**:
   When active, the progress callback updates the terminal in the format:
   `{glyph} {time_field} {size_field} decoded, {rate_field}`
   - `time_field`: Fixed-width (10 characters). Displays cumulative elapsed time across sessions formatted via `str(datetime.timedelta)` when the `datetime` module is available, or `NNNNs` (seconds followed by "s") elsewhere. If total elapsed time reaches the `uint32` maximum ($4,294,967,295$ ms $\approx 49.7$ days), it is clamped and displayed as all spaces.
   - Status eraser line is widened to 56 characters to fully clear the elapsed time indicator.

### 2.2 Normative Block Header Parsing & Validation
Every Block begins with a Block Header (XZ spec §3.1). A first byte of `0x00` is the Index Indicator and begins §5.2.
1. **Header Size**: `(first_byte + 1) * 4` bytes.
2. **Header CRC32**: The last 4 bytes (little-endian) are the CRC32 of all preceding header bytes. It MUST be verified before parsing inner fields (`E_CHECK` on mismatch).
3. **Block Flags (Byte 1)**:
   - **Filter Count (bits 0-1)**: `count = (flags & 0x03) + 1`; `count > 1` → `E_UNSUPPORTED`.
   - **Reserved (bits 2-5)**: MUST be 0 (`E_FORMAT`).
   - **Compressed Size Present (bit 6)** / **Uncompressed Size Present (bit 7)**: if set, a VLI follows. Actual byte counts must match (`E_FORMAT`).
4. **Filter Flags**: Filter ID (VLI) MUST be `0x21` (`E_UNSUPPORTED`). Size of Properties (VLI) MUST be `1` (`E_FORMAT`). Property byte `p` encodes dictionary size (unmasked):
   ```python
   if p > 40:    raise XZFormatError   # E_FORMAT (reserved)
   elif p == 40: dict_size = 0xFFFFFFFF
   else:         dict_size = (2 | (p & 1)) << (p // 2 + 11)
   ```
5. **Header Padding**: 0-3 null bytes before CRC32. Non-zero padding → `E_FORMAT`. Header Size MUST be exactly consumed.
6. **VLI rules**: at most 9 bytes, value < 2^63, minimal encoding → `E_FORMAT` otherwise.

### 2.3 Mandatory Block Check Verification & Pre-Allocation
1. **Check Type**: Bits 0-3 of Stream Flags byte 2. Sizes: `0x00`→0, `0x01`→4, `0x04`→8, `0x0A`→32 bytes.
2. **Pre-Allocation & Zero-RAM Fallbacks**:
   - **CRC32**: Probed in ROM-first order (`ubinascii.crc32` → `binascii.crc32` with C-builtin check). If unavailable: pre-allocated 1024-byte table on systems with $\ge 64	ext{ KiB}$ RAM; bitwise tableless function (`@micropython.native`) consuming **0 bytes of RAM** on extreme microcontrollers.
   - **CRC64**: Pre-allocated 2048-byte table on CPython/PyPy; tableless bitwise function on MicroPython/CircuitPython to prevent 64-bit integer heap allocation.
   - **SHA-256**: Pre-allocated `hashlib.sha256()` accumulator when `check_type == 10`.
3. **Block Padding & Comparison**: After LZMA2 end marker, 0-3 null bytes pad (Header + Compressed Data) to a multiple of 4; non-zero → `E_FORMAT`. Check bytes read and compared (`E_CHECK` on mismatch).

### 2.4 LZMA2 Chunk Format, Reset Semantics & Validity Rules
Each Block's compressed data is a sequence of chunks, each starting with control byte `ctrl`.
- `ctrl == 0x00`: end of LZMA2 data for the Block.
- `ctrl == 0x01 / 0x02`: uncompressed chunk (1-65536 raw bytes); `0x01` resets dictionary.
- `ctrl >= 0x80`: LZMA chunk. `u_size = ((ctrl & 0x1F) << 16) + BE16 + 1`, `c_size = BE16 + 1`, optional property byte if `mode >= 2`.
- `ctrl` in `0x03..0x7F` → `E_FORMAT`.
- **Reset modes**: `0` keep all; `1` reset state; `2` reset state + new properties; `3` reset state + new properties + reset dictionary (`history.reset()`, `reps = [0,0,0,0]`).
- **Property byte**: `v = (pb * 5 + lp) * 9 + lc <= 224` and `lc + lp <= 4` (`E_FORMAT`).
- **Validity rules**: First chunk MUST reset dictionary (`ctrl == 0x01` or `ctrl >= 0xE0`). Mode >= 2 sets properties; mode < 2 before properties set → `E_FORMAT`. Chunk exactness: exactly `u_size` emitted, `c_size` consumed, range decoder `code == 0`. Initial range coder byte MUST be `0x00` (`E_FORMAT`). Inter-block isolation strictly enforced.

### 2.5 Match Execution Invariants (normative)
1. **Distance binding**: Every path binds `d` from decoded or `reps[]` values before copy:
   | Symbol | `d` | `reps` update |
   |:---|:---|:---|
   | Match | decoded distance | `reps = [d, r0, r1, r2]` |
   | Short rep (rep0, len 1) | `reps[0]` | none |
   | **Long rep0** | **`reps[0]`** | none |
   | Rep1 / Rep2 / Rep3 | `reps[i]` | move-to-front |
   `d == 0xFFFFFFFF` is forbidden in LZMA2 → `E_FORMAT`.
2. **State transitions**: Evaluated via immutable module-level tuple `_LIT_TRANS = (0,0,0,0,1,2,3,4,5,6,4,5)` or branchless arithmetic (zero list allocations).
3. **Split-Match Re-entrancy (Freeze/Thaw Invariant)**: When `cur_block_pos` reaches `block_size` in the middle of a match copy, the copy state (`in_match_copy = True`, `match_dist`, `match_rem`) is recorded on the engine. Resumption or thawing immediately resumes copying remaining bytes into the fresh block buffer with zero symbol re-decoding.

---

## 3. Unified Hybrid Storage & Resumption Architecture

### 3.1 The Unified History Engine (`UnifiedHistory`)
All historic uncompressed data is managed by a single, unified two-tier sliding window engine:

1. **Tier 1 (Hot In-Memory Ring Buffer)**:
   - A single contiguous `bytearray` pre-allocated at initialization to the detected RAM budget:
     $$	ext{ram\_cache\_size} = \min(	ext{dict\_size}, \max(2 	imes 	ext{block\_size}, 	ext{free\_RAM} 	imes 0.20))$$
   - Over 95% of LZMA matches fall within the most recent 16-32 KiB. Lookups within the hot buffer are resolved via direct slice operations at native memory speed with zero dictionary lookups and zero system calls.
2. **Tier 2 (Cold Backing Store)**:
   - **Mode A (Seekable Output File)**: When the destination is a seekable file, Tier 2 *is the output file itself*. If a match distance exceeds the hot RAM ring buffer, the engine seeks and reads directly from `output_file.seek(total_written - distance)`. Zero duplicate history is written to disk.
   - **Mode B (Unseekable Output - stdout, pipe, socket)**: Once committed blocks slide out of the hot RAM ring buffer, they append sequentially to `.xz_history/<stream_id>.hist`.
3. **History Operations Protocol**:
   - `append_block(block_data)`: commits block to hot ring buffer (and cold scratch if unseekable). Manages buffer sliding via slice rebinding (`self.history = self.history[drop:]`), avoiding in-place `del` operations which raise `TypeError` on MicroPython `bytearray`.
   - `get_history_slice(distance, length)`: returns slice from hot RAM buffer if `distance <= hot_len`, else reads from cold store. Distance > available history raises `E_FORMAT`.
   - `truncate_to(total_len)`: discards history committed after a checkpoint boundary using `truncate_file_portable()` and slice rebinding.
   - `tail_crc32(n=65536)`: returns CRC32 of the last `min(n, history_len)` bytes for checkpoint validation.
   - `cleanup()`: unlinks all backing files and releases resources.

### 3.2 Transient Storage VFS (`TransientStorage`)
All scratch files (cold history `.hist`, input spool `.in_spool`, paged tables `.plit`, and checkpoint journals `.state_[A|B]`) route through a unified storage driver:
1. **`DiskStorage` (Default)**:
   - Files stored under directory specified by `--resume-dir` (default: `.xz_history`).
   - Multi-process isolation: all artifacts prefixed with `<stream_id>_*`.
   - Atomic replacement: writes to `.tmp_<name>` followed by atomic `os.replace`.
   - Durability: explicit `os.fsync` on data files before journal commit.
   - Automatic zero-trace directory cleanup: when a stream finishes or is cleaned up, it unlinks its artifacts and calls `os.rmdir(".xz_history")`. If sibling processes are still executing, `OSError` is caught silently; when the last active stream finishes, the directory is automatically removed.
2. **`MemoryStorage` (Pluggable Virtual Disk)**:
   - Operates purely in-memory using an internal dictionary of `io.BytesIO` objects.
   - Allows embedded callers or serverless containers to execute with the exact same code paths and zero disk operations.

### 3.3 Zero-Allocation Direct-to-Disk Freeze / Thaw & Lifecycle API
All dictionary-based `export_state` and `import_state` mechanisms are removed. Freezing and thawing stream binary data directly to/from disk journals using Python's native buffer protocol (`f.write()` and `f.readinto()`) with **zero dynamic heap allocations**:

1. **Strictly Scalar State Dictionaries & Hex Ban**:
   - The state dictionary is strictly scalar-only (validated via `_assert_valid_state_dict()`). It contains solely scalar integers, booleans, short strings, and `None`.
   - All arrays, lists of numbers, binary buffers, and hex-encoded strings are strictly banned from the state dict. Any violation raises an immediate `AssertionError`.
   - Hex encoding (`.hex()`, `fromhex()`) is banned across the entire codebase apart from cryptographic hash digests (`hashlib.sha256().hexdigest()`) and CRC integer formatting.
2. **Dedicated Binary Component Files & Explicit Recorded Lengths**:
   - All components that can grow large are stored in separate dedicated binary files:
     - `<prefix>.pcore`: 11 core probability arrays.
     - `<prefix>.plit` : literal probability table `p_lit`.
     - `<prefix>.cblk` : active working block buffer `cur_block`.
     - `<prefix>.inbuf`: unconsumed compressed chunk read-ahead buffer (strictly omitted when input is a seekable file, as the input file itself is seekable).
     - `<prefix>.hbuf` : in-RAM history buffer (strictly omitted when output is a seekable file, as the output file itself is the dictionary).
     - `<prefix>.state`: scalar metadata state dict.
   - The state dictionary explicitly records the exact valid byte lengths: `len_pcore`, `len_plit`, `len_cblk`, `len_inbuf`, `len_hbuf`.
3. **Bounded Thawing & MicroPython Flash Wear Protection**:
   - When thawing, the engine reads *only* up to the recorded length via `f.readinto()`.
   - If a file on disk is longer than recorded (`actual_size > recorded_length`), trailing bytes left over from previous un-truncated writes (such as on MicroPython where `io.FileIO` lacks native `.truncate()`) are strictly ignored. This eliminates flash rewrite cycles and prevents write amplification.
   - If a file on disk is shorter than recorded (`actual_size < recorded_length`), checkpoint corruption is detected.
4. **Asymmetric Invalidation & Recovery Protocol**:
   - **Unseekable Input Stream (`stdin` / pipe)**: Checkpoint corruption is a fatal error (`E_RESOURCE`), because an upstream pipe cannot be rewound to re-decode.
   - **Seekable Input AND Seekable Output (Files on disk)**: Emits a warning to stderr (`xz_decompressor: warning: corrupted checkpoint detected in '<dir>'; invalidating state and restarting decompression from genesis.`), unlinks the bad checkpoint, rewinds both files to offset 0, and restarts cleanly from genesis.
   - **Seekable Input AND Unseekable Output (`stdout` / pipe)**: If `--resume-at=<out_offset>` is provided, emits a warning to stderr, rewinds input to 0, decodes from genesis while discarding uncompressed output bytes prior to `resume_at`, and resumes streaming to the pipe without duplicating a single byte!
5. **Discarding (`engine.discard_checkpoint(descriptor)`)**:
   - Unlinks `.state_A`, `.state_B`, `.pcore`, `.plit`, `.cblk`, `.inbuf`, `.hbuf` and associated `.in_spool` files from transient storage.

### 3.4 Non-Seekable Stream Invariants (Zero Re-Read / Zero Re-Write)
1. **Bounded Rolling Input Spooling (`.in_spool` / `INPUT_BUFFER.bin`)**:
   - When reading from an unseekable stream (stdin/pipe), bytes are streamed through an input spool in small slices (512 B - 1 KiB), completely eliminating the need for a 64 KiB buffer in RAM on microcontrollers.
   - At each block commit boundary (`pos == block_size` or block end), the decompressor re-bases the spool: all bytes committed prior to `engine.overall_input_offset` are pruned, and only unconsumed read-ahead bytes ($\Delta \le 64	ext{ KiB}$) are retained.
   - The on-disk spool file size is strictly bounded to $O(1) \le 64	ext{ KiB}$ for the entire life of the stream, eliminating disk exhaustion and flash write wear on long streams.
   - On resumption, the decompressor drains the unconsumed read-ahead from the spool before reading fresh bytes from the pipe.
   - **Invariant**: Upstream pipe bytes are NEVER re-read, and the spool file never grows unbounded.
2. **Unseekable Output Alignment**:
   - Checkpoints are committed strictly after emitting completed blocks to stdout/pipe.
   - On resumption, `--resume-at=<out_offset>` asserts exact continuation.
   - **Invariant**: Downstream pipe bytes are NEVER re-written or duplicated.

### 3.5 Resource-Adaptive Microcontroller Scaling (~20 KiB RAM Support)
1. **Pre-Allocation Invariant**: On low-memory environments, ALL buffers, arrays, views, and event objects MUST be allocated up-front during engine initialization. Zero dynamic allocations occur during active streaming decoding.
2. **Sliding Memory Allocation Matrix**:
   | Environment | Working RAM | `block_size` | Hot RAM Ring Buffer | History Backing | Literal Prob Table |
   |:---|:---|:---|:---|:---|:---|
   | **MicroPython Extreme** | $< 64	ext{ KiB}$ | **4 KiB** | 0 KiB | Output file or `.hist` scratch | 1.5 KiB (paged to `.plit` if pathological) |
   | **MicroPython Standard** | $128	ext{ KiB} - 2	ext{ MiB}$ | **8 KiB** | 16-32 KiB | Output file or `.hist` scratch | Full in RAM (up to 24.5 KiB) |
   | **Desktop / Server** | $\ge 72	ext{ MiB}$ | **64 KiB** | Full `dict_size` | Output file or RAM | Full in RAM (up to 6.3 MiB) |
3. **Pathological Legacy LZMA Probability Table Paging**:
   - In legacy LZMA (`.lzma`), streams with `lc=8, lp=4` declare a 6.29 MiB table ($4,096$ sub-tables of 1.5 KiB each).
   - On devices where the full table exceeds 20% of free RAM, the engine keeps an active working set in a pre-allocated 1.5 KiB array (`array.array("H", [1024]*768)`) and pages inactive sub-tables to `.xz_history/<stream_id>.plit`.
4. **Watermarked Adaptive Garbage Collection**:
   - Checked once every 16 blocks: `gc.collect()` is invoked ONLY if capability-detected `_gc_mem_free()` exists and reports $< 4	ext{ KiB}$ available RAM.
   - On CPython, PyPy, and GraalPy, `_gc_mem_free` is `None` (zero GC impact).
5. **Traceback Heap Safety Zone**:
   - Top-level execution runs inside a nested `try / finally` holding an emergency 1 KiB cushion (`memory_cushion = bytearray(1024)`).
   - Upon any error, the `finally` block frees the cushion and runs collection *before* exception handlers format diagnostic messages, preventing secondary OOM during traceback printing.

---

## 4. Timeout, Deadline & Watchdog Specifications

### 4.1 CLI Flags & Parameters
1. `--timeout=[<N>s|<N>m|0]` (default `52s`): wall-clock timeout relative to startup (monotonic clock). `0` disables.
2. `--deadline=<iso-8601>`: absolute UTC instant (flexible format).
3. `--cpu-timeout=[<N>s|<N>m|0]` (default `22s`): process CPU time limit.

### 4.2 Dual-Layer Enforcement Mechanism
- **Preemptive signals**: `SIGALRM` and `SIGVTALRM` registered opportunistically. On platforms where signals are restricted (GraalPy sandbox, WebAssembly/PyEmscripten, Windows), registration failures are caught and ignored.
- **Cooperative checks**: Evaluated on committed block boundaries with proportional margin (`coop_margin = min(0.15, timeout_sec * 0.10)`).
- On timeout/deadline trigger: writes mandatory binary checkpoint, flushes output, prints resume telemetry to stderr, and exits status **124** (`E_DEADLINE`).

### 4.3 Continuation Telemetry & Parameter Omission Matrix
The timeout message **always prints `--resume-dir`** to ensure cross-runtime/engine portability, while omitting redundant flags for seekable files:

```text
Decompression timed out (wall-clock deadline reached / CPU time limit reached).
To resume, run: xz_decompressor.py <INPUT> [-o <OUTPUT>] --resume-dir=<DIR> [--resume-id=<ID>] [--resume-from=<IN_OFF>] [--resume-at=<OUT_OFF>]
```

| Input Source | Output Target | `--resume-dir` | `--resume-id` | `--resume-from` | `--resume-at` | Resulting Resume Command Line |
|:---|:---|:---|:---|:---|:---|:---|
| **Seekable File** | **Seekable File** | **Always Printed** | **Omitted** *(derived)* | **Omitted** *(seeks file)* | **Omitted** *(seeks file)* | `xz_decompressor.py in.xz -o out.bin --resume-dir=.xz_history` |
| **Unseekable Pipe** | **Seekable File** | **Always Printed** | **Printed** | **Printed** | **Omitted** *(seeks file)* | `cat in.xz \| xz_decompressor.py - -o out.bin --resume-dir=.xz_history --resume-id=... --resume-from=...` |
| **Seekable File** | **Unseekable Pipe** | **Always Printed** | **Omitted** *(derived)* | **Omitted** *(seeks file)* | **Printed** | `xz_decompressor.py in.xz -o - --resume-dir=.xz_history --resume-at=...` |
| **Unseekable Pipe** | **Unseekable Pipe** | **Always Printed** | **Printed** | **Printed** | **Printed** | `cat in.xz \| xz_decompressor.py - -o - --resume-dir=.xz_history --resume-id=... --resume-from=... --resume-at=...` |

*(Note: In file-to-file decompression, if all needed checkpoint artifacts exist in the default resume directory, running the command without `--resume-dir` also resumes automatically by default).*

---

## 5. Multi-Stream XZ Handling, Index & Stream Footer Semantics

### 5.0 Stream Header (12 bytes)
1. **Magic**: `FD 37 7A 58 5A 00` (`E_FORMAT` on mismatch).
2. **Stream Flags**: Byte 6 MUST be `0x00`; high nibble of Byte 7 MUST be `0`; low nibble is Check type (`E_FORMAT`).
3. **CRC32**: Little-endian CRC32 of the 2 Stream Flags bytes (`E_CHECK`).

### 5.1 Multi-Stream Concatenation, Padding & Output Routing
- **Stream Padding**: 0 or more null bytes whose total count is a multiple of 4 permitted between streams and at EOF. Non-multiple-of-4 or non-null bytes before next stream header → `E_FORMAT`.
- **Output Routing**: Stream 0 writes to `<base>`; Stream $K \ge 1$ writes to `<base>_<K><ext>` (`out.bin` → `out_1.bin`). When writing to stdout all streams concatenate.
- **Output Creation**: Created when Stream starts, ensuring empty streams yield empty files.
- **Boundary Telemetry** (stderr, emitted only when another stream follows):
  `[xz:stream boundary] input_offset=...B stream_in=...B stream_out=...B output_offset=...B starting stream <K> -> <target>`
  After final stream:
  `[xz:stream end] input_offset=...B output_offset=...B streams=<N>`.

### 5.2 Index & Stream Footer Verification
1. **Index**: Indicator `0x00`, record count VLI, `(Unpadded Size, Uncompressed Size)` VLI pairs matching decoded blocks, 0-3 padding nulls, little-endian CRC32 (`E_CHECK` / `E_FORMAT`).
2. **Stream Footer (12 bytes)**: CRC32 of Backward Size + Flags (6 bytes), Backward Size matching Index length, Flags matching Stream Header, Magic `59 5A` (`b"YZ"`).

### 5.5 Normative Legacy LZMA_Alone (`.lzma`) Container Specification
1. **13-Byte Header**:
   - Byte 0: `p = (pb * 5 + lp) * 9 + lc <= 224`, `pb <= 4`, `lp <= 4`, `lc <= 8`.
   - Bytes 1-4: Dictionary size (little-endian uint32). Valid sizes: `0`, `0xFFFFFFFF`, $2^n$, or $2^n + 2^{n-1}$.
   - Bytes 5-12: Uncompressed size (little-endian uint64). `0xFFFFFFFFFFFFFFFF` (unknown, mandatory EOS) or $< 256	ext{ GiB}$.
2. **Range Decoder First Byte**: MUST be `0x00` (`E_FORMAT`).
3. **Format Detection & Empty Input**: Empty input (0 bytes) raises `E_FORMAT` (exit 1). Properties byte `0x00` (`lc=0, lp=0, pb=0`) is valid.
4. **EOF & Truncation Integrity**: Unknown-size streams terminate strictly at explicit EOS marker (`dist == 0xFFFFFFFF`); premature EOF raises `E_FORMAT`. Known-size streams terminate at exact byte count; premature EOF raises `E_FORMAT`.
5. **Trailing Bytes**: Trailing garbage or orphan null bytes rejected with `E_FORMAT`; followed by valid XZ or LZMA header transitions cleanly.

---

## 6. Staging, Filename Resolution & In-Place Directives

- **Auto-Naming**: Case-insensitive `<base>.xz` or `<base>.lzma` → `<base>`; `-` → `stdin.unxz`.
- **Atomic Staging**: Each Stream decodes to `<target>.part` and atomically renames to `<target>` upon stream completion. Stale `.part` files from non-resuming runs are unlinked before output initialization.
- **Direct `--in-place` Mode**: Writes directly to destination targets without `.part`.
- **Preflight Storage Fallback**: If the target storage partition lacks sufficient space for decompressor state, but available system RAM is sufficient (`avail_mem >= state_budget`), the engine automatically falls back to in-memory `MemoryStorage` with an informative warning, allowing decompression to succeed without failing the caller.
- **Flash-Safe Truncation**: All file truncation portably uses `truncate_file_portable()`, which executes in-place when native `truncate()` exists, or streams valid prefixes to `.trunc` and atomically replaces on MicroPython `io.FileIO` (executed solely on crash recovery rollback, incurring 0 flash wear during normal streaming).
- **Failure Behavior**: On `E_DEADLINE` or `SIGINT`, `.part`, journals, and history scratch are preserved. On data errors (`E_FORMAT`, `E_CHECK`), `.part` is retained for forensic recovery and journals are removed.

---

## 7. Command-Line Interface (CLI)

```text
usage: xz_decompressor.py [-h] [-o OUTPUT] [--resume-dir RESUME_DIR]
                          [--resume-mode {auto,checkpoint,verify,off}]
                          [--checkpoint-interval INTERVAL]
                          [--resume-id ID] [--resume-from OFFSET]
                          [--resume-at OFFSET] [--in-place]
                          [--timeout TIMEOUT] [--cpu-timeout CPU_TIMEOUT]
                          [--deadline DEADLINE] [--block-size BLOCK_SIZE]
                          [--storage-dir STORAGE_DIR]
                          [--memory-limit MEMORY_LIMIT] [--no-gc]
                          [--ignore-space-check] [--test] [-v] [INPUT]

Universal Pure-Python Resumable XZ / LZMA2 Streaming Decompressor

positional arguments:
  INPUT                    Path to .xz/.lzma file or '-' for stdin (default: '-')

optional arguments:
  -h, --help               Show this help message and exit
  -o, --output OUTPUT      Output file path or '-' for stdout (default: auto)
  --resume-dir DIR         Working directory for checkpoints and transient scratch (default: .xz_history)
  --resume-mode MODE       auto (default), checkpoint, verify, off
  --checkpoint-interval N  Periodic checkpoint spacing [<N>k|<N>m bytes | <N>s]; 0 disables (default: 8m)
  --resume-id ID           Session/stream identifier for non-seekable resumption
  --resume-from OFFSET     Input byte offset assertion for non-seekable resumption
  --resume-at OFFSET       Output byte offset assertion for non-seekable resumption
  --in-place               Write directly to destination targets without .part staging
  --timeout TIMEOUT        Wall-clock timeout [<N>s|<N>m|0] (default: 52s)
  --cpu-timeout C_TO       CPU timeout [<N>s|<N>m|0] (default: 22s)
  --deadline DEADLINE      Absolute UTC deadline instant (ISO-8601)
  --block-size SIZE        auto (4k/8k if RAM constrained, else 64k) or power-of-two in [4k, 64k]
  --storage-dir DIR        Directory for transient scratch (alias for --resume-dir)
  --memory-limit MEM       Simulate/override python-available RAM limit (e.g. 64m, 128k, 20k)
  --no-gc                  Disable adaptive low-watermark garbage collection
  --ignore-space-check     Bypass preflight storage space checks
  --test                   Run self-contained unit test suite
  -v, --verbose            Diagnostic logging and tracebacks on stderr
```
*Note: The legacy `--history-backend` option is deleted; history storage is unified and automatically resource-tuned.*

---

## 8. Verification Suite Specification (`--test`)

The self-contained test suite validates:
1. **Unified Storage Engine**: Single-backend execution across seekable output files and unseekable streams with matching hash verification.
2. **Ephemeral Buffer View Lifetime**: Assertion that `CHUNK_OUTPUT` views are consumed in-place with zero heap allocations.
3. **Zero-Allocation Direct-to-Disk Freeze/Thaw**: Verification that `freeze()` and `thaw()` stream binary state directly via `readinto()` without generating Python dictionaries.
4. **Resumption Convergence**: Multi-pass resumption under tight timeouts converging to bit-exact match with oracle.
5. **Stream Invariants**: Input read-ahead spooling (zero pipe re-reads) and output stream alignment (zero duplicate stdout writes).
6. **Pathological Legacy LZMA Paging**: Successful decompression of streams declaring extreme probability tables (`lc=8, lp=4` $	o 6.29	ext{ MiB}$) under simulated low-RAM limits ($\le 128	ext{ KiB}$).
7. **Cross-Engine Portability**: Tests pass under CPython, PyPy, and MicroPython/CircuitPython shims with no-table CRC code paths.
8. **Multi-Process Concurrency & Cleanup**: Concurrent streams in `.xz_history` isolate artifacts and automatically unlink the directory when the final stream completes.

---

## 9. Standard Error Codes & Exit Status

| Exit Code | Identifier | Description |
|:---|:---|:---|
| `0` | `SUCCESS` | Decompression completed successfully; all checksums verified. |
| `1` | `E_FORMAT` | Invalid magic, corrupt header, illegal VLI, malformed padding, truncated input, invalid chunk sequence, distance overflow, invalid block size. |
| `1` | `E_UNSUPPORTED`| Filter count > 1, non-LZMA2 filter ID, or reserved Check ID. |
| `1` | `E_CHECK` | Header CRC32, Block Check (CRC32/CRC64/SHA-256), or Index/Footer CRC32 mismatch. |
| `1` | `E_RESOURCE` | Out of memory, insufficient storage space, or failed checkpoint restore. |
| `1` | `E_IO` | File read/write failure, broken pipe, or directory permission error. |
| `124` | `E_DEADLINE` | Wall-clock timeout, absolute deadline, or CPU timeout reached. |
| `130` | `SIGINT` | Interrupted by keyboard interrupt (`Ctrl+C`). |

---

## Appendix A - Changes Across Revisions

### Changes from Revision 10 to Revision 11
| # | Change | Reason (empirically verified against Rev 10 baseline) |
|:---|:---|:---|
| 1 | §2.1: Three-tier adaptive block sizing (512 B, 8192 B, 65536 B) based purely on RAM | Enforces 512-byte blocks when RAM $< 64	ext{ KiB}$, 8192-byte blocks when RAM $< 1	ext{ MiB}$, and 65536-byte blocks otherwise. Overrides strictly restricted to `{512, 8192, 65536}`. Enables postage-stamp microcontrollers to decompress extreme `lzma -9e` streams bit-perfectly. |
| 2 | §2.1, §2.4: Zero closure creation and RangeDecoder reuse in decoding loop | Eradicates per-chunk `_rd_get`, `_vli`, and `RangeDecoder` allocations in `decode_block()`. Renormalization indexes chunk buffers directly via `pos < limit`. |
| 3 | §2.5: Constant immutable `_LIT_TRANS` bytes literal | Replaces per-literal 12-element list allocations with constant `b"\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x04\x05"`, eliminating over 70 million allocations on 71.8 MB streams. |
| 4 | §2.3: Pure-RAM CRC64 determination at startup | Eliminates engine-sniffing (`_is_embedded_python`). Evaluates available RAM once at startup: uses 256-entry table lookup whenever RAM $\ge 64	ext{ KiB}$ (eliminating 568 million bitwise shift iterations), and bitwise loop when RAM $< 64	ext{ KiB}$. |
| 5 | §3.3, §3.4: Standalone `.inbuf` and `.hbuf` with safe generational sharing | `.inbuf` and `.hbuf` remain external standalone files. Consecutive state iterations safely share them until the subsequent iteration commits and prunes obsolete earlier versions. |

### Changes from Revision 8 to Revision 9

| # | Change | Reason (empirically verified against Rev 8 baseline) |
|:---|:---|:---|
| 1 | §3.1: Single Unified History Engine (`UnifiedHistory`) replacing 4 legacy backends | Resolves the 70% throughput regression; eliminates integer division, dict hashing, and generator overhead by using a fast hot RAM ring buffer backed by the seekable output file or sequential scratch file. |
| 2 | §7: Complete deletion of `--history-backend` CLI flag | Eliminates configuration traps and redundant branching; storage tiering is 100% automatic based on destination seekability and RAM budget. |
| 3 | §3.3: Direct-to-disk binary Freeze/Thaw using `readinto()` | Replaces dictionary-based `export_state`/`import_state` (which allocated 49 KiB-12.6 MiB heap strings), achieving true zero-allocation persistence and eliminating OOM on microcontrollers. |
| 4 | §3.2: Pluggable `TransientStorage` VFS (`DiskStorage` default, `MemoryStorage` virtual disk) | Decouples storage mechanisms from engine logic, enabling zero-disk-hop execution in memory or serverless containers. |
| 5 | §2.1.2: Normative Ephemeral Buffer View Lifetime Contract | Formally specifies that `CHUNK_OUTPUT` views are valid only between `yield` and generator re-entry, allowing true zero-allocation streaming from RangeDecoder to `file.write()`. |
| 6 | §4.3: Decoupled Resume State & Seekable File Omission Matrix | Replaces disconnected internal tokens with `ResumeDescriptor` mapping 1:1 with CLI `--resume-*` flags. Omits `--resume-id`, `--resume-from`, and `--resume-at` for seekable files while always printing `--resume-dir`. |
| 7 | §3.4: Unseekable Input Spooling (`.in_spool`) & Output Alignment | Guarantees zero re-reading of upstream pipes and zero re-writing of downstream stdout streams across crash and timeout resumption. |
| 8 | §3.5: Extreme Microcontroller Scaling down to ~20 KiB RAM | Incorporates 4 KiB block buffers, in-place logarithmic doubling match copies, 1.5 KiB paged sub-tables for pathological legacy LZMA, and capability-gated low-watermark GC (`_gc_mem_free`). |
| 9 | §2.3, §3.5: Shadow-Proof ROM-First CRC32 Probing & Heap Cushion | Probes `ubinascii` before `binascii` to resist `mip` filesystem shadowing; implements nested `try/finally` memory cushion for safe OOM traceback printing. |
