# XZ Decompressor (`xz_decompressor`)
Pure-Python XZ / LZMA2 Streaming Decompressor with Resumption

Target Environments: MicroPython, PyPy 3, CPython 3.8+  
Authoritative Standard: The .xz File Format Specification (v1.2.0 / RFC §2 & §3)  
Revision: 6 (Low-Memory Adaptations, MicroPython / CircuitPython, PSRAM VFS, and CLI Overrides)

---

## 1. System Architecture & Scope Definition

This specification defines the architecture, wire protocols, integrity verification, storage model, fault tolerance, and event-driven lifecycle for a zero-dependency, pure-Python XZ streaming decompression engine.

### 1.1 Wire Format Scope & Boundaries
- **Supported Primary Container**: The .xz File Format Specification (v1.2.0 / RFC §2 & §3), supporting multi-stream concatenation, variable null padding, and arbitrary multi-block structures.
- **Supported Filter**: LZMA2 (`Filter ID 0x21`) with arbitrary dictionary sizes up to 64 MiB (dynamically bounded by filter properties).
- **Unsupported Legacy Container (`.lzma`)**: The legacy 13-byte raw LZMA header format (ALZ / LZMA1 SDK) is **not** supported by this engine. Filename convenience mappings accept `.lzma` input paths solely for CLI compatibility, but the input byte stream must conform to the XZ container format. Non-XZ streams must raise `E_FORMAT` immediately upon encountering invalid stream magic.
- **Unsupported Secondary Filters**: Branch/Call/Jump (BCJ) executable filters (`Filter IDs 0x04`–`0x0B`) and the Delta filter (`Filter ID 0x03`) are not included in the default zero-dependency engine. Blocks specifying filter counts greater than 1 or non-LZMA2 filter IDs must be cleanly rejected with `E_UNSUPPORTED` rather than silently decoding corrupted output.

### 1.2 Architectural Diagram

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        CORE PURE-PYTHON XZ / LZMA2 ENGINE                              │
│  • Pure computational range decoding, LZMA2 chunk parsing, probability arithmetic      │
│  • Normative Block Header parsing (Flags, VLI sizes, LZMA2 dictionary property)        │
│  • Mandatory Block Check computation (CRC32, CRC64, SHA-256) on uncompressed data      │
│  • Complete Index & Stream Footer verification (Backward Size, CRC32, Flag symmetry)   │
│  • Decodes directly into internal active block buffer (4 KiB to 64 KiB bytearray)      │
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
│ • DirectoryBlockHistoryStore         │          │ • Single-file FileHistory / MemHist  │
│ • Splits output_0.bin, output_1.bin  │          │ • Streams TAR chunks to unbundler    │
│ • Manages .part staging & --in-place │          │ • Transitions between streams 0/1    │
│ • Verify-Output Resumption Engine    │          │ • Services watchdog heartbeat        │
│ • Dual-Layer Watchdog:               │          │ • Zero disk hops                     │
│   - --timeout (SIGALRM + coop)       │          │                                      │
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
     - *Run-Length Encoding (`distance == 1`)*: Expanded via `cur_block[pos : pos + length] = bytes([cur_block[pos - 1]]) * length`.
     - *Non-Overlapping (`distance >= length`)*: Direct slice assignment `cur_block[pos : pos + length] = cur_block[pos - dist : pos - dist + length]`.
     - *Overlapping (`distance < length`)*: Fast unrolled copy or repeated slice multiplication within `cur_block`.
   - Over 95% of LZMA matches in typical streams are local (`distance <= pos`), eliminating backend lookup calls during active block execution.
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

### 2.2 Normative Block Header Parsing & Validation
Every block within an XZ stream must begin with a valid Block Header conforming to XZ spec §3.1:
1. **Header Size**: Extracted from the first byte as `(first_byte + 1) * 4` bytes.
2. **Header CRC32**: The last 4 bytes of the header are a 32-bit little-endian CRC32 calculated over all preceding bytes of the Block Header. Decoders must verify this CRC32 prior to parsing inner fields, raising `E_CHECK` on failure.
3. **Block Flags (Byte 1)**:
   - **Filter Count (Bits 0–1)**: `count = (flags & 0x03) + 1`. This engine supports single-filter streams (`count == 1`). Streams declaring multiple filters (`count > 1`) must raise `E_UNSUPPORTED`.
   - **Reserved Bits (Bits 2–5)**: Must be `0`. Any non-zero bits must raise `E_FORMAT`.
   - **Compressed Size Present (Bit 6)**: If set, a Variable Length Integer (VLI) is present indicating the total compressed size of the block. If present, the engine must verify that total compressed bytes consumed match this value.
   - **Uncompressed Size Present (Bit 7)**: If set, a VLI is present indicating the expected decompressed size of the block. If present, the engine must verify that total uncompressed bytes produced match this value.
4. **Filter Flags**:
   - **Filter ID**: Encoded as a VLI. Must equal `0x21` (LZMA2). Any other filter ID must raise `E_UNSUPPORTED`.
   - **Size of Properties**: Encoded as a VLI. Must equal `1` for LZMA2.
   - **Filter Properties (1 byte)**: Encodes the LZMA2 dictionary size `D` (`bits 0-5`):
     ```python
     bits = prop_byte & 0x3F
     if bits == 40:
         dict_size = 0xFFFFFFFF
     elif bits < 40:
         dict_size = (2 | (bits & 1)) << (bits // 2 + 11)
     else:
         raise ValueError('Invalid LZMA2 dictionary property')
     ```
     The engine must configure `HistoryBackend.max_history` to allocate at least `min(dict_size, 67108864)` bytes.
5. **Header Padding**: Null bytes (0–3 bytes) aligning the header fields to a 4-byte boundary before the CRC32.

### 2.3 Mandatory Block Check Verification
1. **Check Type Identification**: Configured by Bits 0–3 of the Stream Header Flags:
   - `0x00`: None (0 bytes).
   - `0x01`: CRC32 (4 bytes).
   - `0x04`: CRC64 (8 bytes).
   - `0x0A`: SHA-256 (32 bytes).
   - Any other value is reserved and must raise `E_UNSUPPORTED`.
2. **Payload Check Computation**: During block execution, the engine must compute a continuous running check over all **uncompressed output bytes** emitted by the block.
3. **Check Comparison**: Following block stream termination and 4-byte compressed padding alignment, the engine reads `check_size` bytes from the input and asserts equality against the computed check. A mismatch must raise `E_CHECK`.

### 2.4 LZMA2 Chunk Execution & Reset Semantics
LZMA2 packages data into discrete chunks prefixed by a 1-byte control byte (`ctrl`):
1. **End-of-Payload (`ctrl == 0x00`)**: Signals normal termination of the LZMA2 stream for the current block.
2. **Uncompressed Chunks (`ctrl in (0x01, 0x02)`)**:
   - `ctrl == 0x01`: **Uncompressed with Dictionary Reset**. The engine must invoke `history.reset()` and reset repeat match distances (`reps = [0, 0, 0, 0]`).
   - `ctrl == 0x02`: **Uncompressed without Dictionary Reset**. History and repeat match distances are retained.
3. **LZMA Compressed Chunks (`ctrl >= 0x80`)**:
   - High 5 bits of uncompressed size: `(ctrl & 0x1F) << 16`.
   - Mode bits: `mode = (ctrl >> 5) & 3`.
     - `mode == 0` (`00b`): Keep state, keep properties, keep dictionary.
     - `mode == 1` (`01b`): **Reset State**. Set `state = 0` and reinitialize all probability models (`p_is_match`, `p_is_rep`, `p_pos_slot`, `p_align`, `p_len`, `p_rep_len`) to `PROB_INIT = 1024`.
     - `mode == 2` (`10b`): **Reset State & Properties**. Perform Mode 1 actions, consume 1 property byte (`prop_byte`), recompute `pb`, `lp`, `lc`, and reallocate/reinitialize literal probability array `p_lit`.
     - `mode == 3` (`11b`): **Reset State, Properties & Dictionary**. Perform Mode 2 actions, invoke `history.reset()`, and reset repeat distances (`reps = [0, 0, 0, 0]`).
4. **Inter-Block Isolation**:
   - In multi-block streams, each Block represents an independent filter execution. LZMA state, probabilities, and sliding history must not bleed across block boundaries. The first chunk of every block must perform a dictionary and state reset (`ctrl == 0x01` or `mode == 3`).

---

## 3. Pluggable Block-Level Storage Protocol (`HistoryBackend`)

The decompressor is decoupled from physical storage through an abstract, block-level protocol:

### 3.1 Abstract Protocol Definition
```python
class HistoryBackend:
    def append_block(self, block_data: bytes, block_index: int = 0, input_hash: str = '') -> None:
        # Commit completed uncompressed block to history.
        raise NotImplementedError

    def get_history_slice(self, distance: int, length: int) -> bytes:
        # Retrieve contiguous byte slice from past history.
        raise NotImplementedError

    def checkpoint(self, block_index: int, state_record: dict, input_hash: str = '') -> None:
        # Persist decompression metadata bookmark at block boundary.
        pass

    def restore(self, block_index: int) -> dict:
        # Load decompression metadata for block_index.
        return None

    def evict_prior(self, min_retained_block: int) -> None:
        # Purge blocks older than min_retained_block from circular buffer.
        pass

    def reset(self) -> None:
        # Reset history state between streams or upon LZMA2 dictionary reset.
        pass

    def cleanup(self) -> None:
        # Release backend resources upon successful completion.
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
- **State Bookmarks**: Companion `.state` files store JSON metadata (`block_idx`, `input_hash`, `output_offset`, `uncompressed_len`) acting as audit checkpoints.

#### B. `FileHistory` (Single-File Scratch CAS)
- Backed by a single scratch file (e.g. `.history.tmp`) with an internal LRU page cache.
- Avoids FAT directory exhaustion and flash write-wear on embedded MCUs while supporting full 64 MiB dictionaries.

#### C. `MemoryHistory` (In-Memory Ring Buffer)
- Backed by an in-memory `bytearray` ring buffer.
- Ideal for small dictionary streams, test harnesses, or memory-rich host environments.

### 3.3 Resumption Architecture: Verify-Output Mode
- **Rationale**: Saving mid-stream range-decoder arithmetic state (bit-level fractions, code/range scalars, and ~16 KiB probability models) into disk files introduces extreme I/O overhead and platform divergence. Instead, the engine implements robust, crash-proof **Verify-Output Mode**:
  1. When `--resume-dir` is specified and a partially written output file exists, the decompressor streams from the beginning of the stream.
  2. Each decoded 64 KiB block is compared against the corresponding byte slice in the existing output file.
  3. Disk write I/O is skipped while bytes match.
  4. Upon the first byte mismatch or when the end of the existing file is reached, the output file is truncated to the verified boundary, and the decompressor switches to standard append mode.
- **Stream-Boundary Resumption**:
  - In concatenated multi-stream archives, `--resume-from` is valid when aligned to an exact Stream Header boundary (`\xfd7zXZ\x00`).

---

### 3.4 Low-Memory, MicroPython & CircuitPython Adaptation Matrix
1. **Dynamic History Backend Selection (72 MB Threshold)**:
   - When python-available memory is less than 72 MiB (`< 75,497,472` bytes), the engine automatically switches from in-memory ring buffering (`MemoryHistory`) to disk/scratch-backed stores (`DirectoryBlockHistoryStore` or `FileHistory`).
   - Callers can override auto-selection via `--history-backend=[auto|memory|directory|file]`.
2. **PSRAM VFS & External Storage Prioritization**:
   - To reduce onboard flash wear and optimize I/O performance on MicroPython/CircuitPython, the engine probes candidate mount points in prioritized order:
     1. **PSRAM / RAM-Disk VFS**: `/psram`, `/ramdisk`, `/vfs_ram`, `/tmp` (zero flash wear, maximum throughput).
     2. **External Flash**: `/sd`, `/sdcard`, `/emmc`, `/external` (preferred over onboard SPI flash).
     3. **Local Scratch Directory**: fallback directory.
   - Callers can explicitly force a directory path via `--storage-dir=<path>`.
3. **Small Working Block Buffer (256 KB RAM Threshold)**:
   - On embedded MCUs with less than 256 KiB of available Python RAM (e.g. RP2040, STM32), the active working block buffer defaults to **8 KiB** rather than 64 KiB, reducing heap footprints.
   - Callers can override block size via `--block-size=[<N>|<N>k|<N>m]`.
4. **Inter-Block Garbage Collection**:
   - To prevent memory fragmentation on constrained heaps, `gc.collect()` is invoked between committed blocks by default.
   - Callers can disable this via `--no-gc`.
5. **MicroPython Machine Code Decorators**:
   - Core computational hotspots (`crc32`, `crc64`, `decode_bit`, `decode_bittree`, `decode_reverse_bittree`, `decode_direct_bits`, `decode_len_val`) use `@micropython.native` machine code decorators. A shim fallback is provided to guarantee 100% transparent execution on CPython and PyPy.
6. **CircuitPython Compatibility**:
   - `argparse` and `unittest` are lazy-imported at the point of use with `ImportError` handling. If `unittest` cannot be imported but `argparse` can, `--test` is disabled cleanly with diagnostic notification.

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
- **Cooperative Clock-Checking ('Slightly Early')**:
  - Enforced on all platforms inside the CLI event loop on every decompressor event.
  - Uses a safety margin of ~150–200 ms:
    `wall_elapsed >= timeout_sec - 0.15`
    `cpu_elapsed >= cpu_timeout_sec - 0.15`
  - Exiting slightly early guarantees that the process commits active state, preserves `.part` files, emits the resumption message to `sys.stderr`, and exits before external supervisors issue hard termination.

### 4.3 Interruption Output & Exit Code
Upon wall-clock or CPU timeout:
1. Emits standard resumption message to `sys.stderr`:
   ```text
   \nDecompression timed out (wall-clock deadline reached / CPU time limit reached).
   To resume, run with: --resume-dir=<dir> [--resume-from=<in_offset>] [--resume-at=<out_offset>]
   ```
2. Exits with standard status code **124** (`E_DEADLINE`).

---

## 5. Multi-Stream XZ Handling, Index & Stream Footer Semantics

### 5.1 Multi-Stream Concatenation & RFC §2.1.2 Padding
- Concatenated XZ streams are permitted to have 0 to any multiple of 4 bytes null padding (`0x00`) per RFC §2.1.2. Any non-null byte or null padding sequence whose length is not a multiple of 4 bytes must raise `E_FORMAT`.
- Stream 0 writes to `<base>`, stream `K >= 1` writes to `<base>_<K><ext>`.
- Boundary telemetry emitted to `sys.stderr`:
  - Normal files: `[xz:stream boundary] input_offset=...B stream_in=...B stream_out=...B output_offset=...B starting stream <K> -> <target>`
  - Non-files: `[xz:stream boundary] input_offset=...B stream_in=...B stream_out=...B output_offset=...B starting stream <K> -> <target>+<offset>B`

### 5.2 Normative Index Field Verification
Following all Blocks in a stream, an Index field terminates the payload:
1. **Index Indicator**: Leading byte `0x00`.
2. **Number of Records**: Encoded as a VLI. Must equal the exact number of Blocks decoded in this stream.
3. **Record Validation**: For each record, the VLI pair (`Unpadded Size`, `Uncompressed Size`) must be verified:
   - `Unpadded Size == Block Header Size + Compressed Size + Check Size`.
   - `Uncompressed Size == Total Uncompressed Bytes emitted by Block`.
4. **Index Padding**: 0 to 3 null bytes aligning the Index to a 4-byte boundary.
5. **Index CRC32**: 4-byte little-endian CRC32 calculated over all Index bytes from the `0x00` indicator through the padding. Decoders must verify this CRC32, raising `E_CHECK` on failure.

### 5.3 Normative Stream Footer Verification
Every XZ stream concludes with a 12-byte Stream Footer:
1. **Footer CRC32 (Bytes 0–3)**: Little-endian 32-bit CRC32 calculated over Backward Size (4 bytes) and Stream Flags (2 bytes). Must be computed and verified.
2. **Backward Size (Bytes 4–7)**: 32-bit unsigned integer encoding the size of the Index:
   `real_index_size = (backward_size + 1) * 4`
   Decoders must verify that `real_index_size` equals the actual number of bytes in the Index field.
3. **Stream Flags (Bytes 8–9)**: Must match the Stream Header Flags identically. Any mismatch indicates stream corruption and must raise `E_FORMAT`.
4. **Footer Magic (Bytes 10–11)**: Must equal `0x59, 0x5A` (`b"YZ"`).

---

## 6. Staging, Filename Resolution & In-Place Directives

- **Auto-Naming**: Case-insensitive `<base>.xz` or `<base>.lzma` -> `<base>`, `-` -> `stdin.unxz`, fallback `<name>.unxz`.
- **Atomic Staging**: Files are decoded to `<target>.part` and atomically renamed to `<target>` upon successful stream completion. Any stale `.part` file from a prior non-resuming run is unlinked prior to output initialization.
- **Direct `--in-place` Mode**: Writes directly to destination targets without `.part` intermediaries.
- **Resumption Invariant**: Stream 0 is always invoked with the base filename.

---

## 7. Command-Line Interface (CLI)

```text
usage: xz_decompressor.py [-h] [-o OUTPUT] [--resume-dir RESUME_DIR]
                          [--resume-from RESUME_FROM] [--resume-at RESUME_AT]
                          [--in-place] [--timeout TIMEOUT]
                          [--cpu-timeout CPU_TIMEOUT] [--deadline DEADLINE]
                          [--history-backend {auto,memory,directory,file}]
                          [--block-size BLOCK_SIZE]
                          [--storage-dir STORAGE_DIR]
                          [--memory-limit MEMORY_LIMIT]
                          [--no-gc]
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

The built-in self-test suite (`--test`) must provide 100% self-contained coverage without external test files:
1. **Flexible ISO-8601 Parser**: Validating bare dates, compact notation, optional punctuation, and subsecond fractions against known epochs.
2. **Duration Parser**: Validating `<N>s`, `<N>m`, `0`, and fractional inputs.
3. **Cooperative Timeout Safety**: Asserting early termination (~150 ms margin) and clean status 124 exit.
4. **Multi-Stream & RFC Padding**: Testing concatenation with 0, 4, and 8 null bytes, asserting strict multiple-of-4 alignment.
5. **Block Header & LZMA2 Property Extraction**: Parsing variable dictionary properties and asserting proper buffer sizing.
6. **Block Check Verification**: Verifying CRC32, CRC64, and SHA-256 integrity checks, and confirming fatal errors upon synthetic byte tampering.
7. **Index & Stream Footer Verification**: Asserting Backward Size, Index CRC32, and Footer CRC32 cross-checks.
8. **LZMA2 Chunk Reset Semantics**: Verifying dictionary resets on `mode == 3` and `ctrl == 0x01`.
9. **DirectoryBlockHistoryStore & Verify-Output Recovery**: Simulating interrupted streams, corrupted partial outputs, circular buffer eviction, and automatic boundary repair.

---

## 9. Standard Error Codes & Exit Status

| Exit Code | Identifier | Description |
|:---|:---|:---|
| `0` | `SUCCESS` | Decompression completed successfully; all checksums verified. |
| `1` | `E_FORMAT` | Invalid stream magic, corrupt header, illegal VLI, or malformed padding. |
| `1` | `E_UNSUPPORTED` | Unsupported filter count (> 1), non-LZMA2 filter ID, or reserved flags. |
| `1` | `E_CHECK` | Header CRC32, Block Check (CRC32/CRC64/SHA-256), Index CRC32, or Footer CRC32 mismatch. |
| `1` | `E_RESOURCE` | Out of memory, insufficient disk space, or history buffer allocation error. |
| `1` | `E_IO` | File read/write failure, broken pipe, or directory permission error. |
| `124` | `E_DEADLINE` | Wall-clock timeout, absolute UTC deadline instant, or CPU timeout reached. |
| `130` | `SIGINT` | Interrupted by user keyboard interrupt (`Ctrl+C`). |
