# XZ Decompressor (`xz_decompressor`)
Pure-Python XZ / LZMA2 Streaming Decompressor with Resumption

Target Environments: MicroPython, PyPy 3, CPython 3.8+  
Authoritative Standard: The .xz File Format Specification (v1.2.0 / RFC §2 & §3)  
Revision: 7 (Match-execution invariants, page-based history store, stream-event ordering, checkpoint-based resumption, strict LZMA2 validation, regression-test requirements; see Appendix A)

---

## 1. System Architecture & Scope Definition

This specification defines the architecture, wire protocols, integrity verification, storage model, fault tolerance, and event-driven lifecycle for a zero-dependency, pure-Python XZ streaming decompression engine.

### 1.1 Wire Format Scope & Boundaries
- **Supported Primary Container**: The .xz File Format Specification (v1.2.0 / RFC §2 & §3), supporting multi-stream concatenation, variable null padding, and arbitrary multi-block structures. Each Stream is parsed independently (its Check type may differ from other Streams in the same file).
- **Supported Filter**: LZMA2 (`Filter ID 0x21`) with arbitrary dictionary sizes up to 64 MiB of *usable* history (see §2.2 item 4 for larger declared sizes).
- **Supported Check IDs**: `0x00` None, `0x01` CRC32, `0x04` CRC64, `0x0A` SHA-256. All other IDs raise `E_UNSUPPORTED`.
- **Unsupported Legacy Container (`.lzma`)**: The legacy 13-byte raw LZMA header format (LZMA_Alone) is **not** supported. Filename convenience mappings accept `.lzma` input paths solely for CLI compatibility, but the input must be a valid XZ container. Non-XZ input raises `E_FORMAT` upon invalid Stream magic.
- **Unsupported Secondary Filters**: BCJ (`0x04`–`0x0B`) and Delta (`0x03`) are not included. Blocks with filter count > 1 or a non-LZMA2 filter ID are rejected with `E_UNSUPPORTED` before any output is produced for that Block.
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
│  • Interacts with HistoryBackend via block-level slices (get_history_slice / append)   │
│  • Emits cooperative lifecycle events via block-level generator yielding               │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                     Yields Events:         ▼
            ┌───────────────────────────────┼───────────────────────────────┐
            │ [CHUNK_OUTPUT] (Block slices) │ [STREAM_BOUNDARY]             │
            │ [BLOCK_COMMITTED] (state)     │ [PROGRESS] (Block interval)   │
            └───────────────────────────────┴───────────────────────────────┘
                                            │
                   ┌────────────────────────┴────────────────────────┐
                   ▼                                                 ▼
┌──────────────────────────────────────┐          ┌──────────────────────────────────────┐
│        STANDALONE CLI DRIVER         │          │       EMBEDDED / CALLER DRIVER       │
│        (Target: CLI Utility)         │          │     (In-Memory / Streaming Tar)      │
├──────────────────────────────────────┤          ├──────────────────────────────────────┤
│ • DirectoryBlockHistoryStore         │          │ • Single-file FileHistory / MemHist  │
│ • Splits output, output_1, output_2  │          │ • Streams TAR chunks to unbundler    │
│ • Manages .part staging & --in-place │          │ • Transitions between streams 0/1    │
│ • Checkpoint/Verify Resume Engine    │          │ • Services watchdog heartbeat        │
│ • Dual-Layer Watchdog:               │          │ • Zero disk hops                     │
│   - --timeout (SIGALRM + coop)       │          │                                      │
│   - --cpu-timeout (SIGVTALRM + coop) │          │                                      │
│   - --deadline (ISO-8601 UTC)        │          │                                      │
│ • Prints stderr boundary telemetry   │          │                                      │
│ • Rotating ASCII indicator (TTY only)│          │                                      │
└──────────────────────────────────────┘          └──────────────────────────────────────┘
```

---

## 2. Block-Level Decoding Execution Invariants

### 2.1 The Active Working Block Buffer (`cur_block`)
1. **Buffer Allocation**: The decompressor allocates `cur_block = bytearray(block_size)`. `block_size` MUST be a power of two in `[4 KiB, 64 KiB]` (default 64 KiB). Any other value is rejected (`E_FORMAT`, exit 1) at configuration time.
2. **Distance Convention**: `d` denotes the *zero-based* distance as held in `reps[]` and produced by the distance decoder (actual offset = `d + 1`). With `pos` = current index within `cur_block`:
   - A match is **local** iff `d < pos`; its source index is `pos - 1 - d`.
   - Otherwise it is **inter-block**, needing `h = (d + 1) - pos` bytes (`h >= 1`) from committed history.
3. **Zero-Call Intra-Block Decoding**:
   - **Literals**: assigned directly into `cur_block[pos]`.
   - **Local Matches**: `d == 0` (run-length) is expanded as `bytes([cur_block[pos-1]]) * n`; `d + 1 >= n` uses a direct slice copy; `d + 1 < n` (overlap) uses a byte-wise or doubling copy. Over 95% of matches in typical streams are local.
4. **Inter-Block Matches**: The engine requests `m = min(n, h)` bytes via `history.get_history_slice(h, m)`. If `n > h`, the remaining `n - h` bytes are copied from `cur_block[0 : n - h]` (the match has re-entered the current block; the source index for output byte `j >= h` is `j - h`).
5. **Variable Quantum & Network Stream Support**: If the input is a socket, pipe or small payload, the decompressor does not stall waiting for a full block. Slices as small as 1 byte are flushed when an explicit chunk boundary, end-of-stream or caller flush is reached.
6. **Block Commit**: When `pos == block_size` (or at end of an XZ Block / stream), the block is (a) added to the running Check, (b) appended to history, (c) counted in offsets, (d) yielded as events, in that order.

### 2.2 Normative Block Header Parsing & Validation
Every Block begins with a Block Header (XZ spec §3.1). A first byte of `0x00` is not a Block Header; it is the Index Indicator and begins §5.2.
1. **Header Size**: `(first_byte + 1) * 4` bytes.
2. **Header CRC32**: The last 4 bytes (little-endian) are the CRC32 of all preceding header bytes. It MUST be verified before parsing inner fields (`E_CHECK` on mismatch).
3. **Block Flags (Byte 1)**:
   - **Filter Count (bits 0–1)**: `count = (flags & 0x03) + 1`; `count > 1` → `E_UNSUPPORTED`.
   - **Reserved (bits 2–5)**: MUST be 0 (`E_FORMAT`).
   - **Compressed Size Present (bit 6)** / **Uncompressed Size Present (bit 7)**: if set, a VLI follows (in that order). The engine MUST verify the actual Compressed Size (excluding Block Padding and Check) and Uncompressed Size against them (`E_FORMAT` on mismatch).
4. **Filter Flags**: Filter ID (VLI) MUST be `0x21` (`E_UNSUPPORTED` otherwise). Size of Properties (VLI) MUST be `1` (`E_FORMAT`). The property byte `p` encodes the dictionary size and is **not masked**:
   ```python
   if p > 40:            raise XZFormatError   # E_FORMAT (reserved)
   elif p == 40:         dict_size = 0xFFFFFFFF
   else:                 dict_size = (2 | (p & 1)) << (p // 2 + 11)
   ```
   The history backend MUST provide at least `min(dict_size, 67108864)` bytes. If a stream declares a larger dictionary, decoding proceeds, but a match whose distance exceeds the backend capacity raises `E_RESOURCE`.
5. **Header Padding**: 0–3 null bytes before the CRC32. Non-zero padding → `E_FORMAT`. Header Size MUST be exactly consumed (no unparsed bytes other than padding).
6. **VLI rules** (apply everywhere VLIs appear): at most 9 bytes, value < 2^63, **minimal encoding** (a final byte of `0x00` after a continuation is non-minimal) → `E_FORMAT` otherwise.

### 2.3 Mandatory Block Check Verification
1. **Check Type**: Bits 0–3 of Stream Flags byte 2 (see §5.0). Sizes: `0x00`→0, `0x01`→4, `0x04`→8, `0x0A`→32 bytes.
2. **Computation**: a running check over all uncompressed bytes of the Block.
3. **Block Padding & Comparison**: After the LZMA2 end marker, 0–3 null bytes pad the (Header + Compressed Data) to a multiple of 4; they MUST be zero (`E_FORMAT`). Then `check_size` bytes are read and compared with the computed value (`E_CHECK` on mismatch).
4. **Index Accounting**: `Unpadded Size = Header Size + Compressed Size + Check Size` (excludes Block Padding).

### 2.4 LZMA2 Chunk Format, Reset Semantics & Validity Rules
Each Block's compressed data is a sequence of chunks, each starting with a control byte `ctrl`.

**Chunk headers**
- `ctrl == 0x00`: end of LZMA2 data for the Block.
- `ctrl == 0x01 / 0x02`: uncompressed chunk; followed by a big-endian 16-bit `size-1`, then `size` raw bytes (1–65536). `0x01` also resets the dictionary.
- `ctrl >= 0x80`: LZMA chunk. `u_size = ((ctrl & 0x1F) << 16) + BE16 + 1` (1 – 2 MiB), then `c_size = BE16 + 1` (1 – 64 KiB), then (if `mode >= 2`) one property byte.
- `ctrl` in `0x03..0x7F` → `E_FORMAT`.

**Reset modes** (`mode = (ctrl >> 5) & 3` for LZMA chunks)
- `0`: keep state, properties, dictionary.
- `1`: reset state (`state = 0`, all probability models to `1024`).
- `2`: reset state and read new properties.
- `3`: reset state, read new properties, **reset dictionary** (`history.reset()`; `reps = [0,0,0,0]`).

**Property byte** `v = (pb * 5 + lp) * 9 + lc`: MUST satisfy `v <= 224` and, for LZMA2, `lc + lp <= 4`; otherwise `E_FORMAT`. The literal probability array is reallocated to `0x300 << (lc + lp)` entries.

**Validity state machine** (reset at each Block start: `need_dict_reset = True`, `need_props = True`)
1. If `ctrl == 0x01` or `ctrl >= 0xE0`: dictionary reset, `need_props = True`, `need_dict_reset = False`. Else if `need_dict_reset`: `E_FORMAT` (the first chunk of every Block must reset the dictionary).
2. For `ctrl >= 0xC0`: properties are read; `need_props = False`. For `0x80 <= ctrl < 0xC0`: if `need_props`, `E_FORMAT`.
3. Uncompressed chunks append data to history and leave LZMA state, probabilities and `reps` untouched (conforming encoders follow them with a state reset).
4. **Chunk exactness**: an LZMA chunk MUST produce exactly `u_size` bytes and consume exactly `c_size` compressed bytes, and the range decoder MUST finish with `code == 0`. A match that would cross the end of the chunk's `u_size` → `E_FORMAT`. Violations of any of these → `E_FORMAT`.
5. **Range decoder init**: each LZMA chunk begins with 5 range-coder bytes; the first MUST be `0x00` (`E_FORMAT`).
6. **Inter-Block Isolation**: LZMA state, probabilities, `reps` and history MUST NOT bleed across Blocks (guaranteed by rule 1).

### 2.5 Match Execution Invariants (normative)
These invariants exist because the reference WIP once failed on them; each MUST have a regression test (§8).

1. **Distance binding**: *Every* path that begins a copy MUST bind `d` from decoded or `reps[]` values *before* the copy, with no reliance on a value left over from an earlier symbol:
   | Symbol | `d` | `reps` update |
   |:---|:---|:---|
   | Match | decoded distance | `reps = [d, r0, r1, r2]` |
   | Short rep (rep0, len 1) | `reps[0]` | none |
   | **Long rep0** | **`reps[0]`** | none |
   | Rep1 / Rep2 / Rep3 | `reps[i]` | move-to-front |
   A decoded `d == 0xFFFFFFFF` (end-of-payload marker) is **not permitted** in LZMA2 → `E_FORMAT`.
2. **State transitions**: literal `[0,0,0,0,1,2,3,4,5,6,4,5][state]`; match `7 if state<7 else 10`; rep `8 if state<7 else 11`; short rep `9 if state<7 else 11`. A literal decoded while `state >= 7` uses the matched-literal coder with the byte at `reps[0]`.
3. **Distance validity**: `d` MUST be `< min(dict_size, bytes_in_history_since_last_dictionary_reset)`. A reference to bytes that do not exist → `E_FORMAT` (never substitute zeros).
4. **Re-entrancy**: Because decoding returns to the caller at every block commit, all symbol-level state MUST live on the engine object, not in locals: `state`, `reps`, `pos`, `chunk_decoded`, range-coder `code/range`, compressed-chunk offset, and an in-progress match (`in_match_copy`, `match_d`, `match_rem`). The first operation after re-entry (including after import of a checkpoint) may be any symbol type, including long rep0.

---

## 3. Pluggable Block-Level Storage Protocol (`HistoryBackend`)

### 3.1 Abstract Protocol Definition
```python
class HistoryBackend:
    persistent = False   # True if the window survives process exit (needed for checkpoint resume)

    def append_block(self, block_data: bytes, block_index: int = 0, input_hash: str = '') -> None:
        # Commit completed uncompressed block to history.
        raise NotImplementedError

    def get_history_slice(self, distance: int, length: int) -> bytes:
        # distance is 1-based from the end of committed history (1 = last committed byte).
        # Returns history[len-distance : len-distance+length]. Requires 1 <= length <= distance <= history_len;
        # distance > history_len raises XZFormatError. Callers split overlapping copies (see §2.1.4).
        raise NotImplementedError

    def history_len(self) -> int: ...          # bytes available since last reset (capped at capacity)
    def tail_crc32(self, n: int = 65536) -> int: ...   # CRC32 of the last min(n, history_len) bytes (resume validation)
    def checkpoint(self, block_index: int, state_record: dict, input_hash: str = '') -> None: pass
    def restore(self, block_index: int) -> dict: return None
    def truncate_to(self, history_total: int) -> None: ...  # discard anything committed after a checkpoint
    def evict_prior(self, min_retained_block: int) -> None: pass
    def reset(self) -> None: pass              # between streams or on LZMA2 dictionary reset
    def cleanup(self) -> None: pass            # release resources upon successful completion
```

### 3.2 Concrete Backend Implementations

#### A. `DirectoryBlockHistoryStore` (Default Standalone CLI Backend)
- **Page-based topology (independent of `block_size`)**: History is stored in fixed **64 KiB pages** (`HISTORY_PAGE = 65536`), `<hash>_<page:08d>.page`. The engine's blocks (4–64 KiB, a power of two, so always a divisor of the page size) are **appended in place** to the open page file; no RAM accumulation of a full page is required. A page is sealed when it reaches 64 KiB. The store therefore holds at most `ceil(max_history / 65536) + 1` files (1025 for 64 MiB), regardless of `block_size`.
- **FIFO Eviction**: when page `k >= 1025` is opened, page `k - 1025` is deleted.
- **Atomicity**: sealed pages are immutable. The open page's valid length is recorded in each checkpoint (§3.3); bytes beyond it are discarded on restore (`truncate_to`). Directory metadata writes use `.tmp` + `os.replace`.
- **LRU Block Cache**: 1–2 recent pages cached in RAM to avoid seeks for nearby inter-block lookbacks.
- **Persistence**: `persistent = True`.

#### B. `FileHistory` (Single-File Scratch)
- A single scratch file (e.g. `.history.tmp`) with a small header (version, `max_history`, logical length) and an internal LRU page cache. Avoids FAT directory exhaustion and flash wear on embedded MCUs while supporting full 64 MiB dictionaries. `persistent = True`. Works with any `block_size`.

#### C. `MemoryHistory` (In-Memory Ring Buffer)
- A `bytearray` ring buffer sized to `min(dict_size, 64 MiB)` plus one block. `persistent = False`; checkpoint resume is unavailable (§3.3.1 falls back to Verify-Output).

### 3.3 Resumption Architecture

#### 3.3.1 Modes (`--resume-mode={auto,checkpoint,verify,off}`, default `auto` when `--resume-dir` is given)
- **`checkpoint`**: restore the full decoder state from the newest valid checkpoint (3.3.4) and continue from there. Requires a persistent history backend.
- **`verify`**: Verify-Output mode (3.3.6), used as a degraded fallback.
- **`auto`**: `checkpoint` if the backend is persistent and a valid checkpoint exists; otherwise `verify`.
- **`off`**: ignore any prior state; start clean.

#### 3.3.2 Checkpoint Record
A checkpoint is taken **only at block-commit points** (`pos == 0`), so no partial block need be saved. The record (JSON; binary fields hex-encoded) contains:
- `version`, `txn` (monotonic transaction counter), `input_id` (see 3.3.5), `payload_crc32`.
- Offsets: `input_offset` (next unread input byte), `output_offset` (overall), `stream_index`, `stream_start_in`, `stream_start_out`, `stream_out_offset`, `block_idx`, `history_total`, `history_tail_crc32`.
- XZ Block context: Block Header fields (`check_type`, sizes, `dict_size`), `block_out_start`, compressed/uncompressed byte counts so far, CRC32/CRC64 accumulators, and the list of completed Block records `(unpadded_size, uncompressed_size)` for later Index verification.
- LZMA2 context: `lzma2_phase`, `need_dict_reset`, `need_props`, `lc/lp/pb`, `chunk_uncomp_sz`, `chunk_decoded`, and **`pending_chunk`** (the unconsumed compressed bytes of the current chunk, ≤ 64 KiB), plus `comp_offset` into it.
- Range decoder / LZMA context: `rc_code`, `rc_range`, `state`, `reps[4]`, all probability arrays (as little-endian uint16 hex), and the in-progress match (`in_match_copy`, `match_d`, `match_rem`).
- **SHA-256**: Python `hashlib` objects cannot be serialized. For SHA-256 Blocks the hash is **recomputed on restore** by reading `[block_out_start, output_offset)` back from the output file. If the output is not readable (stdout), SHA-256 checkpoints are only taken at XZ Block boundaries.

Because `input_offset` points *past* the chunk already buffered in `pending_chunk`, resumption needs only that the input be positioned at `input_offset` (seek if seekable, or piped from that offset).

#### 3.3.3 Write-Ordering & Atomic A/B Journal
1. Flush **and fsync** (where `os.fsync` exists) output bytes up to `output_offset`, and the history pages/file up to `history_total`.
2. Write the record to `.tmp_state.json`, fsync it.
3. `atomic_replace` it into `STATE_A.json` or `STATE_B.json` alternately (higher `txn` wins).

A crash between any two steps leaves the previous checkpoint intact. Each journal file embeds `payload_crc32`; on restore both files are read and the valid one with the highest `txn` is used.

#### 3.3.4 Restore Procedure
1. Select the newest valid journal; verify `version`, `payload_crc32` and `input_id` (3.3.5).
2. Verify the output `.part` file length ≥ `output_offset`; **truncate it to exactly `output_offset`**. (Bytes written after the checkpoint will be regenerated.)
3. `history.truncate_to(history_total)` and verify `history_tail_crc32`.
4. Position the input at `input_offset`, restore all engine fields (including `pending_chunk`), recompute SHA-256 if applicable, and continue decoding.
5. Any failed validation in steps 1–3 degrades to Verify-Output (`auto`) or raises `E_RESOURCE` (`checkpoint`), never to silent partial output.

#### 3.3.5 Triggers & Identity
- **Periodic**: every `--checkpoint-interval` (default `8m` of output; `0` disables) — a size or `<N>s`.
- **Mandatory**: on cooperative timeout, deadline, or `KeyboardInterrupt` *before* exit, and at every Stream boundary (minimal state).
- **Preemptive signals** (`SIGALRM`/`SIGVTALRM`) are only a backstop: they abort without writing a checkpoint; the last periodic one is used. The cooperative margin (§4.2) makes this the exception.
- `input_id` = hex(CRC32 of the first 4096 input bytes) + `:` + input size when seekable. It is checked on restore when the input is seekable; for non-seekable input it is trusted.

#### 3.3.6 Verify-Output Mode (degraded fallback)
1. When `--resume-dir` is given and a partially written output exists, the decompressor decodes from the start of the stream (or from `--resume-from` at a Stream boundary).
2. Each decoded block is compared with the corresponding bytes of the existing output; writes are skipped while they match.
3. At the first mismatch, or at the end of the existing file, the output is truncated to the verified boundary and normal appending resumes.

**Documented limitation**: because decoding restarts at the beginning, each run can only advance as far as one timeout window decodes. A stream whose total decode time exceeds `--timeout`/`--cpu-timeout` **cannot converge** in this mode. Callers needing guaranteed convergence MUST use `checkpoint` mode with a persistent backend. The CLI prints a warning when it enters Verify-Output with a timeout set.

#### 3.3.7 Stream-Boundary Resumption
In concatenated multi-stream archives, `--resume-from` is valid when aligned to an exact Stream Header (`\xfd7zXZ\x00`) boundary. For non-seekable input or stdout output, `--resume-from` / `--resume-at` MUST equal the `input_offset` / `output_offset` of the newest checkpoint (as printed in the timeout message) or the Stream boundary telemetry; a mismatch is `E_FORMAT`.

### 3.4 Low-Memory, MicroPython & CircuitPython Adaptation Matrix
1. **Dynamic History Backend Selection (72 MiB Threshold)**: when available memory is below 72 MiB (`< 75,497,472` bytes, i.e. a 64 MiB window plus 8 MiB headroom), `auto` selects `DirectoryBlockHistoryStore` or `FileHistory` instead of `MemoryHistory`. Override with `--history-backend=[auto|memory|directory|file]`. `MemoryHistory` is always sized to the declared dictionary, not the maximum.
2. **PSRAM VFS & External Storage Prioritization**: candidate mount points are probed in order: (1) `/psram`, `/ramdisk`, `/vfs_ram`, `/tmp`; (2) `/sd`, `/sdcard`, `/emmc`, `/external`; (3) a local scratch directory. Override with `--storage-dir=<path>`. Note that RAM-backed mounts are not persistent across power loss: `checkpoint` mode is only crash-safe if the checkpoint and history reside on persistent media (the CLI warns if the chosen directory is RAM-backed).
3. **Small Working Block Buffer (256 KiB RAM threshold)**: below 256 KiB of available RAM the block size defaults to **8 KiB**. Override with `--block-size`. All history backends MUST work correctly for every legal block size (§2.1.1).
4. **Inter-Block Garbage Collection**: `gc.collect()` between committed blocks by default; disabled with `--no-gc`.
5. **MicroPython Machine Code Decorators**: hotspots (`crc32`, `crc64`, `decode_bit`, `decode_bittree`, `decode_reverse_bittree`, `decode_direct_bits`, `decode_len_val`) use `@micropython.native` with a transparent shim on CPython/PyPy.
6. **CircuitPython Compatibility**: `argparse` and `unittest` are lazy-imported with `ImportError` handling. Without `unittest`, `--test` is disabled with a diagnostic. `pathlib` MUST NOT be required on MicroPython (use `os.path`).
7. **Available-memory probe**: MicroPython: `gc.mem_free()`; Linux CPython: `MemAvailable` from `/proc/meminfo`, else `os.sysconf` page counts; if no probe is possible, assume ample memory. `--memory-limit` overrides the probe.

---

## 4. Timeout, Deadline & Watchdog Specifications

### 4.1 CLI Flags & Parameters
1. `--timeout=[<N>s|<N>m|0]` (default `52s`): wall-clock timeout relative to process startup (monotonic clock). `0` disables. Integer/float with case-insensitive `s`/`m` suffix. `wall_remaining = timeout_sec - (perf_counter() - init_perf)`.
2. `--deadline=<iso-8601>`: absolute UTC instant. Accepted forms (case-insensitive `T`/`Z`; `-`, `:`, `T`, `Z` optional): `YYYYMMDD`, `YYYY-MM-DD` (→ `T00:00:00.000Z`); date+hours; date+hours+minutes; date+hours+minutes+seconds; plus subsecond fractions (`YYYYMMDDHHMMSS.fff`, `YYYY-MM-DDTHH:MM:SS.fffZ`). `deadline_remaining = max(0.0, deadline_epoch - now_epoch)`. If both `--timeout` and `--deadline` are given, `effective_timeout = min(timeout_sec, deadline_remaining)` (with `--timeout 0`, the deadline alone applies).
3. `--cpu-timeout=[<N>s|<N>m|0]` (default `22s`): process CPU time, `cpu_remaining = cpu_timeout_sec - (process_time() - init_cpu)`.

### 4.2 Dual-Layer Enforcement Mechanism
- **Preemptive signals**: `SIGALRM` via `setitimer(ITIMER_REAL)`/`alarm` (wall) and `SIGVTALRM` via `setitimer(ITIMER_VIRTUAL)` (CPU), where available. Platforms without them (Windows, MicroPython, non-main threads) skip registration silently.
- **Cooperative checks** (all platforms): evaluated on every decompressor event and at least once per committed block, with a margin of ~150 ms (`elapsed >= timeout - 0.15`). On a cooperative trigger the driver writes the mandatory checkpoint (§3.3.5), preserves `.part` files, prints the resume message, and exits 124 before an external supervisor can kill the process.
- The progress indicator (rotating ASCII glyph) is written to stderr **only when `sys.stderr.isatty()`**, so redirected logs stay clean.

### 4.3 Interruption Output & Exit Code
```text

Decompression timed out (wall-clock deadline reached / CPU time limit reached).
To resume, run with: --resume-dir=<dir> [--resume-from=<in_offset>] [--resume-at=<out_offset>]
```
`--resume-from`/`--resume-at` are printed only when input/output is non-seekable. Exit status **124** (`E_DEADLINE`).

---

## 5. Multi-Stream XZ Handling, Index & Stream Footer Semantics

### 5.0 Stream Header (12 bytes)
1. **Magic** (bytes 0–5): `FD 37 7A 58 5A 00`, else `E_FORMAT`.
2. **Stream Flags** (bytes 6–7): byte 6 MUST be `0x00`; the high nibble of byte 7 MUST be `0`; the low nibble is the Check type (§2.3). Violations → `E_FORMAT` (reserved Check IDs → `E_UNSUPPORTED`).
3. **CRC32** (bytes 8–11): little-endian CRC32 of the 2 Stream Flags bytes (`E_CHECK` on mismatch).

### 5.1 Multi-Stream Concatenation, Padding & Output Routing
- **Stream Padding**: after a Stream Footer, any number of `0x00` bytes whose total count is a multiple of 4 (including at end of file) is permitted (RFC §2.1.2). Padding that is not a multiple of 4, or followed by a byte that is neither EOF nor the start of a Stream Header, → `E_FORMAT`. Truncated input at any point → `E_FORMAT` ("unexpected end of input").
- **Output routing**: Stream 0 writes to `<base>`; Stream `K >= 1` writes to `<base>_<K><ext>` where `<ext>` is `os.path.splitext` of the final path component (`out.bin` → `out_1.bin`; `out` → `out_1`). When writing to stdout all Streams are concatenated.
- **Output creation**: the output file for a Stream is created (opened) **when the Stream starts**, not at its first output byte, so an empty Stream still yields an empty file (matching `xz -d`).
- **Boundary telemetry** (stderr), emitted **only when another Stream follows**:
  - Files: `[xz:stream boundary] input_offset=...B stream_in=...B stream_out=...B output_offset=...B starting stream <K> -> <target>`
  - Non-files: `... starting stream <K> -> <target>+<offset>B`
  After the final Stream: `[xz:stream end] input_offset=...B output_offset=...B streams=<N>`.

### 5.2 Normative Index Field Verification
1. **Index Indicator**: `0x00`.
2. **Number of Records**: VLI; MUST equal the number of Blocks decoded in this Stream.
3. **Records**: VLI pairs `(Unpadded Size, Uncompressed Size)` MUST match the decoded Block (`E_FORMAT`).
4. **Index Padding**: 0–3 null bytes aligning the Index to 4 bytes (non-zero → `E_FORMAT`).
5. **Index CRC32**: little-endian CRC32 over all Index bytes from the indicator through the padding (`E_CHECK`).

### 5.3 Normative Stream Footer Verification
1. **Footer CRC32 (bytes 0–3)**: CRC32 of Backward Size + Stream Flags (6 bytes) (`E_CHECK`).
2. **Backward Size (4–7)**: `real_index_size = (backward_size + 1) * 4` MUST equal the actual Index size (`E_FORMAT`).
3. **Stream Flags (8–9)**: MUST equal the Stream Header flags (`E_FORMAT`).
4. **Magic (10–11)**: `59 5A` (`b"YZ"`).

### 5.4 Event Ordering Invariants (normative)
1. All `CHUNK_OUTPUT` events of Stream *N* precede the `STREAM_BOUNDARY` for Stream *N*, which precedes any `CHUNK_OUTPUT` of Stream *N+1*. Drivers rely on this to route output to the correct file; an engine that emits a boundary late is non-conforming.
2. To know whether another Stream follows, the engine consumes Stream Padding and peeks the next byte *before* emitting the boundary event. The event carries `has_next` (bool); `next_stream_index` is meaningful only when true.
3. Per committed block the order is `CHUNK_OUTPUT`, `BLOCK_COMMITTED` (carrying a JSON-serializable state record), `PROGRESS`.

---

## 6. Staging, Filename Resolution & In-Place Directives

- **Auto-Naming**: case-insensitive `<base>.xz` or `<base>.lzma` → `<base>`; `-` → `stdin.unxz`; fallback `<n>.unxz`.
- **Atomic Staging**: each Stream is decoded to `<target>.part` and atomically renamed to `<target>` when *that Stream* completes (so finished Streams are final while later ones are in progress). A stale `.part` from a prior non-resuming run is unlinked before output initialization.
- **Direct `--in-place` Mode**: writes directly to destination targets without `.part`.
- **Failure behavior**: on `E_DEADLINE`/`SIGINT` the `.part`, the checkpoint journals and history scratch are retained. On `E_FORMAT`/`E_CHECK`/`E_UNSUPPORTED` the `.part` is **retained** (partial data can be valuable for recovery), the checkpoint journals are removed, and the error message reports `verified_output_offset` = end of the last Block whose Check passed. On success all scratch state is cleaned up (`history.cleanup()`) regardless of whether `--resume-dir` was used.
- **Resumption Invariant**: Stream 0 is always invoked with the base filename.

---

## 7. Command-Line Interface (CLI)

```text
usage: xz_decompressor.py [-h] [-o OUTPUT] [--resume-dir RESUME_DIR]
                          [--resume-mode {auto,checkpoint,verify,off}]
                          [--checkpoint-interval INTERVAL]
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
  --resume-dir DIR      Working directory for checkpoints and history scratch
  --resume-mode MODE    auto (default), checkpoint, verify, off
  --checkpoint-interval N  Periodic checkpoint spacing [<N>|<N>k|<N>m|<N>g bytes | <N>s]; 0 disables (default: 8m)
  --resume-from OFFSET  Input offset for non-seekable resumption
  --resume-at OFFSET    Output offset for non-seekable resumption
  --in-place            Write directly to target files without .part staging
  --timeout TIMEOUT     Wall-clock timeout [<N>s|<N>m|0] (default: 52s)
  --cpu-timeout C_TO    CPU timeout [<N>s|<N>m|0] (default: 22s)
  --deadline DEADLINE   Absolute UTC deadline instant (ISO-8601)
  --history-backend BK  Storage: auto (<72MiB switches to disk), memory, directory, file
  --block-size SIZE     auto (8KiB if <256KiB RAM, else 64KiB) or a power-of-two size in [4k, 64k]
  --storage-dir DIR     Directory for history/scratch files (overrides PSRAM/SD detection)
  --memory-limit MEM    Simulate/override python-available RAM limit (e.g. 64m, 128k)
  --no-gc               Disable forced gc.collect() between blocks
  --test                Run the self-contained unit test suite (uses the C `lzma` module where available)
  -v, --verbose         Diagnostic logging and Python tracebacks on stderr
```

Errors are reported as `xz_decompressor: <E_ID>: <message>` (with stream/block/offset context) and **never** as a raw traceback unless `-v` is given.

---

## 8. Verification Suite Specification (`--test`)

The built-in suite MUST be self-contained (no external files). Where the C `lzma` module is available it is used as an oracle; otherwise those cases `skipTest()` and fixed embedded vectors are used. It MUST cover:
1. **Flexible ISO-8601 Parser**: bare dates, compact notation, optional punctuation, subsecond fractions, known epochs.
2. **Duration Parser**: `<N>s`, `<N>m`, `0`, fractions.
3. **Cooperative Timeout**: early termination (~150 ms margin), exit 124, checkpoint written.
4. **Multi-Stream & RFC Padding**: 0, 4, 8 null bytes; non-multiple-of-4 and non-null padding rejected; padding at EOF accepted.
5. **Block Header & LZMA2 Property Extraction**: all valid dictionary properties 0–40; property 41+ rejected; `lc+lp > 4` and `v > 224` rejected; non-minimal VLI rejected.
6. **Block Check Verification**: CRC32, CRC64, SHA-256, None, with tamper detection.
7. **Index & Stream Footer**: Backward Size, Index CRC32, Footer CRC32, flag symmetry.
8. **LZMA2 Chunk Rules**: reset semantics for `mode == 3` and `ctrl == 0x01`; first-chunk-must-reset; no props before props; chunk exactness; match crossing chunk end rejected; distance beyond history rejected; `d == 0xFFFFFFFF` rejected.
9. **Regression: match execution (§2.5)** — all must round-trip against the oracle:
   a. all-zero input of 10 KB, 100 KB and 3 MB (long rep0 immediately after a literal and at the start of a re-entered block);
   b. highly repetitive and mixed random/repetitive inputs, every Check type, presets 0, 6, 9e;
   c. a stream whose first symbol after a block commit is long rep0.
10. **Regression: backends × block sizes**: every backend (`memory`, `directory`, `file`) × block sizes 4k, 8k, 16k, 64k on multi-block data larger than the dictionary window of the chosen preset.
11. **Regression: multi-stream routing**: two or three Streams with padding; assert byte-exact contents of `<base>`, `<base>_1`, `<base>_2`; assert no `starting stream` line after the last Stream; assert an empty Stream yields an empty file; assert the boundary-before-next-chunk ordering of §5.4.
12. **Regression: empty input stream** (`lzma.compress(b'')`): exit 0 and an empty output file.
13. **Regression: non-seekable input**: decoding from a pipe/generator-backed reader that raises on `tell()`/`seek()`, including checkpoint export.
14. **Resumption**: (a) Checkpoint-Resume **converges**: decoding a stream whose total time exceeds the timeout, using repeated runs with a short timeout, MUST finish and match the oracle; (b) crash simulation (kill between each of the three journal steps); (c) corrupted/missing journal falls back to Verify-Output; (d) SHA-256 Block resume; (e) Verify-Output mismatch repair and its warning.
15. **Error mapping**: truncated input, bad magic, `.lzma` input, BCJ filter, reserved Check ID → documented `E_*` identifiers with no traceback.
16. **Directory store**: eviction at 1025 pages, page append/seal, `truncate_to`, cleanup.

---

## 9. Standard Error Codes & Exit Status

| Exit Code | Identifier | Description |
|:---|:---|:---|
| `0` | `SUCCESS` | Decompression completed successfully; all checksums verified. |
| `1` | `E_FORMAT` | Invalid magic, corrupt header, illegal or non-minimal VLI, malformed padding, truncated input, invalid LZMA2 chunk sequence, invalid distance, invalid configuration (e.g. illegal block size). |
| `1` | `E_UNSUPPORTED` | Filter count > 1, non-LZMA2 filter ID, or reserved Check ID. |
| `1` | `E_CHECK` | Header CRC32, Block Check (CRC32/CRC64/SHA-256), Stream Header/Index/Footer CRC32 mismatch. |
| `1` | `E_RESOURCE` | Out of memory, insufficient disk space, history capacity exceeded, or failed checkpoint restore in `checkpoint` mode. |
| `1` | `E_IO` | File read/write failure, broken pipe, or directory permission error. |
| `124` | `E_DEADLINE` | Wall-clock timeout, absolute deadline, or CPU timeout reached. |
| `130` | `SIGINT` | Interrupted by keyboard interrupt (`Ctrl+C`). |

---

## Appendix A — Changes from Revision 6

| # | Change | Reason (empirically verified against the Rev 6 WIP) |
|:---|:---|:---|
| 1 | New §2.5 (distance binding, re-entrancy) + regression tests | Long rep0 left `d` unbound → `UnboundLocalError` on any all-zero/highly repetitive stream |
| 2 | §3.2A page-based store; §2.1.1 block-size rule | Directory store assumed 64 KiB blocks; 4k/8k/16k corrupted output (CRC64 mismatch) |
| 3 | §5.1, §5.4 boundary ordering, `has_next`, output-on-stream-start | Stream N+1 data was written into Stream N's file; phantom "starting stream" line; empty stream produced no file |
| 4 | §3.3.2 offsets tracked by engine; `tell()` not required | Non-seekable input crashed at first block commit (`Illegal seek`) |
| 5 | §3.3 checkpoint-based resumption (**decision**: new default under `--resume-dir`; Verify-Output retained as fallback with documented limitation) | Verify-Output cannot converge when decode time exceeds the timeout (progress plateaued across repeated runs) |
| 6 | §2.2 property byte not masked; reserved values rejected; VLI minimality; padding zero checks | Spec/WIP masked `p & 0x3F`; several malformed inputs were accepted |
| 7 | §2.4 full chunk header layout and validity state machine | Spec lacked the layout; WIP didn't enforce first-chunk-reset / need-props / chunk exactness |
| 8 | §5.0 Stream Header section | Header parsing rules were missing from the spec |
| 9 | §6 failure behavior (retain `.part` on data errors, `verified_output_offset`) (**decision**) | Partial output is valuable for recovery in preservation workflows |
| 10 | §4.2 spinner only on a TTY; §7 error mapping without tracebacks | Spinner glyphs polluted redirected stderr; errors surfaced as raw tracebacks |
| 11 | §8 expanded regression suite | Rev 6 suite (26 tests) passed while all of the above defects were present |
