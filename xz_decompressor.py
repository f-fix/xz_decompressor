import io
"""
Pure-Python XZ / LZMA2 Streaming Decompression Engine.
Conforms to Technical Specification Revision 3.0.0.
Zero external dependencies. Compatible with MicroPython, PyPy, and CPython.
"""

import sys
import os
import struct
import hashlib
import json
import argparse
import unittest
from pathlib import Path

# --- Pure-Python CRC32 (Standard IEEE 802.3 / XZ) ---
CRC32_TABLE = []
for _i in range(256):
    _c = _i
    for _ in range(8):
        _c = (_c >> 1) ^ 0xEDB88320 if (_c & 1) else (_c >> 1)
    CRC32_TABLE.append(_c)

def crc32(data, val=0):
    c = val ^ 0xFFFFFFFF
    for b in data:
        c = CRC32_TABLE[(c ^ b) & 0xFF] ^ (c >> 8)
    return (c ^ 0xFFFFFFFF) & 0xFFFFFFFF

# --- Pure-Python CRC64 (ECMA-182 Reflected / XZ) ---
CRC64_TABLE = []
_POLY64 = 0xC96C5795D7870F42
for _i in range(256):
    _c = _i
    for _ in range(8):
        _c = (_c >> 1) ^ _POLY64 if (_c & 1) else (_c >> 1)
    CRC64_TABLE.append(_c)

def crc64(data, val=0):
    c = val ^ 0xFFFFFFFFFFFFFFFF
    for b in data:
        c = CRC64_TABLE[(c ^ b) & 0xFF] ^ (c >> 8)
    return (c ^ 0xFFFFFFFFFFFFFFFF) & 0xFFFFFFFFFFFFFFFF

def atomic_replace(src, dst):
    try:
        os.replace(src, dst)
    except (AttributeError, OSError):
        try:
            if os.path.exists(dst):
                os.remove(dst)
        except OSError:
            pass
        os.rename(src, dst)

def is_seekable(stream):
    try:
        if hasattr(stream, 'seekable'):
            return stream.seekable()
        stream.seek(stream.tell())
        return True
    except Exception:
        return False


class WallClockTimeout(Exception):
    """Raised when wall-clock timeout or deadline is reached."""
    pass

class CPUTimeout(Exception):
    """Raised when process CPU time timeout is reached."""
    pass

def parse_duration(val_str):
    if val_str is None:
        return None
    val_str = str(val_str).strip().lower()
    if val_str == "0":
        return 0.0
    if val_str.endswith("s"):
        return float(val_str[:-1])
    elif val_str.endswith("m"):
        return float(val_str[:-1]) * 60.0
    else:
        return float(val_str)

def parse_iso8601_flexible(s):
    import re, calendar
    s = str(s).strip()
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1].strip()

    if "T" in s or "t" in s:
        d_part, t_part = re.split(r"[Tt]", s, maxsplit=1)
    elif " " in s:
        d_part, t_part = s.split(" ", 1)
    else:
        if "-" in s:
            parts = s.split("-")
            if len(parts) == 3:
                d_part = s
                t_part = ""
            else:
                raise ValueError(f"Unrecognized date format: {s}")
        else:
            if len(s) == 8:
                d_part = s
                t_part = ""
            elif len(s) > 8:
                d_part = s[:8]
                t_part = s[8:]
            else:
                raise ValueError(f"Date string too short: {s}")

    if "-" in d_part:
        dp = d_part.split("-")
        year, month, day = int(dp[0]), int(dp[1]), int(dp[2])
    else:
        year = int(d_part[:4])
        month = int(d_part[4:6])
        day = int(d_part[6:8])

    hour, minute, second = 0, 0, 0
    subsecond = 0.0

    t_part = t_part.strip()
    if t_part:
        if "." in t_part:
            t_main, t_frac = t_part.split(".", 1)
            subsecond = float("0." + t_frac)
        else:
            t_main = t_part

        if ":" in t_main:
            tp = [int(x) for x in t_main.split(":")]
            if len(tp) == 1:
                hour = tp[0]
            elif len(tp) == 2:
                hour, minute = tp[0], tp[1]
            elif len(tp) >= 3:
                hour, minute, second = tp[0], tp[1], tp[2]
        else:
            if len(t_main) == 2:
                hour = int(t_main)
            elif len(t_main) == 4:
                hour, minute = int(t_main[:2]), int(t_main[2:4])
            elif len(t_main) >= 6:
                hour, minute, second = int(t_main[:2]), int(t_main[2:4]), int(t_main[4:6])
            else:
                raise ValueError(f"Invalid time format: {t_part}")

    return calendar.timegm((year, month, day, hour, minute, second, 0, 0, 0)) + subsecond

# --- Event Protocol ---

class XZEventType:
    CHUNK_OUTPUT = 1
    BLOCK_COMMITTED = 2
    STREAM_BOUNDARY = 3
    PROGRESS = 4

class ChunkOutputEvent:
    def __init__(self, data):
        self.type = XZEventType.CHUNK_OUTPUT
        self.data = data

class BlockCommittedEvent:
    def __init__(self, block_index, block_data, state_record):
        self.type = XZEventType.BLOCK_COMMITTED
        self.block_index = block_index
        self.block_data = block_data
        self.state_record = state_record

class StreamBoundaryEvent:
    def __init__(self, stream_index, next_stream_index,
                 input_stream_len, overall_input_offset,
                 output_stream_len, overall_output_offset):
        self.type = XZEventType.STREAM_BOUNDARY
        self.stream_index = stream_index
        self.next_stream_index = next_stream_index
        self.input_stream_len = input_stream_len
        self.overall_input_offset = overall_input_offset
        self.output_stream_len = output_stream_len
        self.overall_output_offset = overall_output_offset

class ProgressEvent:
    def __init__(self, block_index, bytes_decoded, total_emitted):
        self.type = XZEventType.PROGRESS
        self.block_index = block_index
        self.bytes_decoded = bytes_decoded
        self.total_emitted = total_emitted

# --- Pluggable History Backends ---

class HistoryBackend:
    def append_block(self, block_data, block_index=0, input_hash=""):
        raise NotImplementedError
    def get_history_slice(self, distance, length):
        raise NotImplementedError
    def checkpoint(self, block_index, state_record, input_hash=""):
        pass
    def restore(self, block_index):
        return None
    def evict_prior(self, min_retained_block):
        pass
    def reset(self):
        pass
    def cleanup(self):
        pass

class MemoryHistory(HistoryBackend):
    def __init__(self, max_history=67108864):
        self.max_history = max_history
        self.history = bytearray()
        self.checkpoints = {}

    def append_block(self, block_data, block_index=0, input_hash=""):
        self.history.extend(block_data)
        if len(self.history) > self.max_history * 2:
            del self.history[:len(self.history) - self.max_history]

    def get_history_slice(self, distance, length):
        end_idx = len(self.history) - (distance - 1)
        start_idx = end_idx - length
        if start_idx < 0:
            start_idx = 0
        if end_idx <= 0 or start_idx >= len(self.history):
            return b""
        return bytes(self.history[start_idx:end_idx])

    def checkpoint(self, block_index, state_record, input_hash=""):
        self.checkpoints[block_index] = dict(state_record)

    def restore(self, block_index):
        return self.checkpoints.get(block_index)

    def reset(self):
        self.history = bytearray()
    def cleanup(self):
        self.history = bytearray()
        self.checkpoints.clear()

class DirectoryBlockHistoryStore(HistoryBackend):
    """
    Circular buffer of up to 1025 64 KiB files on disk in a working directory.
    Files are named <input_hash>_<index:08d>.block and <input_hash>_<index:08d>.state.
    Atomic commits via temporary files.
    """
    def __init__(self, work_dir, max_blocks=1025):
        self.work_dir = Path(work_dir).resolve()
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.max_blocks = max_blocks
        self.block_files = {} # block_index -> Path
        self.state_files = {} # block_index -> Path
        self.lru_cache = {}   # block_index -> bytes (up to 2 blocks in RAM)
        self.lru_order = []
        self._scan_existing()

    def _scan_existing(self):
        self.block_files.clear()
        self.state_files.clear()
        for p in sorted(self.work_dir.glob("*_*.block")):
            parts = p.stem.split("_")
            if len(parts) >= 2 and parts[-1].isdigit():
                idx = int(parts[-1])
                self.block_files[idx] = p
        for p in sorted(self.work_dir.glob("*_*.state")):
            parts = p.stem.split("_")
            if len(parts) >= 2 and parts[-1].isdigit():
                idx = int(parts[-1])
                self.state_files[idx] = p

    def append_block(self, block_data, block_index=0, input_hash="0000000000000000"):
        target_name = f"{input_hash}_{block_index:08d}.block"
        target_path = self.work_dir / target_name
        tmp_path = self.work_dir / f".tmp_{target_name}"
        tmp_path.write_bytes(block_data)
        atomic_replace(tmp_path, target_path)
        self.block_files[block_index] = target_path

        # Update RAM cache
        self.lru_cache[block_index] = block_data
        self.lru_order.append(block_index)
        while len(self.lru_order) > 2:
            old_idx = self.lru_order.pop(0)
            self.lru_cache.pop(old_idx, None)

        # Evict blocks older than max_blocks
        if block_index >= self.max_blocks:
            self.evict_prior(block_index - self.max_blocks + 1)

    def get_history_slice(self, distance, length):
        if not self.block_files:
            return b""
        latest_idx = max(self.block_files.keys())
        res = bytearray()
        rem_len = length
        curr_dist = distance

        while rem_len > 0:
            block_offset = (curr_dist - 1) // 65536
            target_block_idx = latest_idx - block_offset
            if target_block_idx not in self.block_files:
                break

            if target_block_idx in self.lru_cache:
                bdata = self.lru_cache[target_block_idx]
            else:
                bdata = self.block_files[target_block_idx].read_bytes()
                self.lru_cache[target_block_idx] = bdata
                self.lru_order.append(target_block_idx)
                if len(self.lru_order) > 2:
                    old_idx = self.lru_order.pop(0)
                    self.lru_cache.pop(old_idx, None)

            pos_from_end = (curr_dist - 1) % 65536
            byte_idx_end = len(bdata) - pos_from_end
            bytes_available = min(rem_len, byte_idx_end)
            byte_idx_start = byte_idx_end - bytes_available
            if byte_idx_start < 0:
                bytes_available += byte_idx_start
                byte_idx_start = 0

            part = bdata[byte_idx_start:byte_idx_end]
            res = part + res
            rem_len -= len(part)
            curr_dist += len(part)
            if len(part) == 0:
                break
        return bytes(res)

    def checkpoint(self, block_index, state_record, input_hash="0000000000000000"):
        target_name = f"{input_hash}_{block_index:08d}.state"
        target_path = self.work_dir / target_name
        tmp_path = self.work_dir / f".tmp_{target_name}"
        tmp_path.write_text(json.dumps(state_record))
        atomic_replace(tmp_path, target_path)
        self.state_files[block_index] = target_path

    def restore(self, block_index):
        if block_index in self.state_files:
            try:
                return json.loads(self.state_files[block_index].read_text())
            except Exception:
                return None
        return None

    def evict_prior(self, min_retained_block):
        to_del = [idx for idx in self.block_files if idx < min_retained_block]
        for idx in to_del:
            try:
                self.block_files[idx].unlink(missing_ok=True)
            except Exception:
                pass
            self.block_files.pop(idx, None)
            if idx in self.state_files:
                try:
                    self.state_files[idx].unlink(missing_ok=True)
                except Exception:
                    pass
                self.state_files.pop(idx, None)
            self.lru_cache.pop(idx, None)

    def reset(self):
        self.lru_cache.clear()
        self.lru_order.clear()
        self.work_dir.mkdir(parents=True, exist_ok=True)

    def cleanup(self):
        self.lru_cache.clear()
        self.lru_order.clear()
        for p in self.work_dir.glob("*"):
            try:
                p.unlink()
            except Exception:
                pass
        try:
            self.work_dir.rmdir()
        except Exception:
            pass

class FileHistory(HistoryBackend):
    def __init__(self, file_path, max_history=67108864):
        self.file_path = Path(file_path).resolve()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(self.file_path, "w+b")
        self.total_written = 0
        self.max_history = max_history

    def append_block(self, block_data, block_index=0, input_hash=""):
        self.f.seek(self.total_written)
        self.f.write(block_data)
        self.f.flush()
        self.total_written += len(block_data)

    def get_history_slice(self, distance, length):
        if distance > self.total_written:
            return b""
        end_pos = self.total_written - (distance - 1)
        start_pos = max(0, end_pos - length)
        read_len = end_pos - start_pos
        self.f.seek(start_pos)
        return self.f.read(read_len)

    def cleanup(self):
        try:
            self.f.close()
            self.file_path.unlink(missing_ok=True)
        except Exception:
            pass

# --- Range Decoder ---

class RangeDecoder:
    def __init__(self, read_byte_fn):
        self.read_byte = read_byte_fn
        self.read_byte() # Discard first 0x00 byte
        self.code = (self.read_byte() << 24) | (self.read_byte() << 16) | (self.read_byte() << 8) | self.read_byte()
        self.range = 0xFFFFFFFF

    def decode_bit(self, probs, index):
        prob = probs[index]
        bound = (self.range >> 11) * prob
        if self.code < bound:
            self.range = bound
            probs[index] = prob + ((2048 - prob) >> 5)
            bit = 0
        else:
            self.range = (self.range - bound) & 0xFFFFFFFF
            self.code = (self.code - bound) & 0xFFFFFFFF
            probs[index] = prob - (prob >> 5)
            bit = 1
        if self.range < 0x01000000:
            self.range = (self.range << 8) & 0xFFFFFFFF
            b = self.read_byte()
            self.code = ((self.code << 8) | b) & 0xFFFFFFFF
        return bit

    def decode_bittree(self, probs, offset, num_bits):
        m = 1
        for _ in range(num_bits):
            m = (m << 1) + self.decode_bit(probs, offset + m)
        return m - (1 << num_bits)

    def decode_reverse_bittree(self, probs, offset, num_bits):
        m = 1
        symbol = 0
        for i in range(num_bits):
            bit = self.decode_bit(probs, offset + m)
            m = (m << 1) + bit
            symbol |= (bit << i)
        return symbol

    def decode_direct_bits(self, num_bits):
        val = 0
        for _ in range(num_bits):
            self.range >>= 1
            self.code = (self.code - self.range) & 0xFFFFFFFF
            if (self.code >> 31) & 1:
                self.code = (self.code + self.range) & 0xFFFFFFFF
                bit = 0
            else:
                bit = 1
            val = (val << 1) | bit
            if self.range < 0x01000000:
                self.range = (self.range << 8) & 0xFFFFFFFF
                b = self.read_byte()
                self.code = ((self.code << 8) | b) & 0xFFFFFFFF
        return val

def decode_len_val(rd, probs, pos_state):
    if rd.decode_bit(probs, 0) == 0:
        return rd.decode_bittree(probs, 2 + (pos_state << 3), 3)
    if rd.decode_bit(probs, 1) == 0:
        return 8 + rd.decode_bittree(probs, 66 + (pos_state << 3), 3)
    return 16 + rd.decode_bittree(probs, 130, 8)

# --- Universal XZ / LZMA2 Decompression Engine ---

class XZStreamDecompressor:
    def __init__(self, input_source, history_backend=None, block_size=65536):
        self.input = input_source
        self.history = history_backend or MemoryHistory()
        self.block_size = block_size
        self.overall_input_offset = 0
        self.overall_output_offset = 0
        self.input_hasher = hashlib.sha256()

    def _read_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.input.read(n - len(buf))
            if not chunk:
                break
            buf.extend(chunk)
        if len(buf) < n:
            raise EOFError(f"Unexpected EOF: expected {n} bytes, got {len(buf)}")
        self.overall_input_offset += n
        self.input_hasher.update(buf)
        return bytes(buf)

    def _read_vli(self):
        val = 0
        shift = 0
        for _ in range(9):
            b = self._read_exact(1)[0]
            val |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                return val
            shift += 7
        raise ValueError("Invalid VLI encoding")

    def decode_events(self):
        stream_index = 0
        while True:
            # Handle stream header and RFC §2.1.2 4-byte null padding between streams
            header_magic = bytearray()
            while True:
                b = self.input.read(1)
                if not b:
                    return # Clean EOF
                self.overall_input_offset += 1
                self.input_hasher.update(b)
                if b == b"\x00":
                    continue
                if b == b"\xfd":
                    header_magic.append(0xFD)
                    rest = self._read_exact(5)
                    header_magic.extend(rest)
                    if bytes(header_magic) == b"\xfd7zXZ\x00":
                        break
                    else:
                        raise ValueError(f"Invalid stream magic: {bytes(header_magic).hex()}")
                else:
                    raise ValueError(f"Unexpected byte before stream header: 0x{b[0]:02x}")

            stream_start_in = self.overall_input_offset - 6
            stream_start_out = self.overall_output_offset

            # Stream Flags (2 bytes) + CRC32 (4 bytes)
            flags = self._read_exact(2)
            check_type = flags[1] & 0x0F
            crc_bytes = self._read_exact(4)
            if crc32(flags) != struct.unpack("<I", crc_bytes)[0]:
                raise ValueError("Stream header CRC32 mismatch")

            # Active block buffer
            block_idx = 0
            cur_block = bytearray(self.block_size)
            cur_block_pos = 0
            stream_out_offset = 0
            self.history.reset()

            # LZMA2 Decoder persistent state across chunks
            PROB_INIT = 1024
            state = 0
            reps = [0, 0, 0, 0]
            lc = 3
            lp = 0
            pb = 2
            p_is_match = [PROB_INIT] * (12 << 4)
            p_is_rep = [PROB_INIT] * 12
            p_is_rep_g0 = [PROB_INIT] * 12
            p_is_rep_g1 = [PROB_INIT] * 12
            p_is_rep_g2 = [PROB_INIT] * 12
            p_is_rep0_long = [PROB_INIT] * (12 << 4)
            p_pos_slot = [PROB_INIT] * (4 << 6)
            p_spec_pos = [PROB_INIT] * 115
            p_align = [PROB_INIT] * 16
            p_len = [PROB_INIT] * 386
            p_rep_len = [PROB_INIT] * 386
            p_lit = [PROB_INIT] * (0x300 << (lc + lp))

            # Decode blocks within this stream
            while True:
                first_byte = self._read_exact(1)[0]
                if first_byte == 0x00:
                    # Index Indicator reached
                    break

                # Block Header Size: (first_byte + 1) * 4
                bh_size = (first_byte + 1) * 4
                bh_data = self._read_exact(bh_size - 1)
                full_bh = bytes([first_byte]) + bh_data
                header_crc = struct.unpack("<I", full_bh[-4:])[0]
                if crc32(full_bh[:-4]) != header_crc:
                    raise ValueError("Block header CRC32 mismatch")

                compressed_bytes_in_block = 0
                while True:
                    ctrl = self._read_exact(1)[0]
                    compressed_bytes_in_block += 1
                    if ctrl == 0x00:
                        break # EOS for this block's LZMA2 stream

                    elif ctrl in (0x01, 0x02):
                        # Uncompressed chunk
                        sz_bytes = self._read_exact(2)
                        compressed_bytes_in_block += 2
                        chunk_sz = ((sz_bytes[0] << 8) | sz_bytes[1]) + 1
                        raw_chunk = self._read_exact(chunk_sz)
                        compressed_bytes_in_block += chunk_sz

                        raw_pos = 0
                        while raw_pos < chunk_sz:
                            space = self.block_size - cur_block_pos
                            take = min(space, chunk_sz - raw_pos)
                            cur_block[cur_block_pos : cur_block_pos + take] = raw_chunk[raw_pos : raw_pos + take]
                            cur_block_pos += take
                            raw_pos += take

                            if cur_block_pos == self.block_size:
                                committed_data = bytes(cur_block)
                                input_h = self.input_hasher.hexdigest()[:16]
                                self.history.append_block(committed_data, block_idx, input_h)
                                self.history.checkpoint(block_idx, {"block_idx": block_idx}, input_h)
                                yield ChunkOutputEvent(committed_data)
                                yield BlockCommittedEvent(block_idx, committed_data, {})
                                yield ProgressEvent(block_idx, len(committed_data), self.overall_output_offset)
                                self.overall_output_offset += len(committed_data)
                                stream_out_offset += len(committed_data)
                                block_idx += 1
                                cur_block_pos = 0

                    elif ctrl >= 0x80:
                        # LZMA compressed chunk
                        mode = (ctrl >> 5) & 3
                        uncomp_high = (ctrl & 0x1F) << 16
                        sz1_2 = self._read_exact(2)
                        sz3_4 = self._read_exact(2)
                        compressed_bytes_in_block += 4
                        chunk_uncomp_sz = (uncomp_high | (sz1_2[0] << 8) | sz1_2[1]) + 1
                        chunk_comp_sz = ((sz3_4[0] << 8) | sz3_4[1]) + 1

                        if mode >= 2:
                            prop_byte = self._read_exact(1)[0]
                            compressed_bytes_in_block += 1
                            pb = prop_byte // 45
                            rem = prop_byte % 45
                            lp = rem // 9
                            lc = rem % 9
                            p_lit = [PROB_INIT] * (0x300 << (lc + lp))

                        if mode >= 1:
                            state = 0

                        chunk_compressed = self._read_exact(chunk_comp_sz)
                        compressed_bytes_in_block += chunk_comp_sz

                        comp_offset = 0
                        def _get_comp_byte():
                            nonlocal comp_offset
                            if comp_offset < len(chunk_compressed):
                                b = chunk_compressed[comp_offset]
                                comp_offset += 1
                                return b
                            return 0

                        rd = RangeDecoder(_get_comp_byte)

                        pos_mask = (1 << pb) - 1
                        lp_mask = (1 << lp) - 1
                        chunk_decoded = 0

                        while chunk_decoded < chunk_uncomp_sz:
                            pos_state = (stream_out_offset + cur_block_pos) & pos_mask
                            match_idx = (state << 4) + pos_state

                            if rd.decode_bit(p_is_match, match_idx) == 0:
                                # Literal
                                if cur_block_pos > 0:
                                    prev_byte = cur_block[cur_block_pos - 1]
                                else:
                                    prev_slice = self.history.get_history_slice(1, 1)
                                    prev_byte = prev_slice[0] if prev_slice else 0

                                lit_state = (((stream_out_offset + cur_block_pos) & lp_mask) << lc) + (prev_byte >> (8 - lc))
                                lit_offset = lit_state * 0x300

                                symbol = 1
                                if state >= 7:
                                    dist = reps[0]
                                    if dist < cur_block_pos:
                                        match_byte = cur_block[cur_block_pos - 1 - dist]
                                    else:
                                        m_slice = self.history.get_history_slice(dist - cur_block_pos + 1, 1)
                                        match_byte = m_slice[0] if m_slice else 0

                                    while symbol < 0x100:
                                        match_bit = (match_byte >> 7) & 1
                                        match_byte = (match_byte << 1) & 0xFF
                                        bit = rd.decode_bit(p_lit, lit_offset + ((1 + match_bit) << 8) + symbol)
                                        symbol = (symbol << 1) | bit
                                        if match_bit != bit:
                                            break
                                while symbol < 0x100:
                                    symbol = (symbol << 1) | rd.decode_bit(p_lit, lit_offset + symbol)

                                cur_block[cur_block_pos] = symbol - 0x100
                                cur_block_pos += 1
                                chunk_decoded += 1
                                state = [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 4, 5][state]

                            else:
                                # Match
                                if rd.decode_bit(p_is_rep, state) == 1:
                                    if rd.decode_bit(p_is_rep_g0, state) == 0:
                                        if rd.decode_bit(p_is_rep0_long, match_idx) == 0:
                                            # Short rep
                                            state = 9 if state < 7 else 11
                                            length = 1
                                            dist = reps[0]
                                            if dist < cur_block_pos:
                                                cur_block[cur_block_pos] = cur_block[cur_block_pos - 1 - dist]
                                            else:
                                                m_slice = self.history.get_history_slice(dist - cur_block_pos + 1, 1)
                                                cur_block[cur_block_pos] = m_slice[0] if m_slice else 0
                                            cur_block_pos += 1
                                            chunk_decoded += 1

                                            if cur_block_pos == self.block_size:
                                                committed_data = bytes(cur_block)
                                                input_h = self.input_hasher.hexdigest()[:16]
                                                self.history.append_block(committed_data, block_idx, input_h)
                                                self.history.checkpoint(block_idx, {"block_idx": block_idx}, input_h)
                                                yield ChunkOutputEvent(committed_data)
                                                yield BlockCommittedEvent(block_idx, committed_data, {})
                                                yield ProgressEvent(block_idx, len(committed_data), self.overall_output_offset)
                                                self.overall_output_offset += len(committed_data)
                                                stream_out_offset += len(committed_data)
                                                block_idx += 1
                                                cur_block_pos = 0
                                            continue
                                    else:
                                        if rd.decode_bit(p_is_rep_g1, state) == 0:
                                            dist = reps[1]
                                        else:
                                            if rd.decode_bit(p_is_rep_g2, state) == 0:
                                                dist = reps[2]
                                            else:
                                                dist = reps[3]
                                                reps[3] = reps[2]
                                            reps[2] = reps[1]
                                        reps[1] = reps[0]
                                        reps[0] = dist
                                    length = 2 + decode_len_val(rd, p_rep_len, pos_state)
                                    state = 8 if state < 7 else 11
                                else:
                                    reps[3] = reps[2]
                                    reps[2] = reps[1]
                                    reps[1] = reps[0]
                                    state = 7 if state < 7 else 10
                                    length = 2 + decode_len_val(rd, p_len, pos_state)
                                    len_state = min(length - 2, 3)
                                    slot = rd.decode_bittree(p_pos_slot, len_state << 6, 6)
                                    if slot < 4:
                                        dist = slot
                                    else:
                                        num_direct_bits = (slot >> 1) - 1
                                        base = (2 | (slot & 1)) << num_direct_bits
                                        if slot < 14:
                                            dist = base + rd.decode_reverse_bittree(p_spec_pos, base - slot, num_direct_bits)
                                        else:
                                            d_bits = rd.decode_direct_bits(num_direct_bits - 4) << 4
                                            a_bits = rd.decode_reverse_bittree(p_align, 0, 4)
                                            dist = base + d_bits + a_bits
                                    reps[0] = dist

                                # Match copy loop
                                dist = reps[0]
                                rem_to_copy = length
                                while rem_to_copy > 0:
                                    space = self.block_size - cur_block_pos
                                    copy_len = min(space, rem_to_copy)

                                    if dist < cur_block_pos:
                                        src_start = cur_block_pos - 1 - dist
                                        if dist == 0:
                                            cur_block[cur_block_pos : cur_block_pos + copy_len] = bytes([cur_block[src_start]]) * copy_len
                                        elif dist >= copy_len:
                                            cur_block[cur_block_pos : cur_block_pos + copy_len] = cur_block[src_start : src_start + copy_len]
                                        else:
                                            for i in range(copy_len):
                                                cur_block[cur_block_pos + i] = cur_block[src_start + i]
                                    else:
                                        hist_needed = min(copy_len, dist - cur_block_pos + 1)
                                        h_slice = self.history.get_history_slice(dist - cur_block_pos + 1, hist_needed)
                                        cur_block[cur_block_pos : cur_block_pos + len(h_slice)] = h_slice
                                        copied = len(h_slice)
                                        if copied < copy_len:
                                            for i in range(copy_len - copied):
                                                cur_block[cur_block_pos + copied + i] = cur_block[cur_block_pos + i]

                                    cur_block_pos += copy_len
                                    chunk_decoded += copy_len
                                    rem_to_copy -= copy_len

                                    if cur_block_pos == self.block_size:
                                        committed_data = bytes(cur_block)
                                        input_h = self.input_hasher.hexdigest()[:16]
                                        self.history.append_block(committed_data, block_idx, input_h)
                                        self.history.checkpoint(block_idx, {"block_idx": block_idx}, input_h)
                                        yield ChunkOutputEvent(committed_data)
                                        yield BlockCommittedEvent(block_idx, committed_data, {})
                                        yield ProgressEvent(block_idx, len(committed_data), self.overall_output_offset)
                                        self.overall_output_offset += len(committed_data)
                                        stream_out_offset += len(committed_data)
                                        block_idx += 1
                                        cur_block_pos = 0

                            if cur_block_pos == self.block_size:
                                committed_data = bytes(cur_block)
                                input_h = self.input_hasher.hexdigest()[:16]
                                self.history.append_block(committed_data, block_idx, input_h)
                                self.history.checkpoint(block_idx, {"block_idx": block_idx}, input_h)
                                yield ChunkOutputEvent(committed_data)
                                yield BlockCommittedEvent(block_idx, committed_data, {})
                                yield ProgressEvent(block_idx, len(committed_data), self.overall_output_offset)
                                self.overall_output_offset += len(committed_data)
                                stream_out_offset += len(committed_data)
                                block_idx += 1
                                cur_block_pos = 0

                # Block padding
                pad_len = (4 - (compressed_bytes_in_block % 4)) % 4
                if pad_len > 0:
                    self._read_exact(pad_len)

                # Block Check (CRC32=4, CRC64=8, SHA256=32)
                check_size = {0: 0, 1: 4, 4: 8, 10: 32}.get(check_type, 0)
                if check_size > 0:
                    self._read_exact(check_size)

            # Flush remaining partial block
            if cur_block_pos > 0:
                partial_data = bytes(cur_block[:cur_block_pos])
                input_h = self.input_hasher.hexdigest()[:16]
                self.history.append_block(partial_data, block_idx, input_h)
                self.history.checkpoint(block_idx, {"block_idx": block_idx}, input_h)
                yield ChunkOutputEvent(partial_data)
                yield BlockCommittedEvent(block_idx, partial_data, {})
                yield ProgressEvent(block_idx, len(partial_data), self.overall_output_offset)
                self.overall_output_offset += len(partial_data)
                block_idx += 1
                cur_block_pos = 0

            # Read Index field (Indicator 0x00 already read)
            index_bytes_count = 1
            def _vli_with_count():
                nonlocal index_bytes_count
                val = 0
                shift = 0
                for _ in range(9):
                    b = self._read_exact(1)[0]
                    index_bytes_count += 1
                    val |= (b & 0x7F) << shift
                    if (b & 0x80) == 0:
                        return val
                    shift += 7
                raise ValueError("Invalid VLI in Index")

            num_records = _vli_with_count()
            for _ in range(num_records):
                _vli_with_count()
                _vli_with_count()

            # Index padding to 4-byte boundary
            idx_pad = (4 - (index_bytes_count % 4)) % 4
            if idx_pad > 0:
                self._read_exact(idx_pad)

            # Index CRC32 (4 bytes)
            self._read_exact(4)

            # Stream Footer (12 bytes)
            footer = self._read_exact(12)
            if not footer.endswith(b"YZ"):
                raise ValueError(f"Invalid Stream Footer magic: {footer[-2:].hex()}")

            stream_end_in = self.overall_input_offset
            stream_end_out = self.overall_output_offset
            in_len = stream_end_in - stream_start_in
            out_len = stream_end_out - stream_start_out

            yield StreamBoundaryEvent(
                stream_index=stream_index,
                next_stream_index=stream_index + 1,
                input_stream_len=in_len,
                overall_input_offset=stream_end_in,
                output_stream_len=out_len,
                overall_output_offset=stream_end_out
            )
            stream_index += 1

# --- Standalone CLI Driver ---

def compute_output_filename(input_path):
    if input_path == "-":
        return "stdin.unxz"
    p = Path(input_path)
    lower_name = p.name.lower()
    if lower_name.endswith(".xz"):
        return str(p.parent / p.name[:-3])
    if lower_name.endswith(".lzma"):
        return str(p.parent / p.name[:-5])
    return str(p.parent / f"{p.name}.unxz")

def format_suffixed_filename(base_path, stream_idx):
    if stream_idx == 0:
        return base_path
    p = Path(base_path)
    ext = p.suffix
    stem = p.name[:-len(ext)] if ext else p.name
    new_name = f"{stem}_{stream_idx}{ext}"
    return str(p.parent / new_name)

def decompress_xz(input_source, output_target=None, resume_dir=None,
                  resume_from=None, resume_at=None, in_place=False,
                  progress_callback=None, history_backend=None,
                  timeout=52.0, cpu_timeout=22.0, deadline=None):
    """
    Callable interface for XZ decompression.
    """
    if isinstance(input_source, (str, Path)):
        if str(input_source) == "-":
            in_stream = sys.stdin.buffer
            if output_target is None:
                output_target = "stdin.unxz"
        else:
            in_stream = open(input_source, "rb")
            if output_target is None:
                output_target = compute_output_filename(input_source)
    else:
        in_stream = input_source
        if output_target is None:
            output_target = "-"

    if history_backend is not None:
        history = history_backend
    elif resume_dir:
        history = DirectoryBlockHistoryStore(resume_dir)
    else:
        history = MemoryHistory()

    is_stream_output = (output_target == "-")
    is_stream_input = (input_source == "-") or not is_seekable(in_stream)

    # Verify resume offsets if specified
    if resume_from is not None:
        if isinstance(history, DirectoryBlockHistoryStore):
            # Verify working directory state matches resume_from
            if not history.state_files:
                raise RuntimeError(f"Hard resumption error: no state found for --resume-from={resume_from}")
        if is_seekable(in_stream):
            in_stream.seek(resume_from)

    engine = XZStreamDecompressor(in_stream, history)

    # Initialize clocks and subtract initial counter readings
    import time, signal, math
    init_perf = time.perf_counter()
    try:
        init_cpu = time.process_time()
    except (AttributeError, OSError):
        init_cpu = init_perf

    # Parse and resolve timeouts
    timeout_sec = parse_duration(timeout) if timeout is not None else 52.0
    cpu_timeout_sec = parse_duration(cpu_timeout) if cpu_timeout is not None else 22.0

    if deadline is not None:
        deadline_epoch = parse_iso8601_flexible(deadline)
        now_epoch = time.time()
        deadline_remaining = max(0.0, deadline_epoch - now_epoch)
        if timeout_sec and timeout_sec > 0:
            timeout_sec = min(timeout_sec, deadline_remaining)
        else:
            timeout_sec = deadline_remaining

    # Set up signal handlers if available
    sig_alarm_old = None
    sig_vtalrm_old = None
    if hasattr(signal, 'SIGALRM') and timeout_sec and timeout_sec > 0:
        def _on_sigalrm(signum, frame):
            raise WallClockTimeout("Wall-clock timeout reached (SIGALRM)")
        try:
            sig_alarm_old = signal.signal(signal.SIGALRM, _on_sigalrm)
            if hasattr(signal, 'setitimer'):
                signal.setitimer(signal.ITIMER_REAL, timeout_sec)
            else:
                signal.alarm(int(math.ceil(timeout_sec)))
        except (ValueError, OSError, AttributeError):
            pass

    if hasattr(signal, 'ITIMER_VIRTUAL') and hasattr(signal, 'SIGVTALRM') and cpu_timeout_sec and cpu_timeout_sec > 0:
        def _on_sigvtalrm(signum, frame):
            raise CPUTimeout("CPU timeout reached (SIGVTALRM)")
        try:
            sig_vtalrm_old = signal.signal(signal.SIGVTALRM, _on_sigvtalrm)
            signal.setitimer(signal.ITIMER_VIRTUAL, cpu_timeout_sec)
        except (ValueError, OSError, AttributeError):
            pass

    curr_out_stream = None
    curr_dest_path = None
    curr_part_path = None
    stream_idx = 0

    if is_stream_output:
        curr_out_stream = sys.stdout.buffer

    # Verify-output mode tracking for resumable disk destination
    verify_output_mode = False
    verify_file_handle = None
    verify_file_len = 0
    verified_offset = 0

    try:
        for event in engine.decode_events():
            # Cooperative clock-checking enforced slightly early (0.15s margin)
            coop_margin = 0.15
            if timeout_sec and timeout_sec > 0:
                if (time.perf_counter() - init_perf) >= (timeout_sec - coop_margin):
                    raise WallClockTimeout("Wall-clock timeout reached (cooperative check)")
            if cpu_timeout_sec and cpu_timeout_sec > 0:
                try:
                    cpu_now = time.process_time()
                except (AttributeError, OSError):
                    cpu_now = time.perf_counter()
                if (cpu_now - init_cpu) >= (cpu_timeout_sec - coop_margin):
                    raise CPUTimeout("CPU timeout reached (cooperative check)")
            if event.type == XZEventType.CHUNK_OUTPUT:
                if not is_stream_output and curr_out_stream is None:
                    base_dest = format_suffixed_filename(output_target, stream_idx)
                    curr_dest_path = Path(base_dest)

                    if in_place:
                        target_file = curr_dest_path
                    else:
                        target_file = Path(f"{base_dest}.part")
                        curr_part_path = target_file

                    # Check if file exists to enter verify-output mode
                    if target_file.exists() and resume_dir:
                        verify_output_mode = True
                        verify_file_len = target_file.stat().st_size
                        verify_file_handle = open(target_file, "r+b")
                        verified_offset = 0
                    else:
                        if not in_place and target_file.exists() and not resume_dir:
                            target_file.unlink()
                        curr_out_stream = open(target_file, "wb")
                        verify_output_mode = False

                if verify_output_mode:
                    # Compare block against existing file
                    space = len(event.data)
                    if verified_offset + space <= verify_file_len:
                        verify_file_handle.seek(verified_offset)
                        existing_chunk = verify_file_handle.read(space)
                        if existing_chunk == event.data:
                            verified_offset += space
                        else:
                            # Mismatch! Truncate output file to verified_offset
                            verify_file_handle.seek(verified_offset)
                            verify_file_handle.truncate()
                            verify_file_handle.write(event.data)
                            verify_file_handle.flush()
                            curr_out_stream = verify_file_handle
                            verify_output_mode = False
                    else:
                        # Reached beyond existing file
                        match_len = verify_file_len - verified_offset
                        if match_len > 0:
                            verify_file_handle.seek(verified_offset)
                            existing_chunk = verify_file_handle.read(match_len)
                            if existing_chunk != event.data[:match_len]:
                                verify_file_handle.seek(verified_offset)
                                verify_file_handle.truncate()
                                verify_file_handle.write(event.data)
                                verify_file_handle.flush()
                                curr_out_stream = verify_file_handle
                                verify_output_mode = False
                                continue
                        verify_file_handle.seek(verify_file_len)
                        verify_file_handle.write(event.data[match_len:])
                        verify_file_handle.flush()
                        curr_out_stream = verify_file_handle
                        verify_output_mode = False
                else:
                    curr_out_stream.write(event.data)
                    curr_out_stream.flush()

            elif event.type == XZEventType.PROGRESS:
                if progress_callback:
                    progress_callback(event)

            elif event.type == XZEventType.STREAM_BOUNDARY:
                if not is_stream_output:
                    if verify_file_handle is not None:
                        verify_file_handle.close()
                        verify_file_handle = None
                    if curr_out_stream is not None:
                        curr_out_stream.close()
                        curr_out_stream = None

                    if not in_place and curr_part_path and curr_part_path.exists():
                        if curr_dest_path.exists():
                            curr_dest_path.unlink()
                        atomic_replace(curr_part_path, curr_dest_path)

                next_stream = event.next_stream_index
                if is_stream_output:
                    dest_desc = f"stdout+{event.overall_output_offset}B"
                else:
                    dest_desc = format_suffixed_filename(output_target, next_stream)

                sys.stderr.write(
                    f"[xz:stream boundary] input_offset={event.overall_input_offset}B "
                    f"stream_in={event.input_stream_len}B stream_out={event.output_stream_len}B "
                    f"output_offset={event.overall_output_offset}B starting stream {next_stream} -> {dest_desc}\n"
                )
                sys.stderr.flush()
                stream_idx = next_stream

        if not is_stream_output:
            if verify_file_handle is not None:
                verify_file_handle.close()
            if curr_out_stream is not None:
                curr_out_stream.close()
            if not in_place and curr_part_path and curr_part_path.exists():
                if curr_dest_path.exists():
                    curr_dest_path.unlink()
                atomic_replace(curr_part_path, curr_dest_path)

        if resume_dir:
            history.cleanup()

    except (KeyboardInterrupt, WallClockTimeout, CPUTimeout) as exc:
        if isinstance(exc, WallClockTimeout):
            msg = "\nDecompression timed out (wall-clock deadline reached)."
        elif isinstance(exc, CPUTimeout):
            msg = "\nDecompression timed out (CPU time limit reached)."
        else:
            msg = "\nDecompression interrupted."

        if resume_dir:
            msg += f" To resume, run with: --resume-dir={resume_dir}"
        if not is_seekable(in_stream):
            msg += f" --resume-from={engine.overall_input_offset}"
        if not is_stream_output and not is_seekable(curr_out_stream):
            msg += f" --resume-at={engine.overall_output_offset}"
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()
        raise
    finally:
        # Reset signal handlers and timers
        if hasattr(signal, 'SIGALRM') and sig_alarm_old is not None:
            try:
                if hasattr(signal, 'setitimer'):
                    signal.setitimer(signal.ITIMER_REAL, 0)
                else:
                    signal.alarm(0)
                signal.signal(signal.SIGALRM, sig_alarm_old)
            except (ValueError, OSError, AttributeError):
                pass
        if hasattr(signal, 'SIGVTALRM') and sig_vtalrm_old is not None:
            try:
                signal.setitimer(signal.ITIMER_VIRTUAL, 0)
                signal.signal(signal.SIGVTALRM, sig_vtalrm_old)
            except (ValueError, OSError, AttributeError):
                pass

    return engine.overall_output_offset

# --- Built-In Unittest Suite ---

class TestXZDecompressor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import lzma
        cls.lzma = lzma

    def test_presets_and_patterns(self):
        lzma = self.lzma
        patterns = [
            b"Hello world! " * 200,
            bytes(range(256)) * 20,
            os.urandom(10000) + b"ABCDEFGH" * 1000,
        ]
        for pat in patterns:
            for preset in [0, 1, 6]:
                xz_data = lzma.compress(pat, preset=preset)
                engine = XZStreamDecompressor(io.BytesIO(xz_data))
                out = bytearray()
                for ev in engine.decode_events():
                    if ev.type == XZEventType.CHUNK_OUTPUT:
                        out.extend(ev.data)
                self.assertEqual(bytes(out), pat)

    def test_multi_stream_concatenation_and_padding(self):
        lzma = self.lzma
        part1 = b"FIRST_STREAM_CONTENT_" * 100
        part2 = b"SECOND_STREAM_CONTENT_" * 100
        xz1 = lzma.compress(part1)
        xz2 = lzma.compress(part2)
        
        # Test with 0, 4, 8 null padding bytes
        for pad_count in [0, 4, 8]:
            combined = xz1 + (b"\x00" * pad_count) + xz2
            engine = XZStreamDecompressor(io.BytesIO(combined))
            streams_seen = []
            out_slices = []
            for ev in engine.decode_events():
                if ev.type == XZEventType.CHUNK_OUTPUT:
                    out_slices.append(ev.data)
                elif ev.type == XZEventType.STREAM_BOUNDARY:
                    streams_seen.append(ev)
            self.assertEqual(len(streams_seen), 2)
            self.assertEqual(b"".join(out_slices), part1 + part2)

    def test_directory_block_history_and_cleanup(self):
        tmp_dir = Path("/tmp/turn6/test_hist")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        hist = DirectoryBlockHistoryStore(tmp_dir, max_blocks=3)
        b1 = b"A" * 65536
        b2 = b"B" * 65536
        b3 = b"C" * 65536
        b4 = b"D" * 65536
        hist.append_block(b1, 0, "hash0")
        hist.append_block(b2, 1, "hash1")
        hist.append_block(b3, 2, "hash2")
        hist.append_block(b4, 3, "hash3")
        # Block 0 should have been evicted
        self.assertNotIn(0, hist.block_files)
        self.assertIn(1, hist.block_files)
        self.assertIn(2, hist.block_files)
        self.assertIn(3, hist.block_files)
        hist.cleanup()
        self.assertFalse(tmp_dir.exists())

    def test_verify_output_mode(self):
        lzma = self.lzma
        import io
        work_dir = Path("/tmp/turn6/test_verify_work")
        out_target = Path("/tmp/turn6/test_verify_out.bin")
        data = b"VERIFY_PAYLOAD_TEST_" * 500
        xz_data = lzma.compress(data)
        
        # Write corrupted partial output file
        out_target.write_bytes(b"WRONG_DATA_AT_START_" + data[20:])
        
        # Decompress with resume-dir triggering verify-output mode
        decompress_xz(io.BytesIO(xz_data), str(out_target), resume_dir=str(work_dir), in_place=True)
        self.assertEqual(out_target.read_bytes(), data)
        out_target.unlink(missing_ok=True)

    def test_flexible_iso8601_parser(self):
        cases = [
            ("20261008", 1791417600.0),
            ("2026-10-08", 1791417600.0),
            ("20261008Z", 1791417600.0),
            ("2026-10-08T00:00:00Z", 1791417600.0),
            ("20261008T12", 1791460800.0),
            ("2026-10-08 12:30", 1791462600.0),
            ("202610081230", 1791462600.0),
            ("2026-10-08T12:30:45Z", 1791462645.0),
            ("20261008123045", 1791462645.0),
            ("2026-10-08T12:30:45.500Z", 1791462645.5),
            ("20261008123045.500", 1791462645.5),
        ]
        for s, expected in cases:
            self.assertEqual(parse_iso8601_flexible(s), expected)

    def test_duration_parser(self):
        self.assertEqual(parse_duration("52s"), 52.0)
        self.assertEqual(parse_duration("1.5m"), 90.0)
        self.assertEqual(parse_duration("0"), 0.0)
        self.assertIsNone(parse_duration(None))

    def test_wall_clock_timeout_cooperative(self):
        lzma = self.lzma
        import io
        data = b"TIMEOUT_TEST_" * 50000
        xz_data = lzma.compress(data)
        # Timeout after 0.01 seconds
        with self.assertRaises(WallClockTimeout):
            decompress_xz(io.BytesIO(xz_data), output_target="-", timeout=0.01, cpu_timeout=0)

def main():
    parser = argparse.ArgumentParser(description="Universal Pure-Python Resumable XZ / LZMA2 Decompressor")
    parser.add_argument("input", nargs="?", default="-", help="Path to .xz file or '-' for stdin")
    parser.add_argument("-o", "--output", default=None, help="Output destination or '-' for stdout")
    parser.add_argument("--resume-dir", default=None, help="Working directory for circular buffer checkpoints")
    parser.add_argument("--resume-from", type=int, default=None, help="Input offset for non-seekable resumption")
    parser.add_argument("--resume-at", type=int, default=None, help="Output offset for non-seekable resumption")
    parser.add_argument("--in-place", action="store_true", help="Write directly to targets without .part staging")
    parser.add_argument("--timeout", default="52s", help="Wall-clock timeout [<N>s|<N>m|0] (default: 52s)")
    parser.add_argument("--cpu-timeout", default="22s", help="CPU timeout [<N>s|<N>m|0] (default: 22s)")
    parser.add_argument("--deadline", default=None, help="Absolute UTC deadline instant (ISO-8601)")
    parser.add_argument("--test", action="store_true", help="Run self-contained unit test suite")
    parser.add_argument("-v", "--verbose", action="store_true", help="Diagnostic logging")
    args = parser.parse_args()

    if args.test:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestXZDecompressor)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        sys.exit(0 if result.wasSuccessful() else 1)

    spinner = ("\\", "|", "/", "-")
    spin_idx = [0]
    def progress_cb(ev):
        glyph = spinner[spin_idx[0] % 4]
        spin_idx[0] += 1
        sys.stderr.write(f"{glyph}\r")
        sys.stderr.flush()

    try:
        decompress_xz(
            input_source=args.input,
            output_target=args.output,
            resume_dir=args.resume_dir,
            resume_from=args.resume_from,
            resume_at=args.resume_at,
            in_place=args.in_place,
            progress_callback=progress_cb,
            timeout=args.timeout,
            cpu_timeout=args.cpu_timeout,
            deadline=args.deadline
        )
    except (WallClockTimeout, CPUTimeout):
        sys.exit(124)
    except KeyboardInterrupt:
        sys.exit(130)

if __name__ == "__main__":
    main()