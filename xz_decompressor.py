import io
"""
Pure-Python XZ / LZMA2 Streaming Decompression Engine.
Zero required external dependencies, apart from --test mode.
Normal operation compatible with MicroPython, PyPy, and CPython.
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


E_FORMAT = 1
E_UNSUPPORTED = 1
E_CHECK = 1
E_RESOURCE = 1
E_IO = 1
E_DEADLINE = 124

class XZFormatError(ValueError):
    pass

class XZUnsupportedError(ValueError):
    pass

class XZCheckError(ValueError):
    pass

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
        if distance <= 0:
            return b""
        start_idx = len(self.history) - distance
        if start_idx < 0:
            start_idx = 0
        end_idx = min(len(self.history), start_idx + length)
        if start_idx >= end_idx:
            return b""
        return bytes(self.history[start_idx:end_idx])

    def export_state(self):
        return {"max_history": self.max_history, "history": bytes(self.history).hex()}

    def import_state(self, state):
        self.max_history = state["max_history"]
        self.history = bytearray(bytes.fromhex(state["history"]))

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
            start_in_block = len(bdata) - 1 - pos_from_end
            if start_in_block < 0:
                break
            bytes_avail = min(rem_len, len(bdata) - start_in_block)
            part = bdata[start_in_block : start_in_block + bytes_avail]
            res.extend(part)
            rem_len -= len(part)
            curr_dist -= len(part)
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
        if distance <= 0:
            return b""
        start_pos = max(0, self.total_written - distance)
        read_len = min(length, self.total_written - start_pos)
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
    def __init__(self, read_byte_fn, init_stream=True):
        self.read_byte = read_byte_fn
        if init_stream:
            self.read_byte() # Discard first 0x00 byte
            self.code = (self.read_byte() << 24) | (self.read_byte() << 16) | (self.read_byte() << 8) | self.read_byte()
            self.range = 0xFFFFFFFF
        else:
            self.code = 0
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
        self.stream_out_offset = 0
        self.block_idx = 0
        self.stream_index = 0
        self.input_hasher = hashlib.sha256()

        self.phase = "STREAM_HEADER"
        self.null_count = 0
        self.stream_start_in = 0
        self.stream_start_out = 0
        self.flags = None
        self.check_type = 0
        self.cur_block = bytearray(self.block_size)
        self.cur_block_pos = 0

        self.bh_size = 0
        self.compressed_bytes_in_block = 0
        self.expected_comp_size = None
        self.expected_uncomp_size = None
        self.block_crc32 = 0
        self.block_crc64 = 0
        self.block_sha256 = None
        self.block_uncomp_bytes = 0
        self.stream_block_records = []
        self.tot_idx = 0
        self.pending_boundary = None

        self.PROB_INIT = 1024
        self.state = 0
        self.reps = [0, 0, 0, 0]
        self.lc = 3
        self.lp = 0
        self.pb = 2
        self._reinit_probs()
        self.p_lit = [self.PROB_INIT] * (0x300 << (self.lc + self.lp))

        self.in_chunk = False
        self.ctrl = 0
        self.mode = 0
        self.chunk_uncomp_sz = 0
        self.chunk_comp_sz = 0
        self.chunk_decoded = 0
        self.chunk_compressed = b""
        self.comp_offset = 0
        self.raw_chunk = b""
        self.raw_pos = 0
        self.rd_code = 0
        self.rd_range = 0

        self.in_match_copy = False
        self.match_dist = 0
        self.match_rem = 0

    def _reinit_probs(self):
        PI = self.PROB_INIT
        self.p_is_match = [PI] * (12 << 4)
        self.p_is_rep = [PI] * 12
        self.p_is_rep_g0 = [PI] * 12
        self.p_is_rep_g1 = [PI] * 12
        self.p_is_rep_g2 = [PI] * 12
        self.p_is_rep0_long = [PI] * (12 << 4)
        self.p_pos_slot = [PI] * (4 << 6)
        self.p_spec_pos = [PI] * 115
        self.p_align = [PI] * 16
        self.p_len = [PI] * 386
        self.p_rep_len = [PI] * 386

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

    def _update_check(self, data):
        self.block_uncomp_bytes += len(data)
        if self.check_type == 1:
            self.block_crc32 = crc32(data, self.block_crc32)
        elif self.check_type == 4:
            self.block_crc64 = crc64(data, self.block_crc64)
        elif self.check_type == 10 and self.block_sha256:
            self.block_sha256.update(data)

    def export_state(self):
        return {
            "overall_input_offset": self.overall_input_offset,
            "overall_output_offset": self.overall_output_offset,
            "stream_out_offset": self.stream_out_offset,
            "block_idx": self.block_idx,
            "stream_index": self.stream_index,
            "phase": self.phase,
            "null_count": self.null_count,
            "stream_start_in": self.stream_start_in,
            "stream_start_out": self.stream_start_out,
            "flags": self.flags.hex() if self.flags else None,
            "check_type": self.check_type,
            "cur_block_pos": self.cur_block_pos,
            "cur_block": self.cur_block[:self.cur_block_pos].hex(),
            "bh_size": self.bh_size,
            "compressed_bytes_in_block": self.compressed_bytes_in_block,
            "expected_comp_size": self.expected_comp_size,
            "expected_uncomp_size": self.expected_uncomp_size,
            "block_crc32": self.block_crc32,
            "block_crc64": self.block_crc64,
            "block_uncomp_bytes": self.block_uncomp_bytes,
            "stream_block_records": self.stream_block_records,
            "tot_idx": self.tot_idx,
            "state": self.state,
            "reps": self.reps,
            "lc": self.lc,
            "lp": self.lp,
            "pb": self.pb,
            "p_is_match": self.p_is_match,
            "p_is_rep": self.p_is_rep,
            "p_is_rep_g0": self.p_is_rep_g0,
            "p_is_rep_g1": self.p_is_rep_g1,
            "p_is_rep_g2": self.p_is_rep_g2,
            "p_is_rep0_long": self.p_is_rep0_long,
            "p_pos_slot": self.p_pos_slot,
            "p_spec_pos": self.p_spec_pos,
            "p_align": self.p_align,
            "p_len": self.p_len,
            "p_rep_len": self.p_rep_len,
            "p_lit": self.p_lit,
            "in_chunk": self.in_chunk,
            "ctrl": self.ctrl,
            "mode": self.mode,
            "chunk_uncomp_sz": self.chunk_uncomp_sz,
            "chunk_comp_sz": self.chunk_comp_sz,
            "chunk_decoded": self.chunk_decoded,
            "chunk_compressed": self.chunk_compressed.hex(),
            "comp_offset": self.comp_offset,
            "raw_chunk": self.raw_chunk.hex(),
            "raw_pos": self.raw_pos,
            "rd_code": self.rd_code,
            "rd_range": self.rd_range,
            "in_match_copy": self.in_match_copy,
            "match_dist": self.match_dist,
            "match_rem": self.match_rem,
            "input_tell": self.input.tell() if hasattr(self.input, "tell") else 0,
            "history": self.history.export_state() if hasattr(self.history, "export_state") else None
        }

    def import_state(self, s):
        self.overall_input_offset = s["overall_input_offset"]
        self.overall_output_offset = s["overall_output_offset"]
        self.stream_out_offset = s["stream_out_offset"]
        self.block_idx = s["block_idx"]
        self.stream_index = s["stream_index"]
        self.phase = s["phase"]
        self.null_count = s["null_count"]
        self.stream_start_in = s["stream_start_in"]
        self.stream_start_out = s["stream_start_out"]
        self.flags = bytes.fromhex(s["flags"]) if s["flags"] else None
        self.check_type = s["check_type"]
        self.cur_block_pos = s["cur_block_pos"]
        self.cur_block = bytearray(self.block_size)
        cb = bytes.fromhex(s["cur_block"])
        self.cur_block[:len(cb)] = cb
        self.bh_size = s["bh_size"]
        self.compressed_bytes_in_block = s["compressed_bytes_in_block"]
        self.expected_comp_size = s["expected_comp_size"]
        self.expected_uncomp_size = s["expected_uncomp_size"]
        self.block_crc32 = s["block_crc32"]
        self.block_crc64 = s["block_crc64"]
        self.block_uncomp_bytes = s["block_uncomp_bytes"]
        self.stream_block_records = s["stream_block_records"]
        self.tot_idx = s.get("tot_idx", 0)
        self.state = s["state"]
        self.reps = s["reps"]
        self.lc = s["lc"]
        self.lp = s["lp"]
        self.pb = s["pb"]
        self.p_is_match = s["p_is_match"]
        self.p_is_rep = s["p_is_rep"]
        self.p_is_rep_g0 = s["p_is_rep_g0"]
        self.p_is_rep_g1 = s["p_is_rep_g1"]
        self.p_is_rep_g2 = s["p_is_rep_g2"]
        self.p_is_rep0_long = s["p_is_rep0_long"]
        self.p_pos_slot = s["p_pos_slot"]
        self.p_spec_pos = s["p_spec_pos"]
        self.p_align = s["p_align"]
        self.p_len = s["p_len"]
        self.p_rep_len = s["p_rep_len"]
        self.p_lit = s["p_lit"]
        self.in_chunk = s["in_chunk"]
        self.ctrl = s["ctrl"]
        self.mode = s["mode"]
        self.chunk_uncomp_sz = s["chunk_uncomp_sz"]
        self.chunk_comp_sz = s["chunk_comp_sz"]
        self.chunk_decoded = s["chunk_decoded"]
        self.chunk_compressed = bytes.fromhex(s["chunk_compressed"])
        self.comp_offset = s["comp_offset"]
        self.raw_chunk = bytes.fromhex(s["raw_chunk"])
        self.raw_pos = s["raw_pos"]
        self.rd_code = s["rd_code"]
        self.rd_range = s["rd_range"]
        self.in_match_copy = s["in_match_copy"]
        self.match_dist = s["match_dist"]
        self.match_rem = s["match_rem"]
        if hasattr(self.input, "seek") and "input_tell" in s:
            self.input.seek(s["input_tell"])
        if hasattr(self.history, "import_state") and s["history"]:
            self.history.import_state(s["history"])

    def decode_block(self):
        while True:
            if self.phase == "STREAM_HEADER":
                while True:
                    b = self.input.read(1)
                    if not b:
                        if self.null_count % 4 != 0:
                            raise XZFormatError("Stream padding not multiple of 4")
                        self.phase = "EOF"
                        return b"", True
                    self.overall_input_offset += 1
                    self.input_hasher.update(b)
                    if b == b"\x00":
                        self.null_count += 1
                        continue
                    if b == b"\xfd":
                        if self.null_count % 4 != 0:
                            raise XZFormatError("Stream padding not multiple of 4")
                        magic = bytearray([0xFD]) + self._read_exact(5)
                        if bytes(magic) != b"\xfd7zXZ\x00":
                            raise XZFormatError("Invalid magic")
                        break
                    raise XZFormatError(f"Unexpected byte 0x{b[0]:02x}")
                self.stream_start_in = self.overall_input_offset - 6
                self.stream_start_out = self.overall_output_offset
                self.flags = self._read_exact(2)
                if self.flags[0] != 0 or (self.flags[1] & 0xF0) != 0:
                    raise XZFormatError("Invalid flags")
                self.check_type = self.flags[1] & 0x0F
                if self.check_type not in (0, 1, 4, 10):
                    raise XZUnsupportedError(f"Unsupported check type: {self.check_type}")
                crc_bytes = self._read_exact(4)
                if crc32(self.flags) != struct.unpack("<I", crc_bytes)[0]:
                    raise XZCheckError("Header CRC32 mismatch")
                self.cur_block = bytearray(self.block_size)
                self.cur_block_pos = 0
                self.stream_out_offset = 0
                self.history.reset()
                self.stream_block_records = []
                self.phase = "BLOCK_START"

            if self.phase == "BLOCK_START":
                fb = self._read_exact(1)[0]
                if fb == 0:
                    self.phase = "INDEX"
                    continue
                self.bh_size = (fb + 1) * 4
                bh_data = self._read_exact(self.bh_size - 1)
                full_bh = bytes([fb]) + bh_data
                if crc32(full_bh[:-4]) != struct.unpack("<I", full_bh[-4:])[0]:
                    raise XZCheckError("Block header CRC mismatch")
                bflags = full_bh[1]
                if (bflags & 0x03) != 0:
                    raise XZUnsupportedError("Multi filter unsupported")
                if (bflags & 0x3C) != 0:
                    raise XZFormatError("Reserved flags non-zero")
                has_comp = bool(bflags & 0x40)
                has_uncomp = bool(bflags & 0x80)
                off = 2
                def _vli():
                    nonlocal off; val = 0; s = 0
                    for _ in range(9):
                        b = full_bh[off]; off += 1
                        val |= (b & 0x7F) << s
                        if (b & 0x80) == 0: return val
                        s += 7
                    raise XZFormatError("Invalid VLI")
                self.expected_comp_size = _vli() if has_comp else None
                self.expected_uncomp_size = _vli() if has_uncomp else None
                fid = _vli()
                if fid != 0x21:
                    raise XZUnsupportedError("Not LZMA2")
                psz = _vli()
                if psz != 1:
                    raise XZFormatError("Invalid prop size")
                pbyte = full_bh[off]; off += 1
                dict_bits = pbyte & 0x3F
                if dict_bits > 40:
                    raise XZFormatError("Invalid dict bits")
                while off < len(full_bh) - 4:
                    if full_bh[off] != 0:
                        raise XZFormatError("Non-zero header padding")
                    off += 1

                self.block_crc32 = 0
                self.block_crc64 = 0
                self.block_sha256 = hashlib.sha256() if self.check_type == 10 else None
                self.block_uncomp_bytes = 0
                self.compressed_bytes_in_block = 0
                self.in_chunk = False
                self.phase = "CHUNKS"

            if self.phase == "CHUNKS":
                while True:
                    if not self.in_chunk:
                        self.ctrl = self._read_exact(1)[0]
                        self.compressed_bytes_in_block += 1
                        if self.ctrl == 0:
                            self.phase = "BLOCK_END"
                            break
                        elif self.ctrl in (1, 2):
                            if self.ctrl == 1:
                                self.history.reset()
                                self.reps = [0, 0, 0, 0]
                            sz_b = self._read_exact(2)
                            self.compressed_bytes_in_block += 2
                            csz = ((sz_b[0] << 8) | sz_b[1]) + 1
                            self.raw_chunk = self._read_exact(csz)
                            self.compressed_bytes_in_block += csz
                            self.raw_pos = 0
                            self.in_chunk = True
                        elif self.ctrl >= 0x80:
                            self.mode = (self.ctrl >> 5) & 3
                            uh = (self.ctrl & 0x1F) << 16
                            s12 = self._read_exact(2)
                            s34 = self._read_exact(2)
                            self.compressed_bytes_in_block += 4
                            self.chunk_uncomp_sz = (uh | (s12[0] << 8) | s12[1]) + 1
                            self.chunk_comp_sz = ((s34[0] << 8) | s34[1]) + 1
                            if self.mode >= 1:
                                self.state = 0
                                self._reinit_probs()
                            if self.mode >= 2:
                                pbyte = self._read_exact(1)[0]
                                self.compressed_bytes_in_block += 1
                                self.pb = pbyte // 45
                                rem = pbyte % 45
                                self.lp = rem // 9
                                self.lc = rem % 9
                                self.p_lit = [self.PROB_INIT] * (0x300 << (self.lc + self.lp))
                            if self.mode == 3:
                                self.history.reset()
                                self.reps = [0, 0, 0, 0]
                            self.chunk_compressed = self._read_exact(self.chunk_comp_sz)
                            self.compressed_bytes_in_block += self.chunk_comp_sz
                            c_off = 0
                            def _get_b():
                                nonlocal c_off
                                if c_off < len(self.chunk_compressed):
                                    b = self.chunk_compressed[c_off]; c_off += 1; return b
                                return 0
                            rd_init = RangeDecoder(_get_b, init_stream=True)
                            self.rd_code = rd_init.code
                            self.rd_range = rd_init.range
                            self.comp_offset = c_off
                            self.chunk_decoded = 0
                            self.in_chunk = True

                    if self.ctrl in (1, 2):
                        while self.raw_pos < len(self.raw_chunk):
                            sp = self.block_size - self.cur_block_pos
                            take = min(sp, len(self.raw_chunk) - self.raw_pos)
                            self.cur_block[self.cur_block_pos : self.cur_block_pos + take] = self.raw_chunk[self.raw_pos : self.raw_pos + take]
                            self.cur_block_pos += take
                            self.raw_pos += take
                            if self.cur_block_pos == self.block_size:
                                committed = bytes(self.cur_block)
                                self._update_check(committed)
                                self.history.append_block(committed, self.block_idx)
                                self.overall_output_offset += len(committed)
                                self.stream_out_offset += len(committed)
                                self.block_idx += 1
                                self.cur_block_pos = 0
                                if self.raw_pos == len(self.raw_chunk):
                                    self.in_chunk = False
                                return committed, False
                        self.in_chunk = False

                    elif self.ctrl >= 0x80:
                        c_off = self.comp_offset
                        comp_bytes = self.chunk_compressed
                        def _rd_get():
                            nonlocal c_off
                            if c_off < len(comp_bytes):
                                b = comp_bytes[c_off]; c_off += 1; return b
                            return 0
                        rd = RangeDecoder(_rd_get, init_stream=False)
                        rd.code = self.rd_code
                        rd.range = self.rd_range

                        pos_mask = (1 << self.pb) - 1
                        lp_mask = (1 << self.lp) - 1

                        while self.chunk_decoded < self.chunk_uncomp_sz:
                            if self.in_match_copy:
                                dist = self.match_dist
                                while self.match_rem > 0:
                                    sp = self.block_size - self.cur_block_pos
                                    copy_len = min(sp, self.match_rem)
                                    if dist < self.cur_block_pos:
                                        src_s = self.cur_block_pos - 1 - dist
                                        if dist == 0:
                                            self.cur_block[self.cur_block_pos : self.cur_block_pos + copy_len] = bytes([self.cur_block[src_s]]) * copy_len
                                        elif dist >= copy_len:
                                            self.cur_block[self.cur_block_pos : self.cur_block_pos + copy_len] = self.cur_block[src_s : src_s + copy_len]
                                        else:
                                            for i in range(copy_len):
                                                self.cur_block[self.cur_block_pos + i] = self.cur_block[src_s + i]
                                    else:
                                        h_dist = dist - self.cur_block_pos + 1
                                        h_req = min(copy_len, h_dist)
                                        hs = self.history.get_history_slice(h_dist, h_req)
                                        self.cur_block[self.cur_block_pos : self.cur_block_pos + len(hs)] = hs
                                        cpd = len(hs)
                                        if cpd < copy_len:
                                            for i in range(copy_len - cpd):
                                                self.cur_block[self.cur_block_pos + cpd + i] = self.cur_block[i]
                                    self.cur_block_pos += copy_len
                                    self.chunk_decoded += copy_len
                                    self.match_rem -= copy_len
                                    if self.cur_block_pos == self.block_size:
                                        committed = bytes(self.cur_block)
                                        self._update_check(committed)
                                        self.history.append_block(committed, self.block_idx)
                                        self.overall_output_offset += len(committed)
                                        self.stream_out_offset += len(committed)
                                        self.block_idx += 1
                                        self.cur_block_pos = 0
                                        self.rd_code = rd.code
                                        self.rd_range = rd.range
                                        self.comp_offset = c_off
                                        if self.match_rem == 0:
                                            self.in_match_copy = False
                                        return committed, False
                                self.in_match_copy = False

                            pos_state = (self.stream_out_offset + self.cur_block_pos) & pos_mask
                            match_idx = (self.state << 4) + pos_state
                            if rd.decode_bit(self.p_is_match, match_idx) == 0:
                                if self.cur_block_pos > 0:
                                    prev_b = self.cur_block[self.cur_block_pos - 1]
                                else:
                                    ps = self.history.get_history_slice(1, 1)
                                    prev_b = ps[0] if ps else 0
                                lit_st = (((self.stream_out_offset + self.cur_block_pos) & lp_mask) << self.lc) + (prev_b >> (8 - self.lc))
                                lit_off = lit_st * 0x300
                                symbol = 1
                                if self.state >= 7:
                                    dist = self.reps[0]
                                    if dist < self.cur_block_pos:
                                        mb = self.cur_block[self.cur_block_pos - 1 - dist]
                                    else:
                                        ms = self.history.get_history_slice(dist - self.cur_block_pos + 1, 1)
                                        mb = ms[0] if ms else 0
                                    while symbol < 0x100:
                                        mbit = (mb >> 7) & 1
                                        mb = (mb << 1) & 0xFF
                                        bit = rd.decode_bit(self.p_lit, lit_off + ((1 + mbit) << 8) + symbol)
                                        symbol = (symbol << 1) | bit
                                        if mbit != bit: break
                                while symbol < 0x100:
                                    symbol = (symbol << 1) | rd.decode_bit(self.p_lit, lit_off + symbol)
                                self.cur_block[self.cur_block_pos] = symbol - 0x100
                                self.cur_block_pos += 1
                                self.chunk_decoded += 1
                                self.state = [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 4, 5][self.state]

                                if self.cur_block_pos == self.block_size:
                                    committed = bytes(self.cur_block)
                                    self._update_check(committed)
                                    self.history.append_block(committed, self.block_idx)
                                    self.overall_output_offset += len(committed)
                                    self.stream_out_offset += len(committed)
                                    self.block_idx += 1
                                    self.cur_block_pos = 0
                                    self.rd_code = rd.code
                                    self.rd_range = rd.range
                                    self.comp_offset = c_off
                                    if self.chunk_decoded == self.chunk_uncomp_sz:
                                        self.in_chunk = False
                                    return committed, False
                            else:
                                if rd.decode_bit(self.p_is_rep, self.state) == 1:
                                    if rd.decode_bit(self.p_is_rep_g0, self.state) == 0:
                                        if rd.decode_bit(self.p_is_rep0_long, match_idx) == 0:
                                            self.state = 9 if self.state < 7 else 11
                                            length = 1
                                            dist = self.reps[0]
                                            if dist < self.cur_block_pos:
                                                self.cur_block[self.cur_block_pos] = self.cur_block[self.cur_block_pos - 1 - dist]
                                            else:
                                                ms = self.history.get_history_slice(dist - self.cur_block_pos + 1, 1)
                                                self.cur_block[self.cur_block_pos] = ms[0] if ms else 0
                                            self.cur_block_pos += 1
                                            self.chunk_decoded += 1
                                            if self.cur_block_pos == self.block_size:
                                                committed = bytes(self.cur_block)
                                                self._update_check(committed)
                                                self.history.append_block(committed, self.block_idx)
                                                self.overall_output_offset += len(committed)
                                                self.stream_out_offset += len(committed)
                                                self.block_idx += 1
                                                self.cur_block_pos = 0
                                                self.rd_code = rd.code
                                                self.rd_range = rd.range
                                                self.comp_offset = c_off
                                                if self.chunk_decoded == self.chunk_uncomp_sz:
                                                    self.in_chunk = False
                                                return committed, False
                                            continue
                                    else:
                                        if rd.decode_bit(self.p_is_rep_g1, self.state) == 0:
                                            dist = self.reps[1]
                                        else:
                                            if rd.decode_bit(self.p_is_rep_g2, self.state) == 0:
                                                dist = self.reps[2]
                                            else:
                                                dist = self.reps[3]
                                                self.reps[3] = self.reps[2]
                                            self.reps[2] = self.reps[1]
                                        self.reps[1] = self.reps[0]
                                        self.reps[0] = dist
                                    length = 2 + decode_len_val(rd, self.p_rep_len, pos_state)
                                    self.state = 8 if self.state < 7 else 11
                                else:
                                    self.reps[3] = self.reps[2]
                                    self.reps[2] = self.reps[1]
                                    self.reps[1] = self.reps[0]
                                    self.state = 7 if self.state < 7 else 10
                                    length = 2 + decode_len_val(rd, self.p_len, pos_state)
                                    l_st = min(length - 2, 3)
                                    slot = rd.decode_bittree(self.p_pos_slot, l_st << 6, 6)
                                    if slot < 4:
                                        dist = slot
                                    else:
                                        nd = (slot >> 1) - 1
                                        base = (2 | (slot & 1)) << nd
                                        if slot < 14:
                                            dist = base + rd.decode_reverse_bittree(self.p_spec_pos, base - slot, nd)
                                        else:
                                            db = rd.decode_direct_bits(nd - 4) << 4
                                            ab = rd.decode_reverse_bittree(self.p_align, 0, 4)
                                            dist = base + db + ab
                                    self.reps[0] = dist

                                self.in_match_copy = True
                                self.match_dist = dist
                                self.match_rem = length
                                while self.match_rem > 0:
                                    sp = self.block_size - self.cur_block_pos
                                    copy_len = min(sp, self.match_rem)
                                    if dist < self.cur_block_pos:
                                        src_s = self.cur_block_pos - 1 - dist
                                        if dist == 0:
                                            self.cur_block[self.cur_block_pos : self.cur_block_pos + copy_len] = bytes([self.cur_block[src_s]]) * copy_len
                                        elif dist >= copy_len:
                                            self.cur_block[self.cur_block_pos : self.cur_block_pos + copy_len] = self.cur_block[src_s : src_s + copy_len]
                                        else:
                                            for i in range(copy_len):
                                                self.cur_block[self.cur_block_pos + i] = self.cur_block[src_s + i]
                                    else:
                                        h_dist = dist - self.cur_block_pos + 1
                                        h_req = min(copy_len, h_dist)
                                        hs = self.history.get_history_slice(h_dist, h_req)
                                        self.cur_block[self.cur_block_pos : self.cur_block_pos + len(hs)] = hs
                                        cpd = len(hs)
                                        if cpd < copy_len:
                                            for i in range(copy_len - cpd):
                                                self.cur_block[self.cur_block_pos + cpd + i] = self.cur_block[i]
                                    self.cur_block_pos += copy_len
                                    self.chunk_decoded += copy_len
                                    self.match_rem -= copy_len
                                    if self.cur_block_pos == self.block_size:
                                        committed = bytes(self.cur_block)
                                        self._update_check(committed)
                                        self.history.append_block(committed, self.block_idx)
                                        self.overall_output_offset += len(committed)
                                        self.stream_out_offset += len(committed)
                                        self.block_idx += 1
                                        self.cur_block_pos = 0
                                        self.rd_code = rd.code
                                        self.rd_range = rd.range
                                        self.comp_offset = c_off
                                        if self.match_rem == 0:
                                            self.in_match_copy = False
                                        return committed, False
                                self.in_match_copy = False

                        self.in_chunk = False
                        self.rd_code = rd.code
                        self.rd_range = rd.range
                        self.comp_offset = c_off

            if self.phase == "BLOCK_END":
                if self.cur_block_pos > 0:
                    partial = bytes(self.cur_block[:self.cur_block_pos])
                    self._update_check(partial)
                    self.history.append_block(partial, self.block_idx)
                    self.overall_output_offset += len(partial)
                    self.stream_out_offset += len(partial)
                    self.block_idx += 1
                    self.cur_block_pos = 0
                    return partial, False

                if self.expected_comp_size is not None and self.compressed_bytes_in_block != self.expected_comp_size:
                    raise XZFormatError("Block compressed size mismatch")
                if self.expected_uncomp_size is not None and self.block_uncomp_bytes != self.expected_uncomp_size:
                    raise XZFormatError("Block uncompressed size mismatch")

                pad_len = (4 - (self.compressed_bytes_in_block % 4)) % 4
                if pad_len > 0:
                    self._read_exact(pad_len)
                chk_sz = {0: 0, 1: 4, 4: 8, 10: 32}.get(self.check_type, 0)
                if chk_sz > 0:
                    chk_b = self._read_exact(chk_sz)
                    if self.check_type == 1:
                        if self.block_crc32 != struct.unpack("<I", chk_b)[0]:
                            raise XZCheckError("Block CRC32 mismatch")
                    elif self.check_type == 4:
                        if self.block_crc64 != struct.unpack("<Q", chk_b)[0]:
                            raise XZCheckError("Block CRC64 mismatch")
                    elif self.check_type == 10:
                        if self.block_sha256.digest() != chk_b:
                            raise XZCheckError("Block SHA-256 mismatch")
                unpad_sz = self.bh_size + self.compressed_bytes_in_block + chk_sz
                self.stream_block_records.append((unpad_sz, self.block_uncomp_bytes))
                self.phase = "BLOCK_START"

            if self.phase == "INDEX":
                idx_b = bytearray([0x00])
                def _vli():
                    val = 0; s = 0
                    for _ in range(9):
                        b = self._read_exact(1)[0]; idx_b.append(b)
                        val |= (b & 0x7F) << s
                        if (b & 0x80) == 0: return val
                        s += 7
                    raise XZFormatError("Invalid VLI")
                n_rec = _vli()
                if n_rec != len(self.stream_block_records):
                    raise XZFormatError("Index record mismatch")
                for r_idx in range(n_rec):
                    u_sz = _vli(); unc_sz = _vli()
                    eu, eunc = self.stream_block_records[r_idx]
                    if u_sz != eu or unc_sz != eunc:
                        raise XZFormatError("Index record size mismatch")
                pad = (4 - (len(idx_b) % 4)) % 4
                if pad > 0:
                    pb = self._read_exact(pad)
                    idx_b.extend(pb)
                icrc = struct.unpack("<I", self._read_exact(4))[0]
                if crc32(idx_b) != icrc:
                    raise XZCheckError("Index CRC mismatch")
                self.tot_idx = len(idx_b) + 4
                self.phase = "STREAM_FOOTER"

            if self.phase == "STREAM_FOOTER":
                footer = self._read_exact(12)
                if not footer.endswith(b"YZ"):
                    raise XZFormatError("Footer magic invalid")
                if crc32(footer[4:10]) != struct.unpack("<I", footer[:4])[0]:
                    raise XZCheckError("Footer CRC mismatch")
                bs = struct.unpack("<I", footer[4:8])[0]
                if self.tot_idx != (bs + 1) * 4:
                    raise XZFormatError("Backward size mismatch")
                if footer[8:10] != self.flags:
                    raise XZFormatError("Footer flags mismatch")
                self.pending_boundary = StreamBoundaryEvent(
                    stream_index=self.stream_index,
                    next_stream_index=self.stream_index + 1,
                    input_stream_len=self.overall_input_offset - self.stream_start_in,
                    overall_input_offset=self.overall_input_offset,
                    output_stream_len=self.overall_output_offset - self.stream_start_out,
                    overall_output_offset=self.overall_output_offset
                )
                self.stream_index += 1
                self.phase = "STREAM_PADDING"
                self.null_count = 0

            if self.phase == "STREAM_PADDING":
                self.phase = "STREAM_HEADER"

    def decode_events(self):
        while True:
            blk, is_eof = self.decode_block()
            if blk:
                yield ChunkOutputEvent(blk)
                yield BlockCommittedEvent(self.block_idx - 1, blk, self.export_state())
                yield ProgressEvent(self.block_idx - 1, len(blk), self.overall_output_offset)
            if self.pending_boundary:
                yield self.pending_boundary
                self.pending_boundary = None
            if is_eof:
                break


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


    def test_interleaved_resumption_with_serialized_state(self):
        lzma = self.lzma
        import random, io, json
        prng_a = random.Random(42)
        prng_b = random.Random(1337)

        raw_a = bytearray()
        for _ in range(200):
            raw_a.extend(prng_a.randbytes(500))
            raw_a.extend(b"REPETITIVE_PATTERN_ALPHA_" * 20)
        raw_a = bytes(raw_a)

        raw_b = bytearray()
        for _ in range(200):
            raw_b.extend(prng_b.randbytes(500))
            raw_b.extend(b"REPETITIVE_PATTERN_BETA__" * 20)
        raw_b = bytes(raw_b)

        xz_a = lzma.compress(raw_a, preset=9 | lzma.PRESET_EXTREME)
        xz_b = lzma.compress(raw_b, preset=1)

        stream_a_io = io.BytesIO(xz_a)
        stream_b_io = io.BytesIO(xz_b)

        state_a = None
        state_b = None
        eof_a = False
        eof_b = False
        out_a = bytearray()
        out_b = bytearray()

        round_num = 0
        while not (eof_a and eof_b):
            round_num += 1
            if not eof_a:
                engine_a = XZStreamDecompressor(stream_a_io)
                if state_a is not None:
                    engine_a.import_state(state_a)
                blk_a, eof_a = engine_a.decode_block()
                if blk_a:
                    out_a.extend(blk_a)
                state_a_json = json.dumps(engine_a.export_state())
                del engine_a
                state_a = json.loads(state_a_json)

            if not eof_b:
                engine_b = XZStreamDecompressor(stream_b_io)
                if state_b is not None:
                    engine_b.import_state(state_b)
                blk_b, eof_b = engine_b.decode_block()
                if blk_b:
                    out_b.extend(blk_b)
                state_b_json = json.dumps(engine_b.export_state())
                del engine_b
                state_b = json.loads(state_b_json)

        self.assertEqual(bytes(out_a), raw_a)
        self.assertEqual(bytes(out_b), raw_b)
        self.assertGreater(round_num, 3)

    def test_block_header_parsing_and_filter_properties(self):
        lzma = self.lzma
        import io
        data = b"BLOCK_HEADER_FILTER_PROP_TEST_" * 30
        for preset in [0, 1, 6]:
            xz_data = lzma.compress(data, preset=preset)
            engine = XZStreamDecompressor(io.BytesIO(xz_data))
            out = b"".join(ev.data for ev in engine.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
            self.assertEqual(out, data)

        xz_tampered = bytearray(lzma.compress(data))
        xz_tampered[13] = 0x01
        h_sz = (xz_tampered[12] + 1) * 4
        h_crc = crc32(xz_tampered[12 : 12 + h_sz - 4])
        xz_tampered[12 + h_sz - 4 : 12 + h_sz] = struct.pack("<I", h_crc)
        with self.assertRaises(XZUnsupportedError):
            engine = XZStreamDecompressor(io.BytesIO(bytes(xz_tampered)))
            list(engine.decode_events())

    def test_block_checks_and_corruption_detection(self):
        lzma = self.lzma
        import io
        data = b"CORRUPTION_INTEGRITY_CHECK_TEST_" * 40
        for chk in [lzma.CHECK_CRC32, lzma.CHECK_CRC64, lzma.CHECK_SHA256, lzma.CHECK_NONE]:
            xz_data = lzma.compress(data, check=chk)
            engine = XZStreamDecompressor(io.BytesIO(xz_data))
            out = b"".join(ev.data for ev in engine.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
            self.assertEqual(out, data)

        xz_data_crc = bytearray(lzma.compress(data, check=lzma.CHECK_CRC32))
        xz_data_crc[-25] ^= 0x55
        with self.assertRaises((XZCheckError, XZFormatError, ValueError)):
            engine = XZStreamDecompressor(io.BytesIO(bytes(xz_data_crc)))
            list(engine.decode_events())

    def test_rfc212_stream_padding_alignment(self):
        lzma = self.lzma
        import io
        data = b"PADDING_ALIGNMENT_TEST_" * 20
        xz1 = lzma.compress(data)
        xz2 = lzma.compress(data)

        for valid_pad in [0, 4, 8]:
            stream = xz1 + (b"\x00" * valid_pad) + xz2
            engine = XZStreamDecompressor(io.BytesIO(stream))
            out = b"".join(ev.data for ev in engine.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
            self.assertEqual(out, data + data)

        for invalid_pad in [1, 2, 3]:
            stream = xz1 + (b"\x00" * invalid_pad) + xz2
            with self.assertRaises(XZFormatError):
                engine = XZStreamDecompressor(io.BytesIO(stream))
                list(engine.decode_events())

    def test_multiblock_and_lzma2_resets(self):
        import io
        b1_data = b"BLOCK_ONE_TEST_STRING_12345" * 10
        b2_data = b"BLOCK_TWO_TEST_STRING_67890" * 10

        def encode_vli(val):
            res = bytearray()
            while val >= 0x80:
                res.append((val & 0x7F) | 0x80)
                val >>= 7
            res.append(val & 0x7F)
            return bytes(res)

        def make_uncomp_chunk(data, reset=True):
            ctrl = 0x01 if reset else 0x02
            sz = len(data) - 1
            return bytes([ctrl, (sz >> 8) & 0xFF, sz & 0xFF]) + data + b"\x00"

        def build_multiblock(blocks, check_type=4):
            stream_flags = bytes([0x00, check_type & 0x0F])
            header = b"\xfd7zXZ\x00" + stream_flags + struct.pack("<I", crc32(stream_flags))
            stream_body = bytearray()
            index_records = []
            for uncomp_data, comp_payload in blocks:
                bh_inner = bytearray([0x00])
                bh_inner.extend(encode_vli(0x21))
                bh_inner.extend(encode_vli(1))
                bh_inner.append(22)
                target_len = ((len(bh_inner) + 1 + 4 + 3) // 4) * 4
                pad_needed = target_len - (len(bh_inner) + 1 + 4)
                bh_inner.extend(b"\x00" * pad_needed)
                first_byte = (target_len // 4) - 1
                bh_no_crc = bytes([first_byte]) + bytes(bh_inner)
                bh = bh_no_crc + struct.pack("<I", crc32(bh_no_crc))
                block_pad_len = (4 - (len(comp_payload) % 4)) % 4
                block_pad = b"\x00" * block_pad_len
                check_bytes = struct.pack("<Q", crc64(uncomp_data))
                stream_body.extend(bh + comp_payload + block_pad + check_bytes)
                unpadded_size = len(bh) + len(comp_payload) + len(check_bytes)
                index_records.append((unpadded_size, len(uncomp_data)))
            idx_body = bytearray([0x00])
            idx_body.extend(encode_vli(len(index_records)))
            for u_sz, unc_sz in index_records:
                idx_body.extend(encode_vli(u_sz))
                idx_body.extend(encode_vli(unc_sz))
            idx_pad_len = (4 - (len(idx_body) % 4)) % 4
            idx_body.extend(b"\x00" * idx_pad_len)
            idx_bytes = bytes(idx_body) + struct.pack("<I", crc32(idx_body))
            backward_size = (len(idx_bytes) // 4) - 1
            footer_mid = struct.pack("<I", backward_size) + stream_flags
            footer = struct.pack("<I", crc32(footer_mid)) + footer_mid + b"YZ"
            return header + bytes(stream_body) + idx_bytes + footer

        c1 = make_uncomp_chunk(b1_data, reset=True)
        c2 = make_uncomp_chunk(b2_data, reset=True)
        stream = build_multiblock([(b1_data, c1), (b2_data, c2)], check_type=4)

        engine = XZStreamDecompressor(io.BytesIO(stream))
        out = b"".join(ev.data for ev in engine.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
        self.assertEqual(out, b1_data + b2_data)

    def test_index_and_stream_footer_integrity(self):
        lzma = self.lzma
        import io
        data = b"INDEX_FOOTER_INTEGRITY_" * 50
        xz_data = bytearray(lzma.compress(data))

        xz_bad_footer = bytearray(xz_data)
        xz_bad_footer[-12] ^= 0xFF
        with self.assertRaises(XZCheckError):
            engine = XZStreamDecompressor(io.BytesIO(bytes(xz_bad_footer)))
            list(engine.decode_events())

        xz_bad_bs = bytearray(xz_data)
        xz_bad_bs[-8] ^= 0x01
        new_mid = xz_bad_bs[-8:-2]
        xz_bad_bs[-12:-8] = struct.pack("<I", crc32(new_mid))
        with self.assertRaises(XZFormatError):
            engine = XZStreamDecompressor(io.BytesIO(bytes(xz_bad_bs)))
            list(engine.decode_events())

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
