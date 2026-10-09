#!/usr/bin/env python3
"""
xz_decompressor - Pure-Python XZ / LZMA2 Streaming Decompression Engine.

Zero required external dependencies, apart from --test mode.

Normal operation compatible with MicroPython, PyPy, and CPython.
"""

import sys
import os
import struct
import hashlib
import json
import io
try:
    from pathlib import Path
except ImportError:
    class _PathFallback:
        def __init__(self, p):
            self._p = str(p)
        def __str__(self): return self._p
        def __fspath__(self): return self._p
        @property
        def parent(self): return _PathFallback(os.path.dirname(self._p) or ".")
        @property
        def name(self): return os.path.basename(self._p)
        @property
        def stem(self):
            n = self.name
            return n.rsplit(".", 1)[0] if "." in n else n
        @property
        def suffix(self):
            n = self.name
            return ("." + n.rsplit(".", 1)[1]) if "." in n else ""
        def resolve(self): return _PathFallback(os.path.abspath(self._p))
        def exists(self): return os.path.exists(self._p)
        def is_dir(self): return os.path.isdir(self._p)
        def is_file(self): return os.path.isfile(self._p)
        def mkdir(self, parents=False, exist_ok=False):
            try:
                os.makedirs(self._p)
            except OSError:
                if not exist_ok: raise
        def unlink(self, missing_ok=False):
            try:
                os.remove(self._p)
            except OSError:
                if not missing_ok: raise
        def rmdir(self):
            try: os.rmdir(self._p)
            except OSError: pass
        def read_bytes(self):
            with open(self._p, "rb") as f: return f.read()
        def write_bytes(self, b):
            with open(self._p, "wb") as f: return f.write(b)
        def read_text(self, encoding="utf-8"):
            with open(self._p, "r", encoding=encoding) as f: return f.read()
        def write_text(self, s, encoding="utf-8"):
            with open(self._p, "w", encoding=encoding) as f: return f.write(s)
        def stat(self): return os.stat(self._p)
        def glob(self, pat):
            import fnmatch
            res = []
            try:
                for fn in os.listdir(self._p):
                    if fnmatch.fnmatch(fn, pat):
                        res.append(_PathFallback(os.path.join(self._p, fn)))
            except OSError:
                pass
            return res
        def __truediv__(self, other):
            return _PathFallback(os.path.join(self._p, str(other)))
    Path = _PathFallback

# MicroPython native machine code decorator shim
try:
    import micropython
except ImportError:
    class _MicroPythonShim:
        @staticmethod
        def native(fn):
            return fn
        @staticmethod
        def viper(fn):
            return fn
    micropython = _MicroPythonShim()

# --- Pure-Python CRC32 (Standard IEEE 802.3 / XZ) ---
CRC32_TABLE = []
for _i in range(256):
    _c = _i
    for _ in range(8):
        _c = (_c >> 1) ^ 0xEDB88320 if (_c & 1) else (_c >> 1)
    CRC32_TABLE.append(_c)

@micropython.native
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

@micropython.native
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

class XZResourceError(RuntimeError):
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
    def __init__(self, stream_index, next_stream_index=None,
                 input_stream_len=0, overall_input_offset=0,
                 output_stream_len=0, overall_output_offset=0, has_next=False):
        self.type = XZEventType.STREAM_BOUNDARY
        self.stream_index = stream_index
        self.next_stream_index = next_stream_index
        self.input_stream_len = input_stream_len
        self.overall_input_offset = overall_input_offset
        self.output_stream_len = output_stream_len
        self.overall_output_offset = overall_output_offset
        self.has_next = has_next

class ProgressEvent:
    def __init__(self, block_index, bytes_decoded, total_emitted):
        self.type = XZEventType.PROGRESS
        self.block_index = block_index
        self.bytes_decoded = bytes_decoded
        self.total_emitted = total_emitted


# --- Low-Memory, MicroPython & Storage Adaptation Helpers ---

def parse_memory_size(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    val_str = str(val).strip().lower()
    if val_str.endswith("k") or val_str.endswith("kb") or val_str.endswith("kib"):
        num_part = val_str.rstrip("kib")
        return int(float(num_part) * 1024)
    elif val_str.endswith("m") or val_str.endswith("mb") or val_str.endswith("mib"):
        num_part = val_str.rstrip("mib")
        return int(float(num_part) * 1024 * 1024)
    elif val_str.endswith("g") or val_str.endswith("gb") or val_str.endswith("gib"):
        num_part = val_str.rstrip("gib")
        return int(float(num_part) * 1024 * 1024 * 1024)
    return int(float(val_str))

def get_available_memory():
    # 1. MicroPython / CircuitPython free heap
    try:
        import gc
        if hasattr(gc, "mem_free"):
            return gc.mem_free()
    except Exception:
        pass
    # 2. Linux / POSIX sysconf
    try:
        pages = os.sysconf("SC_AVPHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        return pages * page_size
    except Exception:
        pass
    # 3. /proc/meminfo
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except Exception:
        pass
    # Fallback to generous 1 GB if unprobed
    return 1024 * 1024 * 1024

def find_preferred_storage_dir(prefix="xz_history"):
    # Priority 1: PSRAM / RAM-disk VFS mount points (highest performance, zero flash wear)
    psram_candidates = ["/psram", "/ramdisk", "/vfs_ram", "/tmp"]
    for cand in psram_candidates:
        p = Path(cand)
        try:
            if p.is_dir() and os.access(str(p), os.W_OK):
                target = p / prefix
                target.mkdir(parents=True, exist_ok=True)
                return str(target), "psram"
        except Exception:
            pass

    # Priority 2: External flash / SD card / eMMC (MicroPython / CircuitPython default over onboard flash)
    ext_candidates = ["/sd", "/sdcard", "/emmc", "/external"]
    for cand in ext_candidates:
        p = Path(cand)
        try:
            if p.is_dir() and os.access(str(p), os.W_OK):
                target = p / prefix
                target.mkdir(parents=True, exist_ok=True)
                return str(target), "external_flash"
        except Exception:
            pass

    # Priority 3: Local scratch directory
    target = Path(f".{prefix}").resolve()
    target.mkdir(parents=True, exist_ok=True)
    return str(target), "local"

def select_block_size(block_size_choice="auto", available_mem=None):
    if block_size_choice not in (None, "auto"):
        return parse_memory_size(block_size_choice)

    if available_mem is None:
        available_mem = get_available_memory()

    # On devices with less than 256KB of python-available RAM, default to 8KiB rather than 64KiB
    if available_mem < 256 * 1024:
        return 8192
    return 65536

def select_history_backend(backend_choice="auto", storage_dir=None, available_mem=None, max_history=67108864):
    if available_mem is None:
        available_mem = get_available_memory()

    if backend_choice == "memory":
        return MemoryHistory(max_history=max_history)
    elif backend_choice == "file":
        if storage_dir is None:
            s_dir, _ = find_preferred_storage_dir()
        else:
            s_dir = storage_dir
        return FileHistory(Path(s_dir) / "history.bin", max_history=max_history)
    elif backend_choice == "directory":
        if storage_dir is None:
            s_dir, _ = find_preferred_storage_dir()
        else:
            s_dir = storage_dir
        return DirectoryBlockHistoryStore(s_dir)

    # Automatic selection based on memory threshold:
    # When python-available memory is less than 72MB (72 * 1024 * 1024 bytes), automatically switch from MemoryHistory to DirectoryBlockHistoryStore or FileHistory
    if available_mem < 72 * 1024 * 1024:
        s_dir, stype = find_preferred_storage_dir() if storage_dir is None else (storage_dir, "custom")
        return DirectoryBlockHistoryStore(s_dir)
    else:
        return MemoryHistory(max_history=max_history)

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
    persistent = False

    def __init__(self, max_history=67108864):
        self.max_history = max_history
        self.history = bytearray()
        self.checkpoints = {}

    def append_block(self, block_data, block_index=0, input_hash=""):
        self.history.extend(block_data)
        if len(self.history) > self.max_history * 2:
            del self.history[:len(self.history) - self.max_history]

    def history_len(self):
        return min(len(self.history), self.max_history)

    def get_history_slice(self, distance, length):
        h_len = self.history_len()
        if distance <= 0 or distance > h_len or length <= 0 or length > distance:
            raise XZFormatError(f"Invalid history reference: distance={distance}, length={length}, available={h_len}")
        start_idx = len(self.history) - distance
        end_idx = start_idx + length
        return bytes(self.history[start_idx:end_idx])

    def tail_crc32(self, n=65536):
        avail = min(n, len(self.history))
        if avail <= 0: return 0
        return crc32(bytes(self.history[-avail:]))

    def truncate_to(self, history_total):
        if len(self.history) > history_total:
            del self.history[history_total:]

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
        self.checkpoints.clear()

    def cleanup(self):
        self.history = bytearray()
        self.checkpoints.clear()

class DirectoryBlockHistoryStore(HistoryBackend):
    """
    Fixed 64 KiB page-based storage (HISTORY_PAGE = 65536) conforming to §3.2A.
    Decoupled from engine block size (4-64 KiB).
    """
    persistent = True
    HISTORY_PAGE = 65536

    def __init__(self, work_dir, max_history=67108864, max_blocks=None):
        self.work_dir = Path(work_dir).resolve()
        self.work_dir.mkdir(parents=True, exist_ok=True)
        if max_blocks is not None:
            self.max_pages = max_blocks
            self.max_history = max_blocks * self.HISTORY_PAGE
        else:
            self.max_history = max_history
            self.max_pages = (max_history // self.HISTORY_PAGE) + 1 # 1025
        self.total_history_written = 0
        self.page_files = {}   # page_index -> Path
        self.block_files = self.page_files # backward compatible alias
        self.state_files = {}  # block_index -> Path
        self.lru_cache = {}    # page_index -> bytearray
        self.lru_order = []
        self._scan_existing()

    def _scan_existing(self):
        self.page_files.clear()
        self.state_files.clear()
        for p in sorted(self.work_dir.glob("*_*.page")):
            parts = p.stem.split("_")
            if len(parts) >= 2 and parts[-1].isdigit():
                idx = int(parts[-1])
                self.page_files[idx] = p
        for p in sorted(self.work_dir.glob("*_*.block")):
            parts = p.stem.split("_")
            if len(parts) >= 2 and parts[-1].isdigit():
                idx = int(parts[-1])
                self.page_files[idx] = p
        for p in sorted(self.work_dir.glob("*_*.state")):
            parts = p.stem.split("_")
            if len(parts) >= 2 and parts[-1].isdigit():
                idx = int(parts[-1])
                self.state_files[idx] = p

    def append_block(self, block_data: bytes, block_index: int = 0, input_hash: str = "0000000000000000") -> None:
        rem = len(block_data)
        off_in_block = 0
        while rem > 0:
            page_idx = self.total_history_written // self.HISTORY_PAGE
            page_off = self.total_history_written % self.HISTORY_PAGE
            space = self.HISTORY_PAGE - page_off
            take = min(rem, space)
            chunk = block_data[off_in_block : off_in_block + take]

            page_name = f"{input_hash}_{page_idx:08d}.page"
            page_path = self.work_dir / page_name

            if not page_path.exists():
                page_path.write_bytes(b"")
            with open(str(page_path), "r+b") as f:
                f.seek(page_off)
                f.write(chunk)
                f.flush()
            self.page_files[page_idx] = page_path

            if page_idx not in self.lru_cache:
                self.lru_cache[page_idx] = bytearray()
                self.lru_order.append(page_idx)
                if len(self.lru_order) > 4:
                    old_idx = self.lru_order.pop(0)
                    self.lru_cache.pop(old_idx, None)
            
            p_buf = self.lru_cache[page_idx]
            if len(p_buf) < page_off:
                p_buf[:] = page_path.read_bytes()
            p_buf[page_off : page_off + len(chunk)] = chunk

            self.total_history_written += take
            off_in_block += take
            rem -= take

            if page_idx >= self.max_pages:
                self.evict_prior(page_idx - self.max_pages + 1)

    def get_history_slice(self, distance: int, length: int) -> bytes:
        h_len = self.history_len()
        if distance <= 0 or distance > h_len or length <= 0 or length > distance:
            raise XZFormatError(f"Invalid history reference: distance={distance}, length={length}, available={h_len}")
        abs_start = self.total_history_written - distance
        res = bytearray()
        curr_abs = abs_start
        rem_len = min(length, distance)

        while rem_len > 0:
            page_idx = curr_abs // self.HISTORY_PAGE
            page_off = curr_abs % self.HISTORY_PAGE
            if page_idx not in self.page_files:
                break
            avail = min(rem_len, self.HISTORY_PAGE - page_off)

            if page_idx in self.lru_cache and len(self.lru_cache[page_idx]) >= page_off + avail:
                part = self.lru_cache[page_idx][page_off : page_off + avail]
            else:
                p_path = self.page_files[page_idx]
                with open(p_path, "rb") as f:
                    f.seek(page_off)
                    part = f.read(avail)
                if page_idx not in self.lru_cache:
                    self.lru_cache[page_idx] = bytearray(p_path.read_bytes())
                    self.lru_order.append(page_idx)
                    if len(self.lru_order) > 4:
                        old_idx = self.lru_order.pop(0)
                        self.lru_cache.pop(old_idx, None)

            res.extend(part)
            curr_abs += len(part)
            rem_len -= len(part)
            if len(part) == 0:
                break
        return bytes(res)

    def history_len(self) -> int:
        return min(self.total_history_written, self.max_history)

    def tail_crc32(self, n: int = 65536) -> int:
        avail = min(n, self.total_history_written)
        if avail <= 0:
            return 0
        tail_bytes = self.get_history_slice(avail, avail)
        return crc32(tail_bytes)

    def checkpoint(self, block_index: int, state_record: dict, input_hash: str = "0000000000000000") -> None:
        target_name = f"{input_hash}_{block_index:08d}.state"
        target_path = self.work_dir / target_name
        tmp_path = self.work_dir / f".tmp_{target_name}"
        tmp_path.write_text(json.dumps(state_record))
        atomic_replace(tmp_path, target_path)
        self.state_files[block_index] = target_path

    def restore(self, block_index: int) -> dict:
        if block_index in self.state_files:
            try:
                return json.loads(self.state_files[block_index].read_text())
            except Exception:
                return None
        return None

    def truncate_to(self, history_total: int) -> None:
        self.total_history_written = history_total
        curr_page = history_total // self.HISTORY_PAGE
        curr_off = history_total % self.HISTORY_PAGE

        to_del = [idx for idx in list(self.page_files.keys()) if idx > curr_page]
        for idx in to_del:
            self.page_files[idx].unlink(missing_ok=True)
            self.page_files.pop(idx, None)
            self.lru_cache.pop(idx, None)

        if curr_page in self.page_files:
            p_path = self.page_files[curr_page]
            with open(p_path, "r+b") as f:
                f.seek(curr_off)
                f.truncate()
            if curr_page in self.lru_cache:
                del self.lru_cache[curr_page][curr_off:]

    def evict_prior(self, min_retained_page: int) -> None:
        to_del = [idx for idx in list(self.page_files.keys()) if idx < min_retained_page]
        for idx in to_del:
            self.page_files[idx].unlink(missing_ok=True)
            self.page_files.pop(idx, None)
            self.lru_cache.pop(idx, None)

    def reset(self) -> None:
        for p in list(self.page_files.values()):
            try: p.unlink(missing_ok=True)
            except OSError: pass
        self.page_files.clear()
        self.lru_cache.clear()
        self.lru_order.clear()
        self.total_history_written = 0

    def cleanup(self) -> None:
        self.lru_cache.clear()
        self.lru_order.clear()
        for p in self.work_dir.glob("*"):
            try: p.unlink()
            except Exception: pass
        try: self.work_dir.rmdir()
        except Exception: pass

class FileHistory(HistoryBackend):
    persistent = True

    def __init__(self, file_path, max_history=67108864):
        self.file_path = Path(file_path).resolve()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(str(self.file_path), "w+b")
        self.total_written = 0
        self.max_history = max_history
        self.checkpoints = {}

    def append_block(self, block_data, block_index=0, input_hash=""):
        self.f.seek(self.total_written)
        self.f.write(block_data)
        self.f.flush()
        self.total_written += len(block_data)

    def history_len(self):
        return min(self.total_written, self.max_history)

    def get_history_slice(self, distance, length):
        h_len = self.history_len()
        if distance <= 0 or distance > h_len or length <= 0 or length > distance:
            raise XZFormatError(f"Invalid history reference: distance={distance}, length={length}, available={h_len}")
        start_pos = max(0, self.total_written - distance)
        read_len = min(length, self.total_written - start_pos)
        self.f.seek(start_pos)
        return self.f.read(read_len)

    def tail_crc32(self, n=65536):
        avail = min(n, self.history_len())
        if avail <= 0: return 0
        data = self.get_history_slice(avail, avail)
        return crc32(data)

    def truncate_to(self, history_total):
        self.total_written = history_total
        self.f.seek(history_total)
        self.f.truncate()

    def checkpoint(self, block_index, state_record, input_hash=""):
        self.checkpoints[block_index] = dict(state_record)

    def restore(self, block_index):
        return self.checkpoints.get(block_index)

    def reset(self):
        try:
            self.f.seek(0)
            self.f.truncate(0)
        except Exception:
            pass
        self.total_written = 0
        self.checkpoints.clear()

    def cleanup(self):
        try:
            self.f.close()
            self.file_path.unlink(missing_ok=True)
        except Exception:
            pass

    def export_state(self):
        return {
            "file_path": str(self.file_path),
            "total_written": self.total_written,
            "max_history": self.max_history
        }

    def import_state(self, state):
        self.file_path = Path(state["file_path"])
        self.total_written = state["total_written"]
        self.max_history = state["max_history"]
        if self.f.closed:
            self.f = open(str(self.file_path), "r+b")
        self.f.seek(self.total_written)

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

    @micropython.native
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

    @micropython.native
    def decode_bittree(self, probs, offset, num_bits):
        m = 1
        for _ in range(num_bits):
            m = (m << 1) + self.decode_bit(probs, offset + m)
        return m - (1 << num_bits)

    @micropython.native
    def decode_reverse_bittree(self, probs, offset, num_bits):
        m = 1
        symbol = 0
        for i in range(num_bits):
            bit = self.decode_bit(probs, offset + m)
            m = (m << 1) + bit
            symbol |= (bit << i)
        return symbol

    @micropython.native
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

@micropython.native
def decode_len_val(rd, probs, pos_state):
    if rd.decode_bit(probs, 0) == 0:
        return rd.decode_bittree(probs, 2 + (pos_state << 3), 3)
    if rd.decode_bit(probs, 1) == 0:
        return 8 + rd.decode_bittree(probs, 66 + (pos_state << 3), 3)
    return 16 + rd.decode_bittree(probs, 130, 8)

# --- Universal XZ / LZMA2 Decompression Engine ---

class XZStreamDecompressor:
    def __init__(self, input_source, history_backend=None, block_size=65536, force_gc=True):
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
        self._peeked_byte = None
        self.force_gc = force_gc
        self.need_dict_reset = True
        self.need_props = True

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
            "need_dict_reset": self.need_dict_reset,
            "need_props": self.need_props,
            "match_dist": self.match_dist,
            "match_rem": self.match_rem,
            "input_offset": self.overall_input_offset,
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
        self.need_dict_reset = s.get("need_dict_reset", True)
        self.need_props = s.get("need_props", True)
        self.match_dist = s["match_dist"]
        self.match_rem = s["match_rem"]
        if hasattr(self.input, "seek") and "input_offset" in s and is_seekable(self.input):
            self.input.seek(s["input_offset"])
        if hasattr(self.history, "import_state") and s["history"]:
            self.history.import_state(s["history"])

    def decode_block(self):
        while True:
            if self.phase == "STREAM_HEADER":
                while True:
                    if self._peeked_byte is not None:
                        b = self._peeked_byte
                        self._peeked_byte = None
                    else:
                        b = self.input.read(1)
                        if b:
                            self.overall_input_offset += 1
                    if not b:
                        if self.null_count % 4 != 0:
                            raise XZFormatError("Stream padding not multiple of 4")
                        self.phase = "EOF"
                        return b"", True
                    self.input_hasher.update(b)
                    if b == b"\x00":
                        self.null_count += 1
                        continue
                    if b == b"\xfd":
                        if self.null_count % 4 != 0:
                            raise XZFormatError("Stream padding not multiple of 4")
                        self.null_count = 0
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
                if pbyte > 40:
                    raise XZFormatError(f"Reserved LZMA2 dictionary property: {pbyte} > 40")
                if pbyte == 40:
                    dict_size = 0xFFFFFFFF
                else:
                    dict_size = (2 | (pbyte & 1)) << (pbyte // 2 + 11)
                self.dict_size = dict_size
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
                self.need_dict_reset = True
                self.need_props = True
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
                            if self.need_dict_reset and self.ctrl != 1:
                                raise XZFormatError("First chunk in Block must reset dictionary (ctrl=0x02 not allowed)")
                            if self.ctrl == 1:
                                self.history.reset()
                                self.reps = [0, 0, 0, 0]
                                self.need_dict_reset = False
                                self.need_props = True
                            sz_b = self._read_exact(2)
                            self.compressed_bytes_in_block += 2
                            csz = ((sz_b[0] << 8) | sz_b[1]) + 1
                            self.raw_chunk = self._read_exact(csz)
                            self.compressed_bytes_in_block += csz
                            self.raw_pos = 0
                            self.in_chunk = True
                        elif self.ctrl < 0x80:
                            raise XZFormatError(f"Reserved LZMA2 control byte: 0x{self.ctrl:02x}")
                        else:
                            self.mode = (self.ctrl >> 5) & 3
                            if self.need_dict_reset and self.mode != 3:
                                raise XZFormatError(f"First LZMA chunk in Block must reset dictionary (mode={self.mode} != 3)")
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
                                if pbyte > 224:
                                    raise XZFormatError(f"Invalid LZMA2 property byte {pbyte} > 224")
                                self.pb = pbyte // 45
                                rem = pbyte % 45
                                self.lp = rem // 9
                                self.lc = rem % 9
                                if self.lc + self.lp > 4:
                                    raise XZFormatError(f"lc + lp = {self.lc + self.lp} > 4 in LZMA2")
                                self.p_lit = [self.PROB_INIT] * (0x300 << (self.lc + self.lp))
                                self.need_props = False
                            elif self.need_props:
                                raise XZFormatError("LZMA chunk requires properties before use")
                            if self.mode == 3:
                                self.history.reset()
                                self.reps = [0, 0, 0, 0]
                                self.need_dict_reset = False
                            self.chunk_compressed = self._read_exact(self.chunk_comp_sz)
                            self.compressed_bytes_in_block += self.chunk_comp_sz
                            if len(self.chunk_compressed) > 0 and self.chunk_compressed[0] != 0:
                                raise XZFormatError(f"First byte of range decoder in chunk must be 0x00, got 0x{self.chunk_compressed[0]:02x}")
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
                                elif self.history.history_len() > 0:
                                    ps = self.history.get_history_slice(1, 1)
                                    prev_b = ps[0] if ps else 0
                                else:
                                    prev_b = 0
                                lit_st = (((self.stream_out_offset + self.cur_block_pos) & lp_mask) << self.lc) + (prev_b >> (8 - self.lc))
                                lit_off = lit_st * 0x300
                                symbol = 1
                                if self.state >= 7:
                                    dist = self.reps[0]
                                    if dist < self.cur_block_pos:
                                        mb = self.cur_block[self.cur_block_pos - 1 - dist]
                                    else:
                                        h_req_dist = dist - self.cur_block_pos + 1
                                        if h_req_dist <= self.history.history_len():
                                            ms = self.history.get_history_slice(h_req_dist, 1)
                                            mb = ms[0] if ms else 0
                                        else:
                                            mb = 0
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
                                    if self.force_gc:
                                        try:
                                            import gc
                                            gc.collect()
                                        except Exception:
                                            pass
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
                                        dist = self.reps[0]
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

                                if dist == 0xFFFFFFFF:
                                    raise XZFormatError("Invalid distance 0xFFFFFFFF in LZMA2")
                                if hasattr(self, "dict_size") and dist >= self.dict_size:
                                    raise XZFormatError(f"Distance {dist} exceeds dictionary size {self.dict_size}")
                                if dist >= self.cur_block_pos + self.history.history_len():
                                    raise XZFormatError(f"Distance {dist} exceeds available history {self.cur_block_pos + self.history.history_len()}")
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
                        if c_off != len(comp_bytes) or rd.code != 0:
                            raise XZFormatError(f"LZMA chunk exactness violation: c_off={c_off}/{len(comp_bytes)} code={rd.code}")

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
                # Consume stream padding and peek next byte (§5.1, §5.4)
                pad_nulls = 0
                has_next = False
                while True:
                    pb = self.input.read(1)
                    if not pb:
                        if pad_nulls % 4 != 0:
                            raise XZFormatError("Stream padding not multiple of 4")
                        has_next = False
                        self.phase = "EOF"
                        break
                    self.overall_input_offset += 1
                    if pb == b"\x00":
                        pad_nulls += 1
                        continue
                    elif pb == b"\xfd":
                        if pad_nulls % 4 != 0:
                            raise XZFormatError("Stream padding not multiple of 4")
                        has_next = True
                        self._peeked_byte = pb
                        self.phase = "STREAM_HEADER"
                        break
                    else:
                        raise XZFormatError(f"Unexpected byte after stream padding: 0x{pb[0]:02x}")

                self.pending_boundary = StreamBoundaryEvent(
                    stream_index=self.stream_index,
                    next_stream_index=self.stream_index + 1 if has_next else None,
                    input_stream_len=self.overall_input_offset - self.stream_start_in - (1 if has_next else 0),
                    overall_input_offset=self.overall_input_offset - (1 if has_next else 0),
                    output_stream_len=self.overall_output_offset - self.stream_start_out,
                    overall_output_offset=self.overall_output_offset,
                    has_next=has_next
                )
                self.stream_index += 1
                return b"", (not has_next)


    def decode_events(self):
        while True:
            if self.pending_boundary:
                ev = self.pending_boundary
                self.pending_boundary = None
                yield ev
            blk, is_eof = self.decode_block()
            if blk:
                yield ChunkOutputEvent(blk)
                yield BlockCommittedEvent(self.block_idx - 1, blk, self.export_state())
                yield ProgressEvent(self.block_idx - 1, len(blk), self.overall_output_offset)
            if self.pending_boundary:
                ev = self.pending_boundary
                self.pending_boundary = None
                yield ev
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
                  progress_callback=None, history_backend="auto",
                  timeout=52.0, cpu_timeout=22.0, deadline=None,
                  block_size="auto", storage_dir=None, memory_limit=None,
                  force_gc=True, resume_mode="auto", checkpoint_interval="8m"):
    """
    Callable interface for XZ decompression with persistent checkpoint resumption.
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

    avail_mem = parse_memory_size(memory_limit) if memory_limit is not None else get_available_memory()
    actual_storage_dir = storage_dir if storage_dir is not None else resume_dir

    if isinstance(history_backend, str):
        history = select_history_backend(history_backend, storage_dir=actual_storage_dir, available_mem=avail_mem)
    elif history_backend is not None:
        history = history_backend
    else:
        history = select_history_backend("auto", storage_dir=actual_storage_dir, available_mem=avail_mem)

    actual_block_size = select_block_size(block_size, available_mem=avail_mem)
    is_stream_output = (output_target == "-")
    is_stream_input = (input_source == "-") or not is_seekable(in_stream)

    engine = XZStreamDecompressor(in_stream, history, block_size=actual_block_size, force_gc=force_gc)

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

    # Checkpoint helpers (§3.3)
    def _compute_input_id(stream):
        if is_seekable(stream):
            cur = stream.tell()
            stream.seek(0)
            hdr = stream.read(4096)
            stream.seek(0, 2)
            sz = stream.tell()
            stream.seek(cur)
            return f"{crc32(hdr):08x}:{sz}"
        return None

    def _load_best_checkpoint(r_dir, exp_input_id=None):
        best_rec = None
        best_txn = -1
        for j_name in ("STATE_A.json", "STATE_B.json"):
            jp = Path(r_dir) / j_name
            if jp.exists():
                try:
                    rec = json.loads(jp.read_text(encoding="utf-8"))
                    state_raw = rec.get("state")
                    stored_crc = rec.get("payload_crc32")
                    calc_crc = crc32(json.dumps(state_raw, sort_keys=True).encode("utf-8"))
                    if stored_crc != calc_crc:
                        continue
                    if exp_input_id and rec.get("input_id"):
                        if rec.get("input_id") != exp_input_id:
                            continue
                    txn = rec.get("txn", 0)
                    if txn > best_txn:
                        best_txn = txn
                        best_rec = rec
                except Exception:
                    pass
        return best_rec

    txn_counter = 0
    def _write_checkpoint(r_dir, engine_obj, hist_obj, inp_id):
        nonlocal txn_counter
        txn_counter += 1
        state_rec = engine_obj.export_state()
        state_rec["history_total"] = hist_obj.total_history_written if hasattr(hist_obj, "total_history_written") else hist_obj.history_len()
        state_rec["history_tail_crc32"] = hist_obj.tail_crc32()
        state_bytes = json.dumps(state_rec, sort_keys=True).encode("utf-8")
        payload_crc = crc32(state_bytes)
        rec = {
            "version": 1,
            "txn": txn_counter,
            "input_id": inp_id,
            "payload_crc32": payload_crc,
            "state": state_rec
        }
        target_j = Path(r_dir) / ("STATE_A.json" if (txn_counter % 2 == 1) else "STATE_B.json")
        tmp_j = Path(r_dir) / ".tmp_state.json"
        tmp_j.write_text(json.dumps(rec), encoding="utf-8")
        atomic_replace(tmp_j, target_j)

    curr_out_stream = None
    curr_dest_path = None
    curr_part_path = None
    stream_idx = 0

    if is_stream_output:
        curr_out_stream = sys.stdout.buffer
    else:
        base_dest = format_suffixed_filename(output_target, 0)
        curr_dest_path = Path(base_dest)
        target_file = curr_dest_path if in_place else Path(f"{base_dest}.part")
        curr_part_path = target_file if not in_place else None

    verify_output_mode = False
    verify_file_handle = None
    verify_file_len = 0
    verified_offset = 0

    inp_id = _compute_input_id(in_stream) if not is_stream_input else None
    checkpoint_resumed = False

    # Checkpoint restore check on start
    if resume_dir and resume_mode != "off":
        ckpt = _load_best_checkpoint(resume_dir, inp_id)
        if ckpt is not None:
            state_dict = ckpt["state"]
            txn_counter = ckpt.get("txn", 0)
            out_off = state_dict.get("overall_output_offset", 0)
            hist_tot = state_dict.get("history_total", 0)
            exp_tail_crc = state_dict.get("history_tail_crc32")
            history.truncate_to(hist_tot)
            if exp_tail_crc is not None and history.tail_crc32() != exp_tail_crc:
                if resume_mode == "checkpoint":
                    raise XZResourceError("History tail CRC32 mismatch on checkpoint restore")
            else:
                if not is_stream_output and curr_part_path and curr_part_path.exists():
                    with open(str(curr_part_path), "r+b") as f:
                        f.seek(out_off)
                        f.truncate()
                    curr_out_stream = open(str(curr_part_path), "a+b")
                    curr_out_stream.seek(out_off)
                in_off = state_dict.get("input_offset", 0)
                if is_seekable(in_stream):
                    in_stream.seek(in_off)
                engine.import_state(state_dict)
                checkpoint_resumed = True
        elif resume_mode == "checkpoint":
            raise XZResourceError("No valid checkpoint found for checkpoint resumption")
        else:
            # Fall back to verify-output mode
            if not is_stream_output and curr_part_path and curr_part_path.exists():
                verify_output_mode = True
                verify_file_len = curr_part_path.stat().st_size
                verify_file_handle = open(str(curr_part_path), "r+b")
                verified_offset = 0
                if timeout_sec > 0 or cpu_timeout_sec > 0:
                    sys.stderr.write("xz_decompressor: warning: Verify-Output mode cannot converge when stream decode time exceeds timeout; use checkpoint mode with a persistent backend.\n")
                    sys.stderr.flush()

    if not is_stream_output and curr_out_stream is None and not verify_output_mode:
        if not in_place and curr_part_path and curr_part_path.exists() and not resume_dir:
            curr_part_path.unlink()
        curr_out_stream = open(str(curr_part_path or curr_dest_path), "wb")

    ckpt_interval_bytes = parse_memory_size(checkpoint_interval) or (8 * 1024 * 1024)
    bytes_since_ckpt = 0

    try:
        for event in engine.decode_events():
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
                if verify_output_mode:
                    space = len(event.data)
                    if verified_offset + space <= verify_file_len:
                        verify_file_handle.seek(verified_offset)
                        existing_chunk = verify_file_handle.read(space)
                        if existing_chunk == event.data:
                            verified_offset += space
                        else:
                            verify_file_handle.seek(verified_offset)
                            verify_file_handle.truncate()
                            verify_file_handle.write(event.data)
                            verify_file_handle.flush()
                            curr_out_stream = verify_file_handle
                            verify_output_mode = False
                    else:
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

            elif event.type == XZEventType.BLOCK_COMMITTED:
                if resume_dir and getattr(history, "persistent", False) and resume_mode != "off":
                    bytes_since_ckpt += len(event.block_data)
                    if ckpt_interval_bytes > 0 and bytes_since_ckpt >= ckpt_interval_bytes:
                        _write_checkpoint(resume_dir, engine, history, inp_id)
                        bytes_since_ckpt = 0

            elif event.type == XZEventType.PROGRESS:
                if progress_callback:
                    progress_callback(event)

            elif event.type == XZEventType.STREAM_BOUNDARY:
                if not is_stream_output:
                    if curr_out_stream is not None:
                        curr_out_stream.close()
                        curr_out_stream = None
                    if verify_file_handle is not None:
                        verify_file_handle.close()
                        verify_file_handle = None

                    if not in_place and curr_part_path and curr_part_path.exists():
                        if curr_dest_path.exists():
                            curr_dest_path.unlink()
                        atomic_replace(curr_part_path, curr_dest_path)

                if event.has_next:
                    next_stream = event.next_stream_index
                    stream_idx = next_stream
                    if is_stream_output:
                        dest_desc = f"stdout+{event.overall_output_offset}B"
                    else:
                        base_dest = format_suffixed_filename(output_target, next_stream)
                        curr_dest_path = Path(base_dest)
                        target_file = curr_dest_path if in_place else Path(f"{base_dest}.part")
                        curr_part_path = target_file if not in_place else None
                        if not in_place and target_file.exists():
                            target_file.unlink()
                        curr_out_stream = open(str(target_file), "wb")
                        dest_desc = str(curr_dest_path)

                    sys.stderr.write(
                        f"[xz:stream boundary] input_offset={event.overall_input_offset}B "
                        f"stream_in={event.input_stream_len}B stream_out={event.output_stream_len}B "
                        f"output_offset={event.overall_output_offset}B starting stream {next_stream} -> {dest_desc}\n"
                    )
                    sys.stderr.flush()
                else:
                    sys.stderr.write(
                        f"[xz:stream end] input_offset={event.overall_input_offset}B "
                        f"output_offset={event.overall_output_offset}B streams={stream_idx + 1}\n"
                    )
                    sys.stderr.flush()

        if not is_stream_output:
            if curr_out_stream is not None:
                curr_out_stream.close()
                curr_out_stream = None
                verify_file_handle = None
            elif verify_file_handle is not None:
                verify_file_handle.close()
                verify_file_handle = None
            if not in_place and curr_part_path and curr_part_path.exists():
                if curr_dest_path.exists():
                    curr_dest_path.unlink()
                atomic_replace(curr_part_path, curr_dest_path)

        if resume_dir:
            for j in ("STATE_A.json", "STATE_B.json", ".tmp_state.json"):
                (Path(resume_dir) / j).unlink(missing_ok=True)
            history.cleanup()

    except (KeyboardInterrupt, WallClockTimeout, CPUTimeout) as exc:
        if resume_dir and getattr(history, "persistent", False) and resume_mode != "off":
            try:
                _write_checkpoint(resume_dir, engine, history, inp_id)
            except Exception:
                pass

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
        if not is_stream_output and curr_out_stream and not is_seekable(curr_out_stream):
            msg += f" --resume-at={engine.overall_output_offset}"
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()
        raise
    finally:
        try:
            if hasattr(signal, 'setitimer') and hasattr(signal, 'ITIMER_REAL'):
                signal.setitimer(signal.ITIMER_REAL, 0)
            elif hasattr(signal, 'alarm'):
                signal.alarm(0)
            if hasattr(signal, 'SIGALRM') and sig_alarm_old is not None:
                signal.signal(signal.SIGALRM, sig_alarm_old)
        except Exception:
            pass
        try:
            if hasattr(signal, 'setitimer') and hasattr(signal, 'ITIMER_VIRTUAL'):
                signal.setitimer(signal.ITIMER_VIRTUAL, 0)
            if hasattr(signal, 'SIGVTALRM') and sig_vtalrm_old is not None:
                signal.signal(signal.SIGVTALRM, sig_vtalrm_old)
        except Exception:
            pass

    return engine.overall_output_offset

try:
    import unittest
except ImportError:
    unittest = None

if unittest is not None:
    _TestCaseBase = unittest.TestCase
else:
    class _TestCaseBase:
        pass

class TestXZDecompressor(_TestCaseBase):
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
        tmp_dir = Path("/tmp/test_hist")
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
        work_dir = Path("/tmp/test_verify_work")
        out_target = Path("/tmp/test_verify_out.bin")
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


    def test_verify_output_mode_multiblock_mismatch(self):
        lzma = self.lzma
        import io
        raw = b"ABCDEFGHIJ" * 20000 # 200,000 bytes spanning 4 blocks
        xz_data = lzma.compress(raw, preset=1)

        work_dir = Path("/tmp/test_verify_multi_work")
        out_target = Path("/tmp/test_verify_multi_out.bin")
        corrupt_raw = raw[:80000] + b"CORRUPTED_BYTES" + raw[80015:150000]
        out_target.write_bytes(corrupt_raw)

        decompress_xz(io.BytesIO(xz_data), str(out_target), resume_dir=str(work_dir), in_place=True)
        self.assertEqual(out_target.read_bytes(), raw)
        out_target.unlink(missing_ok=True)

    def test_proves_right_claim1_distance_indexing(self):
        lzma = self.lzma
        import random, io
        prng = random.Random(42)
        raw = bytearray()
        for _ in range(100):
            raw.extend(prng.randbytes(500))
            raw.extend(b"REPEAT_MATCH_DIST_INVARIANT_" * 20)
        raw = bytes(raw)
        xz_data = lzma.compress(raw, preset=9 | lzma.PRESET_EXTREME)

        engine = XZStreamDecompressor(io.BytesIO(xz_data))
        out = b"".join(ev.data for ev in engine.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
        self.assertEqual(out, raw)

    def test_proves_right_claim3_stream_padding_isolation(self):
        lzma = self.lzma
        import io
        data1 = b"STREAM_ONE_" * 50
        data2 = b"STREAM_TWO_" * 50
        xz1 = lzma.compress(data1)
        xz2 = lzma.compress(data2)

        # 4 null bytes: valid multiple of 4
        valid_stream = xz1 + (b"\x00" * 4) + xz2
        engine_valid = XZStreamDecompressor(io.BytesIO(valid_stream))
        out_valid = b"".join(ev.data for ev in engine_valid.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
        self.assertEqual(out_valid, data1 + data2)

        # 2 null bytes: invalid multiple of 4
        invalid_stream = xz1 + (b"\x00" * 2) + xz2
        engine_invalid = XZStreamDecompressor(io.BytesIO(invalid_stream))
        with self.assertRaises(XZFormatError):
            list(engine_invalid.decode_events())

    def test_proves_right_claim5_compact_iso8601(self):
        # Proves that compact date notations without separators parse accurately
        ts1 = parse_iso8601_flexible("20261008123045")
        ts2 = parse_iso8601_flexible("20261008123045.500")
        self.assertEqual(ts1, 1791462645.0)
        self.assertEqual(ts2, 1791462645.5)

    def test_proves_right_claim6_duration_types(self):
        # Proves that ints, floats, and strings all parse correctly
        self.assertEqual(parse_duration(52), 52.0)
        self.assertEqual(parse_duration(52.0), 52.0)
        self.assertEqual(parse_duration("52"), 52.0)
        self.assertEqual(parse_duration("52s"), 52.0)
        self.assertEqual(parse_duration("1.5m"), 90.0)
        self.assertEqual(parse_duration(0), 0.0)
        self.assertEqual(parse_duration("0"), 0.0)
        self.assertIsNone(parse_duration(None))


    def test_storage_memory_history(self):
        hist = MemoryHistory(max_history=1024)
        hist.append_block(b"BLOCK_A_" * 16, 0)
        hist.append_block(b"BLOCK_B_" * 16, 1)
        # Test forward slice
        sl = hist.get_history_slice(16, 8)
        self.assertEqual(len(sl), 8)
        # Test export/import
        st = hist.export_state()
        hist2 = MemoryHistory(max_history=1024)
        hist2.import_state(st)
        self.assertEqual(hist2.get_history_slice(16, 8), sl)
        hist.cleanup()

    def test_storage_directory_history(self):
        work_dir = Path("/tmp/test_dir_store")
        work_dir.mkdir(parents=True, exist_ok=True)
        hist = DirectoryBlockHistoryStore(work_dir, max_blocks=3)
        hist.append_block(b"ALPHA_" * 10, 0)
        hist.append_block(b"BETA__" * 10, 1)
        hist.checkpoint(1, {"step": 1})
        self.assertEqual(hist.restore(1), {"step": 1})
        sl = hist.get_history_slice(12, 6)
        self.assertEqual(len(sl), 6)
        hist.cleanup()
        self.assertFalse(work_dir.exists())

    def test_storage_file_history(self):
        fpath = Path("/tmp/test_file_store.bin")
        hist = FileHistory(fpath, max_history=1024)
        hist.append_block(b"FILE_BLOCK_1_" * 8, 0)
        hist.append_block(b"FILE_BLOCK_2_" * 8, 1)
        sl = hist.get_history_slice(14, 7)
        self.assertEqual(len(sl), 7)
        hist.checkpoint(1, {"idx": 1})
        self.assertEqual(hist.restore(1), {"idx": 1})
        st = hist.export_state()
        self.assertIn("total_written", st)
        hist.cleanup()
        self.assertFalse(fpath.exists())

    def test_storage_auto_selection_memory_threshold(self):
        # When python-available memory is less than 72MB, switch from MemoryHistory to disk/file
        low_mem = 64 * 1024 * 1024 # 64MB (< 72MB)
        high_mem = 128 * 1024 * 1024 # 128MB (>= 72MB)

        backend_low = select_history_backend("auto", available_mem=low_mem)
        self.assertIsInstance(backend_low, DirectoryBlockHistoryStore)
        backend_low.cleanup()

        backend_high = select_history_backend("auto", available_mem=high_mem)
        self.assertIsInstance(backend_high, MemoryHistory)
        backend_high.cleanup()

        # Explicit overrides
        backend_forced_mem = select_history_backend("memory", available_mem=low_mem)
        self.assertIsInstance(backend_forced_mem, MemoryHistory)

        f_tmp = Path("/tmp/test_f_ovr")
        backend_forced_file = select_history_backend("file", storage_dir=str(f_tmp), available_mem=high_mem)
        self.assertIsInstance(backend_forced_file, FileHistory)
        backend_forced_file.cleanup()

    def test_storage_auto_block_size_threshold(self):
        # On devices with less than 256KB of RAM, default to 8KiB rather than 64KiB
        tiny_mem = 128 * 1024 # 128KB (< 256KB)
        norm_mem = 10 * 1024 * 1024 # 10MB (>= 256KB)

        self.assertEqual(select_block_size("auto", available_mem=tiny_mem), 8192)
        self.assertEqual(select_block_size("auto", available_mem=norm_mem), 65536)

        # Explicit overrides
        self.assertEqual(select_block_size(16384), 16384)
        self.assertEqual(select_block_size("8k"), 8192)
        self.assertEqual(select_block_size("64k"), 65536)

    def test_psram_and_external_storage_detection(self):
        # Test path selection
        target_dir, stype = find_preferred_storage_dir(prefix="test_pref_vfs")
        self.assertTrue(Path(target_dir).exists())
        self.assertIn(stype, ("psram", "external_flash", "local"))

    def test_micropython_decorators_and_gc(self):
        # Verify decorated functions work identically
        self.assertEqual(crc32(b"123456789"), 0xCBF43926)
        self.assertEqual(crc64(b"123456789"), 0x995DC9BBDF1939FA)

        # Test decompress with force_gc=True and force_gc=False
        lzma = self.lzma
        import io
        data = b"GC_TEST_PAYLOAD_" * 100
        xz_data = lzma.compress(data)

        # Force GC enabled
        eng1 = XZStreamDecompressor(io.BytesIO(xz_data), force_gc=True)
        out1 = b"".join(ev.data for ev in eng1.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
        self.assertEqual(out1, data)

        # Force GC disabled
        eng2 = XZStreamDecompressor(io.BytesIO(xz_data), force_gc=False)
        out2 = b"".join(ev.data for ev in eng2.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
        self.assertEqual(out2, data)

    def test_circuitpython_import_handling(self):
        # Test memory size parser
        self.assertEqual(parse_memory_size("64m"), 64 * 1024 * 1024)
        self.assertEqual(parse_memory_size("128k"), 128 * 1024)
        self.assertEqual(parse_memory_size("1g"), 1024 * 1024 * 1024)
        self.assertEqual(parse_memory_size(262144), 262144)
        self.assertIsNone(parse_memory_size(None))


    def test_regression_rep0_long_zeros(self):
        lzma = self.lzma
        import io
        for size in [10000, 100000, 500000]: # 10k, 100k, 500k all-zeros
            raw_zeros = bytes(size)
            for preset in [0, 6]:
                xz_data = lzma.compress(raw_zeros, preset=preset)
                engine = XZStreamDecompressor(io.BytesIO(xz_data))
                out = b"".join(ev.data for ev in engine.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
                self.assertEqual(out, raw_zeros)

    def test_regression_directory_block_sizes(self):
        lzma = self.lzma
        import io, random
        prng = random.Random(999)
        span1 = prng.randbytes(20000)
        match_str = b"LONG_DISTANCE_MATCH_REGRESSION_PATTERN_" * 10
        span2 = prng.randbytes(20000)
        pat_data = span1 + match_str + span2 + match_str
        xz_data = lzma.compress(pat_data, preset=6)

        for b_sz in [4096, 8192, 16384, 65536]:
            w_dir = Path(f"/tmp/test_dir_bs_{b_sz}")
            w_dir.mkdir(parents=True, exist_ok=True)
            store = DirectoryBlockHistoryStore(w_dir)
            engine = XZStreamDecompressor(io.BytesIO(xz_data), history_backend=store, block_size=b_sz)
            out = b"".join(ev.data for ev in engine.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
            self.assertEqual(out, pat_data, f"Failed at block_size={b_sz}")
            store.cleanup()

    def test_regression_multi_stream_routing(self):
        lzma = self.lzma
        import io
        s1 = b"STREAM_ZERO_CONTENT_" * 100
        s2 = b"STREAM_ONE_CONTENT__" * 100
        xz_multi = lzma.compress(s1) + (b"\x00" * 4) + lzma.compress(s2)

        out_base = Path("/tmp/test_route_out.bin")
        out_stream1 = Path("/tmp/test_route_out_1.bin")
        out_base.unlink(missing_ok=True)
        out_stream1.unlink(missing_ok=True)

        decompress_xz(io.BytesIO(xz_multi), str(out_base), in_place=True)

        self.assertTrue(out_base.exists())
        self.assertTrue(out_stream1.exists())
        self.assertEqual(out_base.read_bytes(), s1)
        self.assertEqual(out_stream1.read_bytes(), s2)

        out_base.unlink(missing_ok=True)
        out_stream1.unlink(missing_ok=True)

    def test_regression_empty_input_stream(self):
        lzma = self.lzma
        import io
        empty_xz = lzma.compress(b"")
        out_empty = Path("/tmp/test_empty_stream.bin")
        out_empty.unlink(missing_ok=True)

        decompress_xz(io.BytesIO(empty_xz), str(out_empty), in_place=True)

        self.assertTrue(out_empty.exists())
        self.assertEqual(out_empty.stat().st_size, 0)
        out_empty.unlink(missing_ok=True)

    def test_regression_non_seekable_input(self):
        lzma = self.lzma
        data = b"PIPE_NON_SEEKABLE_DATA_" * 500
        xz_data = lzma.compress(data)

        class NonSeekablePipe(io.RawIOBase):
            def __init__(self, raw_bytes):
                self.buf = io.BytesIO(raw_bytes)
            def read(self, n=-1):
                return self.buf.read(n)
            def readable(self):
                return True
            def seekable(self):
                return False
            def seek(self, *args):
                raise OSError(29, "Illegal seek")
            def tell(self):
                raise OSError(29, "Illegal seek")

        pipe = NonSeekablePipe(xz_data)
        engine = XZStreamDecompressor(pipe)
        out = b"".join(ev.data for ev in engine.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT)
        self.assertEqual(out, data)


    def test_regression_checkpoint_resume_convergence(self):
        lzma = self.lzma
        if lzma is None: self.skipTest("lzma oracle not available")
        import io
        raw = b"CHECKPOINT_CONVERGENCE_TEST_PAYLOAD_" * 5000 # 180,000 bytes spanning multiple blocks
        xz_data = lzma.compress(raw, preset=6)

        r_dir = Path("/tmp/test_ckpt_converge")
        r_dir.mkdir(parents=True, exist_ok=True)
        out_f = Path("/tmp/test_ckpt_converge_out.bin")
        out_f.unlink(missing_ok=True)

        # Repeated short timeout runs until finished
        max_runs = 10
        finished = False
        for run_i in range(max_runs):
            try:
                decompress_xz(
                    io.BytesIO(xz_data),
                    str(out_f),
                    resume_dir=str(r_dir),
                    resume_mode="auto",
                    timeout=0.25,
                    cpu_timeout=0.25,
                    in_place=True
                )
                finished = True
                break
            except (WallClockTimeout, CPUTimeout):
                pass

        self.assertTrue(finished, f"Failed to converge within {max_runs} runs")
        self.assertEqual(out_f.read_bytes(), raw)
        out_f.unlink(missing_ok=True)
        r_dir.rmdir()

    def test_regression_malformed_lzma2_and_properties(self):
        lzma = self.lzma
        if lzma is None: self.skipTest("lzma oracle not available")
        import io, struct
        data = b"MALFORMED_VALIDATION_TEST" * 10
        good_xz = bytearray(lzma.compress(data, check=lzma.CHECK_CRC32))

        # 1. Block header dict prop = 0x40 (> 40)
        bad_prop_xz = bytearray(good_xz)
        bad_prop_xz[16] = 0x40
        h_sz = (bad_prop_xz[12] + 1) * 4
        bad_prop_xz[12 + h_sz - 4 : 12 + h_sz] = struct.pack("<I", crc32(bad_prop_xz[12 : 12 + h_sz - 4]))
        with self.assertRaises(XZFormatError):
            engine = XZStreamDecompressor(io.BytesIO(bytes(bad_prop_xz)))
            list(engine.decode_events())

        # 2. First chunk ctrl byte = 0x02 (uncompressed without dict reset)
        h_end = 12 + h_sz
        if bad_prop_xz[h_end] in (0x01, 0x02):
            bad_ctrl_xz = bytearray(good_xz)
            bad_ctrl_xz[h_end] = 0x02
            with self.assertRaises(XZFormatError):
                engine = XZStreamDecompressor(io.BytesIO(bytes(bad_ctrl_xz)))
                list(engine.decode_events())

    def test_backend_protocol_conformance(self):
        # 1. MemoryHistory protocol
        mh = MemoryHistory(max_history=1024)
        self.assertFalse(mh.persistent)
        mh.append_block(b"0123456789" * 10, 0)
        self.assertEqual(mh.history_len(), 100)
        self.assertGreater(mh.tail_crc32(), 0)
        # Invalid distance > history_len raises XZFormatError
        with self.assertRaises(XZFormatError):
            mh.get_history_slice(101, 1)
        with self.assertRaises(XZFormatError):
            mh.get_history_slice(0, 1)
        mh.truncate_to(50)
        self.assertEqual(mh.history_len(), 50)
        mh.reset()
        self.assertEqual(mh.history_len(), 0)
        mh.cleanup()

        # 2. DirectoryBlockHistoryStore protocol
        w_dir = Path("/tmp/test_dir_proto")
        w_dir.mkdir(parents=True, exist_ok=True)
        ds = DirectoryBlockHistoryStore(w_dir, max_blocks=3)
        self.assertTrue(ds.persistent)
        ds.append_block(b"PAGED_DATA_" * 10, 0)
        self.assertGreater(ds.history_len(), 0)
        self.assertGreater(ds.tail_crc32(), 0)
        with self.assertRaises(XZFormatError):
            ds.get_history_slice(ds.history_len() + 10, 1)
        # Test reset unlinks page files
        self.assertGreater(len(list(w_dir.glob("*.page"))), 0)
        ds.reset()
        self.assertEqual(ds.history_len(), 0)
        self.assertEqual(len(list(w_dir.glob("*.page"))), 0)
        ds.cleanup()

        # 3. FileHistory protocol
        fpath = Path("/tmp/test_fh_proto.bin")
        fh = FileHistory(fpath, max_history=1024)
        self.assertTrue(fh.persistent)
        fh.append_block(b"FILE_DATA_" * 10, 0)
        self.assertGreater(fh.history_len(), 0)
        self.assertGreater(fh.tail_crc32(), 0)
        with self.assertRaises(XZFormatError):
            fh.get_history_slice(fh.history_len() + 10, 1)
        fh.truncate_to(30)
        self.assertEqual(fh.history_len(), 30)
        fh.reset()
        self.assertEqual(fh.history_len(), 0)
        fh.cleanup()

def main():
    try:
        import argparse
    except ImportError:
        argparse = None

    try:
        import unittest
    except ImportError:
        unittest = None

    if argparse is not None:
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
        parser.add_argument("--resume-mode", choices=["auto", "checkpoint", "verify", "off"], default="auto",
                            help="Resumption mode: auto (default), checkpoint, verify, off")
        parser.add_argument("--checkpoint-interval", default="8m",
                            help="Periodic checkpoint spacing [<N>|<N>k|<N>m|<N>g bytes | <N>s] (default: 8m)")
        parser.add_argument("--history-backend", choices=["auto", "memory", "directory", "file"], default="auto",
                            help="Storage backend: auto (<72MB switches to disk), memory, directory, file")
        parser.add_argument("--block-size", default="auto",
                            help="Block size: auto (8KiB if <256KB RAM, else 64KiB), or integer/size string")
        parser.add_argument("--storage-dir", default=None,
                            help="Directory for history/scratch files (overrides PSRAM/SD detection)")
        parser.add_argument("--memory-limit", default=None,
                            help="Simulate/override python-available RAM limit (e.g. 64m, 128k)")
        parser.add_argument("--no-gc", dest="force_gc", action="store_false", default=True,
                            help="Disable forced gc.collect() in between blocks")
        parser.add_argument("-v", "--verbose", action="store_true", help="Diagnostic logging")

        if unittest is not None:
            parser.add_argument("--test", action="store_true", help="Run self-contained unit test suite")

        args = parser.parse_args()

        if hasattr(args, "test") and args.test:
            if unittest is None:
                sys.stderr.write("--test disabled: unittest is not available in this environment\n")
                sys.exit(1)
            suite = unittest.TestLoader().loadTestsFromTestCase(TestXZDecompressor)
            runner = unittest.TextTestRunner(verbosity=2)
            result = runner.run(suite)
            sys.exit(0 if result.wasSuccessful() else 1)
        elif "--test" in sys.argv and unittest is None:
            sys.stderr.write("--test disabled: unittest is not available in this environment\n")
            sys.exit(1)

        input_src = args.input
        output_dst = args.output
        res_dir = args.resume_dir
        res_from = args.resume_from
        res_at = args.resume_at
        inp = args.in_place
        t_out = args.timeout
        c_out = args.cpu_timeout
        d_line = args.deadline
        h_backend = args.history_backend
        b_size = args.block_size
        s_dir = args.storage_dir
        m_limit = args.memory_limit
        f_gc = args.force_gc
    else:
        # Fallback minimal argument parser for CircuitPython without argparse
        if "--test" in sys.argv:
            sys.stderr.write("--test disabled: unittest / argparse is not available in this environment\n")
            sys.exit(1)
        input_src = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else "-"
        output_dst = None
        res_dir = None
        res_from = None
        res_at = None
        inp = False
        t_out = "52s"
        c_out = "22s"
        d_line = None
        h_backend = "auto"
        b_size = "auto"
        s_dir = None
        m_limit = None
        f_gc = True

    spinner = ("\\", "|", "/", "-")
    spin_idx = [0]
    def progress_cb(ev):
        glyph = spinner[spin_idx[0] % 4]
        spin_idx[0] += 1
        if sys.stderr.isatty(): sys.stderr.write(f"{glyph}\r")
        sys.stderr.flush()

    try:
        decompress_xz(
            input_source=input_src,
            output_target=output_dst,
            resume_dir=res_dir,
            resume_from=res_from,
            resume_at=res_at,
            in_place=inp,
            progress_callback=progress_cb,
            history_backend=h_backend,
            timeout=t_out,
            cpu_timeout=c_out,
            deadline=d_line,
            block_size=b_size,
            storage_dir=s_dir,
            memory_limit=m_limit,
            force_gc=f_gc,
            resume_mode=getattr(args, "resume_mode", "auto") if argparse else "auto",
            checkpoint_interval=getattr(args, "checkpoint_interval", "8m") if argparse else "8m"
        )
    except (WallClockTimeout, CPUTimeout):
        sys.exit(124)
    except KeyboardInterrupt:
        sys.exit(130)
    except XZFormatError as exc:
        is_v = args.verbose if argparse is not None else False
        if is_v: raise
        sys.stderr.write(f"xz_decompressor: E_FORMAT: {exc}\n")
        sys.exit(E_FORMAT)
    except XZUnsupportedError as exc:
        is_v = args.verbose if argparse is not None else False
        if is_v: raise
        sys.stderr.write(f"xz_decompressor: E_UNSUPPORTED: {exc}\n")
        sys.exit(E_UNSUPPORTED)
    except XZCheckError as exc:
        is_v = args.verbose if argparse is not None else False
        if is_v: raise
        sys.stderr.write(f"xz_decompressor: E_CHECK: {exc}\n")
        sys.exit(E_CHECK)
    except (EOFError, ValueError) as exc:
        is_v = args.verbose if argparse is not None else False
        if is_v: raise
        sys.stderr.write(f"xz_decompressor: E_FORMAT: {exc}\n")
        sys.exit(E_FORMAT)
    except OSError as exc:
        is_v = args.verbose if argparse is not None else False
        if is_v: raise
        sys.stderr.write(f"xz_decompressor: E_IO: {exc}\n")
        sys.exit(E_IO)
    except Exception as exc:
        is_v = args.verbose if argparse is not None else False
        if is_v: raise
        sys.stderr.write(f"xz_decompressor: E_RESOURCE: {exc}\n")
        sys.exit(E_RESOURCE)

if __name__ == "__main__":
    main()
