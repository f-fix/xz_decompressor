#!/usr/bin/env python3
"""
xz_decompressor - Pure-Python XZ / LZMA2 Streaming Decompression Engine.

Zero required external dependencies, apart from --test mode.

Normal operation compatible with MicroPython, PyPy, and CPython.
"""

import sys
import os
import time
import struct
import hashlib
import io
import array


def ensure_dir(path):
    p_str = str(path).replace("\\", "/").rstrip("/")
    if not p_str or p_str == ".":
        return
    parts = p_str.split("/")
    cur = ""
    for part in parts:
        if not part and not cur:
            cur = "/"
            continue
        if not part or part == ".":
            continue
        cur = (
            f"{cur}/{part}"
            if (cur and cur != "/")
            else (f"/{part}" if cur == "/" else part)
        )
        try:
            os.mkdir(cur)
        except OSError:
            pass


def _cleanup_empty_scratch():
    try:
        p = Path(".xz_history")
        if p.exists() and not list(p.glob("*")):
            p.rmdir()
    except Exception:
        pass


try:
    import atexit

    atexit.register(_cleanup_empty_scratch)
except (ImportError, AttributeError):
    if hasattr(sys, "atexit"):
        sys.atexit(_cleanup_empty_scratch)


def _encode_state_dict(d):
    lines = []
    for k, v in d.items():
        if v is None:
            lines.append(f"{k}:n:")
        elif isinstance(v, bool):
            lines.append(f"{k}:b:{'1' if v else '0'}")
        elif isinstance(v, int):
            lines.append(f"{k}:i:{v}")
        elif isinstance(v, str):
            lines.append(f"{k}:s:{v}")
        elif isinstance(v, (bytes, bytearray)):
            lines.append(f"{k}:x:{bytes(v).hex()}")
        elif isinstance(v, array.array):
            lines.append(f"{k}:A:{bytes(v).hex()}")
        elif isinstance(v, dict):
            sub_pairs = ";".join(f"{dk}={dv}" for dk, dv in v.items())
            lines.append(f"{k}:D:{sub_pairs}")
        elif isinstance(v, list):
            if not v:
                lines.append(f"{k}:L:")
            elif isinstance(v[0], int):
                val_str = ",".join(str(x) for x in v)
                lines.append(f"{k}:Li:{val_str}")
            elif isinstance(v[0], (tuple, list)):
                val_str = ";".join(f"{x[0]},{x[1]}" for x in v)
                lines.append(f"{k}:Lp:{val_str}")
    return "\n".join(lines).encode("utf-8")


def _decode_state_dict(b):
    d = {}
    for line in b.decode("utf-8").splitlines():
        if not line:
            continue
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        k, t, val = parts
        if t == "n":
            d[k] = None
        elif t == "b":
            d[k] = val == "1"
        elif t == "i":
            d[k] = int(val)
        elif t == "s":
            d[k] = val
        elif t == "x":
            d[k] = bytes.fromhex(val) if val else b""
        elif t == "A":
            d[k] = array.array("H", bytes.fromhex(val) if val else b"")
        elif t == "D":
            sub_d = dict(p.split("=", 1) for p in val.split(";") if "=" in p)
            if "max_history" in sub_d:
                try:
                    sub_d["max_history"] = int(sub_d["max_history"])
                except Exception:
                    pass
            d[k] = sub_d
        elif t == "L":
            d[k] = []
        elif t == "Li":
            d[k] = [int(x) for x in val.split(",")] if val else []
        elif t == "Lp":
            d[k] = (
                [tuple(int(y) for y in x.split(",")) for x in val.split(";")]
                if val
                else []
            )
    return d


class _PathFallback:
    def __init__(self, p):
        if isinstance(p, _PathFallback):
            self._p = p._p
        else:
            self._p = str(p)

    def __str__(self):
        return self._p

    def __fspath__(self):
        return self._p

    def __repr__(self):
        return f"Path({self._p!r})"

    def __eq__(self, other):
        return str(self) == str(other)

    def __lt__(self, other):
        return str(self) < str(other)

    def __le__(self, other):
        return str(self) <= str(other)

    def __gt__(self, other):
        return str(self) > str(other)

    def __ge__(self, other):
        return str(self) >= str(other)

    def __hash__(self):
        return hash(self._p)

    @property
    def parent(self):
        norm = self._p.replace("\\", "/").rstrip("/")
        if "/" in norm:
            d = norm.rsplit("/", 1)[0]
            return _PathFallback(d if d else "/")
        return _PathFallback(".")

    @property
    def name(self):
        norm = self._p.replace("\\", "/").rstrip("/")
        if "/" in norm:
            return norm.rsplit("/", 1)[-1]
        return norm

    @property
    def stem(self):
        n = self.name
        return n.rsplit(".", 1)[0] if "." in n else n

    @property
    def suffix(self):
        n = self.name
        return ("." + n.rsplit(".", 1)[1]) if "." in n else ""

    def resolve(self):
        return self

    def exists(self):
        try:
            os.stat(self._p)
            return True
        except OSError:
            return False

    def is_dir(self):
        try:
            s = os.stat(self._p)
            return bool(s[0] & 0o040000) if hasattr(s, "__getitem__") else False
        except OSError:
            return False

    def is_file(self):
        try:
            s = os.stat(self._p)
            return not bool(s[0] & 0o040000)
        except OSError:
            return False

    def mkdir(self, parents=False, exist_ok=False):
        ensure_dir(self._p)

    def unlink(self, missing_ok=False):
        try:
            os.remove(self._p)
        except OSError:
            if not missing_ok:
                raise

    def rmdir(self):
        try:
            os.rmdir(self._p)
        except OSError:
            pass

    def read_bytes(self):
        with open(self._p, "rb") as f:
            return f.read()

    def write_bytes(self, b):
        with open(self._p, "wb") as f:
            return f.write(b)

    def read_text(self, encoding="utf-8"):
        with open(self._p, "r", encoding=encoding) as f:
            return f.read()

    def write_text(self, s, encoding="utf-8"):
        with open(self._p, "w", encoding=encoding) as f:
            return f.write(s)

    def stat(self):
        return os.stat(self._p)

    def glob(self, pat):
        res = []
        try:
            prefix = pat.split("*")[0] if "*" in pat else pat
            suffix = pat.split("*")[-1] if "*" in pat else ""
            for fn in os.listdir(self._p):
                if fn.startswith(prefix) and fn.endswith(suffix):
                    res.append(_PathFallback(f"{self._p.rstrip('/')}/{fn}"))
        except OSError:
            pass
        return res

    def __truediv__(self, other):
        norm = self._p.rstrip("/\\")
        o = str(other).lstrip("/\\")
        return _PathFallback(f"{norm}/{o}")


_is_embedded_python = getattr(
    sys, "implementation", None
) and sys.implementation.name in ("micropython", "circuitpython")

if _is_embedded_python:
    Path = _PathFallback
else:
    try:
        from pathlib import Path

        _test_p = Path(".")
        if not hasattr(_test_p, "mkdir") or (
            isinstance(_test_p, str) and not hasattr(str, "mkdir")
        ):
            Path = _PathFallback
    except Exception:
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

try:
    import binascii

    _native_crc32 = binascii.crc32
except (ImportError, AttributeError):
    _native_crc32 = None

if _native_crc32 is not None:

    @micropython.native
    def crc32(data, val=0):
        return _native_crc32(data, val) & 0xFFFFFFFF

else:
    CRC32_TABLE = array.array("I", [0] * 256)
    for _i in range(256):
        _c = _i
        for _ in range(8):
            _c = (_c >> 1) ^ 0xEDB88320 if (_c & 1) else (_c >> 1)
        CRC32_TABLE[_i] = _c

    @micropython.native
    def crc32(data, val=0):
        c = val ^ 0xFFFFFFFF
        for b in data:
            c = CRC32_TABLE[(c ^ b) & 0xFF] ^ (c >> 8)
        return (c ^ 0xFFFFFFFF) & 0xFFFFFFFF


# --- Pure-Python CRC64 (ECMA-182 Reflected / XZ) ---
CRC64_TABLE = None


def _init_crc64():
    global CRC64_TABLE
    if CRC64_TABLE is None:
        CRC64_TABLE = []
        _POLY64 = 0xC96C5795D7870F42
        for _i in range(256):
            _c = _i
            for _ in range(8):
                _c = (_c >> 1) ^ _POLY64 if (_c & 1) else (_c >> 1)
            CRC64_TABLE.append(_c)


@micropython.native
def crc64(data, val=0):
    if _is_embedded_python:
        c = val ^ 0xFFFFFFFFFFFFFFFF
        _p = 0xC96C5795D7870F42
        for b in data:
            c ^= b
            for _ in range(8):
                c = (c >> 1) ^ _p if (c & 1) else (c >> 1)
        return (c ^ 0xFFFFFFFFFFFFFFFF) & 0xFFFFFFFFFFFFFFFF
    else:
        if CRC64_TABLE is None:
            _init_crc64()
        c = val ^ 0xFFFFFFFFFFFFFFFF
        for b in data:
            c = CRC64_TABLE[(c ^ b) & 0xFF] ^ (c >> 8)
        return (c ^ 0xFFFFFFFFFFFFFFFF) & 0xFFFFFFFFFFFFFFFF


def atomic_replace(src, dst):
    src_str = str(src)
    dst_str = str(dst)
    try:
        os.replace(src_str, dst_str)
    except (AttributeError, OSError):
        try:
            os.remove(dst_str)
        except OSError:
            pass
        os.rename(src_str, dst_str)


def _fsync_fileobj(fileobj):
    """Flush userspace buffers and request durable storage where supported."""
    if fileobj is None:
        return
    try:
        fileobj.flush()
    except Exception:
        pass
    try:
        fileno = fileobj.fileno()
        os.fsync(fileno)
    except (AttributeError, OSError, ValueError):
        # MicroPython ports and some virtual filesystems do not expose fsync.
        pass


def _fsync_path(path):
    try:
        with open(str(path), "rb") as f:
            _fsync_fileobj(f)
    except (OSError, IOError):
        pass


def _fsync_parent(path):
    """Best-effort directory-entry durability after atomic replacement."""
    parent = str(Path(path).parent)
    try:
        fd = os.open(parent, getattr(os, "O_RDONLY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except (AttributeError, OSError, TypeError):
        pass


def is_seekable(stream):
    try:
        if hasattr(stream, "seekable"):
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


def _days_before_year(y):
    return y * 365 + y // 4 - y // 100 + y // 400


def _days_before_month(year, month):
    days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    d = days[month - 1]
    if month > 2 and is_leap:
        d += 1
    return d


def timegm_pure(year, month, day, hour=0, minute=0, second=0):
    days = (
        _days_before_year(year - 1)
        - _days_before_year(1969)
        + _days_before_month(year, month)
        + (day - 1)
    )
    return days * 86400 + hour * 3600 + minute * 60 + second


def parse_iso8601_flexible(s):
    s = str(s).strip()
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1].strip()

    if "T" in s:
        d_part, t_part = s.split("T", 1)
    elif "t" in s:
        d_part, t_part = s.split("t", 1)
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
                hour, minute, second = (
                    int(t_main[:2]),
                    int(t_main[2:4]),
                    int(t_main[4:6]),
                )
            else:
                raise ValueError(f"Invalid time format: {t_part}")

    return timegm_pure(year, month, day, hour, minute, second) + subsecond


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
    def __init__(
        self,
        stream_index,
        next_stream_index=None,
        input_stream_len=0,
        overall_input_offset=0,
        output_stream_len=0,
        overall_output_offset=0,
        has_next=False,
    ):
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


def _generate_stream_id():
    try:
        import urandom as _rnd
    except ImportError:
        try:
            import random as _rnd
        except ImportError:
            _rnd = None
    if _rnd is not None:
        try:
            if hasattr(_rnd, "getrandbits"):
                r1 = _rnd.getrandbits(32)
                r2 = _rnd.getrandbits(32)
                return f"{r1:08x}{r2:08x}"
            elif hasattr(_rnd, "randint"):
                p1 = _rnd.randint(0, 0xFFFF)
                p2 = _rnd.randint(0, 0xFFFF)
                p3 = _rnd.randint(0, 0xFFFF)
                p4 = _rnd.randint(0, 0xFFFF)
                return f"{p1:04x}{p2:04x}{p3:04x}{p4:04x}"
        except Exception:
            pass
    t = int(_get_perf_counter() * 1000000) ^ int(time.time() * 1000)
    return f"{t & 0xFFFFFFFFFFFFFFFF:016x}"


def find_preferred_storage_dir(prefix="xz_history"):
    # Priority 1: PSRAM / RAM-disk VFS mount points (highest performance, zero flash wear)
    psram_candidates = ["/psram", "/ramdisk", "/vfs_ram", "/tmp"]
    for cand in psram_candidates:
        p = Path(cand)
        try:
            if p.is_dir() and (
                os.access(str(p), os.W_OK)
                if (hasattr(os, "access") and hasattr(os, "W_OK"))
                else True
            ):
                target = p / prefix
                return str(target), "psram"
        except Exception:
            pass

    # Priority 2: External flash / SD card / eMMC (MicroPython / CircuitPython default over onboard flash)
    ext_candidates = ["/sd", "/sdcard", "/emmc", "/external"]
    for cand in ext_candidates:
        p = Path(cand)
        try:
            if p.is_dir() and (
                os.access(str(p), os.W_OK)
                if (hasattr(os, "access") and hasattr(os, "W_OK"))
                else True
            ):
                target = p / prefix
                return str(target), "external_flash"
        except Exception:
            pass

    # Priority 3: Local scratch directory
    target = Path(f".{prefix}").resolve()
    return str(target), "local"


def select_block_size(block_size_choice="auto", available_mem=None):
    if block_size_choice not in (None, "auto"):
        return parse_memory_size(block_size_choice)

    if available_mem is None:
        available_mem = get_available_memory()

    # On embedded runtimes, default to 8KiB unless there is at least 80MiB of RAM available
    if _is_embedded_python and available_mem < 80 * 1024 * 1024:
        return 8192
    if available_mem < 512 * 1024:
        return 8192
    return 65536


def select_history_backend(
    backend_choice="auto",
    storage_dir=None,
    available_mem=None,
    max_history=67108864,
    stream_id="0000000000000000",
):
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
        return DirectoryBlockHistoryStore(s_dir, stream_id=stream_id)

    # Automatic selection based on memory threshold:
    # A caller that supplies storage_dir (notably resume_dir) needs durable
    # history even on high-RAM hosts. Do not silently select MemoryHistory.
    if storage_dir is not None:
        return DirectoryBlockHistoryStore(
            storage_dir, stream_id=stream_id, max_history=max_history
        )
    # When available memory is low, use disk-backed history.
    if available_mem < 72 * 1024 * 1024:
        s_dir, stype = (
            find_preferred_storage_dir()
            if storage_dir is None
            else (storage_dir, "custom")
        )
        return DirectoryBlockHistoryStore(s_dir, stream_id=stream_id)
    else:
        return MemoryHistory(max_history=max_history)


# --- Pluggable History Backends ---


class DurableReplayInput:
    """Record bytes read from a non-seekable source and replay post-checkpoint read-ahead.

    The spool contains every byte ever returned to the decoder. A checkpoint records
    the spool length at its commit point. On restore, bytes appended after that point
    are replayed before any further bytes are requested from the live source.
    """

    def __init__(self, source, spool_path):
        self.source = source
        self.path = Path(spool_path)
        ensure_dir(self.path.parent)
        self._spool = open(str(self.path), "a+b")
        self._spool.seek(0, 2)
        self._end = self._spool.tell()
        self._replay_pos = self._end

    def set_replay_offset(self, offset):
        self._replay_pos = max(0, min(int(offset), self._end))

    def read(self, n=-1):
        if n == 0:
            return b""
        # First replay bytes recorded after the last committed checkpoint.
        if self._replay_pos < self._end:
            self._spool.seek(self._replay_pos)
            take = (
                self._end - self._replay_pos
                if n is None or n < 0
                else min(n, self._end - self._replay_pos)
            )
            data = self._spool.read(take)
            self._replay_pos += len(data)
            return data
        data = self.source.read(n)
        if data:
            if isinstance(data, str):
                raise TypeError("binary input stream required")
            self._spool.seek(self._end)
            self._spool.write(data)
            self._spool.flush()
            # Persist read-ahead promptly: otherwise a process crash could lose
            # bytes already consumed from a non-replayable upstream stream.
            try:
                os.fsync(self._spool.fileno())
            except (AttributeError, OSError):
                pass
            self._end += len(data)
            self._replay_pos = self._end
        return data

    def durable_offset(self):
        return self._end

    def sync(self):
        self._spool.flush()
        try:
            os.fsync(self._spool.fileno())
        except (AttributeError, OSError):
            pass

    def seekable(self):
        return False

    def readable(self):
        return True

    def close(self):
        try:
            self._spool.close()
        finally:
            close = getattr(self.source, "close", None)
            # Do not close process-owned stdin/stdout streams.
            if callable(close) and self.source not in (
                getattr(sys, "stdin", None),
                getattr(sys, "stdout", None),
            ):
                close()


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

    def sync(self):
        """Make pending history writes durable when the platform supports it."""
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
            del self.history[: len(self.history) - self.max_history]

    def history_len(self):
        return min(len(self.history), self.max_history)

    def get_history_slice(self, distance, length):
        h_len = self.history_len()
        if distance <= 0 or distance > h_len or length <= 0 or length > distance:
            raise XZFormatError(
                f"Invalid history reference: distance={distance}, length={length}, available={h_len}"
            )
        start_idx = len(self.history) - distance
        end_idx = start_idx + length
        return bytes(self.history[start_idx:end_idx])

    def tail_crc32(self, n=65536):
        avail = min(n, len(self.history))
        if avail <= 0:
            return 0
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

    def __init__(
        self,
        work_dir,
        max_history=67108864,
        max_blocks=None,
        stream_id="0000000000000000",
    ):
        self.work_dir = Path(work_dir).resolve()
        self.stream_id = stream_id or "0000000000000000"
        ensure_dir(self.work_dir)
        if max_blocks is not None:
            self.max_pages = max_blocks
            self.max_history = max_blocks * self.HISTORY_PAGE
        else:
            self.max_history = max_history
            self.max_pages = (max_history // self.HISTORY_PAGE) + 1  # 1025
        self.total_history_written = 0
        self.page_files = {}  # page_index -> Path
        self.block_files = self.page_files  # backward compatible alias
        self.state_files = {}  # block_index -> Path
        self.lru_cache = {}  # page_index -> bytearray
        self.lru_order = []
        self._scan_existing()

    def _scan_existing(self):
        self.page_files.clear()
        self.state_files.clear()
        prefix = f"{self.stream_id}_"
        for p in sorted(self.work_dir.glob(f"{prefix}*.page"), key=str):
            parts = p.stem.split("_")
            if len(parts) >= 2 and parts[-1].isdigit():
                idx = int(parts[-1])
                self.page_files[idx] = p
        for p in sorted(self.work_dir.glob(f"{prefix}*.block"), key=str):
            parts = p.stem.split("_")
            if len(parts) >= 2 and parts[-1].isdigit():
                idx = int(parts[-1])
                self.page_files[idx] = p
        for p in sorted(self.work_dir.glob(f"{prefix}*.state"), key=str):
            parts = p.stem.split("_")
            if len(parts) >= 2 and parts[-1].isdigit():
                idx = int(parts[-1])
                self.state_files[idx] = p

    def append_block(
        self,
        block_data: bytes,
        block_index: int = 0,
        input_hash: str = None,
    ) -> None:
        if not input_hash:
            input_hash = self.stream_id
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

            pass

            self.total_history_written += take
            off_in_block += take
            rem -= take

            if page_idx >= self.max_pages:
                self.evict_prior(page_idx - self.max_pages + 1)

    def get_history_slice(self, distance: int, length: int) -> bytes:
        h_len = self.history_len()
        if distance <= 0 or distance > h_len or length <= 0 or length > distance:
            raise XZFormatError(
                f"Invalid history reference: distance={distance}, length={length}, available={h_len}"
            )
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

            p_path = self.page_files[page_idx]
            with open(str(p_path), "rb") as f:
                f.seek(page_off)
                part = f.read(avail)

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

    def checkpoint(
        self, block_index: int, state_record: dict, input_hash: str = None
    ) -> None:
        if not input_hash:
            input_hash = self.stream_id
        target_name = f"{input_hash}_{block_index:08d}.state"
        target_path = self.work_dir / target_name
        tmp_path = self.work_dir / f".tmp_{target_name}"
        tmp_path.write_bytes(
            _encode_state_dict(state_record)
            if isinstance(state_record, dict)
            else state_record
        )
        atomic_replace(tmp_path, target_path)
        self.state_files[block_index] = target_path

    def restore(self, block_index: int) -> dict:
        if block_index in self.state_files:
            try:
                return _decode_state_dict(self.state_files[block_index].read_bytes())
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
            with open(str(p_path), "r+b") as f:
                f.seek(curr_off)
                f.truncate()
            if curr_page in self.lru_cache:
                del self.lru_cache[curr_page][curr_off:]

    def sync(self):
        for page_path in list(self.page_files.values()):
            _fsync_path(page_path)

    def evict_prior(self, min_retained_page: int) -> None:
        to_del = [
            idx for idx in list(self.page_files.keys()) if idx < min_retained_page
        ]
        for idx in to_del:
            self.page_files[idx].unlink(missing_ok=True)
            self.page_files.pop(idx, None)
            self.lru_cache.pop(idx, None)

    def reset(self) -> None:
        for p in list(self.page_files.values()):
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
        self.page_files.clear()
        self.lru_cache.clear()
        self.lru_order.clear()
        self.total_history_written = 0

    def cleanup(self) -> None:
        self.lru_cache.clear()
        self.lru_order.clear()
        for p in list(self.page_files.values()) + list(self.state_files.values()):
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
        for p in self.work_dir.glob(f"{self.stream_id}_*"):
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
        for p in self.work_dir.glob(f".tmp_{self.stream_id}_*"):
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
        self.page_files.clear()
        self.state_files.clear()
        try:
            if not list(self.work_dir.glob("*")):
                self.work_dir.rmdir()
        except Exception:
            pass


class SeekableOutputFileHistory(HistoryBackend):
    persistent = True

    def __init__(
        self,
        file_handle,
        max_history=67108864,
        block_size=65536,
        max_cache_bytes=0,
    ):
        self.file_handle = file_handle
        self.max_history = max_history
        self.block_size = block_size
        self.max_cache_bytes = max_cache_bytes
        self.total_written = 0
        self.cached_blocks = {}
        self.lru_order = []
        self.current_cache_bytes = 0

    def append_block(self, block_data, block_index=0, input_hash=""):
        self.total_written += len(block_data)
        if self.max_cache_bytes > 0:
            b_idx = (self.total_written - len(block_data)) // self.block_size
            self.cached_blocks[b_idx] = bytes(block_data)
            self.lru_order.append(b_idx)
            self.current_cache_bytes += len(block_data)
            while self.current_cache_bytes > self.max_cache_bytes and self.lru_order:
                oldest_idx = self.lru_order.pop(0)
                old_data = self.cached_blocks.pop(oldest_idx, None)
                if old_data:
                    self.current_cache_bytes -= len(old_data)

    def history_len(self):
        return min(self.total_written, self.max_history)

    def get_history_slice(self, distance, length):
        h_len = self.history_len()
        if distance <= 0 or distance > h_len or length <= 0 or length > distance:
            raise XZFormatError(
                f"Invalid history reference: distance={distance}, length={length}, available={h_len}"
            )
        start_pos = self.total_written - distance
        if self.max_cache_bytes > 0:
            b_start = start_pos // self.block_size
            b_end = (start_pos + length - 1) // self.block_size
            if b_start == b_end and b_start in self.cached_blocks:
                off = start_pos % self.block_size
                return self.cached_blocks[b_start][off : off + length]
            elif (
                all(idx in self.cached_blocks for idx in range(b_start, b_end + 1))
                and b_end - b_start <= 4
            ):
                res = bytearray()
                curr = start_pos
                rem = length
                while rem > 0:
                    idx = curr // self.block_size
                    off = curr % self.block_size
                    avail = min(rem, len(self.cached_blocks[idx]) - off)
                    res.extend(self.cached_blocks[idx][off : off + avail])
                    curr += avail
                    rem -= avail
                return bytes(res)

        cur_pos = self.file_handle.tell()
        self.file_handle.seek(start_pos)
        chunk = self.file_handle.read(length)
        self.file_handle.seek(cur_pos)
        return chunk

    def tail_crc32(self, n=65536):
        avail = min(n, self.total_written)
        if avail <= 0:
            return 0
        start_pos = self.total_written - avail
        cur_pos = self.file_handle.tell()
        self.file_handle.seek(start_pos)
        chunk = self.file_handle.read(avail)
        self.file_handle.seek(cur_pos)
        return crc32(chunk)

    def truncate_to(self, history_total):
        self.total_written = history_total
        self.file_handle.seek(history_total)
        self.file_handle.truncate()
        if self.max_cache_bytes > 0:
            curr_b = history_total // self.block_size
            to_del = [idx for idx in list(self.cached_blocks.keys()) if idx > curr_b]
            for idx in to_del:
                d = self.cached_blocks.pop(idx, None)
                if d:
                    self.current_cache_bytes -= len(d)
                if idx in self.lru_order:
                    self.lru_order.remove(idx)

    def reset(self):
        self.total_written = 0
        self.cached_blocks.clear()
        self.lru_order.clear()
        self.current_cache_bytes = 0

    def cleanup(self):
        self.cached_blocks.clear()
        self.lru_order.clear()
        self.current_cache_bytes = 0


class FileHistory(HistoryBackend):
    persistent = True

    def __init__(self, file_path, max_history=67108864):
        self.file_path = Path(file_path).resolve()
        ensure_dir(self.file_path.parent)
        # Preserve existing history until a validated checkpoint tells us the
        # committed length. Truncating here destroys crash-resume data.
        if self.file_path.exists():
            self.f = open(str(self.file_path), "r+b")
            self.f.seek(0, 2)
            self.total_written = self.f.tell()
        else:
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
            raise XZFormatError(
                f"Invalid history reference: distance={distance}, length={length}, available={h_len}"
            )
        start_pos = max(0, self.total_written - distance)
        read_len = min(length, self.total_written - start_pos)
        self.f.seek(start_pos)
        return self.f.read(read_len)

    def tail_crc32(self, n=65536):
        avail = min(n, self.history_len())
        if avail <= 0:
            return 0
        data = self.get_history_slice(avail, avail)
        return crc32(data)

    def truncate_to(self, history_total):
        self.total_written = history_total
        self.f.seek(history_total)
        self.f.truncate()

    def sync(self):
        _fsync_fileobj(self.f)

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
            "max_history": self.max_history,
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
            self.read_byte()  # Discard first 0x00 byte
            self.code = (
                (self.read_byte() << 24)
                | (self.read_byte() << 16)
                | (self.read_byte() << 8)
                | self.read_byte()
            )
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
            symbol |= bit << i
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


def _is_valid_lzma_dict(d):
    if d in (0, 0xFFFFFFFF):
        return True
    if (d & (d - 1)) == 0:
        return True
    if d % 3 == 0 and ((d // 3) & ((d // 3) - 1)) == 0:
        return True
    return False


@micropython.native
def decode_len_val(rd, probs, pos_state):
    if rd.decode_bit(probs, 0) == 0:
        return rd.decode_bittree(probs, 2 + (pos_state << 3), 3)
    if rd.decode_bit(probs, 1) == 0:
        return 8 + rd.decode_bittree(probs, 130 + (pos_state << 3), 3)
    return 16 + rd.decode_bittree(probs, 258, 8)


# --- Universal XZ / LZMA2 Decompression Engine ---


class XZStreamDecompressor:
    def __init__(
        self,
        input_source,
        history_backend=None,
        block_size=65536,
        force_gc=True,
        zero_copy=False,
    ):
        self.zero_copy = zero_copy
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
        self.is_legacy_lzma = False
        self.legacy_uncomp_size = 0
        self.legacy_has_eos = False
        self.rd = None

        self.PROB_INIT = 1024
        self.state = 0
        self.reps = [0, 0, 0, 0]
        self.lc = 3
        self.lp = 0
        self.pb = 2
        self._reinit_probs()
        self.p_lit = array.array("H", [1024] * (0x300 << (self.lc + self.lp)))

        self.in_chunk = False
        self.ctrl = 0
        self.mode = 0
        self.chunk_uncomp_sz = 0
        self.chunk_comp_sz = 0
        self.chunk_decoded = 0
        self.chunk_compressed = b""
        self.chunk_stream_off = 0
        self.chunk_stream_buf = b""
        self.chunk_comp_rem = 0
        self.comp_offset = 0
        self.raw_chunk = b""
        self.raw_pos = 0
        self.rd_code = 0
        self.rd_range = 0

        self.in_match_copy = False
        self.match_dist = 0
        self.match_rem = 0

        # Front-loaded reusable event singletons to avoid GC garbage spikes in decode loop
        self._chunk_event = ChunkOutputEvent(b"")
        self._block_committed_event = BlockCommittedEvent(0, b"", None)
        self._progress_event = ProgressEvent(0, 0, 0)
        self._enable_state_export = False

    def _reinit_probs(self):
        self.p_is_match = array.array("H", [1024] * (12 << 4))
        self.p_is_rep = array.array("H", [1024] * 12)
        self.p_is_rep_g0 = array.array("H", [1024] * 12)
        self.p_is_rep_g1 = array.array("H", [1024] * 12)
        self.p_is_rep_g2 = array.array("H", [1024] * 12)
        self.p_is_rep0_long = array.array("H", [1024] * (12 << 4))
        self.p_pos_slot = array.array("H", [1024] * (4 << 6))
        self.p_spec_pos = array.array("H", [1024] * 115)
        self.p_align = array.array("H", [1024] * 16)
        self.p_len = array.array("H", [1024] * 514)
        self.p_rep_len = array.array("H", [1024] * 514)

    def _read_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.input.read(min(4096, n - len(buf)))
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
            "cur_block": self.cur_block[: self.cur_block_pos].hex(),
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
            "raw_rem": getattr(self, "raw_rem", 0),
            "chunk_stream_off": getattr(self, "chunk_stream_off", 0),
            "chunk_stream_buf": bytes(getattr(self, "chunk_stream_buf", b"")).hex(),
            "chunk_comp_rem": getattr(self, "chunk_comp_rem", 0),
            "rd_code": self.rd_code,
            "rd_range": self.rd_range,
            "in_match_copy": self.in_match_copy,
            "need_dict_reset": self.need_dict_reset,
            "need_props": self.need_props,
            "match_dist": self.match_dist,
            "match_rem": self.match_rem,
            "input_offset": self.overall_input_offset,
            "history": (
                self.history.export_state()
                if hasattr(self.history, "export_state")
                else None
            ),
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
        self.cur_block[: len(cb)] = cb
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
        self.p_is_match = array.array("H", s["p_is_match"])
        self.p_is_rep = array.array("H", s["p_is_rep"])
        self.p_is_rep_g0 = array.array("H", s["p_is_rep_g0"])
        self.p_is_rep_g1 = array.array("H", s["p_is_rep_g1"])
        self.p_is_rep_g2 = array.array("H", s["p_is_rep_g2"])
        self.p_is_rep0_long = array.array("H", s["p_is_rep0_long"])
        self.p_pos_slot = array.array("H", s["p_pos_slot"])
        self.p_spec_pos = array.array("H", s["p_spec_pos"])
        self.p_align = array.array("H", s["p_align"])
        self.p_len = array.array("H", s["p_len"])
        self.p_rep_len = array.array("H", s["p_rep_len"])
        self.p_lit = array.array("H", s["p_lit"])
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
        self.raw_rem = s.get("raw_rem", 0)
        self.chunk_stream_off = s.get("chunk_stream_off", 0)
        self.chunk_stream_buf = bytes.fromhex(s.get("chunk_stream_buf", ""))
        self.chunk_comp_rem = s.get("chunk_comp_rem", 0)
        self.rd_code = s["rd_code"]
        self.rd_range = s["rd_range"]
        self.in_match_copy = s["in_match_copy"]
        self.need_dict_reset = s.get("need_dict_reset", True)
        self.need_props = s.get("need_props", True)
        self.match_dist = s["match_dist"]
        self.match_rem = s["match_rem"]
        if (
            hasattr(self.input, "seek")
            and "input_offset" in s
            and is_seekable(self.input)
        ):
            self.input.seek(s["input_offset"])
        if hasattr(self.history, "import_state") and s.get("history"):
            self.history.import_state(s["history"])

    def _commit_block(self):
        if self.zero_copy:
            committed = memoryview(self.cur_block)[: self.cur_block_pos]
        else:
            committed = bytes(self.cur_block[: self.cur_block_pos])
        self._update_check(committed)
        self.history.append_block(committed, self.block_idx)
        self.overall_output_offset += len(committed)
        self.stream_out_offset += len(committed)
        self.block_idx += 1
        self.cur_block_pos = 0
        if self.force_gc:
            try:
                import gc

                if hasattr(gc, "mem_free"):
                    if gc.mem_free() < 65536:
                        gc.collect()
                else:
                    gc.collect()
            except Exception:
                pass
        return committed

    def decode_block(self):
        while True:
            if self.phase == "EOF":
                return b"", True
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
                        if self.overall_input_offset == 0 and self.stream_index == 0:
                            raise XZFormatError(
                                "File format not recognized: empty input"
                            )
                        if self.null_count % 4 != 0:
                            raise XZFormatError("Stream padding not multiple of 4")
                        self.phase = "EOF"
                        return b"", True
                    self.input_hasher.update(b)

                    if self.overall_input_offset == 1 and self.stream_index == 0:
                        if b == b"\xfd":
                            magic = bytearray([0xFD]) + self._read_exact(5)
                            if bytes(magic) != b"\xfd7zXZ\x00":
                                raise XZFormatError("Invalid magic")
                            break
                        pbyte = b[0]
                        if pbyte > 224:
                            raise XZFormatError(
                                f"File format not recognized: invalid header byte 0x{pbyte:02x}"
                            )
                        hdr_rest = self._read_exact(12)
                        dict_sz = struct.unpack("<I", hdr_rest[0:4])[0]
                        uncomp_sz = struct.unpack("<Q", hdr_rest[4:12])[0]
                        if not _is_valid_lzma_dict(dict_sz):
                            raise XZFormatError(
                                f"Invalid LZMA dictionary size: {dict_sz}"
                            )
                        if uncomp_sz != 0xFFFFFFFFFFFFFFFF and uncomp_sz >= (
                            256 * 1024 * 1024 * 1024
                        ):
                            raise XZFormatError(
                                f"Invalid LZMA uncompressed size: {uncomp_sz}"
                            )
                        rc_first = self.input.read(1)
                        if not rc_first:
                            raise XZFormatError(
                                "Unexpected EOF reading LZMA range decoder header"
                            )
                        self.overall_input_offset += 1
                        self.input_hasher.update(rc_first)
                        if rc_first[0] != 0:
                            raise XZFormatError(
                                f"First byte of LZMA range decoder must be 0x00, got 0x{rc_first[0]:02x}"
                            )
                        self.pb = pbyte // 45
                        rem = pbyte % 45
                        self.lp = rem // 9
                        self.lc = rem % 9
                        self.dict_size = max(
                            4096, dict_sz if dict_sz != 0xFFFFFFFF else 0x7FFFFFFF
                        )
                        self.legacy_uncomp_size = uncomp_sz
                        self.legacy_has_eos = uncomp_sz == 0xFFFFFFFFFFFFFFFF
                        self._reinit_probs()
                        self.p_lit = array.array(
                            "H", [1024] * (0x300 << (self.lc + self.lp))
                        )
                        self.state = 0
                        self.reps = [0, 0, 0, 0]
                        self.cur_block = bytearray(self.block_size)
                        self.cur_block_pos = 0
                        self.stream_out_offset = 0
                        self.history.reset()
                        self.stream_start_in = 0
                        self.stream_start_out = self.overall_output_offset
                        self.is_legacy_lzma = True

                        def _get_input_b():
                            ib = self.input.read(1)
                            if not ib:
                                raise EOFError(
                                    "Unexpected EOF reading compressed stream"
                                )
                            self.overall_input_offset += 1
                            self.input_hasher.update(ib)
                            return ib[0]

                        c1 = _get_input_b()
                        c2 = _get_input_b()
                        c3 = _get_input_b()
                        c4 = _get_input_b()
                        self.rd = RangeDecoder(_get_input_b, init_stream=False)
                        self.rd.code = (c1 << 24) | (c2 << 16) | (c3 << 8) | c4
                        self.rd.range = 0xFFFFFFFF
                        self.phase = "LEGACY_LZMA"
                        break

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

                    if self.null_count == 0 and b[0] <= 224:
                        pbyte = b[0]
                        hdr_rest = self._read_exact(12)
                        dict_sz = struct.unpack("<I", hdr_rest[0:4])[0]
                        uncomp_sz = struct.unpack("<Q", hdr_rest[4:12])[0]
                        if not _is_valid_lzma_dict(dict_sz) or (
                            uncomp_sz != 0xFFFFFFFFFFFFFFFF
                            and uncomp_sz >= (256 * 1024 * 1024 * 1024)
                        ):
                            raise XZFormatError(f"Unexpected byte 0x{b[0]:02x}")
                        rc_first = self.input.read(1)
                        if not rc_first or rc_first[0] != 0:
                            raise XZFormatError(
                                "Invalid range coder init in legacy LZMA stream"
                            )
                        self.overall_input_offset += 1
                        self.input_hasher.update(rc_first)
                        self.pb = pbyte // 45
                        rem = pbyte % 45
                        self.lp = rem // 9
                        self.lc = rem % 9
                        self.dict_size = max(
                            4096, dict_sz if dict_sz != 0xFFFFFFFF else 0x7FFFFFFF
                        )
                        self.legacy_uncomp_size = uncomp_sz
                        self.legacy_has_eos = uncomp_sz == 0xFFFFFFFFFFFFFFFF
                        self._reinit_probs()
                        self.p_lit = array.array(
                            "H", [1024] * (0x300 << (self.lc + self.lp))
                        )
                        self.state = 0
                        self.reps = [0, 0, 0, 0]
                        self.cur_block = bytearray(self.block_size)
                        self.cur_block_pos = 0
                        self.stream_out_offset = 0
                        self.history.reset()
                        self.stream_start_in = self.overall_input_offset - 14
                        self.stream_start_out = self.overall_output_offset
                        self.is_legacy_lzma = True

                        def _get_input_b():
                            ib = self.input.read(1)
                            if not ib:
                                raise EOFError(
                                    "Unexpected EOF reading compressed stream"
                                )
                            self.overall_input_offset += 1
                            self.input_hasher.update(ib)
                            return ib[0]

                        c1 = _get_input_b()
                        c2 = _get_input_b()
                        c3 = _get_input_b()
                        c4 = _get_input_b()
                        self.rd = RangeDecoder(_get_input_b, init_stream=False)
                        self.rd.code = (c1 << 24) | (c2 << 16) | (c3 << 8) | c4
                        self.rd.range = 0xFFFFFFFF
                        self.phase = "LEGACY_LZMA"
                        break

                    raise XZFormatError(f"Unexpected byte 0x{b[0]:02x}")

                if self.phase == "LEGACY_LZMA":
                    continue
                self.stream_start_in = self.overall_input_offset - 6
                self.stream_start_out = self.overall_output_offset
                self.flags = self._read_exact(2)
                if self.flags[0] != 0 or (self.flags[1] & 0xF0) != 0:
                    raise XZFormatError("Invalid flags")
                self.check_type = self.flags[1] & 0x0F
                if self.check_type not in (0, 1, 4, 10):
                    raise XZUnsupportedError(
                        f"Unsupported check type: {self.check_type}"
                    )
                crc_bytes = self._read_exact(4)
                if crc32(self.flags) != struct.unpack("<I", crc_bytes)[0]:
                    raise XZCheckError("Header CRC32 mismatch")
                self.cur_block = bytearray(self.block_size)
                self.cur_block_pos = 0
                self.stream_out_offset = 0
                self.history.reset()
                self.stream_block_records = []
                self.phase = "BLOCK_START"

            if self.phase == "LEGACY_LZMA":
                pos_mask = (1 << self.pb) - 1
                lp_mask = (1 << self.lp) - 1
                rd = self.rd

                while True:
                    if self.in_match_copy:
                        dist = self.match_dist
                        while self.match_rem > 0:
                            sp = self.block_size - self.cur_block_pos
                            copy_len = min(sp, self.match_rem)
                            if not self.legacy_has_eos:
                                rem_to_uncomp = self.legacy_uncomp_size - (
                                    self.overall_output_offset + self.cur_block_pos
                                )
                                if rem_to_uncomp <= 0:
                                    self.in_match_copy = False
                                    self.match_rem = 0
                                    break
                                if copy_len > rem_to_uncomp:
                                    copy_len = rem_to_uncomp
                            if dist < self.cur_block_pos:
                                src_s = self.cur_block_pos - 1 - dist
                                if dist == 0:
                                    b_val = self.cur_block[src_s]
                                    for _i in range(copy_len):
                                        self.cur_block[self.cur_block_pos + _i] = b_val
                                elif dist >= copy_len:
                                    self.cur_block[
                                        self.cur_block_pos : self.cur_block_pos
                                        + copy_len
                                    ] = self.cur_block[src_s : src_s + copy_len]
                                else:
                                    for i in range(copy_len):
                                        self.cur_block[self.cur_block_pos + i] = (
                                            self.cur_block[src_s + i]
                                        )
                            else:
                                h_dist = dist - self.cur_block_pos + 1
                                h_req = min(copy_len, h_dist)
                                hs = self.history.get_history_slice(h_dist, h_req)
                                self.cur_block[
                                    self.cur_block_pos : self.cur_block_pos + len(hs)
                                ] = hs
                                cpd = len(hs)
                                if cpd < copy_len:
                                    for i in range(copy_len - cpd):
                                        self.cur_block[self.cur_block_pos + cpd + i] = (
                                            self.cur_block[i]
                                        )
                            self.cur_block_pos += copy_len
                            self.match_rem -= copy_len
                            if self.cur_block_pos == self.block_size:
                                committed = self._commit_block()
                                if self.match_rem == 0:
                                    self.in_match_copy = False
                                return committed, False
                        self.in_match_copy = False

                    if not self.legacy_has_eos and (
                        self.overall_output_offset + self.cur_block_pos
                        >= self.legacy_uncomp_size
                    ):
                        break

                    try:
                        pos_state = (
                            self.stream_out_offset + self.cur_block_pos
                        ) & pos_mask
                        match_idx = (self.state << 4) + pos_state
                        if rd.decode_bit(self.p_is_match, match_idx) == 0:
                            if self.cur_block_pos > 0:
                                prev_b = self.cur_block[self.cur_block_pos - 1]
                            elif self.history.history_len() > 0:
                                ps = self.history.get_history_slice(1, 1)
                                prev_b = ps[0] if ps else 0
                            else:
                                prev_b = 0
                            lit_st = (
                                (
                                    (self.stream_out_offset + self.cur_block_pos)
                                    & lp_mask
                                )
                                << self.lc
                            ) + (prev_b >> (8 - self.lc))
                            lit_off = lit_st * 0x300
                            symbol = 1
                            if self.state >= 7:
                                dist = self.reps[0]
                                if dist < self.cur_block_pos:
                                    mb = self.cur_block[self.cur_block_pos - 1 - dist]
                                else:
                                    h_req_dist = dist - self.cur_block_pos + 1
                                    if h_req_dist <= self.history.history_len():
                                        ms = self.history.get_history_slice(
                                            h_req_dist, 1
                                        )
                                        mb = ms[0] if ms else 0
                                    else:
                                        mb = 0
                                while symbol < 0x100:
                                    mbit = (mb >> 7) & 1
                                    mb = (mb << 1) & 0xFF
                                    bit = rd.decode_bit(
                                        self.p_lit, lit_off + ((1 + mbit) << 8) + symbol
                                    )
                                    symbol = (symbol << 1) | bit
                                    if mbit != bit:
                                        break
                            while symbol < 0x100:
                                symbol = (symbol << 1) | rd.decode_bit(
                                    self.p_lit, lit_off + symbol
                                )
                            self.cur_block[self.cur_block_pos] = symbol - 0x100
                            self.cur_block_pos += 1
                            self.state = [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 4, 5][
                                self.state
                            ]

                            if self.cur_block_pos == self.block_size:
                                committed = self._commit_block()
                                return committed, False
                        else:
                            if rd.decode_bit(self.p_is_rep, self.state) == 1:
                                if rd.decode_bit(self.p_is_rep_g0, self.state) == 0:
                                    if (
                                        rd.decode_bit(self.p_is_rep0_long, match_idx)
                                        == 0
                                    ):
                                        self.state = 9 if self.state < 7 else 11
                                        length = 1
                                        dist = self.reps[0]
                                        if dist < self.cur_block_pos:
                                            self.cur_block[self.cur_block_pos] = (
                                                self.cur_block[
                                                    self.cur_block_pos - 1 - dist
                                                ]
                                            )
                                        else:
                                            ms = self.history.get_history_slice(
                                                dist - self.cur_block_pos + 1, 1
                                            )
                                            self.cur_block[self.cur_block_pos] = (
                                                ms[0] if ms else 0
                                            )
                                        self.cur_block_pos += 1
                                        if self.cur_block_pos == self.block_size:
                                            committed = self._commit_block()
                                            return committed, False
                                        continue
                                    dist = self.reps[0]
                                else:
                                    if rd.decode_bit(self.p_is_rep_g1, self.state) == 0:
                                        dist = self.reps[1]
                                    else:
                                        if (
                                            rd.decode_bit(self.p_is_rep_g2, self.state)
                                            == 0
                                        ):
                                            dist = self.reps[2]
                                        else:
                                            dist = self.reps[3]
                                            self.reps[3] = self.reps[2]
                                        self.reps[2] = self.reps[1]
                                    self.reps[1] = self.reps[0]
                                    self.reps[0] = dist
                                length = 2 + decode_len_val(
                                    rd, self.p_rep_len, pos_state
                                )
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
                                        dist = base + rd.decode_reverse_bittree(
                                            self.p_spec_pos, base - slot, nd
                                        )
                                    else:
                                        db = rd.decode_direct_bits(nd - 4) << 4
                                        ab = rd.decode_reverse_bittree(
                                            self.p_align, 0, 4
                                        )
                                        dist = base + db + ab
                                self.reps[0] = dist

                            if dist == 0xFFFFFFFF:
                                # EOS marker in LZMA1!
                                break

                            self.in_match_copy = True
                            self.match_dist = dist
                            self.match_rem = length
                            while self.match_rem > 0:
                                sp = self.block_size - self.cur_block_pos
                                copy_len = min(sp, self.match_rem)
                                if not self.legacy_has_eos:
                                    rem_to_uncomp = self.legacy_uncomp_size - (
                                        self.overall_output_offset + self.cur_block_pos
                                    )
                                    if rem_to_uncomp <= 0:
                                        self.in_match_copy = False
                                        self.match_rem = 0
                                        break
                                    if copy_len > rem_to_uncomp:
                                        copy_len = rem_to_uncomp
                                if dist < self.cur_block_pos:
                                    src_s = self.cur_block_pos - 1 - dist
                                    if dist == 0:
                                        b_val = self.cur_block[src_s]
                                        for _i in range(copy_len):
                                            self.cur_block[self.cur_block_pos + _i] = (
                                                b_val
                                            )
                                    elif dist >= copy_len:
                                        self.cur_block[
                                            self.cur_block_pos : self.cur_block_pos
                                            + copy_len
                                        ] = self.cur_block[src_s : src_s + copy_len]
                                    else:
                                        for i in range(copy_len):
                                            self.cur_block[self.cur_block_pos + i] = (
                                                self.cur_block[src_s + i]
                                            )
                                else:
                                    h_dist = dist - self.cur_block_pos + 1
                                    h_req = min(copy_len, h_dist)
                                    hs = self.history.get_history_slice(h_dist, h_req)
                                    self.cur_block[
                                        self.cur_block_pos : self.cur_block_pos
                                        + len(hs)
                                    ] = hs
                                    cpd = len(hs)
                                    if cpd < copy_len:
                                        for i in range(copy_len - cpd):
                                            self.cur_block[
                                                self.cur_block_pos + cpd + i
                                            ] = self.cur_block[i]
                                self.cur_block_pos += copy_len
                                self.match_rem -= copy_len
                                if self.cur_block_pos == self.block_size:
                                    committed = self._commit_block()
                                    if self.match_rem == 0:
                                        self.in_match_copy = False
                                    return committed, False
                            self.in_match_copy = False
                    except EOFError:
                        raise XZFormatError("Unexpected EOF in compressed stream")

                partial = (
                    bytes(self.cur_block[: self.cur_block_pos])
                    if self.cur_block_pos > 0
                    else b""
                )
                if partial:
                    self.history.append_block(partial, self.block_idx)
                    self.overall_output_offset += len(partial)
                    self.stream_out_offset += len(partial)
                    self.block_idx += 1
                    self.cur_block_pos = 0

                if not self.legacy_has_eos:
                    if self.stream_out_offset != self.legacy_uncomp_size:
                        raise XZFormatError(
                            f"Uncompressed size mismatch: got {self.stream_out_offset}, expected {self.legacy_uncomp_size}"
                        )

                pb = self.input.read(1)
                if not pb:
                    has_next = False
                    self.phase = "EOF"
                else:
                    self.overall_input_offset += 1
                    if pb == b"\xfd":
                        self._peeked_byte = pb
                        has_next = True
                        self.phase = "STREAM_HEADER"
                    elif pb == b"\x00":
                        pad_nulls = 1
                        found_next = False
                        while True:
                            nb = self.input.read(1)
                            if not nb:
                                break
                            self.overall_input_offset += 1
                            if nb == b"\x00":
                                pad_nulls += 1
                                continue
                            elif nb == b"\xfd":
                                if pad_nulls % 4 != 0:
                                    raise XZFormatError(
                                        "Stream padding not multiple of 4"
                                    )
                                self._peeked_byte = nb
                                found_next = True
                                has_next = True
                                self.phase = "STREAM_HEADER"
                                break
                            else:
                                raise XZFormatError(
                                    f"Unexpected byte after trailing zeros: 0x{nb[0]:02x}"
                                )
                        if not found_next:
                            raise XZFormatError(
                                "Trailing bytes after legacy LZMA stream"
                            )
                    else:
                        if pb[0] <= 224 and is_seekable(self.input):
                            cur = self.input.tell()
                            hdr12 = self.input.read(12)
                            if len(hdr12) == 12:
                                d_sz = struct.unpack("<I", hdr12[0:4])[0]
                                u_sz = struct.unpack("<Q", hdr12[4:12])[0]
                                rc_b = self.input.read(1)
                                self.input.seek(cur)
                                if (
                                    _is_valid_lzma_dict(d_sz)
                                    and (
                                        u_sz == 0xFFFFFFFFFFFFFFFF
                                        or u_sz < 256 * 1024**3
                                    )
                                    and rc_b == b"\x00"
                                ):
                                    self._peeked_byte = pb
                                    has_next = True
                                    self.phase = "STREAM_HEADER"
                                else:
                                    raise XZFormatError(
                                        "Trailing bytes after legacy LZMA stream"
                                    )
                            else:
                                self.input.seek(cur)
                                raise XZFormatError(
                                    "Trailing bytes after legacy LZMA stream"
                                )
                        else:
                            raise XZFormatError(
                                "Trailing bytes after legacy LZMA stream"
                            )

                self.pending_boundary = StreamBoundaryEvent(
                    stream_index=self.stream_index,
                    next_stream_index=self.stream_index + 1 if has_next else None,
                    input_stream_len=self.overall_input_offset
                    - self.stream_start_in
                    - (1 if has_next else 0),
                    overall_input_offset=self.overall_input_offset
                    - (1 if has_next else 0),
                    output_stream_len=self.overall_output_offset
                    - self.stream_start_out,
                    overall_output_offset=self.overall_output_offset,
                    has_next=has_next,
                )
                self.stream_index += 1
                if partial:
                    return partial, (not has_next)
                return b"", (not has_next)

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
                    nonlocal off
                    val = 0
                    s = 0
                    for _ in range(9):
                        b = full_bh[off]
                        off += 1
                        val |= (b & 0x7F) << s
                        if (b & 0x80) == 0:
                            return val
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
                pbyte = full_bh[off]
                off += 1
                if pbyte > 40:
                    raise XZFormatError(
                        f"Reserved LZMA2 dictionary property: {pbyte} > 40"
                    )
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
                                raise XZFormatError(
                                    "First chunk in Block must reset dictionary (ctrl=0x02 not allowed)"
                                )
                            if self.ctrl == 1:
                                self.history.reset()
                                self.reps = [0, 0, 0, 0]
                                self.need_dict_reset = False
                                self.need_props = True
                            sz_b = self._read_exact(2)
                            self.compressed_bytes_in_block += 2
                            csz = ((sz_b[0] << 8) | sz_b[1]) + 1
                            self.raw_rem = csz
                            self.raw_chunk = b""
                            self.raw_pos = 0
                            self.in_chunk = True
                        elif self.ctrl < 0x80:
                            raise XZFormatError(
                                f"Reserved LZMA2 control byte: 0x{self.ctrl:02x}"
                            )
                        else:
                            self.mode = (self.ctrl >> 5) & 3
                            if self.need_dict_reset and self.mode != 3:
                                raise XZFormatError(
                                    f"First LZMA chunk in Block must reset dictionary (mode={self.mode} != 3)"
                                )
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
                                    raise XZFormatError(
                                        f"Invalid LZMA2 property byte {pbyte} > 224"
                                    )
                                self.pb = pbyte // 45
                                rem = pbyte % 45
                                self.lp = rem // 9
                                self.lc = rem % 9
                                if self.lc + self.lp > 4:
                                    raise XZFormatError(
                                        f"lc + lp = {self.lc + self.lp} > 4 in LZMA2"
                                    )
                                self.p_lit = array.array(
                                    "H", [1024] * (0x300 << (self.lc + self.lp))
                                )
                                self.need_props = False
                            elif self.need_props:
                                raise XZFormatError(
                                    "LZMA chunk requires properties before use"
                                )
                            if self.mode == 3:
                                self.history.reset()
                                self.reps = [0, 0, 0, 0]
                                self.need_dict_reset = False
                            first_b = self._read_exact(1)[0]
                            self.compressed_bytes_in_block += 1
                            if first_b != 0:
                                raise XZFormatError(
                                    f"First byte of range decoder in chunk must be 0x00, got 0x{first_b:02x}"
                                )
                            init_code = self._read_exact(4)
                            self.compressed_bytes_in_block += 4
                            self.rd_code = (
                                (init_code[0] << 24)
                                | (init_code[1] << 16)
                                | (init_code[2] << 8)
                                | init_code[3]
                            )
                            self.rd_range = 0xFFFFFFFF
                            self.comp_offset = 5
                            self.chunk_comp_rem = self.chunk_comp_sz - 5
                            self.chunk_stream_buf = bytearray()
                            self.chunk_stream_off = 0
                            self.chunk_decoded = 0
                            self.in_chunk = True

                    if self.ctrl in (1, 2):
                        while getattr(self, "raw_rem", 0) > 0:
                            sp = self.block_size - self.cur_block_pos
                            take = min(sp, self.raw_rem)
                            chunk_b = self._read_exact(take)
                            self.compressed_bytes_in_block += take
                            self.cur_block[
                                self.cur_block_pos : self.cur_block_pos + take
                            ] = chunk_b
                            self.cur_block_pos += take
                            self.raw_rem -= take
                            if self.raw_rem == 0:
                                self.in_chunk = False
                            if self.cur_block_pos == self.block_size:
                                committed = bytes(self.cur_block)
                                self._update_check(committed)
                                self.history.append_block(committed, self.block_idx)
                                self.overall_output_offset += len(committed)
                                self.stream_out_offset += len(committed)
                                self.block_idx += 1
                                self.cur_block_pos = 0
                                return committed, False
                        self.in_chunk = False

                    elif self.ctrl >= 0x80:

                        def _rd_get():
                            if self.chunk_stream_off < len(self.chunk_stream_buf):
                                b = self.chunk_stream_buf[self.chunk_stream_off]
                                self.chunk_stream_off += 1
                                self.comp_offset += 1
                                return b
                            if getattr(self, "chunk_comp_rem", 0) <= 0:
                                return 0
                            take = min(512, self.chunk_comp_rem)
                            chunk = self.input.read(take)
                            if not chunk:
                                return 0
                            self.overall_input_offset += len(chunk)
                            self.input_hasher.update(chunk)
                            self.compressed_bytes_in_block += len(chunk)
                            self.chunk_comp_rem -= len(chunk)
                            self.chunk_stream_buf = chunk
                            self.chunk_stream_off = 1
                            self.comp_offset += 1
                            return chunk[0]

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
                                            b_val = self.cur_block[src_s]
                                            for _i in range(copy_len):
                                                self.cur_block[
                                                    self.cur_block_pos + _i
                                                ] = b_val
                                        elif dist >= copy_len:
                                            self.cur_block[
                                                self.cur_block_pos : self.cur_block_pos
                                                + copy_len
                                            ] = self.cur_block[src_s : src_s + copy_len]
                                        else:
                                            for i in range(copy_len):
                                                self.cur_block[
                                                    self.cur_block_pos + i
                                                ] = self.cur_block[src_s + i]
                                    else:
                                        h_dist = dist - self.cur_block_pos + 1
                                        h_req = min(copy_len, h_dist)
                                        hs = self.history.get_history_slice(
                                            h_dist, h_req
                                        )
                                        self.cur_block[
                                            self.cur_block_pos : self.cur_block_pos
                                            + len(hs)
                                        ] = hs
                                        cpd = len(hs)
                                        if cpd < copy_len:
                                            for i in range(copy_len - cpd):
                                                self.cur_block[
                                                    self.cur_block_pos + cpd + i
                                                ] = self.cur_block[i]
                                    self.cur_block_pos += copy_len
                                    self.chunk_decoded += copy_len
                                    self.match_rem -= copy_len
                                    if self.cur_block_pos == self.block_size:
                                        committed = bytes(self.cur_block)
                                        self._update_check(committed)
                                        self.history.append_block(
                                            committed, self.block_idx
                                        )
                                        self.overall_output_offset += len(committed)
                                        self.stream_out_offset += len(committed)
                                        self.block_idx += 1
                                        self.cur_block_pos = 0
                                        self.rd_code = rd.code
                                        self.rd_range = rd.range
                                        # comp_offset tracked in stream
                                        if self.match_rem == 0:
                                            self.in_match_copy = False
                                        return committed, False
                                self.in_match_copy = False

                            pos_state = (
                                self.stream_out_offset + self.cur_block_pos
                            ) & pos_mask
                            match_idx = (self.state << 4) + pos_state
                            if rd.decode_bit(self.p_is_match, match_idx) == 0:
                                if self.cur_block_pos > 0:
                                    prev_b = self.cur_block[self.cur_block_pos - 1]
                                elif self.history.history_len() > 0:
                                    ps = self.history.get_history_slice(1, 1)
                                    prev_b = ps[0] if ps else 0
                                else:
                                    prev_b = 0
                                lit_st = (
                                    (
                                        (self.stream_out_offset + self.cur_block_pos)
                                        & lp_mask
                                    )
                                    << self.lc
                                ) + (prev_b >> (8 - self.lc))
                                lit_off = lit_st * 0x300
                                symbol = 1
                                if self.state >= 7:
                                    dist = self.reps[0]
                                    if dist < self.cur_block_pos:
                                        mb = self.cur_block[
                                            self.cur_block_pos - 1 - dist
                                        ]
                                    else:
                                        h_req_dist = dist - self.cur_block_pos + 1
                                        if h_req_dist <= self.history.history_len():
                                            ms = self.history.get_history_slice(
                                                h_req_dist, 1
                                            )
                                            mb = ms[0] if ms else 0
                                        else:
                                            mb = 0
                                    while symbol < 0x100:
                                        mbit = (mb >> 7) & 1
                                        mb = (mb << 1) & 0xFF
                                        bit = rd.decode_bit(
                                            self.p_lit,
                                            lit_off + ((1 + mbit) << 8) + symbol,
                                        )
                                        symbol = (symbol << 1) | bit
                                        if mbit != bit:
                                            break
                                while symbol < 0x100:
                                    symbol = (symbol << 1) | rd.decode_bit(
                                        self.p_lit, lit_off + symbol
                                    )
                                self.cur_block[self.cur_block_pos] = symbol - 0x100
                                self.cur_block_pos += 1
                                self.chunk_decoded += 1
                                self.state = [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 4, 5][
                                    self.state
                                ]

                                if self.cur_block_pos == self.block_size:
                                    committed = self._commit_block()
                                    if self.force_gc:
                                        try:
                                            import gc

                                            gc.collect()
                                        except Exception:
                                            pass
                                    self.rd_code = rd.code
                                    self.rd_range = rd.range
                                    # comp_offset tracked in stream
                                    if self.chunk_decoded == self.chunk_uncomp_sz:
                                        self.in_chunk = False
                                    return committed, False
                            else:
                                if rd.decode_bit(self.p_is_rep, self.state) == 1:
                                    if rd.decode_bit(self.p_is_rep_g0, self.state) == 0:
                                        if (
                                            rd.decode_bit(
                                                self.p_is_rep0_long, match_idx
                                            )
                                            == 0
                                        ):
                                            self.state = 9 if self.state < 7 else 11
                                            length = 1
                                            dist = self.reps[0]
                                            if dist < self.cur_block_pos:
                                                self.cur_block[self.cur_block_pos] = (
                                                    self.cur_block[
                                                        self.cur_block_pos - 1 - dist
                                                    ]
                                                )
                                            else:
                                                ms = self.history.get_history_slice(
                                                    dist - self.cur_block_pos + 1, 1
                                                )
                                                self.cur_block[self.cur_block_pos] = (
                                                    ms[0] if ms else 0
                                                )
                                            self.cur_block_pos += 1
                                            self.chunk_decoded += 1
                                            if self.cur_block_pos == self.block_size:
                                                committed = self._commit_block()
                                                self.rd_code = rd.code
                                                self.rd_range = rd.range
                                                # comp_offset tracked in stream
                                                if (
                                                    self.chunk_decoded
                                                    == self.chunk_uncomp_sz
                                                ):
                                                    self.in_chunk = False
                                                return committed, False
                                            continue
                                        dist = self.reps[0]
                                    else:
                                        if (
                                            rd.decode_bit(self.p_is_rep_g1, self.state)
                                            == 0
                                        ):
                                            dist = self.reps[1]
                                        else:
                                            if (
                                                rd.decode_bit(
                                                    self.p_is_rep_g2, self.state
                                                )
                                                == 0
                                            ):
                                                dist = self.reps[2]
                                            else:
                                                dist = self.reps[3]
                                                self.reps[3] = self.reps[2]
                                            self.reps[2] = self.reps[1]
                                        self.reps[1] = self.reps[0]
                                        self.reps[0] = dist
                                    length = 2 + decode_len_val(
                                        rd, self.p_rep_len, pos_state
                                    )
                                    self.state = 8 if self.state < 7 else 11
                                else:
                                    self.reps[3] = self.reps[2]
                                    self.reps[2] = self.reps[1]
                                    self.reps[1] = self.reps[0]
                                    self.state = 7 if self.state < 7 else 10
                                    length = 2 + decode_len_val(
                                        rd, self.p_len, pos_state
                                    )
                                    l_st = min(length - 2, 3)
                                    slot = rd.decode_bittree(
                                        self.p_pos_slot, l_st << 6, 6
                                    )
                                    if slot < 4:
                                        dist = slot
                                    else:
                                        nd = (slot >> 1) - 1
                                        base = (2 | (slot & 1)) << nd
                                        if slot < 14:
                                            dist = base + rd.decode_reverse_bittree(
                                                self.p_spec_pos, base - slot, nd
                                            )
                                        else:
                                            db = rd.decode_direct_bits(nd - 4) << 4
                                            ab = rd.decode_reverse_bittree(
                                                self.p_align, 0, 4
                                            )
                                            dist = base + db + ab
                                    self.reps[0] = dist

                                if dist == 0xFFFFFFFF:
                                    raise XZFormatError(
                                        "Invalid distance 0xFFFFFFFF in LZMA2"
                                    )
                                if (
                                    hasattr(self, "dict_size")
                                    and dist >= self.dict_size
                                ):
                                    raise XZFormatError(
                                        f"Distance {dist} exceeds dictionary size {self.dict_size}"
                                    )
                                if (
                                    dist
                                    >= self.cur_block_pos + self.history.history_len()
                                ):
                                    raise XZFormatError(
                                        f"Distance {dist} exceeds available history {self.cur_block_pos + self.history.history_len()}"
                                    )
                                self.in_match_copy = True
                                self.match_dist = dist
                                self.match_rem = length
                                while self.match_rem > 0:
                                    sp = self.block_size - self.cur_block_pos
                                    copy_len = min(sp, self.match_rem)
                                    if dist < self.cur_block_pos:
                                        src_s = self.cur_block_pos - 1 - dist
                                        if dist == 0:
                                            b_val = self.cur_block[src_s]
                                            for _i in range(copy_len):
                                                self.cur_block[
                                                    self.cur_block_pos + _i
                                                ] = b_val
                                        elif dist >= copy_len:
                                            self.cur_block[
                                                self.cur_block_pos : self.cur_block_pos
                                                + copy_len
                                            ] = self.cur_block[src_s : src_s + copy_len]
                                        else:
                                            for i in range(copy_len):
                                                self.cur_block[
                                                    self.cur_block_pos + i
                                                ] = self.cur_block[src_s + i]
                                    else:
                                        h_dist = dist - self.cur_block_pos + 1
                                        h_req = min(copy_len, h_dist)
                                        hs = self.history.get_history_slice(
                                            h_dist, h_req
                                        )
                                        self.cur_block[
                                            self.cur_block_pos : self.cur_block_pos
                                            + len(hs)
                                        ] = hs
                                        cpd = len(hs)
                                        if cpd < copy_len:
                                            for i in range(copy_len - cpd):
                                                self.cur_block[
                                                    self.cur_block_pos + cpd + i
                                                ] = self.cur_block[i]
                                    self.cur_block_pos += copy_len
                                    self.chunk_decoded += copy_len
                                    self.match_rem -= copy_len
                                    if self.cur_block_pos == self.block_size:
                                        committed = bytes(self.cur_block)
                                        self._update_check(committed)
                                        self.history.append_block(
                                            committed, self.block_idx
                                        )
                                        self.overall_output_offset += len(committed)
                                        self.stream_out_offset += len(committed)
                                        self.block_idx += 1
                                        self.cur_block_pos = 0
                                        self.rd_code = rd.code
                                        self.rd_range = rd.range
                                        # comp_offset tracked in stream
                                        if self.match_rem == 0:
                                            self.in_match_copy = False
                                        return committed, False
                                self.in_match_copy = False

                        self.in_chunk = False
                        self.rd_code = rd.code
                        self.rd_range = rd.range
                        if self.comp_offset != self.chunk_comp_sz or rd.code != 0:
                            raise XZFormatError(
                                f"LZMA chunk exactness violation: c_off={self.comp_offset}/{self.chunk_comp_sz} code={rd.code}"
                            )
                        self.chunk_stream_buf = b""

            if self.phase == "BLOCK_END":
                if self.cur_block_pos > 0:
                    partial = self._commit_block()
                    return partial, False

                if (
                    self.expected_comp_size is not None
                    and self.compressed_bytes_in_block != self.expected_comp_size
                ):
                    raise XZFormatError("Block compressed size mismatch")
                if (
                    self.expected_uncomp_size is not None
                    and self.block_uncomp_bytes != self.expected_uncomp_size
                ):
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
                    val = 0
                    s = 0
                    for _ in range(9):
                        b = self._read_exact(1)[0]
                        idx_b.append(b)
                        val |= (b & 0x7F) << s
                        if (b & 0x80) == 0:
                            return val
                        s += 7
                    raise XZFormatError("Invalid VLI")

                n_rec = _vli()
                if n_rec != len(self.stream_block_records):
                    raise XZFormatError("Index record mismatch")
                for r_idx in range(n_rec):
                    u_sz = _vli()
                    unc_sz = _vli()
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
                        raise XZFormatError(
                            f"Unexpected byte after stream padding: 0x{pb[0]:02x}"
                        )

                self.pending_boundary = StreamBoundaryEvent(
                    stream_index=self.stream_index,
                    next_stream_index=self.stream_index + 1 if has_next else None,
                    input_stream_len=self.overall_input_offset
                    - self.stream_start_in
                    - (1 if has_next else 0),
                    overall_input_offset=self.overall_input_offset
                    - (1 if has_next else 0),
                    output_stream_len=self.overall_output_offset
                    - self.stream_start_out,
                    overall_output_offset=self.overall_output_offset,
                    has_next=has_next,
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
                self._chunk_event.data = blk
                yield self._chunk_event

                self._block_committed_event.block_index = self.block_idx - 1
                self._block_committed_event.block_data = blk
                self._block_committed_event.state_record = (
                    self.export_state() if self._enable_state_export else None
                )
                yield self._block_committed_event

                self._progress_event.block_index = self.block_idx - 1
                self._progress_event.bytes_decoded = len(blk)
                self._progress_event.total_emitted = self.overall_output_offset
                yield self._progress_event
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
    name = p.name
    lower_name = name.lower()
    if lower_name.endswith(".txz"):
        return str(p.parent / (name[:-4] + ".tar"))
    if lower_name.endswith(".tlz"):
        return str(p.parent / (name[:-4] + ".tar"))
    if lower_name.endswith(".lzma"):
        return str(p.parent / name[:-5])
    if lower_name.endswith(".xz"):
        return str(p.parent / name[:-3])
    if lower_name.endswith(".lz"):
        return str(p.parent / name[:-3])
    return str(p.parent / f"{name}.unxz")


def format_suffixed_filename(base_path, stream_idx):
    if stream_idx == 0:
        return base_path
    p = Path(base_path)
    ext = p.suffix
    stem = p.name[: -len(ext)] if ext else p.name
    new_name = f"{stem}_{stream_idx}{ext}"
    return str(p.parent / new_name)


def _get_perf_counter():
    try:
        return time.perf_counter()
    except (AttributeError, OSError):
        try:
            return time.ticks_ms() / 1000.0
        except (AttributeError, OSError):
            return time.time()


def _get_process_time():
    try:
        return time.process_time()
    except (AttributeError, OSError):
        return _get_perf_counter()


def format_size(n):
    if n is None:
        return "unknown"
    if n >= 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024 * 1024):.2f} GiB"
    elif n >= 1024 * 1024:
        return f"{n / (1024 * 1024):.2f} MiB"
    elif n >= 1024:
        return f"{n / 1024:.2f} KiB"
    return f"{n} B"


def get_disk_info(path):
    p = Path(path).resolve()
    curr = p
    while not curr.exists():
        parent = curr.parent
        if parent == curr:
            break
        curr = parent

    free_bytes = None
    try:
        import shutil

        if hasattr(shutil, "disk_usage"):
            du = shutil.disk_usage(str(curr))
            free_bytes = du.free
    except Exception:
        pass

    if free_bytes is None:
        try:
            st = os.statvfs(str(curr))
            if hasattr(st, "f_bavail") and hasattr(st, "f_frsize"):
                free_bytes = st.f_bavail * (st.f_frsize or st.f_bsize)
            elif len(st) > 4:
                bsize = st[1] if (len(st) > 1 and st[1]) else st[0]
                free_bytes = st[4] * bsize
        except Exception:
            pass

    mount_desc = str(curr)
    try:
        if hasattr(os, "path") and hasattr(os.path, "ismount"):
            check = curr
            while not os.path.ismount(str(check)):
                parent = check.parent
                if parent == check:
                    break
                check = parent
            mount_desc = str(check)
    except Exception:
        pass

    return free_bytes, mount_desc, curr


def estimate_uncompressed_size_and_dict(stream):
    if not hasattr(stream, "seek") or not hasattr(stream, "tell"):
        return None, None
    try:
        if hasattr(stream, "seekable") and not stream.seekable():
            return None, None
        cur_pos = stream.tell()
    except Exception:
        return None, None

    try:
        stream.seek(0, 2)
        file_len = stream.tell()
        if file_len == 0:
            return 0, 0

        cur_end = file_len
        total_uncomp = 0
        max_dict = 0
        saw_any_known = False

        while cur_end > 0:
            while cur_end > 0:
                stream.seek(cur_end - 1)
                b = stream.read(1)
                if not b or b != b"\x00":
                    break
                cur_end -= 1
            if cur_end == 0:
                break

            if cur_end >= 12:
                footer_pos = cur_end - 12
                stream.seek(footer_pos)
                footer = stream.read(12)
                if footer.endswith(b"YZ"):
                    bw_size = struct.unpack("<I", footer[4:8])[0]
                    idx_size = (bw_size + 1) * 4
                    if footer_pos >= idx_size:
                        idx_pos = footer_pos - idx_size
                        stream.seek(idx_pos)
                        idx_bytes = stream.read(idx_size)
                        if idx_bytes and idx_bytes[0] == 0x00:
                            off = 1

                            def read_vli():
                                nonlocal off
                                val = 0
                                s = 0
                                for _ in range(9):
                                    if off >= len(idx_bytes):
                                        return None
                                    b = idx_bytes[off]
                                    off += 1
                                    val |= (b & 0x7F) << s
                                    if (b & 0x80) == 0:
                                        return val
                                    s += 7
                                return None

                            num_records = read_vli()
                            if num_records is not None:
                                stream_blocks_size = 0
                                stream_uncomp = 0
                                parse_ok = True
                                for _ in range(num_records):
                                    unpadded = read_vli()
                                    uncomp = read_vli()
                                    if unpadded is None or uncomp is None:
                                        parse_ok = False
                                        break
                                    stream_uncomp += uncomp
                                    stream_blocks_size += (unpadded + 3) & ~3

                                if parse_ok:
                                    stream_total_size = (
                                        12 + stream_blocks_size + idx_size + 12
                                    )
                                    stream_start = footer_pos + 12 - stream_total_size
                                    if stream_start >= 0:
                                        stream.seek(stream_start + 12)
                                        b0 = stream.read(1)
                                        if b0:
                                            bh_sz = (b0[0] + 1) * 4
                                            stream.seek(stream_start + 12)
                                            full_bh = stream.read(bh_sz)
                                            if len(full_bh) >= 8:
                                                v_off = 2
                                                bflags = full_bh[1]

                                                def _vli_bh():
                                                    nonlocal v_off
                                                    val = 0
                                                    s = 0
                                                    for _ in range(9):
                                                        if v_off >= len(full_bh):
                                                            return 0
                                                        b = full_bh[v_off]
                                                        v_off += 1
                                                        val |= (b & 0x7F) << s
                                                        if (b & 0x80) == 0:
                                                            return val
                                                        s += 7
                                                    return 0

                                                if bflags & 0x40:
                                                    _vli_bh()
                                                if bflags & 0x80:
                                                    _vli_bh()
                                                _vli_bh()
                                                _vli_bh()
                                                if v_off < len(full_bh):
                                                    pbyte = full_bh[v_off]
                                                    if pbyte <= 40:
                                                        dict_sz = (
                                                            0xFFFFFFFF
                                                            if pbyte == 40
                                                            else (
                                                                (2 | (pbyte & 1))
                                                                << (pbyte // 2 + 11)
                                                            )
                                                        )
                                                        max_dict = max(
                                                            max_dict, dict_sz
                                                        )

                                        total_uncomp += stream_uncomp
                                        max_dict = max(max_dict, 65536)
                                        saw_any_known = True
                                        cur_end = stream_start
                                        continue

            stream.seek(0)
            hdr = stream.read(13)
            if len(hdr) == 13 and hdr[0] <= 224:
                dict_sz = struct.unpack("<I", hdr[1:5])[0]
                uncomp_sz = struct.unpack("<Q", hdr[5:13])[0]
                max_dict = max(max_dict, max(4096, dict_sz))
                if uncomp_sz != 0xFFFFFFFFFFFFFFFF:
                    total_uncomp += uncomp_sz
                    saw_any_known = True
            break

        return (total_uncomp if saw_any_known else None), (
            max_dict if max_dict > 0 else 67108864
        )
    except Exception:
        pass
    finally:
        try:
            stream.seek(cur_pos)
        except Exception:
            pass
    return None, None


def check_preflight_disk_space(
    output_target,
    is_stream_output,
    in_stream,
    storage_dir=None,
    resume_dir=None,
    history_backend="auto",
    available_mem=None,
    ignore_space_check=False,
):
    uncomp_size, dict_size = estimate_uncompressed_size_and_dict(in_stream)

    if not is_stream_output:
        state_budget = 1 * 1024 * 1024
    else:
        if history_backend in ("directory", "file") or (
            history_backend == "auto" and (available_mem or 0) < 72 * 1024 * 1024
        ):
            state_budget = min(dict_size or 67108864, 67108864) + 1 * 1024 * 1024
        else:
            state_budget = 0

    if not is_stream_output and output_target:
        free_out, mount_out, anc_out = get_disk_info(output_target)
        actual_state_path = resume_dir or storage_dir
        if actual_state_path:
            free_state, mount_state, anc_state = get_disk_info(actual_state_path)
        else:
            free_state, mount_state, anc_state = free_out, mount_out, anc_out

        same_mount = mount_out == mount_state
        needed_out = (uncomp_size or 0) + (state_budget if same_mount else 0)

        if free_out is not None and uncomp_size is not None and free_out < needed_out:
            msg = (
                f"Insufficient free disk space on '{mount_out}':\n"
                f"  Required: {format_size(needed_out)} ({needed_out} bytes)\n"
                f"  Available: {format_size(free_out)} ({free_out} bytes)\n"
                f"To choose a different directory/partition for decompressor state, use --storage-dir=<path> or --resume-dir=<path>.\n"
                f"To attempt decompression anyway despite this check, run with --ignore-space-check."
            )
            if not ignore_space_check:
                return False, msg, "error"
            else:
                warn_msg = (
                    f"Insufficient free disk space on '{mount_out}' (ignoring check due to --ignore-space-check):\n"
                    f"  Required: {format_size(needed_out)} ({needed_out} bytes)\n"
                    f"  Available: {format_size(free_out)} ({free_out} bytes)\n"
                    f"Proceeding with decompression anyway."
                )
                return True, warn_msg, "warning"

        if not same_mount and free_state is not None and free_state < state_budget:
            msg = (
                f"Insufficient free disk space for decompressor state on '{mount_state}':\n"
                f"  Required: {format_size(state_budget)} ({state_budget} bytes)\n"
                f"  Available: {format_size(free_state)} ({free_state} bytes)\n"
                f"To choose a different directory/partition for decompressor state, use --storage-dir=<path> or --resume-dir=<path>.\n"
                f"To attempt decompression anyway despite this check, run with --ignore-space-check."
            )
            if not ignore_space_check:
                return False, msg, "error"
            else:
                warn_msg = (
                    f"Insufficient free disk space for decompressor state on '{mount_state}' (ignoring check due to --ignore-space-check):\n"
                    f"  Required: {format_size(state_budget)} ({state_budget} bytes)\n"
                    f"  Available: {format_size(free_state)} ({free_state} bytes)\n"
                    f"Proceeding with decompression anyway."
                )
                return True, warn_msg, "warning"

    return True, None, None


def decompress_xz(
    input_source,
    output_target=None,
    resume_dir=None,
    resume_from=None,
    resume_at=None,
    in_place=False,
    progress_callback=None,
    history_backend="auto",
    timeout=52.0,
    cpu_timeout=22.0,
    deadline=None,
    block_size="auto",
    storage_dir=None,
    memory_limit=None,
    force_gc=True,
    resume_mode="auto",
    checkpoint_interval="8m",
    resume_id=None,
    ignore_space_check=False,
):
    """
    Callable interface for XZ decompression with persistent checkpoint resumption.
    """
    if isinstance(input_source, (str, Path)):
        if str(input_source) == "-":
            in_stream = sys.stdin.buffer
            if output_target is None:
                output_target = "stdin.unxz"
        else:
            in_stream = open(str(input_source), "rb")
            if output_target is None:
                output_target = compute_output_filename(input_source)
    else:
        in_stream = input_source
        if output_target is None:
            output_target = "-"

    avail_mem = (
        parse_memory_size(memory_limit)
        if memory_limit is not None
        else get_available_memory()
    )
    actual_block_size = select_block_size(block_size, available_mem=avail_mem)
    is_stream_output = output_target == "-"
    is_stream_input = (input_source == "-") or not is_seekable(in_stream)

    # Pre-flight disk space budgeting check
    ok_space, space_msg, msg_type = check_preflight_disk_space(
        output_target=output_target,
        is_stream_output=is_stream_output,
        in_stream=in_stream,
        storage_dir=storage_dir,
        resume_dir=resume_dir,
        history_backend=history_backend,
        available_mem=avail_mem,
        ignore_space_check=ignore_space_check,
    )
    if not ok_space:
        sys.stderr.write(f"xz_decompressor: E_RESOURCE: {space_msg}\n")
        sys.stderr.flush()
        raise XZResourceError(space_msg)
    elif space_msg and msg_type == "warning":
        sys.stderr.write(f"xz_decompressor: warning: {space_msg}\n")
        sys.stderr.flush()

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

    if is_stream_input:
        if resume_id is not None:
            stream_id = str(resume_id).strip()
        else:
            stream_id = _generate_stream_id()
        inp_id = stream_id if resume_id is not None else None
    else:
        inp_id = _compute_input_id(in_stream)
        if resume_id is not None:
            stream_id = str(resume_id).strip()
        else:
            stream_id = (
                inp_id.replace(":", "")[:16] if inp_id else _generate_stream_id()
            )

    actual_storage_dir = storage_dir if storage_dir is not None else resume_dir

    replay_input = None
    if resume_dir and resume_mode != "off" and is_stream_input:
        # All input read-ahead is persisted. A restored decoder replays bytes
        # acquired after its checkpoint before asking the upstream stream again.
        replay_input = DurableReplayInput(
            in_stream, Path(resume_dir) / "INPUT_BUFFER.bin"
        )
        in_stream = replay_input

    if (
        not is_stream_output
        and (history_backend in ("auto", "output") and not resume_dir)
        and storage_dir is None
    ):
        history = None
    elif isinstance(history_backend, str):
        history = select_history_backend(
            history_backend,
            storage_dir=actual_storage_dir,
            available_mem=avail_mem,
            stream_id=stream_id,
        )
    elif history_backend is not None:
        history = history_backend
    else:
        history = select_history_backend(
            "auto",
            storage_dir=actual_storage_dir,
            available_mem=avail_mem,
            stream_id=stream_id,
        )

    engine = XZStreamDecompressor(
        in_stream,
        history,
        block_size=actual_block_size,
        force_gc=force_gc,
        zero_copy=True,
    )
    if resume_dir and resume_mode != "off":
        engine._enable_state_export = True

    # Initialize clocks and subtract initial counter readings
    try:
        import math
    except ImportError:
        math = None

    try:
        import signal
    except ImportError:
        signal = None

    init_perf = _get_perf_counter()
    init_cpu = _get_process_time()

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
    if (
        signal is not None
        and hasattr(signal, "SIGALRM")
        and timeout_sec
        and timeout_sec > 0
    ):

        def _on_sigalrm(signum, frame):
            raise WallClockTimeout("Wall-clock timeout reached (SIGALRM)")

        try:
            sig_alarm_old = signal.signal(signal.SIGALRM, _on_sigalrm)
            if hasattr(signal, "setitimer"):
                signal.setitimer(signal.ITIMER_REAL, timeout_sec)
            elif math is not None and hasattr(signal, "alarm"):
                signal.alarm(int(math.ceil(timeout_sec)))
        except (ValueError, OSError, AttributeError):
            pass

    if (
        signal is not None
        and hasattr(signal, "ITIMER_VIRTUAL")
        and hasattr(signal, "SIGVTALRM")
        and cpu_timeout_sec
        and cpu_timeout_sec > 0
    ):

        def _on_sigvtalrm(signum, frame):
            raise CPUTimeout("CPU timeout reached (SIGVTALRM)")

        try:
            sig_vtalrm_old = signal.signal(signal.SIGVTALRM, _on_sigvtalrm)
            signal.setitimer(signal.ITIMER_VIRTUAL, cpu_timeout_sec)
        except (ValueError, OSError, AttributeError):
            pass

    # Checkpoint helpers (§3.3)

    def _load_best_checkpoint(r_dir, exp_input_id=None):
        best_rec = None
        best_txn = -1
        for b_name in (
            "CHECKPOINT_A.bin",
            "CHECKPOINT_B.bin",
            "STATE_A.bin",
            "STATE_B.bin",
        ):
            bp = Path(r_dir) / b_name
            if bp.exists():
                try:
                    data = bp.read_bytes()
                    if len(data) >= 16 and data[:4] == b"XZCK":
                        stored_crc = struct.unpack("<I", data[-4:])[0]
                        calc_crc = crc32(data[:-4])
                        if stored_crc != calc_crc:
                            continue
                        ver, txn = struct.unpack("<HI", data[4:10])
                        id_len = struct.unpack("<H", data[10:12])[0]
                        inp_id_stored = data[12 : 12 + id_len].decode("utf-8")
                        if (
                            exp_input_id
                            and inp_id_stored
                            and inp_id_stored != exp_input_id
                        ):
                            continue
                        if txn > best_txn:
                            payload = data[12 + id_len : -4]
                            state_rec = _decode_state_dict(payload)
                            best_txn = txn
                            best_rec = {
                                "txn": txn,
                                "input_id": inp_id_stored,
                                "state": state_rec,
                            }
                except Exception:
                    pass
        return best_rec

    txn_counter = 0

    def _write_checkpoint(r_dir, engine_obj, hist_obj, inp_id, output_handle=None):
        nonlocal txn_counter
        # Ordering matters: output and dictionary history must reach stable
        # storage before the journal can advertise their offsets.
        _fsync_fileobj(output_handle)
        if hasattr(hist_obj, "sync"):
            hist_obj.sync()
        txn_counter += 1
        ensure_dir(r_dir)
        target_b = Path(r_dir) / (
            "CHECKPOINT_A.bin" if (txn_counter % 2 == 1) else "CHECKPOINT_B.bin"
        )
        tmp_b = Path(r_dir) / ".tmp_checkpoint.bin"
        if replay_input is not None:
            replay_input.sync()
        state_rec = engine_obj.export_state()
        if replay_input is not None:
            state_rec["source_read_offset"] = replay_input.durable_offset()
        state_rec["history_total"] = (
            hist_obj.total_written
            if hasattr(hist_obj, "total_written")
            else hist_obj.history_len()
        )
        state_rec["history_tail_crc32"] = hist_obj.tail_crc32()

        payload_bytes = _encode_state_dict(state_rec)
        inp_bytes = (inp_id or "").encode("utf-8")
        header = (
            b"XZCK" + struct.pack("<HIH", 2, txn_counter, len(inp_bytes)) + inp_bytes
        )
        full_data = header + payload_bytes
        crc = crc32(full_data)

        with open(str(tmp_b), "wb") as tmp_f:
            tmp_f.write(full_data + struct.pack("<I", crc))
            _fsync_fileobj(tmp_f)
        atomic_replace(tmp_b, target_b)
        _fsync_parent(target_b)

    curr_out_stream = None
    curr_dest_path = None
    curr_part_path = None
    stream_idx = 0

    is_implicit_stdin_unxz = str(input_source) == "-" and output_target == "stdin.unxz"

    if is_stream_output:
        curr_out_stream = sys.stdout.buffer
    else:
        base_dest = format_suffixed_filename(output_target, 0)
        curr_dest_path = Path(base_dest)
        target_file = curr_dest_path if in_place else Path(f"{base_dest}.part")
        curr_part_path = target_file if not in_place else None

        if is_implicit_stdin_unxz:
            sys.stderr.write(
                f"[xz:stream boundary] input_offset=0B stream_in=0B stream_out=0B "
                f"output_offset=0B starting stream 0 -> {base_dest}\n"
            )
            sys.stderr.flush()

    verify_output_mode = False
    verify_file_handle = None
    verify_file_len = 0
    verified_offset = 0

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
                    raise XZResourceError(
                        "History tail CRC32 mismatch on checkpoint restore"
                    )
            else:
                active_out = curr_part_path or curr_dest_path
                if not is_stream_output and active_out:
                    if active_out.exists():
                        curr_out_stream = open(str(active_out), "r+b")
                        curr_out_stream.seek(out_off)
                        curr_out_stream.truncate()
                    else:
                        curr_out_stream = open(str(active_out), "w+b")
                    curr_out_stream.seek(out_off)
                in_off = state_dict.get("input_offset", 0)
                if replay_input is not None:
                    # The serialized decoder state owns all bytes read up to
                    # source_read_offset. Replay only later read-ahead bytes.
                    replay_input.set_replay_offset(
                        state_dict.get("source_read_offset", 0)
                    )
                elif is_seekable(in_stream):
                    in_stream.seek(in_off)
                engine.import_state(state_dict)
                checkpoint_resumed = True
        elif resume_mode == "checkpoint":
            raise XZResourceError("No valid checkpoint found for checkpoint resumption")
        else:
            if replay_input is not None:
                # Without a valid checkpoint, decoder state starts from genesis,
                # so replay the entire durable spool before touching the source.
                replay_input.set_replay_offset(0)
            # Fall back to verify-output mode
            if not is_stream_output and curr_part_path and curr_part_path.exists():
                verify_output_mode = True
                verify_file_len = curr_part_path.stat().st_size
                verify_file_handle = open(str(curr_part_path), "r+b")
                verified_offset = 0
                if timeout_sec > 0 or cpu_timeout_sec > 0:
                    sys.stderr.write(
                        "xz_decompressor: warning: Verify-Output mode cannot converge when stream decode time exceeds timeout; use checkpoint mode with a persistent backend.\n"
                    )
                    sys.stderr.flush()

    if not is_stream_output and curr_out_stream is None and not verify_output_mode:
        if (
            not in_place
            and curr_part_path
            and curr_part_path.exists()
            and not resume_dir
        ):
            curr_part_path.unlink()
        curr_out_stream = open(str(curr_part_path or curr_dest_path), "w+b")

    if (
        not is_stream_output
        and curr_out_stream is not None
        and (
            history_backend == "output"
            or (history_backend == "auto" and not resume_dir)
        )
    ):
        if not avail_mem or avail_mem < 256 * 1024:
            c_budget = actual_block_size
        elif avail_mem < 80 * 1024 * 1024:
            # Microcontroller / embedded heap: dedicate at most 20% of RAM to cache, keeping 80% free
            max_blocks = max(1, int((avail_mem * 0.20) // actual_block_size))
            c_budget = min(16 * 1024 * 1024, max_blocks * actual_block_size)
        else:
            # High-RAM / desktop (>=80MB): full 64 MiB window + 1 block
            c_budget = 64 * 1024 * 1024 + actual_block_size
        history = SeekableOutputFileHistory(
            curr_out_stream,
            block_size=actual_block_size,
            max_cache_bytes=c_budget,
        )
        engine.history = history

    ckpt_interval_bytes = parse_memory_size(checkpoint_interval) or (8 * 1024 * 1024)
    bytes_since_ckpt = 0
    have_checkpoint = checkpoint_resumed

    try:
        for event in engine.decode_events():
            if event.type == XZEventType.CHUNK_OUTPUT:
                bytes_since_ckpt += len(event.data)
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
                if (
                    resume_dir
                    and getattr(history, "persistent", False)
                    and resume_mode != "off"
                    and (not have_checkpoint or bytes_since_ckpt >= ckpt_interval_bytes)
                ):
                    # Checkpoints are committed only at a block boundary. The
                    # first one is unconditional; later ones honor the byte interval.
                    _write_checkpoint(
                        resume_dir, engine, history, inp_id, curr_out_stream
                    )
                    have_checkpoint = True
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
                        target_file = (
                            curr_dest_path if in_place else Path(f"{base_dest}.part")
                        )
                        curr_part_path = target_file if not in_place else None
                        if not in_place and target_file.exists():
                            target_file.unlink()
                        curr_out_stream = open(str(target_file), "w+b")
                        dest_desc = str(curr_dest_path)
                        if history_backend == "output" or (
                            history_backend == "auto" and not resume_dir
                        ):
                            if not avail_mem or avail_mem < 256 * 1024:
                                c_budget = actual_block_size
                            elif avail_mem < 80 * 1024 * 1024:
                                max_blocks = max(
                                    1, int((avail_mem * 0.20) // actual_block_size)
                                )
                                c_budget = min(
                                    16 * 1024 * 1024, max_blocks * actual_block_size
                                )
                            else:
                                c_budget = 64 * 1024 * 1024 + actual_block_size
                            history = SeekableOutputFileHistory(
                                curr_out_stream,
                                block_size=actual_block_size,
                                max_cache_bytes=c_budget,
                            )
                            engine.history = history

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

            if event.type != XZEventType.CHUNK_OUTPUT:
                coop_margin = (
                    min(0.15, timeout_sec * 0.10)
                    if timeout_sec and timeout_sec > 0
                    else 0.15
                )
                cpu_coop_margin = (
                    min(0.15, cpu_timeout_sec * 0.10)
                    if cpu_timeout_sec and cpu_timeout_sec > 0
                    else 0.15
                )
                if timeout_sec and timeout_sec > 0:
                    if (_get_perf_counter() - init_perf) >= (timeout_sec - coop_margin):
                        raise WallClockTimeout(
                            "Wall-clock timeout reached (cooperative check)"
                        )
                if cpu_timeout_sec and cpu_timeout_sec > 0:
                    cpu_now = _get_process_time()
                    if (cpu_now - init_cpu) >= (cpu_timeout_sec - cpu_coop_margin):
                        raise CPUTimeout("CPU timeout reached (cooperative check)")

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
            for j in (
                "CHECKPOINT_A.bin",
                "CHECKPOINT_B.bin",
                ".tmp_checkpoint.bin",
                "INPUT_BUFFER.bin",
            ):
                (Path(resume_dir) / j).unlink(missing_ok=True)
        history.cleanup()
        if (storage_dir is None and resume_dir is None) and actual_storage_dir:
            try:
                p_sdir = Path(actual_storage_dir)
                if p_sdir.exists() and not list(p_sdir.glob("*")):
                    p_sdir.rmdir()
            except Exception:
                pass

    except (KeyboardInterrupt, WallClockTimeout, CPUTimeout) as exc:
        # Do not checkpoint here. The decoder generator may already have
        # advanced its internal state to produce an event that the caller has
        # not yet consumed (and therefore has not written to the output file).
        # A timeout-time checkpoint could advertise an output/input offset that
        # is ahead of durable output/history. Resume from the last checkpoint
        # committed at BLOCK_COMMITTED instead; restore truncates speculative
        # output and history back to that consistent boundary.
        if isinstance(exc, WallClockTimeout):
            msg = "\nDecompression timed out (wall-clock deadline reached)."
        elif isinstance(exc, CPUTimeout):
            msg = "\nDecompression timed out (CPU time limit reached)."
        else:
            msg = "\nDecompression interrupted."

        actual_loc = resume_dir or getattr(history, "work_dir", actual_storage_dir)
        if resume_dir:
            msg += f" To resume, run with: --resume-dir={resume_dir}"
        elif actual_loc and getattr(history, "persistent", False):
            msg += f" To resume, run with: --storage-dir={actual_loc}"

        if is_stream_input:
            msg += (
                f" --resume-id={stream_id} --resume-from={engine.overall_input_offset}"
            )
        elif not is_seekable(in_stream):
            msg += f" --resume-from={engine.overall_input_offset}"

        if (
            not is_stream_output
            and curr_out_stream
            and not is_seekable(curr_out_stream)
        ):
            msg += f" --resume-at={engine.overall_output_offset}"
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()
        raise
    except Exception:
        try:
            history.cleanup()
        except Exception:
            pass
        raise
    finally:
        if signal is not None:
            try:
                if hasattr(signal, "setitimer") and hasattr(signal, "ITIMER_REAL"):
                    signal.setitimer(signal.ITIMER_REAL, 0)
                elif hasattr(signal, "alarm"):
                    signal.alarm(0)
                if hasattr(signal, "SIGALRM") and sig_alarm_old is not None:
                    signal.signal(signal.SIGALRM, sig_alarm_old)
            except Exception:
                pass
            try:
                if hasattr(signal, "setitimer") and hasattr(signal, "ITIMER_VIRTUAL"):
                    signal.setitimer(signal.ITIMER_VIRTUAL, 0)
                if hasattr(signal, "SIGVTALRM") and sig_vtalrm_old is not None:
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

    def setUp(self):
        self._orig_stderr = sys.stderr
        self._orig_stdout = sys.stdout
        sys.stderr = io.StringIO()
        self._fake_stdout = io.StringIO()
        sys.stdout = self._fake_stdout
        self._orig_stdout_buffer = getattr(self._orig_stdout, "buffer", None)
        if self._orig_stdout_buffer is not None:
            self._fake_stdout.buffer = io.BytesIO()

    def tearDown(self):
        sys.stderr = self._orig_stderr
        sys.stdout = self._orig_stdout

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
        decompress_xz(
            io.BytesIO(xz_data),
            str(out_target),
            resume_dir=str(work_dir),
            in_place=True,
        )
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
        out_f = Path("/tmp/test_timeout_coop.bin")
        out_f.unlink(missing_ok=True)
        try:
            # Timeout after 0.01 seconds; write to temporary file to avoid stdout bleeding
            with self.assertRaises(WallClockTimeout):
                decompress_xz(
                    io.BytesIO(xz_data),
                    output_target=str(out_f),
                    timeout=0.01,
                    cpu_timeout=0,
                )
        finally:
            out_f.unlink(missing_ok=True)

    def test_interleaved_resumption_with_serialized_state(self):
        lzma = self.lzma
        import random, io

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
                state_a_bytes = _encode_state_dict(engine_a.export_state())
                del engine_a
                state_a = _decode_state_dict(state_a_bytes)

            if not eof_b:
                engine_b = XZStreamDecompressor(stream_b_io)
                if state_b is not None:
                    engine_b.import_state(state_b)
                blk_b, eof_b = engine_b.decode_block()
                if blk_b:
                    out_b.extend(blk_b)
                state_b_bytes = _encode_state_dict(engine_b.export_state())
                del engine_b
                state_b = _decode_state_dict(state_b_bytes)

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
            out = b"".join(
                ev.data
                for ev in engine.decode_events()
                if ev.type == XZEventType.CHUNK_OUTPUT
            )
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
        for chk in [
            lzma.CHECK_CRC32,
            lzma.CHECK_CRC64,
            lzma.CHECK_SHA256,
            lzma.CHECK_NONE,
        ]:
            xz_data = lzma.compress(data, check=chk)
            engine = XZStreamDecompressor(io.BytesIO(xz_data))
            out = b"".join(
                ev.data
                for ev in engine.decode_events()
                if ev.type == XZEventType.CHUNK_OUTPUT
            )
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
            out = b"".join(
                ev.data
                for ev in engine.decode_events()
                if ev.type == XZEventType.CHUNK_OUTPUT
            )
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
            header = (
                b"\xfd7zXZ\x00" + stream_flags + struct.pack("<I", crc32(stream_flags))
            )
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
        out = b"".join(
            ev.data
            for ev in engine.decode_events()
            if ev.type == XZEventType.CHUNK_OUTPUT
        )
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

        raw = b"ABCDEFGHIJ" * 20000  # 200,000 bytes spanning 4 blocks
        xz_data = lzma.compress(raw, preset=1)

        work_dir = Path("/tmp/test_verify_multi_work")
        out_target = Path("/tmp/test_verify_multi_out.bin")
        corrupt_raw = raw[:80000] + b"CORRUPTED_BYTES" + raw[80015:150000]
        out_target.write_bytes(corrupt_raw)

        decompress_xz(
            io.BytesIO(xz_data),
            str(out_target),
            resume_dir=str(work_dir),
            in_place=True,
        )
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
        out = b"".join(
            ev.data
            for ev in engine.decode_events()
            if ev.type == XZEventType.CHUNK_OUTPUT
        )
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
        out_valid = b"".join(
            ev.data
            for ev in engine_valid.decode_events()
            if ev.type == XZEventType.CHUNK_OUTPUT
        )
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
        low_mem = 64 * 1024 * 1024  # 64MB (< 72MB)
        high_mem = 128 * 1024 * 1024  # 128MB (>= 72MB)

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
        backend_forced_file = select_history_backend(
            "file", storage_dir=str(f_tmp), available_mem=high_mem
        )
        self.assertIsInstance(backend_forced_file, FileHistory)
        backend_forced_file.cleanup()

    def test_storage_auto_block_size_threshold(self):
        # On devices with less than 256KB of RAM, default to 8KiB rather than 64KiB
        tiny_mem = 128 * 1024  # 128KB (< 256KB)
        norm_mem = 10 * 1024 * 1024  # 10MB (>= 256KB)

        self.assertEqual(select_block_size("auto", available_mem=tiny_mem), 8192)
        self.assertEqual(select_block_size("auto", available_mem=norm_mem), 65536)

        # Explicit overrides
        self.assertEqual(select_block_size(16384), 16384)
        self.assertEqual(select_block_size("8k"), 8192)
        self.assertEqual(select_block_size("64k"), 65536)

    def test_psram_and_external_storage_detection(self):
        # Test path selection
        target_dir, stype = find_preferred_storage_dir(prefix="test_pref_vfs")
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
        out1 = b"".join(
            ev.data
            for ev in eng1.decode_events()
            if ev.type == XZEventType.CHUNK_OUTPUT
        )
        self.assertEqual(out1, data)

        # Force GC disabled
        eng2 = XZStreamDecompressor(io.BytesIO(xz_data), force_gc=False)
        out2 = b"".join(
            ev.data
            for ev in eng2.decode_events()
            if ev.type == XZEventType.CHUNK_OUTPUT
        )
        self.assertEqual(out2, data)

    def test_circuitpython_import_handling(self):
        # Test memory size parser
        self.assertEqual(parse_memory_size("64m"), 64 * 1024 * 1024)
        self.assertEqual(parse_memory_size("128k"), 128 * 1024)
        self.assertEqual(parse_memory_size("1g"), 1024 * 1024 * 1024)
        self.assertEqual(parse_memory_size(262144), 262144)
        self.assertIsNone(parse_memory_size(None))

    def test_micropython_clock_fallbacks(self):
        # Verify fallback functions return float timestamps
        p = _get_perf_counter()
        c = _get_process_time()
        self.assertIsInstance(p, float)
        self.assertIsInstance(c, float)
        self.assertGreater(p, 0.0)
        self.assertGreater(c, 0.0)

    def test_regression_rep0_long_zeros(self):
        lzma = self.lzma
        import io

        for size in [10000, 100000, 500000]:  # 10k, 100k, 500k all-zeros
            raw_zeros = bytes(size)
            for preset in [0, 6]:
                xz_data = lzma.compress(raw_zeros, preset=preset)
                engine = XZStreamDecompressor(io.BytesIO(xz_data))
                out = b"".join(
                    ev.data
                    for ev in engine.decode_events()
                    if ev.type == XZEventType.CHUNK_OUTPUT
                )
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
            engine = XZStreamDecompressor(
                io.BytesIO(xz_data), history_backend=store, block_size=b_sz
            )
            out = b"".join(
                ev.data
                for ev in engine.decode_events()
                if ev.type == XZEventType.CHUNK_OUTPUT
            )
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

    def test_durable_replay_input_replays_post_checkpoint_read_ahead(self):
        import tempfile
        import io

        with tempfile.TemporaryDirectory(prefix="xz_input_replay_") as td:
            spool = Path(td) / "INPUT_BUFFER.bin"
            source = io.BytesIO(b"abcdefghij")
            first = DurableReplayInput(source, spool)
            self.assertEqual(first.read(4), b"abcd")
            committed_offset = first.durable_offset()
            # Simulate read-ahead after the last checkpoint.
            self.assertEqual(first.read(3), b"efg")
            first.sync()
            first._spool.close()

            continuation = io.BytesIO(b"hijklmnop")
            restored = DurableReplayInput(continuation, spool)
            restored.set_replay_offset(committed_offset)
            replayed = bytearray()
            while len(replayed) < 6:
                chunk = restored.read(6 - len(replayed))
                if not chunk:
                    break
                replayed.extend(chunk)
            self.assertEqual(bytes(replayed), b"efghij")
            self.assertEqual(restored.read(6), b"klmnop")
            restored._spool.close()

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
        out = b"".join(
            ev.data
            for ev in engine.decode_events()
            if ev.type == XZEventType.CHUNK_OUTPUT
        )
        self.assertEqual(out, data)

    def test_regression_checkpoint_resume_convergence(self):
        lzma = self.lzma
        if lzma is None:
            self.skipTest("lzma oracle not available")
        import os
        import tempfile

        # Large, effectively incompressible input ensures multiple invocations
        # are needed. Every invocation reopens the input and restores the most
        # recent committed checkpoint, then the final output must match exactly.
        raw = os.urandom(4 * 1024 * 1024)
        xz_data = lzma.compress(raw, preset=1)
        with tempfile.TemporaryDirectory(prefix="test_ckpt_converge_") as td:
            root = Path(td)
            in_f = root / "input.xz"
            out_f = root / "output.bin"
            r_dir = root / "resume"
            in_f.write_bytes(xz_data)
            timed_out = False
            completed = False
            for _attempt in range(30):
                try:
                    decompress_xz(
                        str(in_f),
                        str(out_f),
                        resume_dir=str(r_dir),
                        resume_mode="auto",
                        timeout=0.20,
                        cpu_timeout=0.20,
                        block_size=256 * 1024,
                        checkpoint_interval="256k",
                        in_place=True,
                    )
                    completed = True
                    break
                except (WallClockTimeout, CPUTimeout):
                    timed_out = True
            self.assertTrue(timed_out, "test must exercise interrupted invocations")
            self.assertTrue(completed, "resumption did not converge within 30 attempts")
            self.assertEqual(out_f.read_bytes(), raw)

    def test_file_history_reopens_without_destroying_committed_data(self):
        import tempfile

        with tempfile.TemporaryDirectory(prefix="xz_file_history_") as td:
            path = Path(td) / "history.bin"
            first = FileHistory(path)
            first.append_block(b"committed-prefix")
            first.sync()
            first.f.close()

            reopened = FileHistory(path)
            self.assertEqual(reopened.total_written, len(b"committed-prefix"))
            self.assertEqual(
                reopened.get_history_slice(
                    len(b"committed-prefix"), len(b"committed-prefix")
                ),
                b"committed-prefix",
            )
            reopened.truncate_to(len(b"committed-prefix"))
            reopened.append_block(b"-tail")
            reopened.sync()
            reopened.f.close()
            self.assertEqual(path.read_bytes(), b"committed-prefix-tail")

    def test_regression_malformed_lzma2_and_properties(self):
        lzma = self.lzma
        if lzma is None:
            self.skipTest("lzma oracle not available")
        import io, struct

        data = b"MALFORMED_VALIDATION_TEST" * 10
        good_xz = bytearray(lzma.compress(data, check=lzma.CHECK_CRC32))

        # 1. Block header dict prop = 0x40 (> 40)
        bad_prop_xz = bytearray(good_xz)
        bad_prop_xz[16] = 0x40
        h_sz = (bad_prop_xz[12] + 1) * 4
        bad_prop_xz[12 + h_sz - 4 : 12 + h_sz] = struct.pack(
            "<I", crc32(bad_prop_xz[12 : 12 + h_sz - 4])
        )
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

    def test_regression_preflight_disk_space_check(self):
        lzma = self.lzma
        import io

        raw = b"SPACE_PREFLIGHT_TEST_PAYLOAD_" * 1000
        xz_data = lzma.compress(raw, preset=1)
        out_f = Path("/tmp/test_space_check_out.bin")
        out_f.unlink(missing_ok=True)

        real_get_disk_info = globals()["get_disk_info"]
        globals()["get_disk_info"] = lambda p: (500, "/mock/mount", Path("/mock/mount"))
        try:
            # 1. Should fail with XZResourceError without --ignore-space-check
            with self.assertRaises(XZResourceError):
                decompress_xz(io.BytesIO(xz_data), str(out_f), ignore_space_check=False)
            self.assertFalse(out_f.exists())

            # 2. Should succeed with warning when ignore_space_check=True
            decompress_xz(io.BytesIO(xz_data), str(out_f), ignore_space_check=True)
            self.assertTrue(out_f.exists())
            self.assertEqual(out_f.read_bytes(), raw)
            out_f.unlink(missing_ok=True)
        finally:
            globals()["get_disk_info"] = real_get_disk_info

    def test_regression_micropython_open_path_fallback(self):
        # MicroPython open() raises TypeError if passed an object that does not inherit from str
        w_dir = _PathFallback("/tmp/test_dir_slice_proto")
        store = DirectoryBlockHistoryStore(w_dir, stream_id="test_stream_str_compat")
        p_data = b"REPETITIVE_SLICE_PATTERN_" * 2621 + b"ABCD"  # 65529 bytes
        p_data += b"\x00" * (65536 - len(p_data))
        store.append_block(p_data, 0)
        store.append_block(p_data, 1)

        # Force disk read via open() in get_history_slice by clearing LRU cache
        store.lru_cache.clear()
        store.lru_order.clear()

        sl = store.get_history_slice(65536 + 200, 100)
        self.assertEqual(len(sl), 100)
        store.cleanup()

    def test_regression_micropython_urandom_getrandbits_limit(self):
        # MicroPython urandom.getrandbits raises ValueError if bits > 32
        class Strict32BitUrandom:
            @staticmethod
            def getrandbits(k):
                if k > 32:
                    raise ValueError("bits must be 32 or less")
                return 0x12345678

        import sys

        sys.modules["urandom"] = Strict32BitUrandom
        try:
            sid = _generate_stream_id()
            self.assertEqual(len(sid), 16)
            self.assertEqual(sid, "1234567812345678")
        finally:
            sys.modules.pop("urandom", None)

    def test_regression_micropython_os_atomic_replace(self):
        global os
        real_os = os

        class MockMicroPythonOS:
            def __init__(self, real):
                self._real = real

            def __getattr__(self, name):
                if name in ("replace", "path", "access", "W_OK"):
                    raise AttributeError(f"module 'os' has no attribute '{name}'")
                return getattr(self._real, name)

        os = MockMicroPythonOS(real_os)
        try:
            s = Path("/tmp/test_mpy_atomic_src.txt")
            d = Path("/tmp/test_mpy_atomic_dst.txt")
            s.write_text("mpy_data")
            d.unlink(missing_ok=True)
            atomic_replace(s, d)
            self.assertTrue(d.exists())
            self.assertEqual(d.read_text(), "mpy_data")
            self.assertFalse(s.exists())

            # Test replace when dst already exists
            s.write_text("mpy_data_v2")
            atomic_replace(s, d)
            self.assertTrue(d.exists())
            self.assertEqual(d.read_text(), "mpy_data_v2")
            self.assertFalse(s.exists())
            d.unlink(missing_ok=True)
        finally:
            os = real_os

    def test_bug1_pb4_length_coding_roundtrip(self):
        lzma = self.lzma
        import io

        pat = b"pb4_length_coding_pattern_abcdefghijklmno"
        data = pat + b"1234567890" * 8
        while len(data) % 16 != 10:
            data += b"z"
        data = data + pat * 3 + b"_end_marker"

        # Pre-compressed LZMA2 (XZ) fixtures for runtimes whose lzma compressor lacks custom filters (e.g. PyPy3)
        xz_fixtures = {
            (0, 0, 4): bytes.fromhex(
                "fd377a585a000004e6d6b4460200210110000000a8708e86e000ff0049b4003818c4d3c0d331776563f1e86288a8d13ff925bf6d0570034f06d146aed5021cde028b7e02a95235dee5296781f0fccbf23357275ea07bae67377997eeb91755d6a3725a4626bd02d00000000031d6a6ab1ded214500016580020000002d328380b1c467fb020000000004595a"
            ),
            (4, 0, 4): bytes.fromhex(
                "fd377a585a000004e6d6b4460200210110000000a8708e86e000ff004cb80038188285f361a504a50b391e0c24e529ad31aa76a6f0cdad64e00e9c25e615a538134759a493d58d5c766f5eba982098a0db3f36be374282499d2a3112bce2f586968f9dcf40f04f7ecc2a000031d6a6ab1ded21450001688002000000f3211d3cb1c467fb020000000004595a"
            ),
            (0, 4, 4): bytes.fromhex(
                "fd377a585a000004e6d6b4460200210110000000a8708e86e000ff0051d80038188285f36194dc673a1a0be6337990d300c17ce86c3662eec15de7dd766a830c351bee0d77b5610bd54e7fff26913544cc5fc0ee009e1f664a13b06242d59b65a800026f899ffe1eac986216cef1b9000000000031d6a6ab1ded214500016d800200000040b0d06cb1c467fb020000000004595a"
            ),
            (2, 2, 4): bytes.fromhex(
                "fd377a585a000004e6d6b4460200210110000000a8708e86e000ff004ec80038188285f3619c64a58736670f26372e1bdb821f487bdd572e7976fe6eb4823396b6721045c804a6bf78db8edd930ef67bd8fb369e30eddf962d897f95590d1168e722f6cc272c2ee78ba5b2531a00000031d6a6ab1ded214500016a8002000000f880d571b1c467fb020000000004595a"
            ),
        }

        # Pre-compressed LZMA1 fixtures for runtimes whose lzma compressor lacks custom ALONE filters (e.g. PyPy3)
        lzma_fixtures = {
            (0, 0, 4): bytes.fromhex(
                "b400001000ffffffffffffffff003818c4d3c0d331776563f1e86288a8d13ff925bf6d05700361231828c7907663aa409c71863342ee8e66a00f8265a1f12d009c14656a2f2d8f465ef73c635e62a886b28bdc3c32b8fff1ce3000"
            ),
            (4, 0, 4): bytes.fromhex(
                "b800001000ffffffffffffffff0038188285f361a504a50b391e0c24e529ad31aa76a6f0cdad64e2bf5de5e59b101d69b2e00a613c26a31892e57f057f660edb69b58adf81acf1149d9a62bcf74b9882faab56983898db411fffeeeaa000"
            ),
            (0, 4, 4): bytes.fromhex(
                "d800001000ffffffffffffffff0038188285f36194dc673a1a0be6337990d300c17ce86c3662eec16b61a845437216aaea3e375bbdf1617b18604d5812571b84dd81c35a77f0ae8c7018b41e0a4e7ea81adc78ed5db33f84b7b6bcd8da0effff294b8000"
            ),
            (2, 2, 4): bytes.fromhex(
                "c800001000ffffffffffffffff0038188285f3619c64a58736670f26372e1bdb821f487bdd572e79fd5ebe97cfa114ef1fead2e265dd0cf7ceca90e8a26c4004b88081933071b865c879777c97b7343ba144d9c51fbaf4b6b5d93373ffe1cea000"
            ),
        }

        combos = [(0, 0, 4), (4, 0, 4), (0, 4, 4), (2, 2, 4)]
        for lc, lp, pb in combos:
            # Test XZ (LZMA2) - Include preset: 0 and fallback to xz_fixtures on PyPy3
            c_xz = None
            try:
                f_xz = [
                    {
                        "id": lzma.FILTER_LZMA2,
                        "preset": 0,
                        "lc": lc,
                        "lp": lp,
                        "pb": pb,
                        "dict_size": 1048576,
                    }
                ]
                c_xz = lzma.compress(data, format=lzma.FORMAT_XZ, filters=f_xz)
            except (NotImplementedError, lzma.LZMAError, Exception):
                c_xz = xz_fixtures.get((lc, lp, pb))

            eng_xz = XZStreamDecompressor(io.BytesIO(c_xz))
            out_xz = b"".join(
                ev.data
                for ev in eng_xz.decode_events()
                if ev.type == XZEventType.CHUNK_OUTPUT
            )
            self.assertEqual(
                out_xz, data, f"Failed XZ pb=4 with lc={lc}, lp={lp}, pb={pb}"
            )

            # Test LZMA Alone (LZMA1) - PyPy3 does not implement custom LZMA1 filters
            c_lzma = None
            try:
                f_lzma = [
                    {
                        "id": lzma.FILTER_LZMA1,
                        "preset": 0,
                        "lc": lc,
                        "lp": lp,
                        "pb": pb,
                        "dict_size": 1048576,
                    }
                ]
                c_lzma = lzma.compress(data, format=lzma.FORMAT_ALONE, filters=f_lzma)
            except (NotImplementedError, lzma.LZMAError, Exception):
                c_lzma = lzma_fixtures.get((lc, lp, pb))

            eng_lzma = XZStreamDecompressor(io.BytesIO(c_lzma))
            out_lzma = b"".join(
                ev.data
                for ev in eng_lzma.decode_events()
                if ev.type == XZEventType.CHUNK_OUTPUT
            )
            self.assertEqual(
                out_lzma, data, f"Failed LZMA pb=4 with lc={lc}, lp={lp}, pb={pb}"
            )

    def test_bug2_legacy_lzma_eof_truncation_detection(self):
        lzma = self.lzma
        import io, struct

        raw = b"TRUNCATION_TEST_PAYLOAD_STRING_" * 20

        # Case A: Known uncompressed size header rewritten
        comp_known = bytearray(lzma.compress(raw, format=lzma.FORMAT_ALONE))
        comp_known[5:13] = struct.pack("<Q", len(raw))

        for cut in [1, 5, 15, len(comp_known) // 2]:
            trunc = bytes(comp_known[:-cut])
            with self.assertRaises((XZFormatError, EOFError)):
                eng = XZStreamDecompressor(io.BytesIO(trunc))
                list(eng.decode_events())

        # Case B: Unknown uncompressed size (EOS marker) truncated
        comp_eos = lzma.compress(raw, format=lzma.FORMAT_ALONE)
        for cut in [2, 6, 12]:
            trunc_eos = comp_eos[:-cut]
            with self.assertRaises((XZFormatError, EOFError)):
                eng = XZStreamDecompressor(io.BytesIO(trunc_eos))
                list(eng.decode_events())

    def test_bug3_legacy_lzma_trailing_bytes_rejection(self):
        lzma = self.lzma
        import io

        raw = b"TRAILING_TEST_PAYLOAD_" * 15
        comp = lzma.compress(raw, format=lzma.FORMAT_ALONE)

        # Trailing random garbage
        with self.assertRaises(XZFormatError):
            eng = XZStreamDecompressor(io.BytesIO(comp + b"TRAILING_GARBAGE"))
            list(eng.decode_events())

        # Four trailing zeros (unsupported after LZMA alone)
        with self.assertRaises(XZFormatError):
            eng = XZStreamDecompressor(io.BytesIO(comp + b"\x00\x00\x00\x00"))
            list(eng.decode_events())

    def test_bug4_legacy_lzma_props_zero(self):
        lzma = self.lzma
        import io

        raw = b"PROPS_ZERO_TEST_PAYLOAD_" * 25
        static_comp = bytes.fromhex(
            "0000001000ffffffffffffffff0028158a671bda02e192937e9742b55c540136da778c385be7d7c9db61e744f5f66ffff930c400"
        )
        try:
            filters = [
                {
                    "id": lzma.FILTER_LZMA1,
                    "preset": 0,
                    "lc": 0,
                    "lp": 0,
                    "pb": 0,
                    "dict_size": 1048576,
                }
            ]
            comp = lzma.compress(raw, format=lzma.FORMAT_ALONE, filters=filters)
        except (NotImplementedError, Exception):
            comp = static_comp

        self.assertEqual(comp[0], 0x00)

        eng = XZStreamDecompressor(io.BytesIO(comp))
        out = b"".join(
            ev.data for ev in eng.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT
        )
        self.assertEqual(out, raw)

    def test_bug5_strict_format_detection_and_empty_input(self):
        import io

        # 1. Text string starting with <= 224 but invalid dict size
        with self.assertRaises(XZFormatError):
            eng = XZStreamDecompressor(io.BytesIO(b"notxz" * 10))
            list(eng.decode_events())

        # 2. Random garbage input
        with self.assertRaises(XZFormatError):
            eng = XZStreamDecompressor(
                io.BytesIO(b"\x5d\x12\x34\x56\x78\x9a\xbc\xde\xf0\x11\x22\x33\x44")
            )
            list(eng.decode_events())

        # 3. Completely empty input (0 bytes) must fail with XZFormatError (exit 1)
        with self.assertRaises(XZFormatError):
            eng = XZStreamDecompressor(io.BytesIO(b""))
            list(eng.decode_events())

    def test_concatenated_intermixed_lzma_xz_streams(self):
        lzma = self.lzma
        import io

        s1 = b"FIRST_STREAM_CONTENT_LZMA_" * 30
        s2 = b"SECOND_STREAM_CONTENT_XZ___" * 30
        c1 = lzma.compress(s1, format=lzma.FORMAT_ALONE)
        c2 = lzma.compress(s2, format=lzma.FORMAT_XZ)

        combined = c1 + c2
        eng = XZStreamDecompressor(io.BytesIO(combined))
        out = b"".join(
            ev.data for ev in eng.decode_events() if ev.type == XZEventType.CHUNK_OUTPUT
        )
        self.assertEqual(out, s1 + s2)


def main():
    try:
        import argparse
    except ImportError:
        argparse = None

    try:
        import unittest
    except ImportError:
        unittest = None

    is_v = False
    if argparse is not None:
        parser = argparse.ArgumentParser(
            description="Universal Pure-Python Resumable XZ / LZMA2 Decompressor"
        )
        parser.add_argument(
            "input", nargs="?", default="-", help="Path to .xz file or '-' for stdin"
        )
        parser.add_argument(
            "-o", "--output", default=None, help="Output destination or '-' for stdout"
        )
        parser.add_argument(
            "--resume-dir",
            default=None,
            help="Working directory for circular buffer checkpoints",
        )
        parser.add_argument(
            "--resume-id",
            default=None,
            help="Stream ID for non-seekable resumption",
        )
        parser.add_argument(
            "--resume-from",
            type=int,
            default=None,
            help="Input offset for non-seekable resumption",
        )
        parser.add_argument(
            "--resume-at",
            type=int,
            default=None,
            help="Output offset for non-seekable resumption",
        )
        parser.add_argument(
            "--in-place",
            action="store_true",
            help="Write directly to targets without .part staging",
        )
        parser.add_argument(
            "--timeout",
            default="52s",
            help="Wall-clock timeout [<N>s|<N>m|0] (default: 52s)",
        )
        parser.add_argument(
            "--cpu-timeout",
            default="22s",
            help="CPU timeout [<N>s|<N>m|0] (default: 22s)",
        )
        parser.add_argument(
            "--deadline", default=None, help="Absolute UTC deadline instant (ISO-8601)"
        )
        parser.add_argument(
            "--resume-mode",
            choices=["auto", "checkpoint", "verify", "off"],
            default="auto",
            help="Resumption mode: auto (default), checkpoint, verify, off",
        )
        parser.add_argument(
            "--checkpoint-interval",
            default="8m",
            help="Periodic checkpoint spacing [<N>|<N>k|<N>m|<N>g bytes | <N>s] (default: 8m)",
        )
        parser.add_argument(
            "--history-backend",
            choices=["auto", "output", "memory", "directory", "file"],
            default="auto",
            help="Storage backend: auto (<72MB switches to disk), memory, directory, file",
        )
        parser.add_argument(
            "--block-size",
            default="auto",
            help="Block size: auto (8KiB if <256KB RAM, else 64KiB), or integer/size string",
        )
        parser.add_argument(
            "--storage-dir",
            default=None,
            help="Directory for history/scratch files (overrides PSRAM/SD detection)",
        )
        parser.add_argument(
            "--memory-limit",
            default=None,
            help="Simulate/override python-available RAM limit (e.g. 64m, 128k)",
        )
        parser.add_argument(
            "--no-gc",
            dest="force_gc",
            action="store_false",
            default=True,
            help="Disable forced gc.collect() in between blocks",
        )
        parser.add_argument(
            "-v", "--verbose", action="store_true", help="Diagnostic logging"
        )
        parser.add_argument(
            "--ignore-space-check",
            action="store_true",
            help="Attempt decompression even if pre-flight disk space check fails",
        )

        if unittest is not None:
            parser.add_argument(
                "--test", action="store_true", help="Run self-contained unit test suite"
            )

        args = parser.parse_args()

        if hasattr(args, "test") and args.test:
            if unittest is None:
                sys.stderr.write(
                    "--test disabled: unittest is not available in this environment\n"
                )
                sys.exit(1)
            suite = unittest.TestLoader().loadTestsFromTestCase(TestXZDecompressor)
            runner = unittest.TextTestRunner(verbosity=2)
            result = runner.run(suite)
            sys.exit(0 if result.wasSuccessful() else 1)
        elif "--test" in sys.argv and unittest is None:
            sys.stderr.write(
                "--test disabled: unittest is not available in this environment\n"
            )
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
        is_v = args.verbose
        r_mode = getattr(args, "resume_mode", "auto")
        c_int = getattr(args, "checkpoint_interval", "8m")
        res_id = getattr(args, "resume_id", None)
        ign_space = getattr(args, "ignore_space_check", False)
        del parser
        del args
        del argparse
        argparse = None
        try:
            import gc

            gc.collect()
        except Exception:
            pass
    else:
        # Robust full-featured argument parser for MicroPython / runtimes without argparse
        input_src = "-"
        output_dst = None
        res_dir = None
        res_id = None
        res_from = None
        res_at = None
        inp = False
        t_out = "52s"
        c_out = "22s"
        d_line = None
        r_mode = "auto"
        c_int = "8m"
        h_backend = "auto"
        b_size = "auto"
        s_dir = None
        m_limit = None
        f_gc = True
        is_v = False
        ign_space = False

        argv = sys.argv[1:]
        i = 0
        while i < len(argv):
            arg = argv[i]
            if arg == "-v" or arg == "--verbose":
                is_v = True
            elif arg == "--no-gc":
                f_gc = False
            elif arg == "--in-place":
                inp = True
            elif arg == "--ignore-space-check":
                ign_space = True
            elif arg == "--test":
                if unittest is None:
                    sys.stderr.write(
                        "--test disabled: unittest is not available in this environment\n"
                    )
                    sys.exit(1)
                suite = unittest.TestLoader().loadTestsFromTestCase(TestXZDecompressor)
                runner = unittest.TextTestRunner(verbosity=2)
                result = runner.run(suite)
                sys.exit(0 if result.wasSuccessful() else 1)
            elif arg == "-o" and i + 1 < len(argv):
                i += 1
                output_dst = argv[i]
            elif arg.startswith("-o="):
                output_dst = arg[3:]
            elif arg == "--output" and i + 1 < len(argv):
                i += 1
                output_dst = argv[i]
            elif arg.startswith("--output="):
                output_dst = arg[9:]
            elif arg == "--timeout" and i + 1 < len(argv):
                i += 1
                t_out = argv[i]
            elif arg.startswith("--timeout="):
                t_out = arg[10:]
            elif arg == "--cpu-timeout" and i + 1 < len(argv):
                i += 1
                c_out = argv[i]
            elif arg.startswith("--cpu-timeout="):
                c_out = arg[14:]
            elif arg == "--deadline" and i + 1 < len(argv):
                i += 1
                d_line = argv[i]
            elif arg.startswith("--deadline="):
                d_line = arg[11:]
            elif arg == "--resume-dir" and i + 1 < len(argv):
                i += 1
                res_dir = argv[i]
            elif arg.startswith("--resume-dir="):
                res_dir = arg[13:]
            elif arg == "--resume-id" and i + 1 < len(argv):
                i += 1
                res_id = argv[i]
            elif arg.startswith("--resume-id="):
                res_id = arg[12:]
            elif arg == "--resume-from" and i + 1 < len(argv):
                i += 1
                res_from = int(argv[i])
            elif arg.startswith("--resume-from="):
                res_from = int(arg[14:])
            elif arg == "--resume-at" and i + 1 < len(argv):
                i += 1
                res_at = int(argv[i])
            elif arg.startswith("--resume-at="):
                res_at = int(arg[12:])
            elif arg == "--resume-mode" and i + 1 < len(argv):
                i += 1
                r_mode = argv[i]
            elif arg.startswith("--resume-mode="):
                r_mode = arg[14:]
            elif arg == "--checkpoint-interval" and i + 1 < len(argv):
                i += 1
                c_int = argv[i]
            elif arg.startswith("--checkpoint-interval="):
                c_int = arg[22:]
            elif arg == "--history-backend" and i + 1 < len(argv):
                i += 1
                h_backend = argv[i]
            elif arg.startswith("--history-backend="):
                h_backend = arg[18:]
            elif arg == "--block-size" and i + 1 < len(argv):
                i += 1
                b_size = argv[i]
            elif arg.startswith("--block-size="):
                b_size = arg[13:]
            elif arg == "--storage-dir" and i + 1 < len(argv):
                i += 1
                s_dir = argv[i]
            elif arg.startswith("--storage-dir="):
                s_dir = arg[14:]
            elif arg == "--memory-limit" and i + 1 < len(argv):
                i += 1
                m_limit = argv[i]
            elif arg.startswith("--memory-limit="):
                m_limit = arg[15:]
            elif not arg.startswith("-"):
                input_src = arg
            i += 1

    if __name__ == "__main__":
        if (
            "TestXZDecompressor" in globals()
            and globals()["TestXZDecompressor"] is not None
        ):
            del globals()["TestXZDecompressor"]
        if "unittest" in globals() and globals()["unittest"] is not None:
            del globals()["unittest"]
        try:
            import gc

            gc.collect()
        except Exception:
            pass

    def _supports_unicode():
        try:
            enc = getattr(sys.stderr, "encoding", None) or ""
            if "utf" in enc.lower():
                return True
        except Exception:
            pass
        if _is_embedded_python:
            return True
        return False

    if _supports_unicode():
        # 8-state three-dot braille spinner
        spinner = (
            "\u280b",
            "\u2819",
            "\u2838",
            "\u28b0",
            "\u28e0",
            "\u28c4",
            "\u2846",
            "\u2807",
        )
    else:
        spinner = ("\\", "|", "/", "-")

    def format_human_size(n):
        if n is None or n < 0:
            return "0 B"
        if n >= 1024 * 1024 * 1024:
            return f"{n / (1024 * 1024 * 1024):.1f} GiB"
        elif n >= 1024 * 1024:
            return f"{n / (1024 * 1024):.1f} MiB"
        elif n >= 1024:
            return f"{n / 1024:.1f} KiB"
        return f"{int(n)} B"

    def format_human_rate(r):
        if r is None or r <= 0:
            return "0 B/s"
        if r >= 1024 * 1024 * 1024:
            return f"{r / (1024 * 1024 * 1024):.1f} GiB/s"
        elif r >= 1024 * 1024:
            return f"{r / (1024 * 1024):.1f} MiB/s"
        elif r >= 1024:
            return f"{r / 1024:.1f} KiB/s"
        return f"{int(r)} B/s"

    spin_idx = [0]
    last_spin_time = [_get_perf_counter()]
    last_spin_bytes = [0]
    out_is_stdout = output_dst == "-"
    stderr_has_isatty = hasattr(sys.stderr, "isatty")
    spinner_active = [False]

    def progress_cb(ev):
        now = _get_perf_counter()
        dt = now - last_spin_time[0]
        db = ev.total_emitted - last_spin_bytes[0]
        rate = (db / dt) if dt > 0 else 0.0
        last_spin_time[0] = now
        last_spin_bytes[0] = ev.total_emitted

        glyph = spinner[spin_idx[0] % len(spinner)]
        spin_idx[0] += 1
        if (stderr_has_isatty and sys.stderr.isatty()) or (not out_is_stdout):
            line = f"{glyph} {format_human_size(ev.total_emitted):>10} decoded, {format_human_rate(rate):>12}"
            sys.stderr.write(f"{line:<42}\r")
            spinner_active[0] = True
            try:
                sys.stderr.flush()
            except Exception:
                pass

    memory_cushion = bytearray(1024)  # 1 KB cushion for OOM exception printing
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
            resume_mode=r_mode,
            checkpoint_interval=c_int,
            resume_id=res_id,
            ignore_space_check=ign_space,
        )
        if spinner_active[0]:
            sys.stderr.write(" " * 42 + "\r")
            try:
                sys.stderr.flush()
            except Exception:
                pass
    except (WallClockTimeout, CPUTimeout):
        sys.exit(124)
    except KeyboardInterrupt:
        sys.exit(130)
    except XZFormatError as exc:
        memory_cushion = None
        try:
            import gc

            gc.collect()
        except Exception:
            pass
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_FORMAT: {exc}\n")
        sys.exit(E_FORMAT)
    except XZUnsupportedError as exc:
        memory_cushion = None
        try:
            import gc

            gc.collect()
        except Exception:
            pass
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_UNSUPPORTED: {exc}\n")
        sys.exit(E_UNSUPPORTED)
    except XZCheckError as exc:
        memory_cushion = None
        try:
            import gc

            gc.collect()
        except Exception:
            pass
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_CHECK: {exc}\n")
        sys.exit(E_CHECK)
    except (EOFError, ValueError) as exc:
        memory_cushion = None
        try:
            import gc

            gc.collect()
        except Exception:
            pass
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_FORMAT: {exc}\n")
        sys.exit(E_FORMAT)
    except OSError as exc:
        memory_cushion = None
        try:
            import gc

            gc.collect()
        except Exception:
            pass
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_IO: {exc}\n")
        sys.exit(E_IO)
    except Exception as exc:
        memory_cushion = None
        try:
            import gc

            gc.collect()
        except Exception:
            pass
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_RESOURCE: {exc}\n")
        sys.exit(E_RESOURCE)


if __name__ == "__main__":
    main()
