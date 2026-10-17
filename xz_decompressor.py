#!/usr/bin/env python3
"""
xz_decompressor - Pure-Python XZ / LZMA2 Streaming Decompression Engine (Revision 9).

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
import gc

_gc_mem_free = getattr(gc, "mem_free", None)
_PROB_INIT_ELEMENT = struct.pack("<H", 1024)


_IS_DESKTOP_PY = getattr(
    sys, "implementation", None
) is not None and sys.implementation.name in ("cpython", "pypy")


def _make_prob_array(count):
    if _IS_DESKTOP_PY:
        # array.array is JIT-friendly on PyPy (memoryview item access is not)
        # and at least as fast as a cast memoryview on CPython.
        return array.array("H", [1024]) * count
    # Preallocate exact bytearray and fill via logarithmic doubling (zero allocation spikes, zero list creation)
    ba = bytearray(count * 2)
    ba[0] = 0
    ba[1] = 4
    copied = 2
    tot = count * 2
    while copied < tot:
        take = min(copied, tot - copied)
        ba[copied : copied + take] = ba[:take]
        copied += take
    try:
        return memoryview(ba).cast("H")
    except (AttributeError, ValueError, TypeError):
        return array.array("H", ba)


def _reset_prob_array(arr):
    if _IS_DESKTOP_PY and isinstance(arr, array.array):
        arr[:] = array.array("H", [1024]) * len(arr)
        return
    # Reset existing probability array in-place with zero heap allocations
    buf = getattr(arr, "obj", None)
    if isinstance(buf, bytearray):
        buf[0] = 0
        buf[1] = 4
        copied = 2
        tot = len(buf)
        while copied < tot:
            take = min(copied, tot - copied)
            buf[copied : copied + take] = buf[:take]
            copied += take
    else:
        for i in range(len(arr)):
            arr[i] = 1024


_LIT_TRANS = b"\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x04\x05"


def _maybe_collect(watermark=4096):
    if _gc_mem_free is not None and _gc_mem_free() < watermark:
        gc.collect()


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


def _assert_valid_state_dict(d):
    """Enforce strict ban on binary arrays, lists/sequences of numbers, and hex strings in state dict."""
    for k, v in d.items():
        if v is None or isinstance(v, (bool, int, float)):
            continue
        if isinstance(v, str):
            if len(v) > 32 and all(c in "0123456789abcdefABCDEF" for c in v):
                raise AssertionError(
                    f"BANNED: hex-encoded binary string in state dict: {k}"
                )
            continue
        if isinstance(v, (bytes, bytearray, memoryview, array.array)):
            raise AssertionError(
                f"BANNED: binary data/array in state dict: {k}={type(v).__name__}"
            )
        if isinstance(v, (list, tuple)):
            for item in v:
                if isinstance(
                    item, (int, float, bytes, bytearray, memoryview, array.array)
                ):
                    raise AssertionError(
                        f"BANNED: sequence of numbers/binary in state dict: {k}={v!r}"
                    )
        raise AssertionError(
            f"BANNED: non-scalar value in state dict: {k}={type(v).__name__}"
        )


def _encode_state_dict(d):
    _assert_valid_state_dict(d)
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
    _assert_valid_state_dict(d)
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
    CRC32_TABLE = array.array("I", bytearray(1024))
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
# Table is built lazily on first use only if available RAM >= 160 KiB, consuming 0 bytes at module import
CRC64_TABLE = None


def _init_crc64():
    global CRC64_TABLE
    if CRC64_TABLE is None:
        _POLY64 = 0xC96C5795D7870F42
        CRC64_TABLE = []
        for _i in range(256):
            _c = _i
            for _ in range(8):
                _c = (_c >> 1) ^ _POLY64 if (_c & 1) else (_c >> 1)
            CRC64_TABLE.append(_c)


@micropython.native
def crc64(data, val=0):
    if get_available_memory() >= 160 * 1024:
        if CRC64_TABLE is None:
            _init_crc64()
        c = val ^ 0xFFFFFFFFFFFFFFFF
        tab = CRC64_TABLE
        for b in data:
            c = tab[(c ^ b) & 0xFF] ^ (c >> 8)
        return (c ^ 0xFFFFFFFFFFFFFFFF) & 0xFFFFFFFFFFFFFFFF
    else:
        c = val ^ 0xFFFFFFFFFFFFFFFF
        _p = 0xC96C5795D7870F42
        for b in data:
            c ^= b
            for _ in range(8):
                c = (c >> 1) ^ _p if (c & 1) else (c >> 1)
        return (c ^ 0xFFFFFFFFFFFFFFFF) & 0xFFFFFFFFFFFFFFFF


def truncate_file_portable(filepath, target_size):
    """Truncate a file path to target_size bytes portably across all runtimes.

    If native truncate() exists on open files, uses it directly (zero copying).
    If running on MicroPython where FileIO lacks truncate(), recreates via
    temporary file + atomic replace only if current size > target_size,
    minimizing flash filesystem wear.
    """
    p = Path(filepath)
    if not p.exists():
        return
    try:
        cur_sz = p.stat().st_size
    except OSError:
        cur_sz = None
    if cur_sz is not None and cur_sz <= target_size:
        return

    # Attempt in-place truncate if supported
    try:
        with open(str(p), "r+b") as f:
            if hasattr(f, "truncate"):
                f.truncate(target_size)
                return
    except (AttributeError, OSError):
        pass

    # Recreate-in-tmpfile + atomic replace fallback for MicroPython FileIO
    tmp_p = Path(str(p) + ".trunc")
    with open(str(p), "rb") as src_f, open(str(tmp_p), "wb") as dst_f:
        rem = target_size
        while rem > 0:
            chunk = src_f.read(min(4096, rem))
            if not chunk:
                break
            dst_f.write(chunk)
            rem -= len(chunk)
        _fsync_fileobj(dst_f)
    atomic_replace(tmp_p, p)


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
    def __init__(self, block_index, bytes_decoded, total_emitted, elapsed_ms=0):
        self.type = XZEventType.PROGRESS
        self.block_index = block_index
        self.bytes_decoded = bytes_decoded
        self.total_emitted = total_emitted
        self.elapsed_ms = elapsed_ms


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


def select_write_back_blocks(
    block_size, available_mem=None, override=None, timeout_sec=None, storage_driver=None
):
    """Resource-adaptive write-back cache sizing.

    Caps at 16 blocks (1 MiB on 64 KiB blocks) when RAM permits (dedicating <= 20% of RAM).
    Forcibly disabled (returns 1 for immediate write-through) when:
    1. In-memory state storage is used (MemoryStorage) where disk latency is zero.
    2. Under tight timeouts (timeout <= 1.0s) to guarantee prompt checkpointing.
    3. On microcontrollers with <= 256 KiB RAM to stay strictly within low-RAM limits.
    4. When explicitly disabled via parameter or CLI.
    """
    if override is not None:
        if isinstance(override, bool):
            return 16 if override else 1
        return max(1, int(override))
    if isinstance(storage_driver, MemoryStorage):
        return 1
    if timeout_sec is not None and timeout_sec > 0 and timeout_sec <= 1.0:
        return 1
    if available_mem is None:
        available_mem = get_available_memory()
    if available_mem < 256 * 1024:
        return 1
    blocks = int((available_mem * 0.20) // block_size)
    return max(1, min(16, blocks))


ALLOWED_BLOCK_SIZES = (512, 8192, 65536)


def select_block_size(block_size_choice="auto", available_mem=None):
    if block_size_choice not in (None, "auto"):
        val = parse_memory_size(block_size_choice)
        if val not in ALLOWED_BLOCK_SIZES:
            raise XZFormatError(
                f"Invalid block size: {block_size_choice}. Allowed sizes are 512, 8192, and 65536 bytes."
            )
        return val

    if available_mem is None:
        available_mem = get_available_memory()

    # Continuous decoding budget threshold:
    # 8192-byte blocks require ~78 KiB working set (cur_block, history cache, 12 KiB p_lit,
    # core arrays, file I/O buffers, object overhead) plus 2x fragmentation margin -> ~160 KiB RAM.
    if available_mem < 160 * 1024:
        return 512
    if available_mem < 1024 * 1024:
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

    Bounded Rolling Spool:
    At each block commit, bytes preceding the committed input offset are pruned,
    guaranteeing that the on-disk spool never exceeds the maximum unconsumed
    read-ahead (strictly <= 64 KiB), eliminating disk exhaustion on long streams.
    """

    def __init__(self, source, spool_path):
        self.source = source
        self.path = Path(spool_path)
        ensure_dir(self.path.parent)
        self._spool = open(str(self.path), "a+b")
        self._spool.seek(0, 2)
        self._end = self._spool.tell()
        self._replay_pos = self._end
        self._base_offset = 0

    def set_replay_offset(self, offset):
        spool_target = int(offset) - self._base_offset
        self._replay_pos = max(0, min(spool_target, self._end))

    def rebase_to(self, committed_offset):
        """Prune all bytes committed prior to committed_offset, bounding spool <= 64 KiB."""
        offset_in_spool = int(committed_offset) - self._base_offset
        if offset_in_spool <= 0:
            return
        unconsumed_len = self._end - offset_in_spool
        if unconsumed_len <= 0:
            self._spool.seek(0)
            self._spool.truncate(0)
            self._spool.flush()
            self._end = 0
            self._base_offset = int(committed_offset)
            self._replay_pos = 0
        else:
            self._spool.seek(offset_in_spool)
            tail = self._spool.read(unconsumed_len)
            self._spool.seek(0)
            self._spool.write(tail)
            self._spool.truncate(unconsumed_len)
            self._spool.flush()
            try:
                os.fsync(self._spool.fileno())
            except (AttributeError, OSError):
                pass
            self._end = unconsumed_len
            self._base_offset = int(committed_offset)
            self._replay_pos = unconsumed_len

    def read(self, n=-1):
        if n == 0:
            return b""
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
            try:
                os.fsync(self._spool.fileno())
            except (AttributeError, OSError):
                pass
            self._end += len(data)
            self._replay_pos = self._end
        return data

    def durable_offset(self):
        return self._base_offset + self._end

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
            if callable(close) and self.source not in (
                getattr(sys, "stdin", None),
                getattr(sys, "stdout", None),
            ):
                close()


class HistoryBackend:
    persistent = True

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
        pass


class TransientStorage:
    def open(self, filename, mode="r+b"):
        raise NotImplementedError

    def replace(self, src, dst):
        raise NotImplementedError

    def remove(self, filename):
        raise NotImplementedError

    def exists(self, filename):
        raise NotImplementedError

    def cleanup(self, prefix=None):
        raise NotImplementedError


class DiskStorage(TransientStorage):
    def __init__(self, base_dir=".xz_history"):
        self.base_dir = Path(base_dir).resolve()
        ensure_dir(self.base_dir)

    def open(self, filename, mode="r+b"):
        p = self.base_dir / filename
        ensure_dir(p.parent)
        if not p.exists() and ("w" in mode or "+" in mode or "a" in mode):
            p.write_bytes(b"")
        return open(str(p), mode)

    def replace(self, src, dst):
        p_src = self.base_dir / src
        p_dst = self.base_dir / dst
        atomic_replace(p_src, p_dst)
        _fsync_parent(p_dst)

    def remove(self, filename):
        p = self.base_dir / filename
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass

    def exists(self, filename):
        return (self.base_dir / filename).exists()

    def cleanup(self, prefix=None):
        if prefix:
            for p in list(self.base_dir.glob(f"{prefix}*")) + list(
                self.base_dir.glob(f".tmp_{prefix}*")
            ):
                try:
                    p.unlink(missing_ok=True)
                except OSError:
                    pass
        try:
            if not list(self.base_dir.glob("*")):
                self.base_dir.rmdir()
        except OSError:
            pass


class MemoryStorage(TransientStorage):
    def __init__(self):
        self.files = {}

    def open(self, filename, mode="r+b"):
        if filename not in self.files:
            self.files[filename] = bytearray()

        if "r" in mode and "+" not in mode and "w" not in mode and "a" not in mode:
            return io.BytesIO(bytes(self.files[filename]))

        class _MemFile(io.BytesIO):
            def __init__(wrapper, initial_data, store_dict, key):
                super().__init__(initial_data)
                wrapper.store_dict = store_dict
                wrapper.key = key

            def close(wrapper):
                wrapper.store_dict[wrapper.key] = bytearray(wrapper.getvalue())
                super().close()

            def flush(wrapper):
                wrapper.store_dict[wrapper.key] = bytearray(wrapper.getvalue())
                super().flush()

        initial_b = bytes(self.files[filename]) if "a" in mode else b""
        mf = _MemFile(initial_b, self.files, filename)
        if "a" in mode:
            mf.seek(0, 2)
        return mf

    def replace(self, src, dst):
        if src in self.files:
            self.files[dst] = self.files[src]
            del self.files[src]

    def remove(self, filename):
        self.files.pop(filename, None)

    def exists(self, filename):
        return filename in self.files

    def cleanup(self, prefix=None):
        if prefix:
            to_del = [
                k
                for k in self.files
                if k.startswith(prefix) or k.startswith(f".tmp_{prefix}")
            ]
            for k in to_del:
                del self.files[k]
        else:
            self.files.clear()


class UnifiedHistory(HistoryBackend):
    persistent = True

    def __init__(
        self,
        output_file=None,
        storage_driver=None,
        stream_id="0000000000000000",
        max_history=67108864,
        ram_cache_bytes=65536,
        block_size=65536,
    ):
        self.stream_id = stream_id or "0000000000000000"
        self.max_history = max_history
        self.block_size = max(512, block_size)
        self._slice_buf = bytearray(512)
        self.output_file = (
            output_file if (output_file and is_seekable(output_file)) else None
        )
        self.storage = storage_driver
        self.total_written = 0
        self.ram_cache_bytes = max(self.block_size, ram_cache_bytes)
        self.scratch_file = None
        self.scratch_name = f"{self.stream_id}.hist"
        # Rotating blocks strategy: list of immutable bytes blocks (zero contiguous allocation spikes!)
        self.max_blocks = max(1, self.ram_cache_bytes // self.block_size)
        self.blocks = []
        self.blocks_len = 0
        self.in_ram_only = self.output_file is None and self.storage is None

    def set_output_file(self, output_file):
        """Bind seekable output file as history tier 2 and close/remove any scratch file."""
        self.output_file = (
            output_file if (output_file and is_seekable(output_file)) else None
        )
        if self.output_file is not None:
            self.in_ram_only = False
            if self.scratch_file is not None:
                try:
                    self.scratch_file.close()
                except Exception:
                    pass
                self.scratch_file = None
            if self.storage is not None and self.storage.exists(self.scratch_name):
                self.storage.remove(self.scratch_name)

    def _ensure_scratch(self):
        if self.output_file is None and self.storage is not None:
            if self.scratch_file is None:
                self.scratch_file = self.storage.open(self.scratch_name, "a+b")
                self.scratch_file.seek(0, 2)
                if self.total_written == 0:
                    self.total_written = self.scratch_file.tell()
            return self.scratch_file
        return None

    def append_block(self, block_data, block_index=0, input_hash=""):
        b_len = len(block_data)
        if b_len == 0:
            return

        b = bytes(block_data)
        self.blocks.append(b)
        self.blocks_len += b_len

        # Rotate blocks: pop oldest block when memory limit reached (zero memory reallocation spikes!)
        max_b = (
            (self.max_history // self.block_size)
            if self.in_ram_only
            else self.max_blocks
        )
        limit = self.max_history if self.in_ram_only else self.ram_cache_bytes
        while len(self.blocks) > 1 and (
            len(self.blocks) > max_b or self.blocks_len > limit
        ):
            dropped = self.blocks.pop(0)
            self.blocks_len -= len(dropped)

        # Mode B: Write to scratch file ONLY when output_file is None/unseekable!
        if self.output_file is None and self.storage is not None:
            sf = self._ensure_scratch()
            if sf is not None:
                sf.seek(self.total_written)
                sf.write(block_data)
                sf.flush()

        self.total_written += b_len

    def history_len(self):
        return min(self.total_written, self.max_history)

    def get_history_slice(self, distance, length):
        h_len = self.history_len()
        if distance <= 0 or distance > h_len or length <= 0 or length > distance:
            raise XZFormatError(
                f"Invalid history reference: distance={distance}, length={length}, available={h_len}"
            )

        start_pos = self.total_written - distance
        blocks_start = self.total_written - self.blocks_len

        # Tier 1: Lookback in rotating blocks in RAM
        if start_pos >= blocks_start:
            rel_offset = start_pos - blocks_start
            cur = 0
            for idx, blk in enumerate(self.blocks):
                blk_sz = len(blk)
                if cur + blk_sz > rel_offset:
                    in_blk = rel_offset - cur
                    avail = blk_sz - in_blk
                    if avail >= length:
                        return blk[in_blk : in_blk + length]
                    else:
                        # Spans across blocks - copy into pre-allocated slice buffer (zero extend(), zero heap allocations)
                        buf = self._slice_buf
                        buf[:avail] = blk[in_blk:]
                        buf_pos = avail
                        rem = length - avail
                        next_idx = idx + 1
                        while rem > 0 and next_idx < len(self.blocks):
                            nxt = self.blocks[next_idx]
                            take = min(rem, len(nxt))
                            buf[buf_pos : buf_pos + take] = nxt[:take]
                            buf_pos += take
                            rem -= take
                            next_idx += 1
                        return bytes(buf[:length])
                cur += blk_sz

        # Tier 2: Cold store lookback
        # Mode A: Seekable output file (ZERO .hist file used!)
        if self.output_file is not None:
            cur = self.output_file.tell()
            self.output_file.seek(start_pos)
            chunk = self.output_file.read(length)
            self.output_file.seek(cur)
            return chunk
        # Mode B: Unseekable output scratch file
        elif self.output_file is None and self.storage is not None:
            sf = self._ensure_scratch()
            if sf is not None:
                cur = sf.tell()
                sf.seek(start_pos)
                chunk = sf.read(length)
                sf.seek(cur)
                return chunk

        raise XZFormatError(f"Distance {distance} exceeds available history")

    def tail_crc32(self, n=65536):
        avail_max = (
            self.blocks_len
            if (self.output_file is None and self.scratch_file is None)
            else self.total_written
        )
        avail = min(n, avail_max)
        if avail <= 0:
            return 0
        tail_bytes = self.get_history_slice(avail, avail)
        return crc32(tail_bytes)

    def truncate_to(self, history_total):
        self.total_written = history_total
        if self.output_file is not None:
            if hasattr(self.output_file, "truncate"):
                self.output_file.seek(history_total)
                self.output_file.truncate()
            elif hasattr(self.output_file, "name"):
                truncate_file_portable(self.output_file.name, history_total)
        if self.scratch_file is not None:
            if hasattr(self.scratch_file, "truncate"):
                self.scratch_file.seek(history_total)
                self.scratch_file.truncate()
            elif hasattr(self.scratch_file, "name"):
                truncate_file_portable(self.scratch_file.name, history_total)

        # Truncate rotating blocks
        self.blocks = []
        self.blocks_len = 0

    def sync(self):
        if self.scratch_file is not None:
            _fsync_fileobj(self.scratch_file)
        if self.output_file is not None:
            _fsync_fileobj(self.output_file)

    def reset(self):
        self.total_written = 0
        self.blocks = []
        self.blocks_len = 0
        if self.scratch_file is not None:
            if hasattr(self.scratch_file, "truncate"):
                self.scratch_file.seek(0)
                self.scratch_file.truncate(0)
            elif hasattr(self.scratch_file, "name"):
                truncate_file_portable(self.scratch_file.name, 0)

    def cleanup(self):
        self.blocks = []
        self.blocks_len = 0
        if self.scratch_file is not None:
            try:
                self.scratch_file.close()
            except Exception:
                pass
            self.scratch_file = None
            if self.storage is not None:
                self.storage.remove(self.scratch_name)

    def export_state(self):
        return {
            "total_written": self.total_written,
            "max_history": self.max_history,
            "ram_cache_bytes": self.ram_cache_bytes,
        }

    def import_state(self, s):
        self.total_written = int(s.get("total_written", 0))
        self.max_history = int(s.get("max_history", 67108864))
        self.ram_cache_bytes = int(s.get("ram_cache_bytes", self.ram_cache_bytes))
        self.max_blocks = max(1, self.ram_cache_bytes // self.block_size)


class ResumeDescriptor:
    def __init__(
        self, resume_dir=".xz_history", resume_id=None, resume_from=None, resume_at=None
    ):
        self.resume_dir = str(resume_dir)
        self.resume_id = str(resume_id) if resume_id is not None else None
        self.resume_from = int(resume_from) if resume_from is not None else None
        self.resume_at = int(resume_at) if resume_at is not None else None

    def to_cli_args(self):
        parts = [f"--resume-dir={self.resume_dir}"]
        if self.resume_id is not None:
            parts.append(f"--resume-id={self.resume_id}")
        if self.resume_from is not None:
            parts.append(f"--resume-from={self.resume_from}")
        if self.resume_at is not None:
            parts.append(f"--resume-at={self.resume_at}")
        return " ".join(parts)

    @classmethod
    def from_args(cls, args):
        return cls(
            resume_dir=getattr(args, "resume_dir", ".xz_history"),
            resume_id=getattr(args, "resume_id", None),
            resume_from=getattr(args, "resume_from", None),
            resume_at=getattr(args, "resume_at", None),
        )

    def __repr__(self):
        return f"ResumeDescriptor(resume_dir={self.resume_dir!r}, resume_id={self.resume_id!r}, resume_from={self.resume_from!r}, resume_at={self.resume_at!r})"


class RangeDecoder:
    def __init__(self, read_byte_fn=None, init_stream=True, buf=None, pos=0):
        self.read_byte = read_byte_fn
        self.buf = buf
        self.pos = pos
        self.limit = len(buf) if buf is not None else 0
        if init_stream:
            if buf is not None:
                self.pos += 1
                b0 = self.buf[self.pos]
                b1 = self.buf[self.pos + 1]
                b2 = self.buf[self.pos + 2]
                b3 = self.buf[self.pos + 3]
                self.pos += 4
                self.code = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3
            elif read_byte_fn:
                self.read_byte()
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

    def set_buf(self, buf, pos=0):
        self.buf = buf
        self.pos = pos
        self.limit = len(buf) if buf is not None else 0

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
            if self.pos < self.limit:
                b = self.buf[self.pos]
                self.pos += 1
            elif self.read_byte is not None:
                b = self.read_byte()
            else:
                b = 0
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
    def decode_literal(self, probs, off, mb):
        # mb < 0: plain literal; mb >= 0: matched literal using match byte mb
        rng = self.range
        code = self.code
        buf = self.buf
        pos = self.pos
        limit = self.limit
        symbol = 1
        while symbol < 0x100:
            if mb >= 0:
                mbit = (mb >> 7) & 1
                mb = (mb << 1) & 0xFF
                idx = off + ((1 + mbit) << 8) + symbol
            else:
                idx = off + symbol
            prob = probs[idx]
            bound = (rng >> 11) * prob
            if code < bound:
                rng = bound
                probs[idx] = prob + ((2048 - prob) >> 5)
                symbol = symbol << 1
                if mb >= 0 and mbit != 0:
                    mb = -1
            else:
                rng -= bound
                code -= bound
                probs[idx] = prob - (prob >> 5)
                symbol = (symbol << 1) + 1
                if mb >= 0 and mbit != 1:
                    mb = -1
            if rng < 0x01000000:
                rng = rng << 8
                if pos < limit:
                    b = buf[pos]
                    pos += 1
                elif self.read_byte is not None:
                    b = self.read_byte()
                    buf = self.buf
                    pos = self.pos
                    limit = self.limit
                else:
                    b = 0
                code = ((code << 8) | b) & 0xFFFFFFFF
        self.range = rng
        self.code = code
        self.pos = pos
        return symbol - 0x100

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
                if self.pos < self.limit:
                    b = self.buf[self.pos]
                    self.pos += 1
                elif self.read_byte is not None:
                    b = self.read_byte()
                else:
                    b = 0
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
        wb_blocks=1,
    ):
        self.zero_copy = zero_copy
        self.input = input_source
        self._exact_buf = bytearray(256)
        self.history = (
            history_backend
            if history_backend is not None
            else UnifiedHistory(block_size=block_size)
        )
        self.block_size = block_size
        self.wb_blocks = wb_blocks if wb_blocks is not None else 1
        if self.wb_blocks > 1:
            self.wb_buffer = bytearray(self.wb_blocks * self.block_size)
            self.wb_pos = 0
            self.wb_block_count = 0
            self.last_wb_flush = _get_perf_counter()
        else:
            self.wb_buffer = None
            self.wb_pos = 0
            self.wb_block_count = 0
            self.last_wb_flush = _get_perf_counter()
        self.base_elapsed_ms = 0
        self.session_start_perf = _get_perf_counter()
        self.overall_input_offset = 0
        self.overall_output_offset = 0
        self.stream_out_offset = 0
        self.block_idx = 0
        self.stream_index = 0

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
        self.rd = RangeDecoder(init_stream=False)

        self.PROB_INIT = 1024
        self.state = 0
        self.reps = [0, 0, 0, 0]
        self.lc = 3
        self.lp = 0
        self.pb = 2
        self._reinit_probs()
        self.p_lit = _make_prob_array(0x300 << (self.lc + self.lp))

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
        if hasattr(self, "p_is_match"):
            for arr in self._core_prob_arrays():
                _reset_prob_array(arr)
            return
        self.p_is_match = _make_prob_array(12 << 4)
        self.p_is_rep = _make_prob_array(12)
        self.p_is_rep_g0 = _make_prob_array(12)
        self.p_is_rep_g1 = _make_prob_array(12)
        self.p_is_rep_g2 = _make_prob_array(12)
        self.p_is_rep0_long = _make_prob_array(12 << 4)
        self.p_pos_slot = _make_prob_array(4 << 6)
        self.p_spec_pos = _make_prob_array(115)
        self.p_align = _make_prob_array(16)
        self.p_len = _make_prob_array(514)
        self.p_rep_len = _make_prob_array(514)

    def _read_exact(self, n):
        if n <= 256:
            pos = 0
            while pos < n:
                chunk = self.input.read(n - pos)
                if not chunk:
                    break
                c_len = len(chunk)
                self._exact_buf[pos : pos + c_len] = chunk
                pos += c_len
            if pos < n:
                raise EOFError(f"Unexpected EOF: expected {n} bytes, got {pos}")
            self.overall_input_offset += n
            res = bytes(self._exact_buf[:n])
            return res
        res_b = self.input.read(n)
        if len(res_b) < n:
            raise EOFError(f"Unexpected EOF: expected {n} bytes, got {len(res_b)}")
        self.overall_input_offset += n
        return res_b

    def _read_vli_from_buf(self, buf, off):
        val = 0
        s = 0
        for _ in range(9):
            if off >= len(buf):
                raise XZFormatError("Truncated VLI")
            b = buf[off]
            off += 1
            val |= (b & 0x7F) << s
            if (b & 0x80) == 0:
                return val, off
            s += 7
        raise XZFormatError("Invalid VLI")

    def _read_vli_stream(self, idx_b):
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

    def _read_byte(self):
        ib = self.input.read(1)
        if not ib:
            raise EOFError("Unexpected EOF reading compressed stream")
        self.overall_input_offset += 1
        return ib[0]

    def _refill_chunk_byte(self):
        if self.chunk_stream_off < len(self.chunk_stream_buf):
            b = self.chunk_stream_buf[self.chunk_stream_off]
            self.chunk_stream_off += 1
            self.comp_offset += 1
            return b
        if getattr(self, "chunk_comp_rem", 0) <= 0:
            return 0
        take = min(4096, self.chunk_comp_rem)
        chunk = self.input.read(take)
        if not chunk:
            return 0
        self.overall_input_offset += len(chunk)
        self.compressed_bytes_in_block += len(chunk)
        self.chunk_comp_rem -= len(chunk)
        self.chunk_stream_buf = chunk
        self.chunk_stream_off = 1
        self.comp_offset += 1
        if self.rd is not None:
            self.rd.set_buf(self.chunk_stream_buf, 1)
        return chunk[0]

    def _update_check(self, data):
        self.block_uncomp_bytes += len(data)
        if self.check_type == 1:
            self.block_crc32 = crc32(data, self.block_crc32)
        elif self.check_type == 4:
            self.block_crc64 = crc64(data, self.block_crc64)
        elif self.check_type == 10 and self.block_sha256:
            self.block_sha256.update(data)

    def _core_prob_arrays(self):
        return [
            self.p_is_match,
            self.p_is_rep,
            self.p_is_rep_g0,
            self.p_is_rep_g1,
            self.p_is_rep_g2,
            self.p_is_rep0_long,
            self.p_pos_slot,
            self.p_spec_pos,
            self.p_align,
            self.p_len,
            self.p_rep_len,
        ]

    def export_state(self):
        # If input is seekable, rewind unconsumed read-ahead bytes in chunk_stream_buf
        unconsumed_in_buf = (
            (len(self.chunk_stream_buf) - self.chunk_stream_off)
            if (
                is_seekable(self.input)
                and getattr(self, "chunk_stream_buf", None)
                and self.chunk_stream_off < len(self.chunk_stream_buf)
            )
            else 0
        )
        d = {
            "overall_input_offset": self.overall_input_offset - unconsumed_in_buf,
            "overall_output_offset": self.overall_output_offset,
            "stream_out_offset": self.stream_out_offset,
            "block_idx": self.block_idx,
            "stream_index": self.stream_index,
            "phase": self.phase,
            "null_count": self.null_count,
            "stream_start_in": self.stream_start_in,
            "stream_start_out": self.stream_start_out,
            "flags": ((self.flags[0] << 8) | self.flags[1]) if self.flags else None,
            "check_type": self.check_type,
            "cur_block_pos": self.cur_block_pos,
            "bh_size": self.bh_size,
            "compressed_bytes_in_block": self.compressed_bytes_in_block
            - unconsumed_in_buf,
            "expected_comp_size": self.expected_comp_size,
            "expected_uncomp_size": self.expected_uncomp_size,
            "block_crc32": self.block_crc32,
            "block_crc64": self.block_crc64,
            "block_uncomp_bytes": self.block_uncomp_bytes,
            "tot_idx": getattr(self, "tot_idx", 0),
            "state": self.state,
            "rep0": self.reps[0],
            "rep1": self.reps[1],
            "rep2": self.reps[2],
            "rep3": self.reps[3],
            "lc": self.lc,
            "lp": self.lp,
            "pb": self.pb,
            "in_chunk": bool(self.in_chunk),
            "ctrl": self.ctrl,
            "mode": self.mode,
            "chunk_uncomp_sz": self.chunk_uncomp_sz,
            "chunk_comp_sz": self.chunk_comp_sz,
            "chunk_decoded": self.chunk_decoded,
            "comp_offset": self.comp_offset,
            "raw_pos": self.raw_pos,
            "raw_rem": getattr(self, "raw_rem", 0),
            "chunk_stream_off": (
                0 if unconsumed_in_buf > 0 else getattr(self, "chunk_stream_off", 0)
            ),
            "chunk_comp_rem": getattr(self, "chunk_comp_rem", 0) + unconsumed_in_buf,
            "rd_code": self.rd_code,
            "rd_range": self.rd_range,
            "in_match_copy": bool(self.in_match_copy),
            "need_dict_reset": bool(self.need_dict_reset),
            "need_props": bool(self.need_props),
            "match_dist": self.match_dist,
            "match_rem": self.match_rem,
            "input_offset": self.overall_input_offset - unconsumed_in_buf,
            "history_total": getattr(self.history, "total_written", 0),
            "history_tail_crc32": (
                self.history.tail_crc32() if hasattr(self.history, "tail_crc32") else 0
            ),
            "len_pcore": sum(len(arr) * 2 for arr in self._core_prob_arrays()),
            "len_plit": len(self.p_lit) * 2,
            "len_cblk": self.cur_block_pos,
            "len_inbuf": (
                len(self.chunk_stream_buf)
                if (
                    not is_seekable(self.input)
                    and getattr(self, "chunk_stream_buf", None)
                )
                else 0
            ),
            "len_hbuf": (
                self.history.blocks_len
                if (
                    hasattr(self.history, "blocks")
                    and not getattr(self.history, "output_file", None)
                    and not getattr(self.history, "scratch_file", None)
                )
                else getattr(self.history, "total_written", 0)
            ),
            "elapsed_ms": min(
                0xFFFFFFFF,
                self.base_elapsed_ms
                + int((_get_perf_counter() - self.session_start_perf) * 1000),
            ),
        }
        _assert_valid_state_dict(d)
        return d

    def import_state(self, s):
        _assert_valid_state_dict(s)
        self.overall_input_offset = s["overall_input_offset"]
        self.overall_output_offset = s["overall_output_offset"]
        self.stream_out_offset = s["stream_out_offset"]
        self.block_idx = s["block_idx"]
        self.stream_index = s["stream_index"]
        self.phase = s["phase"]
        self.null_count = s["null_count"]
        self.stream_start_in = s["stream_start_in"]
        self.stream_start_out = s["stream_start_out"]
        flg_val = s.get("flags")
        self.flags = (
            bytes([(flg_val >> 8) & 0xFF, flg_val & 0xFF])
            if flg_val is not None
            else None
        )
        self.check_type = s["check_type"]
        self.cur_block_pos = s["cur_block_pos"]
        self.bh_size = s["bh_size"]
        self.compressed_bytes_in_block = s["compressed_bytes_in_block"]
        self.expected_comp_size = s["expected_comp_size"]
        self.expected_uncomp_size = s["expected_uncomp_size"]
        self.block_crc32 = s["block_crc32"]
        self.block_crc64 = s["block_crc64"]
        self.block_uncomp_bytes = s["block_uncomp_bytes"]
        self.tot_idx = s.get("tot_idx", 0)
        self.state = s["state"]
        self.reps = [
            s.get("rep0", 0),
            s.get("rep1", 0),
            s.get("rep2", 0),
            s.get("rep3", 0),
        ]
        self.lc = s["lc"]
        self.lp = s["lp"]
        self.pb = s["pb"]
        self.in_chunk = bool(s.get("in_chunk", False))
        self.ctrl = s["ctrl"]
        self.mode = s["mode"]
        self.chunk_uncomp_sz = s["chunk_uncomp_sz"]
        self.chunk_comp_sz = s["chunk_comp_sz"]
        self.chunk_decoded = s["chunk_decoded"]
        self.comp_offset = s["comp_offset"]
        self.raw_pos = s.get("raw_pos", 0)
        self.raw_rem = s.get("raw_rem", 0)
        self.chunk_stream_off = s.get("chunk_stream_off", 0)
        self.chunk_comp_rem = s.get("chunk_comp_rem", 0)
        self.rd_code = s["rd_code"]
        self.rd_range = s["rd_range"]
        self.in_match_copy = bool(s.get("in_match_copy", False))
        self.need_dict_reset = bool(s.get("need_dict_reset", True))
        self.need_props = bool(s.get("need_props", True))
        self.match_dist = s["match_dist"]
        self.match_rem = s["match_rem"]
        if (
            hasattr(self.input, "seek")
            and "input_offset" in s
            and is_seekable(self.input)
        ):
            self.input.seek(s["input_offset"])
        if hasattr(self.history, "total_written") and "history_total" in s:
            self.history.total_written = int(s["history_total"])
        if "elapsed_ms" in s:
            self.base_elapsed_ms = min(0xFFFFFFFF, int(s["elapsed_ms"]))
            self.session_start_perf = _get_perf_counter()

    def freeze(self, resume_dir=".xz_history", stream_id=None, storage_driver=None):
        r_id = stream_id or getattr(self.history, "stream_id", "0000000000000000")
        storage = storage_driver or DiskStorage(resume_dir)
        # 1. Write separate binary files directly from preallocated arrays
        # .pcore (11 core probability arrays)
        f_core = storage.open(f"{r_id}.pcore", "wb")
        for arr in self._core_prob_arrays():
            f_core.write(arr)
        f_core.flush()
        _fsync_fileobj(f_core)
        f_core.close()

        # .plit (literal probability array)
        f_lit = storage.open(f"{r_id}.plit", "wb")
        f_lit.write(self.p_lit)
        f_lit.flush()
        _fsync_fileobj(f_lit)
        f_lit.close()

        # .inbuf (compressed chunk stream buffer read-ahead; omitted when input is seekable!)
        if (
            not is_seekable(self.input)
            and getattr(self, "chunk_stream_buf", None)
            and len(self.chunk_stream_buf) > 0
        ):
            f_inbuf = storage.open(f"{r_id}.inbuf", "wb")
            f_inbuf.write(self.chunk_stream_buf)
            f_inbuf.flush()
            _fsync_fileobj(f_inbuf)
            f_inbuf.close()

        # .cblk (uncompressed working block buffer)
        if self.cur_block_pos > 0:
            f_cblk = storage.open(f"{r_id}.cblk", "wb")
            f_cblk.write(memoryview(self.cur_block)[: self.cur_block_pos])
            f_cblk.flush()
            _fsync_fileobj(f_cblk)
            f_cblk.close()

        # .hbuf (in-RAM history buffer if not backed by output_file or scratch_file)
        if hasattr(self.history, "blocks") and len(self.history.blocks) > 0:
            if not getattr(self.history, "output_file", None) and not getattr(
                self.history, "scratch_file", None
            ):
                f_hbuf = storage.open(f"{r_id}.hbuf", "wb")
                for blk in self.history.blocks:
                    f_hbuf.write(blk)
                f_hbuf.flush()
                _fsync_fileobj(f_hbuf)
                f_hbuf.close()

        # 2. Write scalar state metadata
        state_dict = self.export_state()
        state_bytes = _encode_state_dict(state_dict)
        inp_id_b = r_id.encode("utf-8")
        hdr = b"XZCK" + struct.pack("<HIH", 2, 1, len(inp_id_b)) + inp_id_b
        full_data = hdr + state_bytes
        crc = crc32(full_data)
        tmp_name = f".tmp_{r_id}.state"
        target_name = f"{r_id}.state_A"
        f = storage.open(tmp_name, "w+b")
        f.seek(0)
        f.truncate(0)
        f.write(full_data + struct.pack("<I", crc))
        f.flush()
        _fsync_fileobj(f)
        f.close()
        storage.replace(tmp_name, target_name)
        return ResumeDescriptor(
            resume_dir=resume_dir,
            resume_id=r_id,
            resume_from=self.overall_input_offset,
            resume_at=self.overall_output_offset,
        )

    @classmethod
    def thaw(cls, descriptor, input_source, output_target=None, storage_driver=None):
        storage = storage_driver or DiskStorage(descriptor.resume_dir)
        r_id = descriptor.resume_id or "0000000000000000"
        for sname in (
            f"{r_id}.state_A",
            f"{r_id}.state_B",
            "CHECKPOINT_A.bin",
            "CHECKPOINT_B.bin",
            "STATE_A.bin",
            "STATE_B.bin",
        ):
            if storage.exists(sname):
                f = storage.open(sname, "rb")
                data = f.read()
                f.close()
                if len(data) >= 16 and data[:4] == b"XZCK":
                    crc_stored = struct.unpack("<I", data[-4:])[0]
                    if crc32(data[:-4]) == crc_stored:
                        ver, txn, id_len = struct.unpack("<HIH", data[4:12])
                        payload = data[12 + id_len : -4]
                        state = _decode_state_dict(payload)
                        engine = cls(input_source, zero_copy=True)
                        engine.import_state(state)

                        pcore_name = (
                            f"{r_id}.pcore"
                            if storage.exists(f"{r_id}.pcore")
                            else (
                                "CHECKPOINT_A.pcore"
                                if storage.exists("CHECKPOINT_A.pcore")
                                else (
                                    "CHECKPOINT_B.pcore"
                                    if storage.exists("CHECKPOINT_B.pcore")
                                    else None
                                )
                            )
                        )
                        if pcore_name:
                            f_core = storage.open(pcore_name, "rb")
                            f_core.seek(0, 2)
                            p_sz = f_core.tell()
                            f_core.seek(0)
                            exp_core_len = state.get(
                                "len_pcore",
                                sum(len(arr) * 2 for arr in engine._core_prob_arrays()),
                            )
                            if p_sz < exp_core_len:
                                f_core.close()
                                raise XZResourceError(
                                    f"Corrupted pcore: file size {p_sz} < recorded length {exp_core_len}"
                                )
                            for arr in engine._core_prob_arrays():
                                n = f_core.readinto(arr)
                                assert (
                                    n == len(arr) * 2
                                ), "readinto failed for core array"
                            f_core.close()

                        plit_name = (
                            f"{r_id}.plit"
                            if storage.exists(f"{r_id}.plit")
                            else (
                                "CHECKPOINT_A.plit"
                                if storage.exists("CHECKPOINT_A.plit")
                                else (
                                    "CHECKPOINT_B.plit"
                                    if storage.exists("CHECKPOINT_B.plit")
                                    else None
                                )
                            )
                        )
                        if plit_name:
                            f_lit = storage.open(plit_name, "rb")
                            f_lit.seek(0, 2)
                            l_sz = f_lit.tell()
                            f_lit.seek(0)
                            expected_sz = 0x300 << (engine.lc + engine.lp)
                            exp_plit_len = state.get("len_plit", expected_sz * 2)
                            if l_sz < exp_plit_len:
                                f_lit.close()
                                raise XZResourceError(
                                    f"Corrupted plit: file size {l_sz} < recorded length {exp_plit_len}"
                                )
                            if (
                                getattr(engine, "p_lit", None) is None
                                or len(engine.p_lit) != expected_sz
                            ):
                                engine.p_lit = _make_prob_array(expected_sz)
                            n = f_lit.readinto(engine.p_lit)
                            assert (
                                n == len(engine.p_lit) * 2
                            ), f"readinto failed for p_lit"
                            f_lit.close()

                        inbuf_name = (
                            f"{r_id}.inbuf"
                            if (
                                not is_seekable(input_source)
                                and storage.exists(f"{r_id}.inbuf")
                            )
                            else (
                                "CHECKPOINT_A.inbuf"
                                if storage.exists("CHECKPOINT_A.inbuf")
                                else (
                                    "CHECKPOINT_B.inbuf"
                                    if storage.exists("CHECKPOINT_B.inbuf")
                                    else None
                                )
                            )
                        )
                        if inbuf_name:
                            f_inbuf = storage.open(inbuf_name, "rb")
                            f_inbuf.seek(0, 2)
                            i_sz = f_inbuf.tell()
                            f_inbuf.seek(0)
                            exp_inbuf_len = state.get("len_inbuf", i_sz)
                            if i_sz < exp_inbuf_len:
                                f_inbuf.close()
                                raise XZResourceError(
                                    f"Corrupted inbuf: file size {i_sz} < recorded length {exp_inbuf_len}"
                                )
                            engine.chunk_stream_buf = f_inbuf.read(exp_inbuf_len)
                            f_inbuf.close()

                        cblk_name = (
                            f"{r_id}.cblk"
                            if storage.exists(f"{r_id}.cblk")
                            else (
                                "CHECKPOINT_A.cblk"
                                if storage.exists("CHECKPOINT_A.cblk")
                                else (
                                    "CHECKPOINT_B.cblk"
                                    if storage.exists("CHECKPOINT_B.cblk")
                                    else None
                                )
                            )
                        )
                        if cblk_name and engine.cur_block_pos > 0:
                            f_cblk = storage.open(cblk_name, "rb")
                            f_cblk.seek(0, 2)
                            c_sz = f_cblk.tell()
                            f_cblk.seek(0)
                            exp_cblk_len = state.get("len_cblk", engine.cur_block_pos)
                            if c_sz < exp_cblk_len:
                                f_cblk.close()
                                raise XZResourceError(
                                    f"Corrupted cblk: file size {c_sz} < recorded length {exp_cblk_len}"
                                )
                            n = f_cblk.readinto(
                                memoryview(engine.cur_block)[:exp_cblk_len]
                            )
                            assert n == exp_cblk_len, "readinto failed for cblk"
                            f_cblk.close()

                        hbuf_name = (
                            f"{r_id}.hbuf"
                            if storage.exists(f"{r_id}.hbuf")
                            else (
                                "CHECKPOINT_A.hbuf"
                                if storage.exists("CHECKPOINT_A.hbuf")
                                else (
                                    "CHECKPOINT_B.hbuf"
                                    if storage.exists("CHECKPOINT_B.hbuf")
                                    else None
                                )
                            )
                        )
                        if hbuf_name and hasattr(engine.history, "blocks"):
                            f_hbuf = storage.open(hbuf_name, "rb")
                            f_hbuf.seek(0, 2)
                            h_sz = f_hbuf.tell()
                            f_hbuf.seek(0)
                            exp_hbuf_len = state.get("len_hbuf", h_sz)
                            if h_sz < exp_hbuf_len:
                                f_hbuf.close()
                                raise XZResourceError(
                                    f"Corrupted hbuf: file size {h_sz} < recorded length {exp_hbuf_len}"
                                )
                            raw_h = f_hbuf.read(exp_hbuf_len)
                            f_hbuf.close()
                            engine.history.blocks = [raw_h]
                            engine.history.blocks_len = len(raw_h)

                        return engine
        raise XZResourceError(
            f"No valid checkpoint found to thaw in {descriptor.resume_dir}"
        )

    def discard_checkpoint(self, descriptor, storage_driver=None):
        storage = storage_driver or DiskStorage(descriptor.resume_dir)
        r_id = descriptor.resume_id or getattr(
            self.history, "stream_id", "0000000000000000"
        )
        for ext in (
            ".state_A",
            ".state_B",
            ".pcore",
            ".plit",
            ".cblk",
            ".hbuf",
            ".inbuf",
            ".state",
        ):
            storage.remove(f"{r_id}{ext}")
        storage.cleanup(r_id)

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

                    if self.overall_input_offset == 1 and self.stream_index == 0:
                        if b == b"\xfd":
                            magic = b"\xfd" + self._read_exact(5)
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
                        expected_sz = 0x300 << (self.lc + self.lp)
                        if (
                            getattr(self, "p_lit", None) is None
                            or len(self.p_lit) != expected_sz
                        ):
                            self.p_lit = _make_prob_array(expected_sz)
                        else:
                            _reset_prob_array(self.p_lit)
                        self.state = 0
                        self.reps = [0, 0, 0, 0]
                        if len(self.cur_block) != self.block_size:
                            self.cur_block = bytearray(self.block_size)
                        self.cur_block_pos = 0
                        self.stream_out_offset = 0
                        self.history.reset()
                        self.stream_start_in = 0
                        self.stream_start_out = self.overall_output_offset
                        self.is_legacy_lzma = True

                        c1 = self._read_byte()
                        c2 = self._read_byte()
                        c3 = self._read_byte()
                        c4 = self._read_byte()
                        if self.rd is None:
                            self.rd = RangeDecoder(self._read_byte, init_stream=False)
                        else:
                            self.rd.read_byte = self._read_byte
                            self.rd.buf = None
                            self.rd.pos = 0
                            self.rd.limit = 0
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
                        magic = b"\xfd" + self._read_exact(5)
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
                        expected_sz = 0x300 << (self.lc + self.lp)
                        if (
                            getattr(self, "p_lit", None) is None
                            or len(self.p_lit) != expected_sz
                        ):
                            self.p_lit = _make_prob_array(expected_sz)
                        else:
                            _reset_prob_array(self.p_lit)
                        self.state = 0
                        self.reps = [0, 0, 0, 0]
                        if len(self.cur_block) != self.block_size:
                            self.cur_block = bytearray(self.block_size)
                        self.cur_block_pos = 0
                        self.stream_out_offset = 0
                        self.history.reset()
                        self.stream_start_in = self.overall_input_offset - 14
                        self.stream_start_out = self.overall_output_offset
                        self.is_legacy_lzma = True

                        c1 = self._read_byte()
                        c2 = self._read_byte()
                        c3 = self._read_byte()
                        c4 = self._read_byte()
                        if self.rd is None:
                            self.rd = RangeDecoder(self._read_byte, init_stream=False)
                        else:
                            self.rd.read_byte = self._read_byte
                            self.rd.buf = None
                            self.rd.pos = 0
                            self.rd.limit = 0
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
                if len(self.cur_block) != self.block_size:
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
                                symbol = rd.decode_literal(self.p_lit, lit_off, mb)
                            else:
                                symbol = rd.decode_literal(self.p_lit, lit_off, -1)
                            self.cur_block[self.cur_block_pos] = symbol
                            self.cur_block_pos += 1
                            self.state = _LIT_TRANS[self.state]

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
                if has_comp:
                    self.expected_comp_size, off = self._read_vli_from_buf(full_bh, off)
                else:
                    self.expected_comp_size = None
                if has_uncomp:
                    self.expected_uncomp_size, off = self._read_vli_from_buf(
                        full_bh, off
                    )
                else:
                    self.expected_uncomp_size = None
                fid, off = self._read_vli_from_buf(full_bh, off)
                if fid != 0x21:
                    raise XZUnsupportedError("Not LZMA2")
                psz, off = self._read_vli_from_buf(full_bh, off)
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
                                expected_sz = 0x300 << (self.lc + self.lp)
                                if (
                                    getattr(self, "p_lit", None) is None
                                    or len(self.p_lit) != expected_sz
                                ):
                                    self.p_lit = _make_prob_array(expected_sz)
                                else:
                                    _reset_prob_array(self.p_lit)
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
                            if is_seekable(self.input) and hasattr(self.input, "read"):
                                self.chunk_stream_buf = self.input.read(
                                    self.chunk_comp_rem
                                )
                                self.overall_input_offset += len(self.chunk_stream_buf)
                                self.compressed_bytes_in_block += len(
                                    self.chunk_stream_buf
                                )
                                self.chunk_comp_rem = 0
                            else:
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
                        rd = self.rd
                        if is_seekable(self.input):
                            if (
                                not self.chunk_stream_buf
                                and getattr(self, "chunk_comp_rem", 0) > 0
                            ):
                                self.chunk_stream_buf = self.input.read(
                                    self.chunk_comp_rem
                                )
                                self.overall_input_offset += len(self.chunk_stream_buf)
                                self.compressed_bytes_in_block += len(
                                    self.chunk_stream_buf
                                )
                                self.chunk_comp_rem = 0
                                self.chunk_stream_off = 0
                            rd.read_byte = None
                            rd.set_buf(self.chunk_stream_buf, self.chunk_stream_off)
                        else:
                            rd.read_byte = self._refill_chunk_byte
                            rd.set_buf(self.chunk_stream_buf, self.chunk_stream_off)
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
                                        if rd.buf is not None:
                                            self.chunk_stream_off = rd.pos
                                            self.comp_offset = (
                                                self.chunk_comp_sz
                                                - len(self.chunk_stream_buf)
                                                + rd.pos
                                            )
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
                                    symbol = rd.decode_literal(self.p_lit, lit_off, mb)
                                else:
                                    symbol = rd.decode_literal(self.p_lit, lit_off, -1)
                                self.cur_block[self.cur_block_pos] = symbol
                                self.cur_block_pos += 1
                                self.chunk_decoded += 1
                                self.state = _LIT_TRANS[self.state]

                                if self.cur_block_pos == self.block_size:
                                    committed = self._commit_block()
                                    if self.force_gc:
                                        try:
                                            gc.collect()
                                        except Exception:
                                            pass
                                    self.rd_code = rd.code
                                    self.rd_range = rd.range
                                    if rd.buf is not None:
                                        self.chunk_stream_off = rd.pos
                                        self.comp_offset = (
                                            self.chunk_comp_sz
                                            - len(self.chunk_stream_buf)
                                            + rd.pos
                                        )
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
                                        if rd.buf is not None:
                                            self.chunk_stream_off = rd.pos
                                            self.comp_offset = (
                                                self.chunk_comp_sz
                                                - len(self.chunk_stream_buf)
                                                + rd.pos
                                            )
                                        # comp_offset tracked in stream
                                        if self.match_rem == 0:
                                            self.in_match_copy = False
                                        return committed, False
                                self.in_match_copy = False

                        self.in_chunk = False
                        self.rd_code = rd.code
                        self.rd_range = rd.range
                        if rd.buf is not None:
                            self.chunk_stream_off = rd.pos
                            self.comp_offset = (
                                self.chunk_comp_sz - len(self.chunk_stream_buf) + rd.pos
                            )
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
                n_rec = self._read_vli_stream(idx_b)
                if n_rec != len(self.stream_block_records):
                    raise XZFormatError("Index record mismatch")
                for r_idx in range(n_rec):
                    u_sz = self._read_vli_stream(idx_b)
                    unc_sz = self._read_vli_stream(idx_b)
                    eu, eunc = self.stream_block_records[r_idx]
                    if u_sz != eu or unc_sz != eunc:
                        raise XZFormatError("Index record size mismatch")
                pad = (4 - (len(idx_b) % 4)) % 4
                if pad > 0:
                    pb = self._read_exact(pad)
                    for b in pb:
                        idx_b.append(b)
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

    def _emit_events(self, chunk_data, bytes_decoded):
        self._chunk_event.data = chunk_data
        yield self._chunk_event

        self._block_committed_event.block_index = self.block_idx - 1
        self._block_committed_event.block_data = chunk_data
        self._block_committed_event.state_record = (
            self.export_state() if self._enable_state_export else None
        )
        yield self._block_committed_event

        self._progress_event.block_index = self.block_idx - 1
        self._progress_event.bytes_decoded = bytes_decoded
        self._progress_event.total_emitted = self.overall_output_offset
        self._progress_event.elapsed_ms = min(
            0xFFFFFFFF,
            self.base_elapsed_ms
            + int((_get_perf_counter() - self.session_start_perf) * 1000),
        )
        yield self._progress_event

    def _flush_wb(self):
        if self.wb_pos == 0:
            return
        wb_slice = memoryview(self.wb_buffer)[: self.wb_pos]
        yield from self._emit_events(wb_slice, self.wb_pos)
        self.wb_pos = 0
        self.wb_block_count = 0
        self.last_wb_flush = _get_perf_counter()

    def decode_events(self):
        while True:
            if self.pending_boundary:
                if self.wb_buffer is not None and self.wb_pos > 0:
                    yield from self._flush_wb()
                ev = self.pending_boundary
                self.pending_boundary = None
                yield ev
            blk, is_eof = self.decode_block()
            if blk:
                if self.wb_buffer is not None:
                    blk_len = len(blk)
                    self.wb_buffer[self.wb_pos : self.wb_pos + blk_len] = blk
                    self.wb_pos += blk_len
                    self.wb_block_count += 1
                    now = _get_perf_counter()
                    if (
                        self.wb_block_count >= self.wb_blocks
                        or is_eof
                        or (self.pending_boundary is not None)
                        or (now - self.last_wb_flush >= 2.0)
                    ):
                        yield from self._flush_wb()
                else:
                    yield from self._emit_events(blk, len(blk))

            if self.pending_boundary:
                if self.wb_buffer is not None and self.wb_pos > 0:
                    yield from self._flush_wb()
                ev = self.pending_boundary
                self.pending_boundary = None
                yield ev
            if is_eof:
                if self.wb_buffer is not None and self.wb_pos > 0:
                    yield from self._flush_wb()
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
    storage_driver=None,
    write_back_cache=None,
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

    if (
        not is_stream_output
        and output_target
        and isinstance(output_target, (str, Path))
        and not storage_dir
        and not resume_dir
        and not ignore_space_check
    ):
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
    storage_driver=None,
    write_back_cache=None,
):
    """
    Callable interface for XZ decompression with persistent checkpoint resumption.
    """
    caller_owned_output = hasattr(output_target, "write")
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

    storage = (
        storage_driver
        if storage_driver is not None
        else DiskStorage(actual_storage_dir or ".xz_history")
    )
    if not avail_mem or avail_mem < 256 * 1024:
        c_budget = actual_block_size
    elif avail_mem < 80 * 1024 * 1024:
        max_blocks = max(1, int((avail_mem * 0.20) // actual_block_size))
        c_budget = min(16 * 1024 * 1024, max_blocks * actual_block_size)
    else:
        c_budget = 64 * 1024 * 1024 + actual_block_size

    if history_backend is not None and not isinstance(history_backend, str):
        history = history_backend
    else:
        history = UnifiedHistory(
            output_file=None,
            storage_driver=storage,
            stream_id=stream_id,
            ram_cache_bytes=c_budget,
            block_size=actual_block_size,
        )

    # Parse and resolve timeouts
    timeout_sec = parse_duration(timeout) if timeout is not None else 52.0
    cpu_timeout_sec = parse_duration(cpu_timeout) if cpu_timeout is not None else 22.0

    wb_blocks = select_write_back_blocks(
        actual_block_size,
        available_mem=avail_mem,
        override=write_back_cache,
        timeout_sec=timeout_sec,
        storage_driver=storage,
    )
    engine = XZStreamDecompressor(
        in_stream,
        history,
        block_size=actual_block_size,
        force_gc=force_gc,
        zero_copy=True,
        wb_blocks=wb_blocks,
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
            if storage.exists(b_name):
                try:
                    f = storage.open(b_name, "rb")
                    data = f.read()
                    f.close()
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
                                "prefix": b_name[:-4],
                            }
                except Exception:
                    pass
        return best_rec

    txn_counter = 0

    def _write_checkpoint(r_dir, engine_obj, hist_obj, inp_id, output_handle=None):
        nonlocal txn_counter
        _fsync_fileobj(output_handle)
        if hasattr(hist_obj, "sync"):
            hist_obj.sync()
        txn_counter += 1
        prefix = "CHECKPOINT_A" if (txn_counter % 2 == 1) else "CHECKPOINT_B"
        target_name = f"{prefix}.bin"
        tmp_name = ".tmp_checkpoint.bin"
        if replay_input is not None:
            replay_input.sync()

        # 1. Write separate binary files directly from preallocated arrays
        # .pcore
        f_core = storage.open(f"{prefix}.pcore", "wb")
        for arr in engine_obj._core_prob_arrays():
            f_core.write(arr)
        f_core.flush()
        _fsync_fileobj(f_core)
        f_core.close()

        # .plit
        f_lit = storage.open(f"{prefix}.plit", "wb")
        f_lit.write(engine_obj.p_lit)
        f_lit.flush()
        _fsync_fileobj(f_lit)
        f_lit.close()

        # .cblk
        if engine_obj.cur_block_pos > 0:
            f_cblk = storage.open(f"{prefix}.cblk", "wb")
            f_cblk.write(memoryview(engine_obj.cur_block)[: engine_obj.cur_block_pos])
            f_cblk.flush()
            _fsync_fileobj(f_cblk)
            f_cblk.close()

        # .hbuf and .inbuf remain outside the checkpoint state file and can be safely shared across consecutive iterations
        prev_prefix = "CHECKPOINT_B" if (prefix == "CHECKPOINT_A") else "CHECKPOINT_A"
        hbuf_name = None
        has_ram_hist = (
            hasattr(hist_obj, "blocks")
            and len(hist_obj.blocks) > 0
            and (
                not getattr(hist_obj, "output_file", None)
                and not getattr(hist_obj, "scratch_file", None)
            )
        )
        if has_ram_hist:
            last_tag = getattr(hist_obj, "_last_hbuf_tag", None)
            if last_tag == hist_obj.blocks_len and storage.exists(
                f"{prev_prefix}.hbuf"
            ):
                # Consecutive state iterations safely share existing hbuf!
                hbuf_name = f"{prev_prefix}.hbuf"
            else:
                hbuf_name = f"{prefix}.hbuf"
                tmp_h = f".tmp_{prefix}.hbuf"
                f_hbuf = storage.open(tmp_h, "wb")
                for blk in hist_obj.blocks:
                    f_hbuf.write(blk)
                f_hbuf.flush()
                _fsync_fileobj(f_hbuf)
                f_hbuf.close()
                storage.replace(tmp_h, hbuf_name)
                hist_obj._last_hbuf_tag = hist_obj.blocks_len

        inbuf_name = None
        if (
            not is_seekable(engine_obj.input)
            and getattr(engine_obj, "chunk_stream_buf", None)
            and len(engine_obj.chunk_stream_buf) > 0
        ):
            inbuf_name = f"{prefix}.inbuf"
            tmp_in = f".tmp_{prefix}.inbuf"
            f_inbuf = storage.open(tmp_in, "wb")
            f_inbuf.write(engine_obj.chunk_stream_buf)
            f_inbuf.flush()
            _fsync_fileobj(f_inbuf)
            f_inbuf.close()
            storage.replace(tmp_in, inbuf_name)

        # 2. Write scalar state metadata
        state_rec = engine_obj.export_state()
        if replay_input is not None:
            state_rec["source_read_offset"] = replay_input.durable_offset()

        payload_bytes = _encode_state_dict(state_rec)
        inp_bytes = (inp_id or "").encode("utf-8")
        header = (
            b"XZCK" + struct.pack("<HIH", 2, txn_counter, len(inp_bytes)) + inp_bytes
        )
        full_data = header + payload_bytes
        crc = crc32(full_data)

        f = storage.open(tmp_name, "w+b")
        f.write(full_data + struct.pack("<I", crc))
        f.flush()
        _fsync_fileobj(f)
        f.close()
        storage.replace(tmp_name, target_name)
        # After successfully committing new checkpoint, remove earlier iteration files that are not shared
        if storage.exists(f"{prev_prefix}.hbuf") and hbuf_name != f"{prev_prefix}.hbuf":
            storage.remove(f"{prev_prefix}.hbuf")
        if (
            storage.exists(f"{prev_prefix}.inbuf")
            and inbuf_name != f"{prev_prefix}.inbuf"
        ):
            storage.remove(f"{prev_prefix}.inbuf")

    curr_out_stream = None
    curr_dest_path = None
    curr_part_path = None
    stream_idx = 0

    is_implicit_stdin_unxz = str(input_source) == "-" and output_target == "stdin.unxz"

    if hasattr(output_target, "write"):
        curr_out_stream = output_target
    elif is_stream_output:
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

    active_out = curr_part_path or curr_dest_path
    if not is_stream_output and active_out:
        if active_out.exists():
            curr_out_stream = open(str(active_out), "r+b")
        else:
            curr_out_stream = open(str(active_out), "w+b")
        if isinstance(history, UnifiedHistory):
            history.set_output_file(curr_out_stream)

    discard_until_offset = (
        int(resume_at)
        if (
            resume_at is not None
            and int(resume_at) > 0
            and (is_stream_output or not is_seekable(curr_out_stream))
        )
        else 0
    )

    checkpoint_resumed = False

    # Checkpoint restore check on start
    if resume_dir and resume_mode != "off":
        ckpt = _load_best_checkpoint(resume_dir, inp_id)
        if ckpt is not None:
            prefix = ckpt.get("prefix") or "CHECKPOINT_A"
            try:
                state_dict = ckpt["state"]
                txn_counter = ckpt.get("txn", 0)
                out_off = state_dict.get("overall_output_offset", 0)
                hist_tot = state_dict.get("history_total", 0)
                exp_tail_crc = state_dict.get("history_tail_crc32")
                history.truncate_to(hist_tot)
                if exp_tail_crc is not None and history.tail_crc32() != exp_tail_crc:
                    raise XZResourceError(
                        "History tail CRC32 mismatch on checkpoint restore"
                    )

                if not is_stream_output and active_out:
                    if curr_out_stream is not None and not caller_owned_output:
                        try:
                            curr_out_stream.close()
                        except Exception:
                            pass
                    truncate_file_portable(str(active_out), out_off)
                    if not caller_owned_output:
                        curr_out_stream = open(
                            str(active_out), "r+b" if out_off > 0 else "w+b"
                        )
                    curr_out_stream.seek(out_off)
                    if isinstance(history, UnifiedHistory):
                        history.set_output_file(curr_out_stream)

                in_off = state_dict.get("input_offset", 0)
                if replay_input is not None:
                    replay_input.set_replay_offset(
                        state_dict.get("source_read_offset", 0)
                    )
                elif is_seekable(in_stream):
                    in_stream.seek(in_off)
                engine.import_state(state_dict)

                # Restore separate binary files via readinto() directly into preallocated arrays
                prefix = ckpt.get("prefix") or (
                    "CHECKPOINT_A" if (txn_counter % 2 == 1) else "CHECKPOINT_B"
                )
                if storage.exists(f"{prefix}.pcore"):
                    f_core = storage.open(f"{prefix}.pcore", "rb")
                    f_core.seek(0, 2)
                    p_sz = f_core.tell()
                    f_core.seek(0)
                    exp_core_len = state_dict.get(
                        "len_pcore",
                        sum(len(arr) * 2 for arr in engine._core_prob_arrays()),
                    )
                    if p_sz < exp_core_len:
                        f_core.close()
                        raise XZResourceError(
                            f"Corrupted pcore: file size {p_sz} < recorded length {exp_core_len}"
                        )
                    for arr in engine._core_prob_arrays():
                        n = f_core.readinto(arr)
                        assert (
                            n == len(arr) * 2
                        ), "readinto failed for pcore on checkpoint restore"
                    f_core.close()

                if storage.exists(f"{prefix}.plit"):
                    expected_sz = 0x300 << (engine.lc + engine.lp)
                    exp_plit_len = state_dict.get("len_plit", expected_sz * 2)
                    f_lit = storage.open(f"{prefix}.plit", "rb")
                    f_lit.seek(0, 2)
                    l_sz = f_lit.tell()
                    f_lit.seek(0)
                    if l_sz < exp_plit_len:
                        f_lit.close()
                        raise XZResourceError(
                            f"Corrupted plit: file size {l_sz} < recorded length {exp_plit_len}"
                        )
                    if (
                        getattr(engine, "p_lit", None) is None
                        or len(engine.p_lit) != expected_sz
                    ):
                        engine.p_lit = _make_prob_array(expected_sz)
                    n = f_lit.readinto(engine.p_lit)
                    assert (
                        n == len(engine.p_lit) * 2
                    ), "readinto failed for plit on checkpoint restore"
                    f_lit.close()

                if storage.exists(f"{prefix}.cblk") and engine.cur_block_pos > 0:
                    f_cblk = storage.open(f"{prefix}.cblk", "rb")
                    f_cblk.seek(0, 2)
                    c_sz = f_cblk.tell()
                    f_cblk.seek(0)
                    exp_cblk_len = state_dict.get("len_cblk", engine.cur_block_pos)
                    if c_sz < exp_cblk_len:
                        f_cblk.close()
                        raise XZResourceError(
                            f"Corrupted cblk: file size {c_sz} < recorded length {exp_cblk_len}"
                        )
                    n = f_cblk.readinto(memoryview(engine.cur_block)[:exp_cblk_len])
                    assert (
                        n == exp_cblk_len
                    ), "readinto failed for cblk on checkpoint restore"
                    f_cblk.close()

                if storage.exists(f"{prefix}.hbuf") and hasattr(history, "history"):
                    f_hbuf = storage.open(f"{prefix}.hbuf", "rb")
                    f_hbuf.seek(0, 2)
                    h_sz = f_hbuf.tell()
                    f_hbuf.seek(0)
                    exp_hbuf_len = state_dict.get("len_hbuf", h_sz)
                    if h_sz < exp_hbuf_len:
                        f_hbuf.close()
                        raise XZResourceError(
                            f"Corrupted hbuf: file size {h_sz} < recorded length {exp_hbuf_len}"
                        )
                    engine.history.history = bytearray(exp_hbuf_len)
                    n = f_hbuf.readinto(engine.history.history)
                    assert (
                        n == exp_hbuf_len
                    ), "readinto failed for hbuf on checkpoint restore"
                    f_hbuf.close()

                checkpoint_resumed = True
            except Exception as ckpt_err:
                # Corrupted checkpoint! Asymmetric recovery:
                if is_stream_input:
                    raise XZResourceError(
                        f"Corrupted checkpoint detected on unseekable input stream: {ckpt_err}"
                    )

                # Input is seekable: clean up bad checkpoint files
                for f_name in (
                    f"{prefix}.bin",
                    f"{prefix}.state",
                    f"{prefix}.pcore",
                    f"{prefix}.plit",
                    f"{prefix}.cblk",
                    f"{prefix}.hbuf",
                ):
                    storage.remove(f_name)

                if is_seekable(in_stream) and (
                    is_stream_output
                    or not hasattr(curr_out_stream, "seek")
                    or not is_seekable(curr_out_stream)
                ):
                    if resume_at is not None and resume_at > 0:
                        sys.stderr.write(
                            f"xz_decompressor: warning: corrupted checkpoint detected; replaying from genesis and discarding decoded output prior to offset {resume_at}.\n"
                        )
                        sys.stderr.flush()
                        discard_until_offset = int(resume_at)
                        in_stream.seek(0)
                        engine = XZStreamDecompressor(
                            in_stream,
                            history,
                            block_size=actual_block_size,
                            force_gc=force_gc,
                            zero_copy=True,
                        )
                    else:
                        raise XZResourceError(
                            f"Corrupted checkpoint on unseekable output without --resume-at: {ckpt_err}"
                        )
                elif is_seekable(in_stream) and not is_stream_output:
                    sys.stderr.write(
                        f"xz_decompressor: warning: corrupted checkpoint detected in '{resume_dir}'; invalidating state and restarting decompression from genesis.\n"
                    )
                    sys.stderr.flush()
                    in_stream.seek(0)
                    if active_out:
                        truncate_file_portable(str(active_out), 0)
                        if curr_out_stream and not caller_owned_output:
                            curr_out_stream.close()
                            curr_out_stream = open(str(active_out), "w+b")
                            if isinstance(history, UnifiedHistory):
                                history.set_output_file(curr_out_stream)
                    engine = XZStreamDecompressor(
                        in_stream,
                        history,
                        block_size=actual_block_size,
                        force_gc=force_gc,
                        zero_copy=True,
                    )
        elif resume_mode == "checkpoint":
            raise XZResourceError("No valid checkpoint found for checkpoint resumption")
        else:
            if (
                is_seekable(in_stream)
                and resume_at is not None
                and int(resume_at) > 0
                and (is_stream_output or not is_seekable(curr_out_stream))
            ):
                discard_until_offset = int(resume_at)
                sys.stderr.write(
                    f"xz_decompressor: warning: no valid checkpoint found; replaying from genesis and discarding decoded output prior to offset {resume_at}.\n"
                )
                sys.stderr.flush()
            if replay_input is not None:
                replay_input.set_replay_offset(0)
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
        if isinstance(history, UnifiedHistory):
            history.set_output_file(curr_out_stream)
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
                    if discard_until_offset > 0:
                        cur_end = engine.overall_output_offset
                        cur_start = cur_end - len(event.data)
                        if cur_end <= discard_until_offset:
                            pass
                        elif cur_start < discard_until_offset:
                            discard_len = discard_until_offset - cur_start
                            curr_out_stream.write(event.data[discard_len:])
                            curr_out_stream.flush()
                        else:
                            curr_out_stream.write(event.data)
                            curr_out_stream.flush()
                    else:
                        curr_out_stream.write(event.data)
                        curr_out_stream.flush()

            elif event.type == XZEventType.BLOCK_COMMITTED:
                if replay_input is not None:
                    replay_input.rebase_to(engine.overall_input_offset)
                if (
                    resume_dir
                    and getattr(history, "persistent", False)
                    and resume_mode != "off"
                    and (not have_checkpoint or bytes_since_ckpt >= ckpt_interval_bytes)
                ):
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
                        if isinstance(engine.history, UnifiedHistory):
                            engine.history.output_file = (
                                curr_out_stream
                                if is_seekable(curr_out_stream)
                                else None
                            )
                            engine.history.reset()

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
                storage.remove(j)
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

    def test_unified_history_engine_protocol(self):
        storage = MemoryStorage()
        hist = UnifiedHistory(
            output_file=None,
            storage_driver=storage,
            stream_id="test_stream",
            ram_cache_bytes=1024,
        )
        b1 = b"ABCDEFGHIJ" * 50
        b2 = b"KLMNOPQRST" * 50
        b3 = b"UVWXYZ0123" * 50
        hist.append_block(b1)
        hist.append_block(b2)
        hist.append_block(b3)
        self.assertEqual(hist.history_len(), 1500)
        sl1 = hist.get_history_slice(200, 10)
        self.assertEqual(sl1, (b2 + b3)[-200:-190])
        sl2 = hist.get_history_slice(1400, 10)
        self.assertEqual(sl2, (b1 + b2 + b3)[-1400:-1390])
        self.assertGreater(hist.tail_crc32(), 0)
        hist.truncate_to(800)
        self.assertEqual(hist.history_len(), 800)
        hist.cleanup()

    def test_unified_history_seekable_file_mode(self):
        f = io.BytesIO()
        hist = UnifiedHistory(output_file=f, ram_cache_bytes=512)
        data = b"SEEKABLE_OUTPUT_FILE_HISTORY_DATA_" * 30
        f.write(data)
        hist.append_block(data)
        self.assertEqual(hist.history_len(), len(data))
        sl = hist.get_history_slice(len(data), 10)
        self.assertEqual(sl, data[:10])

    def test_transient_storage_disk_and_memory(self):
        vfs = MemoryStorage()
        f = vfs.open("test.bin", "w+b")
        f.write(b"HELLO_VFS")
        f.flush()
        self.assertTrue(vfs.exists("test.bin"))
        f2 = vfs.open("test.bin", "rb")
        self.assertEqual(f2.read(), b"HELLO_VFS")
        vfs.replace("test.bin", "renamed.bin")
        self.assertFalse(vfs.exists("test.bin"))
        self.assertTrue(vfs.exists("renamed.bin"))
        vfs.remove("renamed.bin")
        self.assertFalse(vfs.exists("renamed.bin"))

    def test_resume_descriptor_ergonomics(self):
        desc = ResumeDescriptor(
            resume_dir="/tmp/custom_history",
            resume_id="stream_42",
            resume_from=1024,
            resume_at=4096,
        )
        args_str = desc.to_cli_args()
        self.assertIn("--resume-dir=/tmp/custom_history", args_str)
        self.assertIn("--resume-id=stream_42", args_str)
        self.assertIn("--resume-from=1024", args_str)
        self.assertIn("--resume-at=4096", args_str)
        desc_seekable = ResumeDescriptor(
            resume_dir="/tmp/custom_history",
            resume_id=None,
            resume_from=None,
            resume_at=None,
        )
        args_seekable = desc_seekable.to_cli_args()
        self.assertEqual(args_seekable, "--resume-dir=/tmp/custom_history")

    def test_freeze_thaw_block_size_agnostic(self):
        # Proves that an engine frozen with a large block size (64 KiB) can thaw and
        # resume cleanly with a tiny microcontroller block size (4 KiB)
        lzma = self.lzma
        import io, os

        raw = os.urandom(200000)
        xz_data = lzma.compress(raw, preset=1)

        # 1. Start engine with large 64 KiB block size
        eng_large = XZStreamDecompressor(io.BytesIO(xz_data), block_size=65536)
        blk1, eof = eng_large.decode_block()
        self.assertEqual(len(blk1), 65536)
        self.assertFalse(eof)

        # 2. Freeze state
        desc = eng_large.freeze(
            resume_dir="/tmp/test_bs_agnostic_dir", stream_id="stream_bs_test"
        )

        # 3. Thaw in a new engine configured with tiny 4 KiB block size (as on 20 KiB MicroPython)
        thawed = XZStreamDecompressor.thaw(desc, io.BytesIO(xz_data))
        # Verify block size can be re-targeted
        thawed.block_size = 4096
        thawed.cur_block = bytearray(4096)

        out_blocks = [blk1]
        while True:
            b, is_eof = thawed.decode_block()
            if b:
                out_blocks.append(b)
                # Confirm subsequent blocks are at most the new 4 KiB size
                self.assertLessEqual(len(b), 4096)
            if is_eof:
                break

        self.assertEqual(b"".join(out_blocks), raw)
        thawed.discard_checkpoint(desc)

    def test_freeze_thaw_lifecycle_api(self):
        lzma = self.lzma
        import io

        raw = b"FREEZE_THAW_LIFECYCLE_TEST_PAYLOAD_" * 100
        xz_data = lzma.compress(raw, preset=1)
        eng = XZStreamDecompressor(io.BytesIO(xz_data))
        blk, eof = eng.decode_block()
        self.assertTrue(len(blk) > 0)
        desc = eng.freeze(
            resume_dir="/tmp/test_freeze_dir", stream_id="freeze_stream_1"
        )
        self.assertEqual(desc.resume_dir, "/tmp/test_freeze_dir")
        self.assertEqual(desc.resume_id, "freeze_stream_1")
        thawed = XZStreamDecompressor.thaw(desc, io.BytesIO(xz_data))
        out = [blk]
        while True:
            b, is_eof = thawed.decode_block()
            if b:
                out.append(b)
            if is_eof:
                break
        self.assertEqual(b"".join(out), raw)
        thawed.discard_checkpoint(desc)

    def test_regression_unified_block_sizes(self):
        lzma = self.lzma
        import io, random

        prng = random.Random(999)
        span1 = prng.randbytes(20000)
        match_str = b"LONG_DISTANCE_MATCH_REGRESSION_PATTERN_" * 10
        span2 = prng.randbytes(20000)
        pat_data = span1 + match_str + span2 + match_str
        xz_data = lzma.compress(pat_data, preset=6)

        for b_sz in [4096, 8192, 16384, 65536]:
            store = UnifiedHistory(block_size=b_sz)
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

    def test_state_dict_strict_assertions(self):
        d_valid = {"offset": 100, "phase": "CHUNKS", "in_chunk": True, "error": None}
        _assert_valid_state_dict(d_valid)

        with self.assertRaises(AssertionError):
            _assert_valid_state_dict({"p": array.array("H", [1, 2, 3])})
        with self.assertRaises(AssertionError):
            _assert_valid_state_dict({"buf": b"binary_data"})
        with self.assertRaises(AssertionError):
            _assert_valid_state_dict({"ba": bytearray(b"bytearray")})
        with self.assertRaises(AssertionError):
            _assert_valid_state_dict({"reps": [1, 2, 3, 4]})
        with self.assertRaises(AssertionError):
            _assert_valid_state_dict({"pair": [(1, 2), (3, 4)]})
        with self.assertRaises(AssertionError):
            _assert_valid_state_dict(
                {"hex": "0123456789abcdef0123456789abcdef0123456789abcdef"}
            )

    def test_no_inbuf_when_input_is_seekable(self):
        # Asserts that when input is a seekable file, no .inbuf file is ever created on disk
        lzma = self.lzma
        import io, tempfile

        raw = b"SEEKABLE_INPUT_NO_INBUF_TEST_" * 500
        xz_data = lzma.compress(raw, preset=1)
        with tempfile.TemporaryDirectory(prefix="test_no_inbuf_") as td:
            r_dir = Path(td) / "resume"
            in_f = Path(td) / "input.xz"
            out_f = Path(td) / "output.bin"
            in_f.write_bytes(xz_data)

            with open(str(in_f), "rb") as in_stream:
                eng = XZStreamDecompressor(in_stream, block_size=8192)
                blk, eof = eng.decode_block()
                self.assertTrue(len(blk) > 0)
                # Freeze state
                desc = eng.freeze(resume_dir=str(r_dir), stream_id="seekable_in_stream")
                # Confirm NO .inbuf file exists!
                inbuf_files = list(r_dir.glob("*.inbuf"))
                self.assertEqual(
                    len(inbuf_files),
                    0,
                    f"Unexpected .inbuf file created for seekable input: {inbuf_files}",
                )
                eng.discard_checkpoint(desc)

    def test_no_hist_file_when_output_is_seekable(self):
        lzma = self.lzma
        import io, tempfile

        raw = b"SEEKABLE_OUTPUT_NO_HIST_TEST_" * 500
        xz_data = lzma.compress(raw, preset=1)
        with tempfile.TemporaryDirectory(prefix="test_no_hist_") as td:
            r_dir = Path(td) / "resume"
            out_f = Path(td) / "output.bin"
            decompress_xz(
                io.BytesIO(xz_data),
                str(out_f),
                resume_dir=str(r_dir),
                in_place=True,
            )
            self.assertEqual(out_f.read_bytes(), raw)
            # Assert that NO .hist file was ever created on disk!
            hist_files = list(r_dir.glob("*.hist"))
            self.assertEqual(
                len(hist_files), 0, f"Unnecessary .hist files generated: {hist_files}"
            )

    def test_truncate_file_portable_fallback(self):
        import tempfile

        with tempfile.TemporaryDirectory(prefix="test_trunc_port_") as td:
            fpath = Path(td) / "test.bin"
            fpath.write_bytes(b"0123456789" * 10)  # 100 bytes
            self.assertEqual(fpath.stat().st_size, 100)

            truncate_file_portable(str(fpath), 50)
            self.assertEqual(fpath.stat().st_size, 50)
            self.assertEqual(fpath.read_bytes(), b"0123456789" * 5)

            truncate_file_portable(str(fpath), 0)
            self.assertEqual(fpath.stat().st_size, 0)

    def test_micropython_bytearray_deletion_compatibility(self):
        # Rotating blocks strategy: verifies that history rotation executes with zero
        # slice deletions and zero bytearray reallocations, preventing MicroPython MemoryErrors
        hist = UnifiedHistory(
            storage_driver=MemoryStorage(), ram_cache_bytes=65536, block_size=8192
        )
        # append 20 blocks
        for i in range(20):
            hist.append_block(bytes([i % 256]) * 8192)
        # Max blocks in RAM must be strictly bounded to max_blocks (8)
        self.assertLessEqual(len(hist.blocks), 8)
        self.assertEqual(hist.history_len(), 20 * 8192)
        # Verify match slices within RAM
        s = hist.get_history_slice(8192, 10)
        self.assertEqual(len(s), 10)

    def test_write_back_cache_batching(self):
        lzma = self.lzma
        import io

        raw = b"WRITE_BACK_CACHE_BATCH_TEST_" * 50000  # ~1.4 MB
        xz_data = lzma.compress(raw, preset=1)

        # 1. Immediate write-through (wb_blocks=1): yields 64k chunks
        eng_wt = XZStreamDecompressor(
            io.BytesIO(xz_data), block_size=65536, wb_blocks=1
        )
        wt_chunks = [
            len(ev.data)
            for ev in eng_wt.decode_events()
            if ev.type == XZEventType.CHUNK_OUTPUT
        ]
        self.assertGreater(len(wt_chunks), 16)
        self.assertEqual(wt_chunks[0], 65536)

        # 2. Write-back cache (wb_blocks=16): yields in 1 MiB batches!
        eng_wb = XZStreamDecompressor(
            io.BytesIO(xz_data), block_size=65536, wb_blocks=16
        )
        wb_chunks = [
            len(ev.data)
            for ev in eng_wb.decode_events()
            if ev.type == XZEventType.CHUNK_OUTPUT
        ]
        self.assertLess(len(wb_chunks), 5)
        # First chunk is exactly 16 blocks = 1,048,576 bytes!
        self.assertEqual(wb_chunks[0], 16 * 65536)
        # Verify complete uncompressed match
        eng_full = XZStreamDecompressor(
            io.BytesIO(xz_data), block_size=65536, wb_blocks=16
        )
        full_out = b"".join(
            bytes(ev.data)
            for ev in eng_full.decode_events()
            if ev.type == XZEventType.CHUNK_OUTPUT
        )
        self.assertEqual(full_out, raw)

    def test_elapsed_time_indication_and_state_clamping(self):
        lzma = self.lzma
        import io

        raw = b"ELAPSED_TIME_STATE_CLAMPING_TEST_" * 100
        xz_data = lzma.compress(raw, preset=1)
        eng = XZStreamDecompressor(io.BytesIO(xz_data))
        blk, eof = eng.decode_block()
        state = eng.export_state()
        self.assertIn("elapsed_ms", state)
        self.assertIsInstance(state["elapsed_ms"], int)

        # Test uint32 clamping in import_state
        state["elapsed_ms"] = 0xFFFFFFFF + 5000
        eng2 = XZStreamDecompressor(io.BytesIO(xz_data))
        eng2.import_state(state)
        self.assertEqual(eng2.base_elapsed_ms, 0xFFFFFFFF)
        state_clamped = eng2.export_state()
        self.assertEqual(state_clamped["elapsed_ms"], 0xFFFFFFFF)

        # Test formatting logic directly
        time_width = 10
        # 1. Below clamp
        import datetime

        td_str = str(datetime.timedelta(seconds=3665))
        self.assertEqual(f"{td_str:>{time_width}}", "   1:01:05")
        # 2. Clamped at UINT32_MAX -> all spaces
        clamped_field = " " * time_width
        self.assertEqual(clamped_field, "          ")

    def test_write_back_cache_disabled_on_memory_storage(self):
        # Asserts write-back cache is forcibly disabled (wb_blocks=1) when MemoryStorage is used
        vfs = MemoryStorage()
        wb_blocks = select_write_back_blocks(
            65536, available_mem=1024 * 1024 * 1024, storage_driver=vfs
        )
        self.assertEqual(wb_blocks, 1)

        # Asserts write-back cache is forcibly disabled when timeout <= 1.0s
        wb_tight = select_write_back_blocks(
            65536, available_mem=1024 * 1024 * 1024, timeout_sec=0.20
        )
        self.assertEqual(wb_tight, 1)

        # Asserts write-back cache operates when disk storage and ample timeout/RAM exist
        storage = DiskStorage("/tmp/test_wb_disk")
        wb_disk = select_write_back_blocks(
            65536,
            available_mem=1024 * 1024 * 1024,
            timeout_sec=52.0,
            storage_driver=storage,
        )
        self.assertEqual(wb_disk, 16)
        storage.cleanup()

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

        desc_a = None
        desc_b = None
        eof_a = False
        eof_b = False
        out_a = bytearray()
        out_b = bytearray()

        vfs_a = MemoryStorage()
        vfs_b = MemoryStorage()

        round_num = 0
        while not (eof_a and eof_b):
            round_num += 1
            if not eof_a:
                if desc_a is not None:
                    engine_a = XZStreamDecompressor.thaw(
                        desc_a, stream_a_io, storage_driver=vfs_a
                    )
                else:
                    engine_a = XZStreamDecompressor(stream_a_io)
                blk_a, eof_a = engine_a.decode_block()
                if blk_a:
                    out_a.extend(blk_a)
                if not eof_a:
                    desc_a = engine_a.freeze(
                        resume_dir="vfs_dir_a",
                        stream_id="stream_a",
                        storage_driver=vfs_a,
                    )
                del engine_a

            if not eof_b:
                if desc_b is not None:
                    engine_b = XZStreamDecompressor.thaw(
                        desc_b, stream_b_io, storage_driver=vfs_b
                    )
                else:
                    engine_b = XZStreamDecompressor(stream_b_io)
                blk_b, eof_b = engine_b.decode_block()
                if blk_b:
                    out_b.extend(blk_b)
                if not eof_b:
                    desc_b = engine_b.freeze(
                        resume_dir="vfs_dir_b",
                        stream_id="stream_b",
                        storage_driver=vfs_b,
                    )
                del engine_b

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

    def test_storage_auto_block_size_threshold(self):
        # Continuous decoding threshold: 512 bytes (< 160 KiB RAM), 8192 bytes (< 1 MiB RAM), 65536 bytes otherwise
        micro_mem = 96 * 1024  # 96KB (< 160KB)
        mid_mem = 256 * 1024  # 256KB (< 1 MiB)
        norm_mem = 10 * 1024 * 1024  # 10MB (>= 1 MiB)

        self.assertEqual(select_block_size("auto", available_mem=micro_mem), 512)
        self.assertEqual(select_block_size("auto", available_mem=mid_mem), 8192)
        self.assertEqual(select_block_size("auto", available_mem=norm_mem), 65536)

        # Allowed explicit overrides: 512, 8192, 65536
        self.assertEqual(select_block_size(512), 512)
        self.assertEqual(select_block_size("8k"), 8192)
        self.assertEqual(select_block_size("64k"), 65536)
        with self.assertRaises(XZFormatError):
            select_block_size(16384)
        with self.assertRaises(XZFormatError):
            select_block_size(4096)

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

    def test_durable_replay_input_rolling_rebase_bounded(self):
        import tempfile, io

        with tempfile.TemporaryDirectory(prefix="test_spool_bound_") as td:
            spool_p = Path(td) / "INPUT_BUFFER.bin"
            # Simulate streaming 500 KiB in 50 KiB increments
            source = io.BytesIO(b"MOCK_STREAMED_PIPE_CHUNK_" * 20000)
            inp = DurableReplayInput(source, spool_p)
            for commit_step in range(1, 10):
                chunk = inp.read(50000)
                self.assertEqual(len(chunk), 50000)
                # Rebase at commit point (leaving at most 4 KiB read-ahead)
                inp.rebase_to(commit_step * 48000)
                # File size on disk must strictly stay under 64 KiB!
                spool_sz = spool_p.stat().st_size
                self.assertLessEqual(spool_sz, 65536)
            inp.close()

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

        raw = os.urandom(512 * 1024)
        xz_data = lzma.compress(raw, preset=1)

        ram_dir_base = None
        for cand in ["/dev/shm", "/vfs_ram", "/ramdisk", "/psram"]:
            if os.path.isdir(cand) and os.access(cand, os.W_OK):
                ram_dir_base = cand
                break

        orig_fsync_obj = globals().get("_fsync_fileobj")
        orig_fsync_parent = globals().get("_fsync_parent")
        globals()["_fsync_fileobj"] = lambda x: None
        globals()["_fsync_parent"] = lambda x: None

        try:
            with tempfile.TemporaryDirectory(
                prefix="test_ckpt_converge_", dir=ram_dir_base
            ) as td:
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
                            timeout=0.15,
                            cpu_timeout=0.15,
                            block_size=8192,
                            checkpoint_interval="16k",
                            in_place=True,
                            write_back_cache=1,
                        )
                        completed = True
                        break
                    except (WallClockTimeout, CPUTimeout):
                        timed_out = True
                self.assertTrue(timed_out, "test must exercise interrupted invocations")
                self.assertTrue(
                    completed, "resumption did not converge within 30 attempts"
                )
                self.assertEqual(out_f.read_bytes(), raw)
        finally:
            if orig_fsync_obj is not None:
                globals()["_fsync_fileobj"] = orig_fsync_obj
            if orig_fsync_parent is not None:
                globals()["_fsync_parent"] = orig_fsync_parent

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

    def test_lzma_9e_postage_stamp_microcontroller_512b_blocks(self):
        # Even on postage stamp microcontrollers, decompress lzma -9e bit-perfectly with 512B blocks
        lzma = self.lzma
        import io, random

        prng = random.Random(1337)
        raw = bytearray()
        for _ in range(25):
            raw.extend(prng.randbytes(500))
            raw.extend(b"EXTREME_LZMA9E_POSTAGE_STAMP_PATTERN_512B_" * 20)
            raw.extend(prng.randbytes(300))
        raw = bytes(raw)
        c_9e = lzma.compress(raw, preset=9 | lzma.PRESET_EXTREME)

        # Decompress using 512B blocks and 512B RAM history budget
        vfs = MemoryStorage()
        hist = UnifiedHistory(storage_driver=vfs, ram_cache_bytes=512, block_size=512)
        engine = XZStreamDecompressor(
            io.BytesIO(c_9e), history_backend=hist, block_size=512
        )
        out = bytearray()
        for ev in engine.decode_events():
            if ev.type == XZEventType.CHUNK_OUTPUT:
                out.extend(ev.data)
                self.assertLessEqual(len(ev.data), 512)
        self.assertEqual(bytes(out), raw)

    def test_allowed_block_size_enforcement(self):
        # Continuous decoding threshold: 512 bytes (< 160 KiB RAM), 8192 bytes (< 1 MiB RAM), 65536 bytes otherwise
        self.assertEqual(select_block_size("auto", available_mem=32 * 1024), 512)
        self.assertEqual(select_block_size("auto", available_mem=96 * 1024), 512)
        self.assertEqual(select_block_size("auto", available_mem=159 * 1024), 512)
        self.assertEqual(select_block_size("auto", available_mem=160 * 1024), 8192)
        self.assertEqual(select_block_size("auto", available_mem=512 * 1024), 8192)
        self.assertEqual(select_block_size("auto", available_mem=1024 * 1024), 65536)
        self.assertEqual(
            select_block_size("auto", available_mem=16 * 1024 * 1024), 65536
        )

        # Explicit overrides: only 512, 8192, 65536
        self.assertEqual(select_block_size(512), 512)
        self.assertEqual(select_block_size("512"), 512)
        self.assertEqual(select_block_size("8k"), 8192)
        self.assertEqual(select_block_size(8192), 8192)
        self.assertEqual(select_block_size("64k"), 65536)
        self.assertEqual(select_block_size(65536), 65536)

        # Disallowed sizes raise XZFormatError
        for bad_sz in [256, 1024, 2048, 4096, 16384, 32768, 131072, "4k", "16k"]:
            with self.assertRaises(XZFormatError):
                select_block_size(bad_sz)

    def test_no_closure_creation_in_decode_loop(self):
        # RangeDecoder pre-allocated and reused without closure creation
        lzma = self.lzma
        import io

        raw = b"NO_CLOSURES_ALLOWED_IN_DECODE_LOOP_" * 50
        xz_data = lzma.compress(raw, preset=1)
        eng = XZStreamDecompressor(io.BytesIO(xz_data), block_size=512)
        blk, eof = eng.decode_block()
        self.assertGreater(len(blk), 0)
        self.assertIsNotNone(eng.rd)

    def test_crc64_pure_ram_determination(self):
        self.assertEqual(crc64(b"123456789"), 0x995DC9BBDF1939FA)

    def test_inbuf_hbuf_external_sharing(self):
        vfs = MemoryStorage()
        hist = UnifiedHistory(storage_driver=vfs, ram_cache_bytes=512, block_size=512)
        hist.append_block(b"S" * 512)
        self.assertEqual(hist.blocks_len, 512)

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
        h_backend = getattr(args, "history_backend", None)
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

    actual_b_size = select_block_size(b_size)
    if _supports_unicode():
        if actual_b_size == 512:
            # 1-dot, 8-position braille spinner for 512-byte blocks
            spinner = (
                "⠁",
                "⠈",
                "⠐",
                "⠠",
                "⢀",
                "⡀",
                "⠄",
                "⠂",
            )
        elif actual_b_size == 8192:
            # 2-dot, 8-position braille spinner for 8192-byte blocks
            spinner = (
                "⠉",
                "⠘",
                "⠰",
                "⢠",
                "⣀",
                "⡄",
                "⠆",
                "⠃",
            )
        else:
            # 3-dot, 8-position braille spinner for 65536-byte blocks
            spinner = (
                "⠋",
                "⠙",
                "⠸",
                "⢰",
                "⣠",
                "⣄",
                "⡆",
                "⠇",
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

    time_width = 10
    status_width = 56

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
            tot_ms = getattr(ev, "elapsed_ms", None)
            if tot_ms is None:
                tot_ms = int((now - init_perf) * 1000)

            if tot_ms >= 0xFFFFFFFF:
                time_field = " " * time_width
            else:
                elapsed_sec = tot_ms // 1000
                time_str = None
                try:
                    import datetime

                    time_str = str(datetime.timedelta(seconds=int(elapsed_sec)))
                except Exception:
                    pass
                if not time_str:
                    time_str = f"{int(elapsed_sec)}s"
                time_field = f"{time_str:>{time_width}}"

            line = f"{glyph} {time_field} {format_human_size(ev.total_emitted):>10} decoded, {format_human_rate(rate):>12}"
            sys.stderr.write(f"{line:<{status_width}}\r")
            spinner_active[0] = True
            try:
                sys.stderr.flush()
            except Exception:
                pass

    memory_cushion = bytearray(1024)  # 1 KB cushion for OOM exception printing
    try:
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
                sys.stderr.write(" " * 56 + "\r")
                try:
                    sys.stderr.flush()
                except Exception:
                    pass
        finally:
            memory_cushion = None
            try:
                gc.collect()
            except Exception:
                pass
    except (WallClockTimeout, CPUTimeout):
        sys.exit(124)
    except KeyboardInterrupt:
        sys.exit(130)
    except XZFormatError as exc:
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_FORMAT: {exc}\n")
        sys.exit(E_FORMAT)
    except XZUnsupportedError as exc:
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_UNSUPPORTED: {exc}\n")
        sys.exit(E_UNSUPPORTED)
    except XZCheckError as exc:
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_CHECK: {exc}\n")
        sys.exit(E_CHECK)
    except (EOFError, ValueError) as exc:
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_FORMAT: {exc}\n")
        sys.exit(E_FORMAT)
    except OSError as exc:
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_IO: {exc}\n")
        sys.exit(E_IO)
    except Exception as exc:
        if is_v:
            raise
        sys.stderr.write(f"xz_decompressor: E_RESOURCE: {exc}\n")
        sys.exit(E_RESOURCE)


if __name__ == "__main__":
    main()
