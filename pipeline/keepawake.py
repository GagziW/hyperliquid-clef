"""Keep the computer awake during a long paper run (Windows, macOS and Linux)."""

import os
import shutil
import subprocess
import sys
from contextlib import contextmanager


@contextmanager
def keep_awake():
    """Prevent idle sleep while the block runs. Closing a laptop lid can still sleep it."""
    if sys.platform == "win32":
        import ctypes

        es_continuous, es_system_required = 0x80000000, 0x00000001
        kernel32 = ctypes.windll.kernel32
        kernel32.SetThreadExecutionState(es_continuous | es_system_required)
        try:
            yield "windows"
        finally:
            kernel32.SetThreadExecutionState(es_continuous)
        return
    if sys.platform == "darwin" and shutil.which("caffeinate"):
        command = ["caffeinate", "-i", "-w", str(os.getpid())]
    elif shutil.which("systemd-inhibit"):
        command = ["systemd-inhibit", "--what=idle:sleep", "--why=paper trading run", "sleep", "infinity"]
    else:
        yield None
        return
    try:
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError:
        yield None
        return
    try:
        yield command[0]
    finally:
        process.terminate()
