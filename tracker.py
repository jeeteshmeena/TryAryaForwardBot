"""
Centralized Real-Time Stats Tracker
====================================
Thread-safe, singleton tracker for all bot operations.
Tracks: download/upload speed, total bytes, files forwarded, active transfers.
All modules (batchjob, jobs, normal forwarding) must use this.
"""
import time
import threading


class _Tracker:
    """Global singleton — import and use directly."""

    def __init__(self):
        self._lock = threading.Lock()

        # ── Totals (session) ──────────────────────────────────────────────
        self.total_download_bytes: int = 0
        self.total_upload_bytes: int = 0
        self.total_files_forwarded: int = 0

        # ── Speed tracking ────────────────────────────────────────────────
        # Rolling window: list of (timestamp, bytes) for last N seconds
        self._dl_window: list[tuple[float, int]] = []
        self._ul_window: list[tuple[float, int]] = []
        self._WINDOW_SEC = 10  # 10-second rolling window for accuracy

        # ── Active transfer count ─────────────────────────────────────────
        self.active_downloads: int = 0
        self.active_uploads: int = 0

        # ── Delta tracking for progress callbacks ─────────────────────────
        self._dl_last_bytes: int = 0
        self._ul_last_bytes: int = 0

    # ── Speed helpers ─────────────────────────────────────────────────────

    def _prune(self, window: list, now: float) -> list:
        cutoff = now - self._WINDOW_SEC
        return [e for e in window if e[0] >= cutoff]

    def _calc_speed(self, window: list) -> float:
        """Returns speed in MB/s from rolling window."""
        if len(window) < 2:
            return 0.0
        now = time.time()
        window = self._prune(window, now)
        if len(window) < 2:
            return 0.0
        dt = window[-1][0] - window[0][0]
        if dt <= 0:
            return 0.0
        total = sum(b for _, b in window)
        return (total / dt) / (1024 * 1024)  # MB/s

    # ── Public API ────────────────────────────────────────────────────────

    def record_download_progress(self, current: int, total: int):
        """Called from Pyrogram progress callback during download."""
        now = time.time()
        with self._lock:
            # Compute delta from last callback
            last = self._dl_last_bytes
            delta = current - last if current > last else current
            self._dl_last_bytes = current
            if delta > 0:
                self._dl_window.append((now, delta))
                self._dl_window = self._prune(self._dl_window, now)[-50:]

    def record_upload_progress(self, current: int, total: int):
        """Called from Pyrogram progress callback during upload."""
        now = time.time()
        with self._lock:
            last = self._ul_last_bytes
            delta = current - last if current > last else current
            self._ul_last_bytes = current
            if delta > 0:
                self._ul_window.append((now, delta))
                self._ul_window = self._prune(self._ul_window, now)[-50:]

    def add_download_bytes(self, nbytes: int):
        """Call once after a complete download."""
        with self._lock:
            self.total_download_bytes += nbytes

    def add_upload_bytes(self, nbytes: int):
        """Call once after a complete upload."""
        with self._lock:
            self.total_upload_bytes += nbytes

    def inc_files_forwarded(self, n: int = 1):
        """Call after each successfully forwarded/copied file."""
        with self._lock:
            self.total_files_forwarded += n

    def start_download(self):
        with self._lock:
            self.active_downloads += 1

    def end_download(self):
        with self._lock:
            self.active_downloads = max(0, self.active_downloads - 1)
            self._dl_last_bytes = 0
            self._dl_window.clear()

    def start_upload(self):
        with self._lock:
            self.active_uploads += 1

    def end_upload(self):
        with self._lock:
            self.active_uploads = max(0, self.active_uploads - 1)
            self._ul_last_bytes = 0
            self._ul_window.clear()

    # ── Read API ──────────────────────────────────────────────────────────

    @property
    def download_speed_mbps(self) -> float:
        with self._lock:
            return self._calc_speed(self._dl_window)

    @property
    def upload_speed_mbps(self) -> float:
        with self._lock:
            return self._calc_speed(self._ul_window)

    def snapshot(self) -> dict:
        """Returns a dict with all current values."""
        with self._lock:
            return {
                "dl_speed": self._calc_speed(self._dl_window),
                "ul_speed": self._calc_speed(self._ul_window),
                "total_dl_bytes": self.total_download_bytes,
                "total_ul_bytes": self.total_upload_bytes,
                "total_files_fwd": self.total_files_forwarded,
                "active_downloads": self.active_downloads,
                "active_uploads": self.active_uploads,
            }


# Singleton instance — all modules import this
stats = _Tracker()
