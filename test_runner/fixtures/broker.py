import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from fixtures.log_helper import log


@dataclass
class NeonBroker:
    """An object managing storage_broker instance"""

    logfile: Path
    port: int
    neon_binpath: Path
    handle: Optional[subprocess.Popen[Any]] = None  # handle of running daemon

    def listen_addr(self):
        return f"127.0.0.1:{self.port}"

    def client_url(self):
        return f"http://{self.listen_addr()}"

    def check_status(self):
        return True  # TODO

    def try_start(self):
        if self.handle is not None:
            log.debug(f"storage_broker is already running on port {self.port}")
            return

        listen_addr = self.listen_addr()
        log.info(f'starting storage_broker to listen incoming connections at "{listen_addr}"')
        with open(self.logfile, "wb") as logfile:
            args = [
                str(self.neon_binpath / "storage_broker"),
                f"--listen-addr={listen_addr}",
            ]
            self.handle = subprocess.Popen(args, stdout=logfile, stderr=logfile)

        # wait for start
        started_at = time.time()
        while True:
            try:
                self.check_status()
            except Exception as e:
                elapsed = time.time() - started_at
                if elapsed > 5:
                    raise RuntimeError(
                        f"timed out waiting {elapsed:.0f}s for storage_broker start: {e}"
                    ) from e
                time.sleep(0.5)
            else:
                break  # success

    def stop(self, immediate: bool = False):
        if self.handle is not None:
            if immediate:
                self.handle.kill()
            else:
                self.handle.terminate()
            self.handle.wait()
            self.handle = None
