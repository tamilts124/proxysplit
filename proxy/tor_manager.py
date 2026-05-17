"""
proxy/tor_manager.py
TorInstance + TorInstanceManager: launch, monitor, and rotate Tor circuits.
"""

import asyncio
import os
import shutil
import subprocess
import threading
import time
from typing import Optional

from proxy.logging_setup import log

# Set by main after session pool is ready
_SESSION_POOL_REF = None  # import proxy.session_pool and set after init

TOR_MANAGER: Optional["TorInstanceManager"] = None


def _hash_tor_password(password: str) -> str:
    tor_bin = shutil.which("tor")
    if not tor_bin:
        return ""
    try:
        out = subprocess.check_output(
            [tor_bin, "--hash-password", password],
            stderr=subprocess.DEVNULL, text=True).strip()
        for line in out.splitlines():
            if line.startswith("16:"):
                return line
    except Exception:
        pass
    return ""


class TorInstance:
    def __init__(self, index, socks_port, control_port, data_dir,
                 password="", launch_timeout=90, newnym_cooldown=10):
        self.index           = index
        self.socks_port      = socks_port
        self.control_port    = control_port
        self.data_dir        = data_dir
        self.password        = password
        self.launch_timeout  = launch_timeout
        self.newnym_cooldown = newnym_cooldown
        self.proxy_url       = f"socks5://127.0.0.1:{socks_port}"
        self._proc           = None
        self._stdout_thread  = None
        self._bootstrap_ev   = threading.Event()
        self._bootstrap_err  = None
        self._last_newnym    = 0.0

    def start(self):
        os.makedirs(self.data_dir, exist_ok=True)
        torrc = os.path.join(self.data_dir, "torrc")
        lines = [
            f"SocksPort {self.socks_port}",
            f"ControlPort {self.control_port}",
            f"DataDirectory {self.data_dir}",
            "Log notice stdout",
            "CookieAuthentication 0",
        ]
        if self.password:
            lines.append(f"HashedControlPassword {_hash_tor_password(self.password)}")
        with open(torrc, "w") as f:
            f.write("\n".join(lines) + "\n")
        tor_bin = shutil.which("tor")
        if not tor_bin:
            raise RuntimeError("Tor binary not found in PATH.")
        log.info(f"   [Tor #{self.index}] Starting SOCKS={self.socks_port} CTRL={self.control_port}")
        self._proc = subprocess.Popen(
            [tor_bin, "-f", torrc], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True,
        )
        self._bootstrap_ev.clear()
        self._bootstrap_err = None
        self._stdout_thread = threading.Thread(target=self._read_stdout, daemon=True)
        self._stdout_thread.start()
        if not self._bootstrap_ev.wait(timeout=self.launch_timeout):
            self.stop()
            raise RuntimeError(f"Tor #{self.index} did not bootstrap in {self.launch_timeout}s")
        if self._bootstrap_err:
            self.stop()
            raise RuntimeError(f"Tor #{self.index} failed: {self._bootstrap_err}")

    def _read_stdout(self):
        try:
            for line in self._proc.stdout:
                line = line.rstrip()
                if log.isEnabledFor(10):  # DEBUG
                    log.debug(f"   [Tor #{self.index}] {line}")
                if "Bootstrapped 100%" in line:
                    log.info(f"   [Tor #{self.index}] ✓ Bootstrap complete")
                    self._bootstrap_ev.set()
                elif "[err]" in line.lower() or "problem bootstrapping" in line.lower():
                    self._bootstrap_err = line
                    self._bootstrap_ev.set()
        except Exception as exc:
            self._bootstrap_err = str(exc)
            self._bootstrap_ev.set()

    def stop(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        self._proc = None

    async def refresh_circuit(self) -> bool:
        elapsed = time.monotonic() - self._last_newnym
        if elapsed < self.newnym_cooldown:
            await asyncio.sleep(self.newnym_cooldown - elapsed)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", self.control_port), timeout=5)
            cmd = f'AUTHENTICATE "{self.password}"\r\n' if self.password else "AUTHENTICATE\r\n"
            writer.write(cmd.encode()); await writer.drain()
            resp = await asyncio.wait_for(reader.readline(), timeout=5)
            if not resp.startswith(b"250"):
                writer.close(); return False
            writer.write(b"SIGNAL NEWNYM\r\n"); await writer.drain()
            resp = await asyncio.wait_for(reader.readline(), timeout=5)
            try:
                writer.close(); await writer.wait_closed()
            except Exception:
                pass
            if resp.startswith(b"250"):
                self._last_newnym = time.monotonic()
                log.info(f"   [Tor #{self.index}] ✓ Circuit refreshed")
                return True
            return False
        except Exception as exc:
            log.warning(f"   [Tor #{self.index}] refresh error: {exc}")
            return False

    @property
    def alive(self):
        return self._proc is not None and self._proc.poll() is None

    @property
    def stats(self):
        return {
            "index": self.index, "socks_port": self.socks_port,
            "control_port": self.control_port, "alive": self.alive,
            "last_newnym_ago_s": round(time.monotonic() - self._last_newnym, 1)
            if self._last_newnym else None,
        }


class TorInstanceManager:
    def __init__(self, config: dict, num: int):
        self.num_instances  = num
        self.base_socks     = config.get("base_tor_port", 9050)
        self.base_ctrl      = config.get("base_tor_control_port", 9150)
        self.base_data      = config.get("tor_data_dir", "tor_data")
        self.password       = config.get("tor_password", "")
        self.launch_timeout = config.get("tor_launch_timeout", 90)
        self.refresh_iv     = config.get("tor_refresh_interval", 0)
        self.newnym_cd      = config.get("tor_newnym_cooldown", 10)
        self.instances: list[TorInstance] = []
        self._refresh_task  = None

    def start_all(self):
        log.info(f"Starting {self.num_instances} Tor instance(s)…")
        for i in range(self.num_instances):
            inst = TorInstance(
                index=i,
                socks_port=self.base_socks + i,
                control_port=self.base_ctrl + i,
                data_dir=os.path.join(self.base_data, f"instance_{i}"),
                password=self.password,
                launch_timeout=self.launch_timeout,
                newnym_cooldown=self.newnym_cd,
            )
            inst.start(); self.instances.append(inst)
        log.info(f"✓ All {self.num_instances} Tor instance(s) running")

    def stop_all(self):
        for i in self.instances:
            i.stop()
        self.instances.clear()

    @property
    def proxy_urls(self):
        return [i.proxy_url for i in self.instances if i.alive]

    async def refresh_all_circuits(self) -> dict:
        from proxy import session_pool as sp
        results = await asyncio.gather(*[i.refresh_circuit() for i in self.instances],
                                       return_exceptions=True)
        outcome = {i.index: bool(ok) if not isinstance(ok, Exception) else False
                   for i, ok in zip(self.instances, results)}
        if sp.SESSION_POOL:
            for inst in self.instances:
                await sp.SESSION_POOL.invalidate(inst.proxy_url)
        return outcome

    async def start_auto_refresh(self):
        if self.refresh_iv <= 0:
            return
        log.info(f"   Tor auto-refresh every {self.refresh_iv}s")

        async def _loop():
            while True:
                await asyncio.sleep(self.refresh_iv)
                res = await self.refresh_all_circuits()
                ok = sum(1 for v in res.values() if v)
                log.info(f"   Refreshed {ok}/{len(res)} Tor circuits")

        self._refresh_task = asyncio.ensure_future(_loop())

    def cancel_auto_refresh(self):
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()

    @property
    def status(self):
        return {
            "instances": [i.stats for i in self.instances],
            "alive_count": sum(1 for i in self.instances if i.alive),
            "total_count": self.num_instances,
            "auto_refresh_interval_s": self.refresh_iv,
            "newnym_cooldown_s": self.newnym_cd,
        }
