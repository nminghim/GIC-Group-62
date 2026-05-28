#!/usr/bin/env python3
"""
Ground Responsive Inflation Platform (GRIP) - Python Server
Listens for ESP32 connections, provides terrain-specific tyre profiles,
displays live telemetry, and sends control commands.
"""

import asyncio
import os
import json
import time
from datetime import datetime
from typing import Optional

# ── Server config ──────────────────────────────────────────────────────────
HOST = "0.0.0.0"
PORT = 5555

# ── Tyre profiles (GRIP Terrain Calibration Modes) ──────────────────────────
PROFILES: dict[str, dict] = {
    "comfort": {
        "name": "comfort",
        "description": "Normal Tarmac/Highway",
        "target_psi": 34.0,
        "tolerance_psi": 1.0,
        "icon": "CO",
    },
    "gravel_snow": {
        "name": "gravel_snow",
        "description": "Slippery/Uneven Surfaces",
        "target_psi": 30.0,
        "tolerance_psi": 1.0,
        "icon": "GS",
    },
    "mud_ruts": {
        "name": "mud_ruts",
        "description": "Mud/Ruts/Soft Soil",
        "target_psi": 25.0,
        "tolerance_psi": 1.0,
        "icon": "MR",
    },
    "sand": {
        "name": "sand",
        "description": "Soft Sand/Dunes",
        "target_psi": 18.0,
        "tolerance_psi": 0.8,
        "icon": "SD",
    },
    "rock_crawl": {
        "name": "rock_crawl",
        "description": "Extreme Rocky Terrain",
        "target_psi": 15.0,
        "tolerance_psi": 0.7,
        "icon": "RC",
    },
    "wade": {
        "name": "wade",
        "description": "Water Wading/Deep Crossing",
        "target_psi": 36.0,
        "tolerance_psi": 1.0,
        "icon": "WA",
    },
    "eco": {
        "name": "eco",
        "description": "Optimized Efficiency",
        "target_psi": 39.0,
        "tolerance_psi": 1.0,
        "icon": "EC",
    },
    "heavy_load": {
        "name": "heavy_load",
        "description": "Heavy Load/Trailer Towing",
        "target_psi": 42.0,
        "tolerance_psi": 1.0,
        "icon": "HL",
    },
}


class DeviceState:
    def __init__(self):
        self.connected: bool = False
        self.pressure_psi: float = 0.0
        self.target_psi: float = 0.0
        self.tolerance_psi: float = 1.0
        self.profile: str = "none"
        self.state: str = "idle"
        self.pump: bool = False
        self.valve: bool = False
        self.last_seen: float = 0.0
        self.events: list[str] = []
        self.errors: list[str] = []
        self.writer: Optional[asyncio.StreamWriter] = None

    def log_event(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        self.events.append(entry)
        if len(self.events) > 50:
            self.events.pop(0)

    def log_error(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = f"[{ts}] ERROR: {msg}"
        self.errors.append(entry)
        self.log_event(entry)


device = DeviceState()

# ── Web server state and broadcasting ───────────────────────────────────────
http_clients: list[asyncio.Queue] = []


def get_device_state_dict() -> dict:
    return {
        "connected": device.connected,
        "pressure_psi": device.pressure_psi,
        "target_psi": device.target_psi,
        "tolerance_psi": device.tolerance_psi,
        "profile": device.profile,
        "state": device.state,
        "pump": device.pump,
        "valve": device.valve,
        "events": list(device.events[-15:]),
        "errors": list(device.errors[-15:]),
    }


def broadcast_state():
    state_data = get_device_state_dict()
    for q in list(http_clients):
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            q.put_nowait(state_data)
        except Exception:
            pass


# ── Protocol helpers ────────────────────────────────────────────────────────
def build_cmd(cmd: str, **kwargs) -> str:
    payload = {"cmd": cmd}
    payload.update(kwargs)
    return json.dumps(payload) + "\n"


async def send_command(cmd: str, **kwargs):
    if device.writer is None or device.writer.is_closing():
        print("  [!] No device connected")
        return
    msg = build_cmd(cmd, **kwargs)
    try:
        device.writer.write(msg.encode())
        await device.writer.drain()
        device.log_event(f"sent: {msg.strip()}")
        broadcast_state()
    except Exception as e:
        device.log_error(f"Send failed: {e}")
        broadcast_state()


async def send_profile(profile_name: str):
    if profile_name not in PROFILES:
        print(f"  [!] Unknown profile: {profile_name}")
        return
    p = PROFILES[profile_name]
    await send_command(
        "set_profile",
        name=p["name"],
        target_psi=p["target_psi"],
        tolerance_psi=p["tolerance_psi"],
    )


# ── TCP Client connection handling ──────────────────────────────────────────
async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    device.connected = True
    device.writer = writer
    device.last_seen = time.time()
    device.log_event(f"ESP32 connected from {peer}")
    broadcast_state()

    try:
        while True:
            line_bytes = await reader.readline()
            if not line_bytes:
                break
            line = line_bytes.decode("utf-8", errors="ignore").strip()
            if not line:
                continue

            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                device.log_error("Invalid JSON from device")
                continue

            msg_type = msg.get("type")
            if msg_type == "telemetry":
                device.pressure_psi = float(msg.get("pressure_psi", 0.0))
                device.target_psi = float(msg.get("target_psi", 0.0))
                device.tolerance_psi = float(msg.get("tolerance_psi", 1.0))
                device.profile = msg.get("profile", "none")
                device.state = msg.get("state", "idle")
                device.pump = bool(msg.get("pump", False))
                device.valve = bool(msg.get("valve", False))
                device.last_seen = time.time()
                broadcast_state()
            elif msg_type == "event":
                evt = msg.get("event")
                psi = msg.get("pressure_psi", 0.0)
                device.log_event(f"[EVENT] {evt} (pressure: {psi:.1f} PSI)")
                broadcast_state()
            elif msg_type == "error":
                err = msg.get("message", "unknown error")
                device.log_error(f"Device error: {err}")
                broadcast_state()

    except asyncio.CancelledError:
        pass
    except Exception as e:
        device.log_error(f"Connection error: {e}")
    finally:
        device.connected = False
        device.writer = None
        device.pump = False
        device.valve = False
        device.log_event("ESP32 disconnected")
        broadcast_state()
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ── CLI Loop ─────────────────────────────────────────────────────────────────
HELP_TEXT = """
Commands:
  1-9          Select a tyre profile by number (must type 'start' to begin)
  start        Begin inflation/deflation to target
  stop         Stop all activity immediately
  status       Request current status from device
  deflate N    Set custom target to N PSI (must type 'start' to begin)
  custom N     Set custom target to N PSI (must type 'start' to begin)
  log          Show full event log
  profiles     List all profiles
  help         Show this help
  quit / exit  Shut down server
"""


async def cli_loop():
    profile_keys = list(PROFILES.keys())

    print("=== Ground Responsive Inflation Platform (GRIP) Server ===")
    print(f"TCP socket listening on {HOST}:{PORT}")
    print(f"HTTP server listening on {HTTP_HOST}:{HTTP_PORT}")
    print("Waiting for ESP32 connection...")
    print("\nProfiles available:")
    for i, (k, p) in enumerate(PROFILES.items(), 1):
        print(
            f"  {i}. {p['name']:12s} -> {p['target_psi']:.1f} PSI  ({p['description']})"
        )

    print(HELP_TEXT)

    loop = asyncio.get_event_loop()
    while True:
        try:
            raw = await loop.run_in_executor(None, lambda: input("pump> "))
        except (EOFError, KeyboardInterrupt):
            print("\nShutting down...")
            break
        except Exception as e:
            print(
                f"\n[!] CLI standard input unavailable ({e}). Running in background-only mode."
            )
            await asyncio.Event().wait()
            break

        parts = raw.strip().lower().split()
        if not parts:
            continue
        cmd = parts[0]

        if cmd in ("quit", "exit", "q"):
            break
        elif cmd == "help":
            print(HELP_TEXT)
        elif cmd == "profiles":
            for i, (k, p) in enumerate(PROFILES.items(), 1):
                print(
                    f"  {i}. {k:12s} -> {p['target_psi']:.1f} PSI  ({p['description']})"
                )
        elif cmd == "log":
            for e in device.events:
                print(f"  {e}")
        elif cmd == "status":
            await send_command("status")
        elif cmd == "start":
            await send_command("start")
        elif cmd == "stop":
            await send_command("stop")
        elif cmd == "deflate" and len(parts) == 2:
            try:
                psi = float(parts[1])
                await send_command(
                    "set_profile",
                    name="custom",
                    target_psi=psi,
                    tolerance_psi=1.0,
                )
                print(
                    f"  -> Target set to {psi:.1f} PSI (type 'start' to begin operation)"
                )
            except ValueError:
                print("  Usage: deflate <PSI>")
        elif cmd == "custom" and len(parts) == 2:
            try:
                psi = float(parts[1])
                await send_command(
                    "set_profile",
                    name="custom",
                    target_psi=psi,
                    tolerance_psi=1.0,
                )
                print(
                    f"  -> Custom target set to {psi:.1f} PSI (type 'start' to begin operation)"
                )
            except ValueError:
                print("  Usage: custom <PSI>")
        elif cmd.isdigit():
            idx = int(cmd) - 1
            if 0 <= idx < len(profile_keys):
                key = profile_keys[idx]
                print(f"  -> Selected profile: {key} (type 'start' to begin operation)")
                await send_profile(key)
            else:
                print(f"  Profile number 1-{len(profile_keys)} expected")
        else:
            print(f"  Unknown command: {cmd}  (type 'help')")

        # Live status updates printed by display_task
        pass

    if device.writer:
        await send_command("stop")


async def display_task():
    while True:
        await asyncio.sleep(2.0)
        if device.connected:
            age = time.time() - device.last_seen
            if age < 5.0:
                print(
                    f"  [{device.state:10s}] "
                    f"{device.pressure_psi:6.1f} PSI -> target {device.target_psi:.1f} PSI  "
                    f"pump={'ON ' if device.pump else 'off'}  valve={'OPEN  ' if device.valve else 'closed'}"
                )


# ── HTTP Web Dashboard Server ────────────────────────────────────────────────
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8090


async def handle_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        req_line = await reader.readline()
        if not req_line:
            return
        parts = req_line.decode("utf-8", errors="ignore").split()
        if len(parts) < 2:
            return
        method, path = parts[0], parts[1]

        content_length = 0
        while True:
            line = await reader.readline()
            line_str = line.decode("utf-8", errors="ignore").strip()
            if not line_str:
                break
            if line_str.lower().startswith("content-length:"):
                content_length = int(line_str.split(":", 1)[1].strip())

        if method == "GET" and path == "/":
            html_path = os.path.join(os.path.dirname(__file__), "www", "index.html")
            if not os.path.exists(html_path):
                html_path = os.path.join(os.path.dirname(__file__), "index.html")
            try:
                with open(html_path, "r", encoding="utf-8") as f:
                    body = f.read()
                resp_bytes = body.encode("utf-8")
                response_headers = (
                    "HTTP/1.1 200 OK\r\n"
                    "Content-Type: text/html; charset=utf-8\r\n"
                    f"Content-Length: {len(resp_bytes)}\r\n"
                    "Connection: close\r\n\r\n"
                )
                writer.write(response_headers.encode("utf-8") + resp_bytes)
                await writer.drain()
            except Exception as e:
                err_msg = f"Error reading index.html: {e}"
                resp_bytes = err_msg.encode("utf-8")
                writer.write(
                    f"HTTP/1.1 500 Internal Error\r\nContent-Length: {len(resp_bytes)}\r\nConnection: close\r\n\r\n".encode()
                    + resp_bytes
                )
                await writer.drain()

        elif method == "GET" and path == "/events":
            response_headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream\r\n"
                "Cache-Control: no-cache\r\n"
                "Connection: keep-alive\r\n"
                "Access-Control-Allow-Origin: *\r\n\r\n"
            )
            writer.write(response_headers.encode("utf-8"))
            await writer.drain()

            q = asyncio.Queue(maxsize=10)
            http_clients.append(q)

            initial_state = get_device_state_dict()
            writer.write(f"data: {json.dumps(initial_state)}\n\n".encode("utf-8"))
            await writer.drain()

            try:
                while True:
                    state_data = await q.get()
                    writer.write(f"data: {json.dumps(state_data)}\n\n".encode("utf-8"))
                    await writer.drain()
            except Exception:
                pass
            finally:
                if q in http_clients:
                    http_clients.remove(q)

        elif method == "POST" and path == "/api/command":
            body_bytes = (
                await reader.readexactly(content_length) if content_length > 0 else b""
            )
            resp_body = {"status": "ok"}
            try:
                cmd_data = json.loads(body_bytes.decode("utf-8"))
                cmd = cmd_data.get("cmd")

                if cmd == "start":
                    await send_command("start")
                elif cmd == "stop":
                    await send_command("stop")
                elif cmd == "set_profile":
                    prof_name = cmd_data.get("profile")
                    await send_profile(prof_name)
                elif cmd == "deflate":
                    psi_val = float(cmd_data.get("target_psi"))
                    await send_command(
                        "set_profile",
                        name="custom",
                        target_psi=psi_val,
                        tolerance_psi=1.0,
                    )
                elif cmd == "custom":
                    psi_val = float(cmd_data.get("target_psi"))
                    await send_command(
                        "set_profile",
                        name="custom",
                        target_psi=psi_val,
                        tolerance_psi=1.0,
                    )
                else:
                    resp_body = {
                        "status": "error",
                        "message": f"Unknown command: {cmd}",
                    }
            except Exception as e:
                resp_body = {"status": "error", "message": str(e)}

            resp_bytes = json.dumps(resp_body).encode("utf-8")
            response_headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(resp_bytes)}\r\n"
                "Connection: close\r\n"
                "Access-Control-Allow-Origin: *\r\n\r\n"
            )
            writer.write(response_headers.encode("utf-8") + resp_bytes)
            await writer.drain()

        else:
            resp_bytes = b"Not Found"
            response_headers = (
                "HTTP/1.1 404 Not Found\r\n"
                f"Content-Length: {len(resp_bytes)}\r\n"
                "Connection: close\r\n\r\n"
            )
            writer.write(response_headers.encode("utf-8") + resp_bytes)
            await writer.drain()

    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ── Entry point ──────────────────────────────────────────────────────────────
async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    http_server = await asyncio.start_server(handle_http, HTTP_HOST, HTTP_PORT)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    http_addrs = ", ".join(str(s.getsockname()) for s in http_server.sockets)
    print(f"TCP server listening on {addrs}")
    print(f"HTTP server listening on {http_addrs}")

    async with server, http_server:
        await asyncio.gather(
            server.serve_forever(),
            http_server.serve_forever(),
            cli_loop(),
            display_task(),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBye.")
