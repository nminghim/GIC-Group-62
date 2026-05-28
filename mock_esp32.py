import socket
import json
import time
import threading

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5555

class MockEsp32:
    def __init__(self):
        self.pressure_psi = 30.0
        self.target_psi = 34.0
        self.tolerance_psi = 1.0
        self.profile = "comfort"
        self.state = "idle"
        self.pump = False
        self.valve = False
        self.active = False
        self.running = True
        self.sock = None

    def send_json(self, data):
        if not self.sock:
            return
        try:
            msg = json.dumps(data) + "\n"
            self.sock.sendall(msg.encode("utf-8"))
        except Exception as e:
            print(f"Error sending telemetry: {e}")

    def send_telemetry(self):
        data = {
            "type": "telemetry",
            "pressure_psi": round(self.pressure_psi, 1),
            "target_psi": self.target_psi,
            "tolerance_psi": self.tolerance_psi,
            "profile": self.profile,
            "pump": self.pump,
            "valve": self.valve,
            "state": self.state,
            "millis": int(time.time() * 1000)
        }
        self.send_json(data)

    def send_event(self, event_name):
        data = {
            "type": "event",
            "event": event_name,
            "pressure_psi": round(self.pressure_psi, 1)
        }
        self.send_json(data)
        print(f"[MOCK EVENT] Sent event: {event_name} at {self.pressure_psi:.1f} PSI")

    def send_error(self, message):
        data = {
            "type": "error",
            "message": message
        }
        self.send_json(data)
        print(f"[MOCK ERROR] Sent error: {message}")

    def handle_command(self, cmd_data):
        cmd = cmd_data.get("cmd")
        print(f"[MOCK CMD] Received command: {cmd}")

        if cmd == "set_profile":
            self.profile = cmd_data.get("name", "custom")
            self.target_psi = cmd_data.get("target_psi", self.target_psi)
            self.tolerance_psi = cmd_data.get("tolerance_psi", self.tolerance_psi)
            print(f"[MOCK STATE] Profile updated: {self.profile} (target={self.target_psi} PSI)")
            self.send_event("profile_set")

        elif cmd == "start":
            self.active = True
            print("[MOCK STATE] Started calibration cycle")

        elif cmd == "stop":
            self.active = False
            self.pump = False
            self.valve = False
            self.state = "idle"
            print("[MOCK STATE] Stopped cycle")
            self.send_event("stopped")

        elif cmd == "deflate_to":
            self.target_psi = cmd_data.get("target_psi", self.target_psi)
            self.active = True
            print(f"[MOCK STATE] Starting deflation target: {self.target_psi} PSI")

        elif cmd == "status":
            self.send_telemetry()

    def socket_receiver(self):
        buffer = ""
        while self.running:
            try:
                data = self.sock.recv(1024)
                if not data:
                    print("Disconnected from server")
                    break
                buffer += data.decode("utf-8")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if line:
                        try:
                            cmd_data = json.loads(line)
                            self.handle_command(cmd_data)
                        except Exception as je:
                            print(f"JSON parsing error: {je}")
            except Exception as e:
                print(f"Socket receiver error: {e}")
                break

    def control_loop(self):
        while self.running:
            time.sleep(0.1)
            if not self.active:
                self.pump = False
                self.valve = False
                self.state = "idle"
                continue

            diff = self.target_psi - self.pressure_psi
            if abs(diff) <= self.tolerance_psi:
                self.pump = False
                self.valve = False
                self.state = "idle"
                self.active = False
                self.send_event("target_reached")
            elif diff > 0:
                self.pump = True
                self.valve = False
                self.state = "inflating"
                # Inflate 0.1 PSI per 100ms
                self.pressure_psi += 0.1
            else:
                self.pump = False
                self.valve = True
                self.state = "deflating"
                # Deflate 0.1 PSI per 100ms
                self.pressure_psi -= 0.1

    def run(self):
        print(f"Connecting to {SERVER_HOST}:{SERVER_PORT}...")
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((SERVER_HOST, SERVER_PORT))
            print("Connected successfully!")
        except Exception as e:
            print(f"Failed to connect: {e}")
            return

        # Start receiver thread
        recv_thread = threading.Thread(target=self.socket_receiver, daemon=True)
        recv_thread.start()

        # Start control loop thread
        control_thread = threading.Thread(target=self.control_loop, daemon=True)
        control_thread.start()

        # Main thread sends telemetry periodically
        try:
            while self.running:
                self.send_telemetry()
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("Shutting down simulator...")
        finally:
            self.running = False
            self.sock.close()

if __name__ == "__main__":
    MockEsp32().run()
