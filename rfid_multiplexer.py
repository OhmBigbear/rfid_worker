#!/usr/bin/env python3
import os, sys, time, socket, struct, threading, signal
import yaml
import psycopg2
import psycopg2.extras
from typing import Optional
from loguru import logger

# ----------------- LOGGING SETUP -----------------
def setup_logger():
    # Configurable via environment variables
    LOG_LEVEL       = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR         = os.getenv("LOG_DIR", "/var/log/rfid")
    LOG_FILE        = os.getenv("LOG_FILE", "multiplexer.log")
    LOG_ROTATION    = os.getenv("LOG_ROTATION", "100 MB")    # rotate when file reaches size
    LOG_RETENTION   = os.getenv("LOG_RETENTION", "14 days")  # keep older logs for this period
    LOG_COMPRESSION = os.getenv("LOG_COMPRESSION", "xz")     # compress rotated logs
    LOG_JSON        = os.getenv("LOG_JSON", "0") == "1"      # serialize as JSON if set

    # Ensure log directory exists (fallback to current dir if cannot create)
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
    except Exception:
        LOG_DIR = "."

    logger.remove()  # remove default sink
    # stdout (journald/systemd will pick it up)
    logger.add(sys.stdout, level=LOG_LEVEL, enqueue=True, backtrace=False, diagnose=False)
    # rotating logfile
    logger.add(
        os.path.join(LOG_DIR, LOG_FILE),
        level=LOG_LEVEL,
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        compression=LOG_COMPRESSION,
        enqueue=True,
        backtrace=False,
        diagnose=False,
        serialize=LOG_JSON,
    )

# ----------------- FRAME HELPERS -----------------
HDR  = b"\xA5\x5A"
TAIL = b"\x0D\x0A"

def xor_checksum(payload: bytes) -> int:
    c = 0
    for b in payload:
        c ^= b
    return c & 0xFF

def make_frame(frame_type: int, data: bytes) -> bytes:
    # total length = 2(header) + 2(len) + 1(type) + N(data) + 1(chk) + 2(tail)
    total_len = 2 + 2 + 1 + len(data) + 1 + 2
    length_be = struct.pack(">H", total_len)
    core = length_be + bytes([frame_type]) + data
    crc  = bytes([xor_checksum(core)])
    return HDR + core + crc + TAIL

def parse_frame(stream_read):
    """Read and validate a full frame from the stream using provided reader fn."""
    buf = b""
    while True:
        b1 = stream_read(1)
        if not b1:
            return None
        buf += b1
        if len(buf) >= 2 and buf[-2:] == HDR:
            break
    try:
        length = struct.unpack(">H", stream_read(2))[0]
    except Exception:
        return None
    rest = stream_read(length - 4)
    if len(rest) != (length - 4):
        logger.debug("Partial frame received (len mismatch)")
        return None
    frame = HDR + struct.pack(">H", length) + rest
    if frame[-2:] != TAIL:
        logger.debug("Invalid tail")
        return None
    core = frame[2:-2]
    if xor_checksum(core) != core[-1]:
        logger.debug("Checksum mismatch")
        return None
    ftype = core[2]
    data  = core[3:-1]
    return (ftype, data, frame)

def pc_to_epc_len(pc_hi, pc_lo) -> int:
    pc_val = (pc_hi << 8) | pc_lo
    epc_words = (pc_val >> 11) & 0x1F
    return epc_words * 2

def twos_complement_16_to_signed(v: int) -> int:
    return v - 0x10000 if v & 0x8000 else v

def parse_inventory_data_mode0(data: bytes):
    """Parse Mode=0x00: PC(2) + EPC(var) + RSSI(2) + Ant(1) + [Freq(3)]"""
    if len(data) < 5:
        return None
    pc_hi, pc_lo = data[0], data[1]
    epc_len = pc_to_epc_len(pc_hi, pc_lo)
    pos = 2
    if len(data) < pos + epc_len + 3:
        return None
    epc = data[pos:pos+epc_len]; pos += epc_len
    rssi_raw = (data[pos] << 8) | data[pos+1]; pos += 2
    rssi_dbm = twos_complement_16_to_signed(rssi_raw) / 10.0
    ant_num = data[pos]; pos += 1
    freq_khz = None
    if len(data) >= pos + 3:
        freq_khz = (data[pos] << 16) | (data[pos+1] << 8) | data[pos+2]
    return {
        "pc": (pc_hi << 8) | pc_lo,
        "epc": epc,
        "rssi_dbm": rssi_dbm,
        "antenna": ant_num,
        "freq_khz": freq_khz,
    }

# ----------------- TRANSPORTS -----------------
class SerialConn:
    def __init__(self, port, baud):
        import serial
        self.s = serial.Serial(port, baudrate=baud, bytesize=8, parity='N', stopbits=1, timeout=1)
    def write(self, b: bytes):
        self.s.write(b)
    def read(self, n: int) -> bytes:
        return self.s.read(n)

class TcpConn:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port))
        self.sock.settimeout(1.0)
    def write(self, b: bytes):
        self.sock.sendall(b)
    def read(self, n: int) -> bytes:
        chunks = b""
        while len(chunks) < n:
            try:
                part = self.sock.recv(n - len(chunks))
            except socket.timeout:
                part = b""
            if not part:
                time.sleep(0.01)
                continue
            chunks += part
        return chunks

# ----------------- WORKER -----------------
stop_all = False

def reader_worker(cfg_reader: dict, db_dsn: str, default_inv: int):
    """One worker per reader: connect, start inventory, parse, insert to DB."""
    global stop_all
    name = cfg_reader.get("reader_id", "READER")
    enabled = cfg_reader.get("enabled", True)
    if not enabled:
        logger.info("[{}] disabled, skip", name)
        return

    connection = cfg_reader.get("connection", "serial").lower()
    inventory_count = cfg_reader.get("inventory_count", default_inv)

    bound = logger.bind(reader_id=name, connection=connection)

    while not stop_all:
        try:
            db = psycopg2.connect(db_dsn)
            db.autocommit = True
            cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Transport setup
            if connection == "serial":
                port = cfg_reader["serial"]["port"]
                baud = int(cfg_reader["serial"].get("baudrate", 115200))
                transport = SerialConn(port, baud)
                src = f"serial:{port}"
            elif connection == "tcp":
                host = cfg_reader["tcp"]["host"]; port = int(cfg_reader["tcp"]["port"])
                transport = TcpConn(host, port)
                src = f"tcp:{host}:{port}"
            else:
                bound.error("Unknown connection type: {}", connection)
                return

            log = bound.bind(src=src)
            # Start continuous inventory (0x82)
            data = struct.pack(">H", inventory_count & 0xFFFF)
            transport.write(make_frame(0x82, data))
            log.info("Inventory started (count={})", inventory_count)

            # Read loop
            while not stop_all:
                parsed = parse_frame(transport.read)
                if not parsed:
                    continue
                ftype, payload, raw = parsed
                if ftype not in (0x83, 0x81):
                    continue
                rec = parse_inventory_data_mode0(payload)
                if not rec:
                    continue

                cur.execute(
                    """
                    INSERT INTO rfid.reads
                      (ts, reader_id, antenna, epc, pc, rssi_dbm, freq_khz, raw_frame, src)
                    VALUES
                      (now(), %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        name,
                        int(rec["antenna"]),
                        psycopg2.Binary(rec["epc"]),
                        int(rec["pc"]),
                        float(rec["rssi_dbm"]),
                        None if rec["freq_khz"] is None else int(rec["freq_khz"]),
                        psycopg2.Binary(raw),
                        src,
                    )
                )
                # For deep debugging, uncomment:
                # log.debug("Insert EPC {} ant={} rssi={}", rec["epc"].hex(), rec["antenna"], rec["rssi_dbm"])

        except KeyboardInterrupt:
            break
        except Exception as e:
            bound.exception("Error: {}. Reconnecting in 3s...", e)
            time.sleep(3)
        finally:
            # Try to stop inventory (0x8C) and close resources
            try:
                if 'transport' in locals():
                    transport.write(make_frame(0x8C, b""))
            except Exception:
                pass
            try:
                if 'db' in locals():
                    db.close()
            except Exception:
                pass

    bound.info("Worker stopped.")

# ----------------- MAIN -----------------
def main():
    setup_logger()
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default="rfid_config.yaml", help="Path to YAML config")
    args = parser.parse_args()

    # Load config
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    db_dsn = cfg["db_dsn"]
    default_inv = int(cfg.get("default_inventory_count", 0))
    readers = cfg.get("readers", [])

    logger.info("Starting {} reader workers", len(readers))

    def handle_sig(sig, frame):
        global stop_all
        logger.warning("Signal {} received. Stopping...", sig)
        stop_all = True
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    threads = []
    for r in readers:
        t = threading.Thread(target=reader_worker, args=(r, db_dsn, default_inv), daemon=True)
        t.start()
        threads.append(t)

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass

    logger.info("All workers finished. Bye.")

if __name__ == "__main__":
    main()
