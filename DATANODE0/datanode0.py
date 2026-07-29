import os, socket, threading, time, json, struct, logging, hashlib

# CONFIGURATION

def load_config():
    with open("config.json") as f:
        return json.load(f)

config = load_config()

# LOGGING
logging.basicConfig(
    level=logging.DEBUG,  # Debug level for detailed output!
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
)
log = logging.getLogger("datanode0")


# HELPERS

def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        part = conn.recv(n - len(buf))
        if not part:
            log.debug(f"[RECV_EXACT] Connection closed before expected bytes ({len(buf)}/{n})")
            return None
        buf += part
    log.debug(f"[RECV_EXACT] Received {n} bytes")
    return buf

def checksum(data):
    return hashlib.md5(data).hexdigest()


# DATANODE 0 MAIN
def datanode_0(node_id, host, port, storage_dir, namenode_host, namenode_heartbeat_port):
    os.makedirs(storage_dir, exist_ok=True)
    log.info(f"[START] Datanode0 primary {node_id} at {host}:{port}")

    # (2) Heartbeat sender
    def send_heartbeat():
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.sendto(node_id.encode(), (namenode_host, namenode_heartbeat_port))
                s.close()
                log.debug(f"[HEARTBEAT] Sent from {node_id} to {namenode_host}:{namenode_heartbeat_port}")
            except Exception as e:
                log.error(f"[HEARTBEAT_FAIL] {e}")
            time.sleep(config["heartbeat_interval_sec"])

    # (1,3,4,5) Store & Retrieve logic
    def listen_for_chunks():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        log.info(f"[LISTEN] Datanode0 listening on {host}:{port}")

        while True:
            conn, addr = s.accept()
            log.debug(f"[CONNECTION] Accepted connection from {addr}")
            try:
                cmd = b""
                while not cmd.endswith(b"\n"):
                    part = conn.recv(1)
                    if not part:
                        log.debug(f"[CONNECTION_CLOSED] No data for command from {addr}")
                        break
                    cmd += part
                cmd = cmd.decode().strip()
                log.debug(f"[COMMAND] Received command '{cmd}' from {addr}")

                hdr_len_bytes = recv_exact(conn, 4)
                if not hdr_len_bytes:
                    log.error("[HEADER_ERROR] Could not receive header length")
                    conn.close()
                    continue
                hdr_len = struct.unpack(">I", hdr_len_bytes)[0]
                log.debug(f"[HEADER_LENGTH] Length: {hdr_len} bytes")
                header_raw = recv_exact(conn, hdr_len)
                if not header_raw:
                    log.error("[HEADER_ERROR] Could not receive header")
                    conn.close()
                    continue
                header = json.loads(header_raw.decode())
                log.debug(f"[HEADER] Parsed: {header}")

                if cmd == "STORE":
                    log.info(f"[STORE_REQUEST] Storing chunk {header['chunk_name']} ({header['size']} bytes)")
                    chunk_name = header["chunk_name"]
                    size = header["size"]
                    conn.sendall(b"READY")

                    data = b""
                    while len(data) < size:
                        part = conn.recv(size - len(data))
                        if not part:
                            log.warning(f"[STORE_DATA_ERROR] Connection closed during chunk transfer for {chunk_name}")
                            break
                        data += part
                    log.debug(f"[STORE_DATA] Received {len(data)} bytes for chunk {chunk_name}")

                    # Data integrity check
                    digest = checksum(data)
                    log.info(f"[STORE_CHECKSUM] {chunk_name} MD5={digest}")
                    chunk_path = os.path.join(storage_dir, chunk_name)
                    with open(chunk_path, "wb") as f:
                        f.write(data)
                    log.info(f"[STORE_OK] Chunk stored at {chunk_path}")

                elif cmd == "GET":
                    chunk_name = header["chunk_name"]
                    path = os.path.join(storage_dir, chunk_name)
                    log.info(f"[GET_REQUEST] Retrieve chunk {chunk_name} from {path}")
                    if not os.path.isfile(path):
                        log.error(f"[GET_ERROR] Missing {chunk_name}")
                        conn.sendall(b"ERROR:NOT_FOUND")
                        continue
                    with open(path, "rb") as f:
                        data = f.read()
                    digest = checksum(data)
                    log.info(f"[GET_CHECKSUM] {chunk_name} MD5={digest}")
                    if "checksum" in header and header["checksum"] != digest:
                        log.error(f"[INTEGRITY_FAIL] {chunk_name} MD5 mismatch GET (expected {header['checksum']})")
                        conn.sendall(b"ERROR:INTEGRITY_FAIL")
                        continue
                    conn.sendall(data)
                    log.info(f"[GET_OK] Sent {chunk_name} ({len(data)} bytes) to client {addr}")

                else:
                    log.error(f"[CMD_ERROR] Unknown command: {cmd}")

            except Exception as e:
                log.exception(f"[ERROR] Exception handling client {addr}: {e}")
            finally:
                conn.close()
                log.debug(f"[CONNECTION_CLOSED] Closed connection with {addr}")

    # (2b) Block Report sender
    def send_block_report():
        while True:
            time.sleep(10)
            try:
                blocks = []
                if os.path.exists(storage_dir):
                    for fname in os.listdir(storage_dir):
                        if ".chunk" in fname:
                            parts = fname.rsplit(".chunk", 1)
                            if len(parts) == 2:
                                filename, chunk_id_str = parts
                                try:
                                    blocks.append({
                                        "filename": filename,
                                        "chunk_id": int(chunk_id_str)
                                    })
                                except ValueError:
                                    pass
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((namenode_host, config["namenode"]["client_port"]))
                req = {
                    "action": "block_report",
                    "dn_id": node_id,
                    "blocks": blocks
                }
                data = json.dumps(req).encode("utf-8")
                header = struct.pack(">I", len(data))
                s.sendall(header + data)
                
                # Recv response
                hdr = s.recv(4)
                if hdr:
                    length = struct.unpack(">I", hdr)[0]
                    s.recv(length)
                s.close()
                log.debug(f"[BLOCK_REPORT] Sent {len(blocks)} blocks from {node_id}")
            except Exception as e:
                log.error(f"[BLOCK_REPORT_FAIL] {e}")

    threading.Thread(target=send_heartbeat, daemon=True, name="dn0-heartbeat").start()
    threading.Thread(target=send_block_report, daemon=True, name="dn0-blockreport").start()
    listen_for_chunks()

if __name__ == "__main__":
    cfg = config["datanodes"]["dn0"]
    nn = config["namenode"]

    datanode_0("dn0", cfg["host"], cfg["port"], cfg["storage_dir"],
               nn["host"], nn["heartbeat_port"])
