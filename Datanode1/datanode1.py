import os, socket, threading, time, json, struct, logging, hashlib

# ===============================
# CONFIGURATION
# ===============================
def load_config():
    with open("config.json") as f:
        return json.load(f)

config = load_config()

# ===============================
# LOGGING
# ===============================
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
)
log = logging.getLogger("datanode1")

# ===============================
# HELPERS
# ===============================
def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        part = conn.recv(n - len(buf))
        if not part:
            return None
        buf += part
    return buf

def checksum(data):
    return hashlib.md5(data).hexdigest()

# ===============================
# DATANODE 1 MAIN LOGIC
# ===============================
def datanode_1(node_id, host, port, storage_dir, namenode_host, namenode_heartbeat_port):
    os.makedirs(storage_dir, exist_ok=True)
    log.info(f"[START] Datanode1 (replica) {node_id} running at {host}:{port}")

    # --------------------------------
    # (2) Heartbeat thread
    # --------------------------------
    def send_heartbeat():
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.sendto(node_id.encode(), (namenode_host, namenode_heartbeat_port))
                s.close()
                log.debug(f"[HEARTBEAT] Sent heartbeat from {node_id}")
            except Exception as e:
                log.error(f"[HEARTBEAT_FAIL] {e}")
            time.sleep(config["heartbeat_interval_sec"])

    # --------------------------------
    # (1), (3), (4): Chunk storage and retrieval
    # (2b) Block Report logic
    def trigger_block_report():
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

    # (1,3,4,5) Store & Retrieve logic
    def listen_for_chunks():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        log.info(f"[LISTEN] Datanode1 listening on {host}:{port}")

        while True:
            conn, addr = s.accept()
            try:
                # Read command
                cmd = b""
                while not cmd.endswith(b"\n"):
                    part = conn.recv(1)
                    if not part:
                        break
                    cmd += part
                cmd = cmd.decode().strip()
                log.debug(f"[CMD] Received command '{cmd}' from {addr}")

                # Read header
                hdr_len_raw = recv_exact(conn, 4)
                if not hdr_len_raw:
                    log.error("[HEADER_ERROR] Incomplete header length")
                    conn.close()
                    continue

                hdr_len = struct.unpack(">I", hdr_len_raw)[0]
                header_raw = recv_exact(conn, hdr_len)
                if not header_raw:
                    log.error("[HEADER_ERROR] Incomplete header")
                    conn.close()
                    continue

                header = json.loads(header_raw.decode())

                if cmd in ("REPLICATE", "STORE"):
                    log.info(f"[STORE_REQUEST] Storing chunk {header['chunk_name']} ({header['size']} bytes)")
                    chunk_name = header["chunk_name"]
                    size = header["size"]
                    replicas = header.get("replicas", [])
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
                    trigger_block_report()

                    # Pipeline forwarding
                    if replicas:
                        next_host, next_port = replicas[0]
                        remaining_replicas = replicas[1:]
                        log.info(f"[PIPELINE] Forwarding {chunk_name} to {next_host}:{next_port}")
                        try:
                            ts = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            ts.connect((next_host, next_port))
                            ts.sendall(b"STORE\n")
                            theader = {"chunk_name": chunk_name, "size": len(data), "replicas": remaining_replicas}
                            tpayload = json.dumps(theader).encode("utf-8")
                            ts.sendall(struct.pack(">I", len(tpayload)) + tpayload)
                            
                            ack = ts.recv(32)
                            if ack == b"READY":
                                ts.sendall(data)
                                final_ack = ts.recv(32)
                                if final_ack == b"OK":
                                    log.info(f"[PIPELINE_SUCCESS] {chunk_name} successfully pipelined")
                                    conn.sendall(b"OK")
                                else:
                                    log.error(f"[PIPELINE_FAIL] Downstream failed: {final_ack}")
                                    conn.sendall(b"ERROR:DOWNSTREAM_FAILED")
                            else:
                                log.error(f"[PIPELINE_FAIL] Downstream did not ack READY")
                                conn.sendall(b"ERROR:DOWNSTREAM_NOT_READY")
                        except Exception as e:
                            log.error(f"[PIPELINE_FAIL] Exception: {e}")
                            conn.sendall(b"ERROR:EXCEPTION")
                        finally:
                            if 'ts' in locals():
                                ts.close()
                    else:
                        conn.sendall(b"OK")

                elif cmd == "GET":
                    chunk_name = header["chunk_name"]
                    path = os.path.join(storage_dir, chunk_name)
                    if not os.path.isfile(path):
                        log.error(f"[GET_ERROR] Missing replica {chunk_name}")
                        conn.sendall(b"ERROR:NOT_FOUND")
                        continue

                    with open(path, "rb") as f:
                        data = f.read()
                    stored_sum = checksum(data)
                    # Optionally verify checksum if provided
                    if "checksum" in header and header["checksum"] != stored_sum:
                        log.error(f"[INTEGRITY_FAIL] Replica {chunk_name} MD5 mismatch on GET")
                        conn.sendall(b"ERROR:INTEGRITY_FAIL")
                        continue

                    conn.sendall(data)
                    log.info(f"[GET_OK] Sent {chunk_name} ({len(data)} bytes) MD5={stored_sum}")

                elif cmd == "FORWARD":
                    chunk_name = header["chunk_name"]
                    target_host = header["target_host"]
                    target_port = header["target_port"]
                    path = os.path.join(storage_dir, chunk_name)
                    log.info(f"[FORWARD_REQUEST] Copying {chunk_name} to {target_host}:{target_port}")
                    if not os.path.isfile(path):
                        log.error(f"[FORWARD_FAIL] Missing {chunk_name}")
                        conn.sendall(b"ERROR:NOT_FOUND")
                        continue
                        
                    try:
                        with open(path, "rb") as f:
                            data = f.read()
                        
                        ts = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        ts.connect((target_host, target_port))
                        ts.sendall(b"REPLICATE\n")
                        theader = {"chunk_name": chunk_name, "size": len(data)}
                        tpayload = json.dumps(theader).encode("utf-8")
                        ts.sendall(struct.pack(">I", len(tpayload)) + tpayload)
                        
                        ack = ts.recv(32)
                        if ack == b"READY":
                            ts.sendall(data)
                            final_ack = ts.recv(32)
                            if final_ack == b"OK":
                                log.info(f"[FORWARD_SUCCESS] Sent {chunk_name} to {target_host}:{target_port}")
                                conn.sendall(b"OK")
                            else:
                                log.error(f"[FORWARD_FAIL] Target returned {final_ack}")
                                conn.sendall(b"ERROR:TARGET_FAIL")
                        else:
                            log.error(f"[FORWARD_FAIL] Target did not ack READY")
                            conn.sendall(b"ERROR:TARGET_NOT_READY")
                    except Exception as e:
                        log.error(f"[FORWARD_FAIL] Exception: {e}")
                        conn.sendall(b"ERROR:EXCEPTION")
                    finally:
                        if 'ts' in locals():
                            ts.close()

                else:
                    log.error(f"[CMD_ERROR] Unknown command: {cmd}")

            except Exception as e:
                log.exception(f"[ERROR] {e}")
            finally:
                conn.close()

    # (2b) Block Report sender
    def send_block_report():
        while True:
            time.sleep(10)
            trigger_block_report()

    threading.Thread(target=send_heartbeat, daemon=True, name="dn1-heartbeat").start()
    threading.Thread(target=send_block_report, daemon=True, name="dn1-blockreport").start()
    listen_for_chunks()

# ===============================
# ENTRY POINT
# ===============================
if __name__ == "__main__":
    cfg = config["datanodes"]["dn1"]
    nn = config["namenode"]
    datanode_1("dn1", cfg["host"], cfg["port"], cfg["storage_dir"], nn["host"], nn["heartbeat_port"])
