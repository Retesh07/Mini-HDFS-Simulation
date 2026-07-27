#!/usr/bin/env python3
import socket
import threading
import json
import time
import struct
import os
import logging
from typing import Dict, List, Tuple, Set

# =========================
# Logging setup
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
log = logging.getLogger("namenode")

# =========================
# Load Configuration
# =========================
def load_config():
    with open("config.json") as f:
        return json.load(f)

config = load_config()

HOST = config["namenode"]["host"]
CLIENT_PORT = int(config["namenode"]["client_port"])
HEARTBEAT_PORT = int(config["namenode"]["heartbeat_port"])
DATANODES: Dict[str, Dict] = config["datanodes"]
REPLICATION = int(config["replication_factor"])
HEARTBEAT_TIMEOUT = float(config["heartbeat_timeout_sec"])
CHUNK_BYTES = config.get("chunk_size_mb", 2) * 1024 * 1024

# =========================
# Metadata persistence
# =========================
METADATA_FILE = "metadata.json"

def save_metadata(metadata):
    try:
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=2)
        log.info(f"[SAVE] Metadata saved ({len(metadata)} files)")
    except Exception as e:
        log.error(f"[ERROR] Could not save metadata: {e}")

def load_metadata():
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE) as f:
                data = json.load(f)
                log.info(f"[LOAD] Metadata loaded ({len(data)} files)")
                return data
        except Exception as e:
            log.error(f"[ERROR] Could not load metadata: {e}")
    return {}

# =========================
# Global State
# =========================
metadata: Dict[str, List[Dict]] = load_metadata()
chunk_locations: Dict[Tuple[str, int], Set[str]] = {}
heartbeat_table: Dict[str, float] = {dn_id: 0.0 for dn_id in DATANODES.keys()}
state_lock = threading.Lock()

# =========================
# JSON Message Helpers
# =========================
MAX_JSON_FRAME = 128 * 1024 * 1024

def send_json(conn, obj, who="peer"):
    data = json.dumps(obj).encode("utf-8")
    header = struct.pack(">I", len(data))
    conn.sendall(header + data)
    log.debug(f"[SEND → {who}] {obj}")

def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

def recv_json(conn, who="peer"):
    header = recv_exact(conn, 4)
    if not header:
        return None
    (length,) = struct.unpack(">I", header)
    if length <= 0 or length > MAX_JSON_FRAME:
        raise ValueError("Invalid frame length")
    body = recv_exact(conn, length)
    return json.loads(body.decode("utf-8"))

# =========================
# Helper Functions
# =========================
def live_datanodes_ids():
    now = time.time()
    return [dn for dn, last in heartbeat_table.items() if (now - last) <= HEARTBEAT_TIMEOUT]

def datanode_host_port(dn_id: str) -> Tuple[str, int]:
    info = DATANODES[dn_id]
    return info["host"], int(info["port"])

def plan_two_replicas(cid: int, live_ids: List[str]) -> List[Tuple[str, int]]:
    if set(live_ids) == {"dn0", "dn1"}:
        primary = "dn0" if cid % 2 == 0 else "dn1"
        secondary = "dn1" if primary == "dn0" else "dn0"
        return [datanode_host_port(primary), datanode_host_port(secondary)]
    elif "dn0" in live_ids:
        return [datanode_host_port("dn0")]
    elif "dn1" in live_ids:
        return [datanode_host_port("dn1")]
    else:
        return []

# =========================
# Client Handler
# =========================
def handle_client(conn, addr):
    cname = f"{addr[0]}:{addr[1]}"
    try:
        req = recv_json(conn, who=cname)
        if not req:
            return
        action = req.get("action")

        # ---- UPLOAD ----
        if action in ("upload", "upload_request"):
            filename = req["filename"]
            num_chunks = int(req["num_chunks"])
            chunk_sizes = req.get("chunk_sizes", [])
            log.info(f"[UPLOAD INIT] {filename}, chunks={num_chunks}")

            with state_lock:
                live = live_datanodes_ids()
                if not live:
                    send_json(conn, {"status": "error", "message": "No live datanodes"})
                    return

                plan = []
                for cid in range(num_chunks):
                    endpoints = plan_two_replicas(cid, live)
                    plan.append({
                        "chunk_id": cid,
                        "chunk_name": f"{filename}.chunk{cid}",
                        "datanodes": endpoints
                    })

                metadata[filename] = []
                for cid in range(num_chunks):
                    metadata[filename].append({
                        "chunk_id": cid,
                        "chunk_name": f"{filename}.chunk{cid}",
                        "replicas": plan[cid]["datanodes"],
                        "size": chunk_sizes[cid] if cid < len(chunk_sizes) else CHUNK_BYTES
                    })
                save_metadata(metadata)

            send_json(conn, {"status": "ok", "plan": plan})

        # ---- COMMIT ----
        elif action == "commit_upload":
            filename = req["filename"]
            with state_lock:
                if filename in metadata:
                    save_metadata(metadata)
                    send_json(conn, {"status": "ok", "message": "commit recorded"})
                    log.info(f"[COMMIT] {filename} recorded")
                else:
                    send_json(conn, {"status": "error", "message": "unknown file"})

        # ---- DOWNLOAD ----
        elif action in ("download", "download_request"):
            filename = req["filename"]
            with state_lock:
                if filename in metadata:
                    live = set(live_datanodes_ids())
                    if not live:
                        send_json(conn, {"status": "error", "message": "All datanodes are offline. Start the datanodes and try again."})
                        return
                    result = []
                    has_unavailable_chunk = False
                    for rec in metadata[filename]:
                        live_repls = []
                        for host, port in rec["replicas"]:
                            for k, v in DATANODES.items():
                                if v["host"] == host and int(v["port"]) == int(port) and k in live:
                                    live_repls.append((host, port))
                        if not live_repls:
                            has_unavailable_chunk = True
                            break
                        result.append({
                            "chunk_id": rec["chunk_id"],
                            "chunk_name": rec["chunk_name"],
                            "datanodes": live_repls,
                            "size": rec["size"]
                        })
                    if has_unavailable_chunk:
                        send_json(conn, {"status": "error", "message": f"File '{filename}' is unavailable — the datanodes holding its chunks are offline."})
                    else:
                        send_json(conn, {"status": "ok", "metadata": result})
                else:
                    send_json(conn, {"status": "error", "message": f"File '{filename}' not found on the Namenode."})

        # ---- LIST FILES ----
        elif action == "list_files":
            with state_lock:
                files = list(metadata.keys())
            send_json(conn, {"status": "ok", "files": files})

        # ---- BLOCK REPORT ----
        elif action == "block_report":
            dn_id = req["dn_id"]
            blocks = req.get("blocks", [])
            with state_lock:
                for key in list(chunk_locations.keys()):
                    if dn_id in chunk_locations[key]:
                        chunk_locations[key].remove(dn_id)
                for b in blocks:
                    key = (b["filename"], int(b["chunk_id"]))
                    refs = chunk_locations.setdefault(key, set())
                    refs.add(dn_id)
            send_json(conn, {"status": "ok"})
            log.info(f"[BLOCK REPORT] {dn_id}: {len(blocks)} blocks")

        # ---- SYSTEM STATUS ----
        elif action == "system_status":
            with state_lock:
                live_ids = set(live_datanodes_ids())
                
                # Build nodes info
                nodes_info = {}
                for dn_id, info in DATANODES.items():
                    nodes_info[dn_id] = {
                        "host": info["host"],
                        "port": info["port"],
                        "alive": dn_id in live_ids
                    }
                
                # Build files info
                files_info = {}
                for fname, chunks in metadata.items():
                    files_info[fname] = [
                        {
                            "chunk_name": ch["chunk_name"],
                            "datanodes": ch["replicas"]
                        } for ch in chunks
                    ]
                
                # Build integrity info
                integrity_info = {}
                for fname, chunks in metadata.items():
                    file_ok = True
                    for ch in chunks:
                        key = (fname, ch["chunk_id"])
                        holders = chunk_locations.get(key, set())
                        has_alive_replica = False
                        for dn_id in holders:
                            if dn_id in live_ids:
                                has_alive_replica = True
                                break
                        if not has_alive_replica:
                            file_ok = False
                            break
                    integrity_info[fname] = file_ok
                    
            send_json(conn, {
                "status": "ok",
                "nodes": nodes_info,
                "files": files_info,
                "integrity": integrity_info
            })

        else:
            send_json(conn, {"status": "error", "message": "unknown action"})

    except Exception as e:
        log.error(f"[ERROR] {cname}: {e}")
    finally:
        conn.close()

# =========================
# Listener Threads
# =========================
def client_listener():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", CLIENT_PORT))
        s.listen()
        log.info(f"[CLIENT LISTENER] on {CLIENT_PORT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

def heartbeat_listener():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("0.0.0.0", HEARTBEAT_PORT))
        log.info(f"[HEARTBEAT LISTENER] on {HEARTBEAT_PORT}")
        while True:
            msg, addr = s.recvfrom(1024)
            dn_id = msg.decode().strip()
            with state_lock:
                if dn_id in heartbeat_table:
                    heartbeat_table[dn_id] = time.time()
            log.debug(f"[HEARTBEAT] from {dn_id}")

def heartbeat_monitor():
    while True:
        time.sleep(5)
        now = time.time()
        with state_lock:
            for dn_id, last in heartbeat_table.items():
                if now - last > HEARTBEAT_TIMEOUT:
                    log.warning(f"[DOWN] {dn_id} missed heartbeats")
                    for key in list(chunk_locations.keys()):
                        if dn_id in chunk_locations[key]:
                            chunk_locations[key].remove(dn_id)
                else:
                    log.info(f"[ALIVE] {dn_id}")

def replication_healer():
    while True:
        time.sleep(10)
        with state_lock:
            live = set(live_datanodes_ids())

            # Safety guard: if ALL datanodes are offline we cannot confirm whether
            # chunks are truly lost — skip the stale-file cleanup entirely so we
            # don't delete metadata for files that are still safe on disk.
            if not live:
                log.info("[HEALER] All datanodes offline — skipping stale metadata cleanup to preserve recovery")
                save_metadata(metadata)
                continue

            stale_files = []
            for filename, chunks in metadata.items():
                all_chunks_missing = True
                for rec in chunks:
                    key = (filename, rec["chunk_id"])
                    holders = chunk_locations.get(key, set())
                    live_holders = holders & live
                    if len(live_holders) > 0:
                        all_chunks_missing = False
                        # Only heal if at least 1 live holder exists (can replicate from it)
                        if len(live_holders) < REPLICATION:
                            candidates = [dn for dn in DATANODES.keys() if dn not in holders and dn in live]
                            for dn_id in candidates[:REPLICATION - len(live_holders)]:
                                rec["replicas"].append(datanode_host_port(dn_id))
                                log.info(f"[HEAL] {filename} chunk {rec['chunk_id']} → added {dn_id}")
                        # Deduplicate replicas list
                        seen = set()
                        unique_replicas = []
                        for r in rec["replicas"]:
                            key_r = (r[0], int(r[1]))
                            if key_r not in seen:
                                seen.add(key_r)
                                unique_replicas.append(r)
                        rec["replicas"] = unique_replicas
                if all_chunks_missing:
                    stale_files.append(filename)
            # Remove files with no chunks on any live Datanode
            for fname in stale_files:
                del metadata[fname]
                log.info(f"[CLEANUP] Removed stale metadata for '{fname}' (confirmed: no chunks on any live Datanode)")
            save_metadata(metadata)

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    log.info(f"[NAMENODE STARTED] Host={HOST}, ClientPort={CLIENT_PORT}, HBPort={HEARTBEAT_PORT}")
    threading.Thread(target=client_listener, daemon=True).start()
    threading.Thread(target=heartbeat_listener, daemon=True).start()
    threading.Thread(target=heartbeat_monitor, daemon=True).start()
    threading.Thread(target=replication_healer, daemon=True).start()

    while True:
        time.sleep(60)
