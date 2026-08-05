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
metadata: Dict[str, Dict] = load_metadata()
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
    if not live_ids:
        return []
        
    sorted_live = sorted(live_ids)
    num_replicas = min(REPLICATION, len(sorted_live))
    start_idx = cid % len(sorted_live)
    
    replicas = []
    for i in range(num_replicas):
        idx = (start_idx + i) % len(sorted_live)
        dn_id = sorted_live[idx]
        replicas.append(datanode_host_port(dn_id))
        
    return replicas

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
            file_id = req["file_id"]
            num_chunks = int(req["num_chunks"])
            chunk_sizes = req.get("chunk_sizes", [])
            log.info(f"[UPLOAD INIT] {filename} (ID: {file_id}), chunks={num_chunks}")

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
                        "chunk_name": f"{file_id}.chunk{cid}",
                        "datanodes": endpoints
                    })

                metadata[file_id] = {
                    "original_filename": filename,
                    "chunks": []
                }
                for cid in range(num_chunks):
                    metadata[file_id]["chunks"].append({
                        "chunk_id": cid,
                        "chunk_name": f"{file_id}.chunk{cid}",
                        "replicas": plan[cid]["datanodes"],
                        "size": chunk_sizes[cid] if cid < len(chunk_sizes) else CHUNK_BYTES
                    })
                save_metadata(metadata)

            send_json(conn, {"status": "ok", "plan": plan})

        # ---- COMMIT ----
        elif action == "commit_upload":
            file_id = req["file_id"]
            with state_lock:
                if file_id in metadata:
                    save_metadata(metadata)
                    send_json(conn, {"status": "ok", "message": "commit recorded"})
                    log.info(f"[COMMIT] {file_id} recorded")
                else:
                    send_json(conn, {"status": "error", "message": "unknown file"})

        # ---- DOWNLOAD ----
        elif action in ("download", "download_request"):
            file_id = req["file_id"]
            with state_lock:
                if file_id in metadata:
                    live = set(live_datanodes_ids())
                    if not live:
                        send_json(conn, {"status": "error", "message": "All datanodes are offline. Start the datanodes and try again."})
                        return
                    result = []
                    has_unavailable_chunk = False
                    for rec in metadata[file_id]["chunks"]:
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
                        send_json(conn, {"status": "error", "message": f"File '{metadata[file_id]['original_filename']}' is unavailable — the datanodes holding its chunks are offline."})
                    else:
                        send_json(conn, {"status": "ok", "metadata": result, "original_filename": metadata[file_id]["original_filename"]})
                else:
                    send_json(conn, {"status": "error", "message": f"File ID '{file_id}' not found on the Namenode."})

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

        # ---- CHUNK UPLOAD FAILED ----
        elif action == "chunk_upload_failed":
            file_id = req["file_id"]
            chunk_id = int(req["chunk_id"])
            failed_host, failed_port = req["failed_datanode"]
            
            with state_lock:
                if file_id not in metadata or chunk_id >= len(metadata[file_id]["chunks"]):
                    send_json(conn, {"status": "error", "message": "Unknown file or chunk"})
                    return
                
                chunk_rec = metadata[file_id]["chunks"][chunk_id]
                replicas = chunk_rec["replicas"]
                
                # Remove the failed datanode from replicas
                chunk_rec["replicas"] = [
                    r for r in replicas
                    if not (r[0] == failed_host and int(r[1]) == int(failed_port))
                ]
                
                live = live_datanodes_ids()
                if not live:
                    send_json(conn, {"status": "error", "message": "No live datanodes available"})
                    return
                    
                # Find candidates that are not already in replicas
                existing_replicas = set((r[0], int(r[1])) for r in chunk_rec["replicas"])
                candidates = []
                for dn_id in live:
                    host, port = datanode_host_port(dn_id)
                    if (host, port) not in existing_replicas:
                        candidates.append((host, port))
                        
                if not candidates:
                    send_json(conn, {"status": "error", "message": "No replacement datanodes available"})
                    return
                    
                # Pick the first available candidate
                new_dn = candidates[0]
                chunk_rec["replicas"].append(new_dn)
                save_metadata(metadata)
                
                log.info(f"[RECOVERY] {file_id} chunk {chunk_id}: replaced {failed_host}:{failed_port} with {new_dn[0]}:{new_dn[1]}")
                send_json(conn, {"status": "ok", "replacement_datanode": new_dn})

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
                for file_id, data in metadata.items():
                    files_info[file_id] = {
                        "original_filename": data["original_filename"],
                        "chunks": [
                            {
                                "chunk_name": ch["chunk_name"],
                                "datanodes": ch["replicas"]
                            } for ch in data["chunks"]
                        ]
                    }
                
                # Build integrity info
                integrity_info = {}
                for file_id, data in metadata.items():
                    file_ok = True
                    for ch in data["chunks"]:
                        key = (file_id, ch["chunk_id"])
                        holders = chunk_locations.get(key, set())
                        has_alive_replica = False
                        for dn_id in holders:
                            if dn_id in live_ids:
                                has_alive_replica = True
                                break
                        if not has_alive_replica:
                            file_ok = False
                            break
                    integrity_info[file_id] = file_ok
                    
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

def trigger_replication(source_host: str, source_port: int, dest_host: str, dest_port: int, chunk_name: str) -> bool:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(30.0)
        s.connect((source_host, source_port))
        
        s.sendall(b"FORWARD\n")
        req = {
            "chunk_name": chunk_name,
            "target_host": dest_host,
            "target_port": dest_port
        }
        data = json.dumps(req).encode("utf-8")
        s.sendall(struct.pack(">I", len(data)) + data)
        
        ack = s.recv(1024).decode("utf-8").strip()
        s.close()
        
        if ack == "OK":
            return True
        else:
            log.error(f"[REPLICATION_FAIL] Source returned: {ack}")
            return False
    except Exception as e:
        log.error(f"[REPLICATION_FAIL] Exception during FORWARD to {source_host}:{source_port}: {e}")
        return False

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
            for file_id, data in metadata.items():
                all_chunks_missing = True
                for rec in data["chunks"]:
                    key = (file_id, rec["chunk_id"])
                    holders = chunk_locations.get(key, set())
                    live_holders = holders & live
                    if len(live_holders) > 0:
                        all_chunks_missing = False
                        # Only heal if at least 1 live holder exists (can replicate from it)
                        if len(live_holders) < REPLICATION:
                            source_dn_id = list(live_holders)[0]
                            source_host, source_port = datanode_host_port(source_dn_id)
                            
                            candidates = [dn for dn in DATANODES.keys() if dn not in holders and dn in live]
                            for dest_dn_id in candidates[:REPLICATION - len(live_holders)]:
                                dest_host, dest_port = datanode_host_port(dest_dn_id)
                                log.info(f"[HEAL_START] Orchestrating replication of {rec['chunk_name']} from {source_dn_id} to {dest_dn_id}")
                                
                                success = trigger_replication(source_host, source_port, dest_host, dest_port, rec['chunk_name'])
                                if success:
                                    rec["replicas"].append((dest_host, dest_port))
                                    log.info(f"[HEAL_SUCCESS] {rec['chunk_name']} successfully replicated to {dest_dn_id}")
                                else:
                                    log.warning(f"[HEAL_FAIL] Failed to replicate {rec['chunk_name']} to {dest_dn_id}")
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
                    stale_files.append(file_id)
            # Remove files with no chunks on any live Datanode
            for fid in stale_files:
                del metadata[fid]
                log.info(f"[CLEANUP] Removed stale metadata for '{fid}' (confirmed: no chunks on any live Datanode)")
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
