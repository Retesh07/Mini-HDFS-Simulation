import socket
import json
import os
import hashlib
import struct
import logging
import threading
import time
import uuid
from flask import Flask, render_template_string, request, jsonify, send_file
from threading import Thread

# --- Logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | CLIENT | %(message)s",
)
log = logging.getLogger("client")

# --- Load configuration file ---
with open("config.json") as f:
    CONFIG = json.load(f)

NAMENODE_HOST = CONFIG["namenode"]["host"]
NAMENODE_PORT = CONFIG["namenode"]["client_port"]
CHUNK_SIZE = int(CONFIG["chunk_size_mb"]) * 1024 * 1024  # bytes

# --- Framed JSON helpers ---
def send_json(sock, obj, who="namenode"):
    data = json.dumps(obj).encode("utf-8")
    header = struct.pack(">I", len(data))
    sock.sendall(header + data)
    log.debug(f"[SEND → {who}] {len(data)} bytes | {obj}")

def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

def recv_json(sock, who="namenode"):
    hdr = recv_exact(sock, 4)
    if not hdr:
        return None
    (length,) = struct.unpack(">I", hdr)
    body = recv_exact(sock, length)
    if not body:
        return None
    obj = json.loads(body.decode("utf-8"))
    log.debug(f"[RECV ← {who}] {obj}")
    return obj

def send_to_namenode(message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        log.info(f"Connecting to Namenode {NAMENODE_HOST}:{NAMENODE_PORT} ...")
        s.connect((NAMENODE_HOST, NAMENODE_PORT))
        send_json(s, message, who="namenode")
        return recv_json(s, who="namenode")
    except Exception as e:
        log.error(f"Error communicating with Namenode: {e}")
        return None
    finally:
        s.close()

# --- Upload helpers ---
def split_file(filename):
    chunks, checksums = [], []
    with open(filename, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
            checksums.append(hashlib.md5(chunk).hexdigest())
    return chunks, checksums

def send_chunk(target_host, target_port, chunk_name, data, chunk_index, total_chunks, replicas=None):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        log.info(f"Uploading chunk {chunk_index+1}/{total_chunks} ({chunk_name}) to {target_host}:{target_port} (Pipeline: {replicas})")
        s.connect((target_host, target_port))
        s.sendall(b"STORE\n")
        header = {"chunk_name": chunk_name, "size": len(data), "replicas": replicas or []}
        payload = json.dumps(header).encode("utf-8")
        s.sendall(struct.pack(">I", len(payload)) + payload)
        ack = s.recv(32)
        if ack != b"READY":
            log.error(f"Datanode didn't acknowledge READY for {chunk_name}")
            return False
        s.sendall(data)
        
        final_ack = s.recv(32)
        if final_ack == b"OK":
            log.info(f"✅ Sent {chunk_name} successfully")
            return True
        else:
            log.error(f"Pipeline failed for {chunk_name}: {final_ack}")
            return False
    except Exception as e:
        log.error(f"Error sending chunk {chunk_name}: {e}")
        return False
    finally:
        s.close()

def upload_file(filename):
    if not os.path.exists(filename):
        log.error(f"File not found: {filename}")
        return
    chunks, checksums = split_file(filename)
    num_chunks = len(chunks)
    log.info(f"Split into {num_chunks} chunks")

    file_id = str(uuid.uuid4())
    resp = send_to_namenode({"action": "upload_request", "filename": os.path.basename(filename), "file_id": file_id, "num_chunks": num_chunks})
    if not resp or resp.get("status") != "ok":
        log.error("Upload request failed.")
        return

    plan = resp["plan"]
    for i, chunk_info in enumerate(plan):
        data = chunks[i]
        replicas = chunk_info["datanodes"]
        if not replicas:
            continue
            
        current_host, current_port = replicas[0]
        remaining_replicas = replicas[1:]
        
        success = send_chunk(current_host, current_port, chunk_info["chunk_name"], data, i, num_chunks, replicas=remaining_replicas)
        
        # Retry loop if upload fails
        max_retries = 3
        attempts = 0
        while not success and attempts < max_retries:
            attempts += 1
            log.warning(f"Chunk pipeline upload failed at {current_host}:{current_port}. Requesting replacement datanode...")
            req = {
                "action": "chunk_upload_failed",
                "file_id": file_id,
                "chunk_id": i,
                "failed_datanode": [current_host, current_port]
            }
            fallback_resp = send_to_namenode(req)
            
            if fallback_resp and fallback_resp.get("status") == "ok":
                new_dn = fallback_resp["replacement_datanode"]
                
                # Update replicas list
                replicas = [r for r in replicas if not (r[0] == current_host and int(r[1]) == int(current_port))]
                replicas.append((new_dn[0], int(new_dn[1])))
                
                current_host, current_port = replicas[0]
                remaining_replicas = replicas[1:]
                
                log.info(f"Retrying pipeline upload of {chunk_info['chunk_name']} starting at datanode {current_host}:{current_port}")
                success = send_chunk(current_host, current_port, chunk_info["chunk_name"], data, i, num_chunks, replicas=remaining_replicas)
            else:
                error_msg = fallback_resp.get("message", "Unknown error") if fallback_resp else "Namenode connection failed"
                log.error(f"Failed to get replacement datanode: {error_msg}")
                break

    commit = send_to_namenode({"action": "commit_upload", "file_id": file_id})
    if commit and commit.get("status") == "ok":
        log.info("✅ Upload complete and committed.")
    else:
        log.warning("Upload complete but commit not confirmed.")

# --- Download helpers ---
def download_file(file_id, output_path):
    meta = send_to_namenode({"action": "download_request", "file_id": file_id})
    if not meta:
        error_msg = "Could not connect to Namenode. Is it running?"
        log.error(f"Download failed: {error_msg}")
        return error_msg
    if meta.get("status") != "ok":
        error_msg = meta.get("message", "Download request failed")
        log.error(f"Download failed: {error_msg}")
        return error_msg
    chunks_meta = meta["metadata"]
    with open(output_path, "wb") as out:
        for chunk in chunks_meta:
            host, port = chunk["datanodes"][0]
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((host, port))
                s.sendall(b"GET\n")
                header = {"chunk_name": chunk["chunk_name"]}
                data = json.dumps(header).encode("utf-8")
                s.sendall(struct.pack(">I", len(data)) + data)
                buf = b""
                while True:
                    packet = s.recv(65536)
                    if not packet:
                        break
                    buf += packet
                out.write(buf)
            finally:
                s.close()
    log.info(f"✅ File reconstructed as {output_path}")
    return None  # success

# --- Flask Dashboard ---
app = Flask(__name__)
LOG_BUFFER = []

class LogCaptureHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        LOG_BUFFER.append(msg)
        if len(LOG_BUFFER) > 200:
            LOG_BUFFER.pop(0)
log.addHandler(LogCaptureHandler())

HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Mini HDFS — Dashboard</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg-primary: #0f1117;
      --bg-secondary: #1a1d27;
      --bg-card: #1e2130;
      --bg-input: #252838;
      --border: #2d3148;
      --text-primary: #e4e6f0;
      --text-secondary: #8b8fa8;
      --text-muted: #5c6080;
      --accent-blue: #4f8cff;
      --accent-green: #34d399;
      --accent-red: #f87171;
      --accent-amber: #fbbf24;
      --accent-purple: #a78bfa;
      --gradient-blue: linear-gradient(135deg, #4f8cff 0%, #6366f1 100%);
      --gradient-green: linear-gradient(135deg, #34d399 0%, #10b981 100%);
      --shadow: 0 4px 24px rgba(0,0,0,0.3);
      --radius: 12px;
    }

    body {
      font-family: 'Inter', -apple-system, sans-serif;
      background: var(--bg-primary);
      color: var(--text-primary);
      min-height: 100vh;
      line-height: 1.6;
    }

    /* Header */
    .header {
      background: var(--bg-secondary);
      border-bottom: 1px solid var(--border);
      padding: 20px 0;
    }
    .header-inner {
      max-width: 1200px;
      margin: 0 auto;
      padding: 0 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .logo {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .logo-icon {
      width: 40px; height: 40px;
      background: var(--gradient-blue);
      border-radius: 10px;
      display: flex; align-items: center; justify-content: center;
      font-size: 20px;
    }
    .logo h1 {
      font-size: 20px;
      font-weight: 700;
      letter-spacing: -0.5px;
    }
    .logo span { color: var(--accent-blue); }
    .header-badge {
      font-size: 12px;
      color: var(--text-muted);
      background: var(--bg-card);
      padding: 6px 12px;
      border-radius: 20px;
      border: 1px solid var(--border);
    }

    /* Main */
    .main {
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px;
    }

    /* Status cards row */
    .status-row {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }
    .stat-card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 20px;
      transition: transform 0.2s, border-color 0.2s;
    }
    .stat-card:hover { transform: translateY(-2px); border-color: var(--accent-blue); }
    .stat-label {
      font-size: 12px;
      font-weight: 500;
      text-transform: uppercase;
      letter-spacing: 0.8px;
      color: var(--text-muted);
      margin-bottom: 8px;
    }
    .stat-value {
      font-size: 28px;
      font-weight: 700;
      letter-spacing: -1px;
    }
    .stat-value.green { color: var(--accent-green); }
    .stat-value.red { color: var(--accent-red); }
    .stat-value.blue { color: var(--accent-blue); }
    .stat-value.amber { color: var(--accent-amber); }

    /* Grid layout */
    .grid-2col {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 24px;
      margin-bottom: 24px;
    }
    @media (max-width: 768px) {
      .grid-2col { grid-template-columns: 1fr; }
    }

    /* Cards */
    .card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      overflow: hidden;
    }
    .card-header {
      padding: 16px 20px;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .card-header h2 {
      font-size: 15px;
      font-weight: 600;
    }
    .card-icon {
      width: 32px; height: 32px;
      border-radius: 8px;
      display: flex; align-items: center; justify-content: center;
      font-size: 16px;
    }
    .card-icon.upload-icon { background: rgba(52, 211, 153, 0.15); }
    .card-icon.download-icon { background: rgba(79, 140, 255, 0.15); }
    .card-icon.nodes-icon { background: rgba(251, 191, 36, 0.15); }
    .card-icon.files-icon { background: rgba(167, 139, 250, 0.15); }
    .card-icon.logs-icon { background: rgba(248, 113, 113, 0.15); }
    .card-body { padding: 20px; }

    /* Form elements */
    .file-input-wrapper {
      border: 2px dashed var(--border);
      border-radius: 10px;
      padding: 28px;
      text-align: center;
      cursor: pointer;
      transition: border-color 0.2s, background 0.2s;
      margin-bottom: 14px;
    }
    .file-input-wrapper:hover { border-color: var(--accent-green); background: rgba(52, 211, 153, 0.05); }
    .file-input-wrapper.active { border-color: var(--accent-green); background: rgba(52, 211, 153, 0.08); }
    .file-input-wrapper input[type="file"] { display: none; }
    .file-input-wrapper .upload-label {
      font-size: 14px;
      color: var(--text-secondary);
    }
    .file-input-wrapper .upload-label strong { color: var(--accent-green); }
    .file-input-wrapper .file-name {
      margin-top: 8px;
      font-size: 13px;
      color: var(--accent-green);
      font-weight: 500;
    }

    input[type="text"] {
      width: 100%;
      padding: 12px 16px;
      background: var(--bg-input);
      border: 1px solid var(--border);
      border-radius: 8px;
      color: var(--text-primary);
      font-size: 14px;
      font-family: 'Inter', sans-serif;
      outline: none;
      transition: border-color 0.2s;
      margin-bottom: 14px;
    }
    input[type="text"]:focus { border-color: var(--accent-blue); }
    input[type="text"]::placeholder { color: var(--text-muted); }

    .btn {
      width: 100%;
      padding: 12px;
      border: none;
      border-radius: 8px;
      font-size: 14px;
      font-weight: 600;
      font-family: 'Inter', sans-serif;
      cursor: pointer;
      transition: transform 0.1s, opacity 0.2s;
      color: #fff;
      letter-spacing: 0.3px;
    }
    .btn:hover { opacity: 0.9; }
    .btn:active { transform: scale(0.98); }
    .btn-upload { background: var(--gradient-green); }
    .btn-download { background: var(--gradient-blue); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }

    .status-msg {
      margin-top: 12px;
      font-size: 13px;
      min-height: 20px;
      text-align: center;
    }
    .status-msg.success { color: var(--accent-green); }
    .status-msg.error { color: var(--accent-red); }
    .status-msg.pending { color: var(--accent-amber); }

    /* Node list */
    .node-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 16px;
      border-bottom: 1px solid var(--border);
    }
    .node-item:last-child { border-bottom: none; }
    .node-left { display: flex; align-items: center; gap: 12px; }
    .node-dot {
      width: 10px; height: 10px;
      border-radius: 50%;
    }
    .node-dot.alive {
      background: var(--accent-green);
      box-shadow: 0 0 8px rgba(52, 211, 153, 0.5);
      animation: pulse-green 2s infinite;
    }
    .node-dot.dead {
      background: var(--accent-red);
      box-shadow: 0 0 8px rgba(248, 113, 113, 0.5);
    }
    @keyframes pulse-green {
      0%, 100% { box-shadow: 0 0 4px rgba(52, 211, 153, 0.4); }
      50% { box-shadow: 0 0 12px rgba(52, 211, 153, 0.7); }
    }
    .node-name { font-weight: 600; font-size: 14px; }
    .node-addr { font-size: 12px; color: var(--text-muted); font-family: monospace; }
    .node-status { font-size: 12px; font-weight: 500; padding: 4px 10px; border-radius: 20px; }
    .node-status.online { background: rgba(52,211,153,0.12); color: var(--accent-green); }
    .node-status.offline { background: rgba(248,113,113,0.12); color: var(--accent-red); }

    /* File list */
    .file-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 16px;
      border-bottom: 1px solid var(--border);
    }
    .file-item:last-child { border-bottom: none; }
    .file-info { display: flex; align-items: center; gap: 10px; }
    .file-icon { font-size: 18px; }
    .file-name-text { font-size: 14px; font-weight: 500; }
    .file-chunks {
      font-size: 12px;
      color: var(--text-muted);
    }
    .file-download-btn {
      background: rgba(79,140,255,0.12);
      color: var(--accent-blue);
      border: none;
      padding: 6px 14px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      font-family: 'Inter', sans-serif;
      transition: background 0.2s;
    }
    .file-download-btn:hover { background: rgba(79,140,255,0.25); }
    .empty-state {
      padding: 40px;
      text-align: center;
      color: var(--text-muted);
      font-size: 14px;
    }

    /* Logs */
    .log-area {
      background: var(--bg-primary);
      color: var(--accent-green);
      font-family: 'Courier New', monospace;
      font-size: 12px;
      line-height: 1.7;
      padding: 16px;
      height: 280px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-all;
    }
    .log-area::-webkit-scrollbar { width: 6px; }
    .log-area::-webkit-scrollbar-track { background: transparent; }
    .log-area::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
  </style>
</head>
<body>

  <!-- Header -->
  <div class="header">
    <div class="header-inner">
      <div class="logo">
        <div class="logo-icon">💾</div>
        <h1>Mini <span>HDFS</span></h1>
      </div>
      <div class="header-badge">Distributed File System Dashboard</div>
    </div>
  </div>

  <!-- Main Content -->
  <div class="main">

    <!-- Status Cards -->
    <div class="status-row">
      <div class="stat-card">
        <div class="stat-label">Datanodes Online</div>
        <div class="stat-value green" id="nodesOnline">—</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Files</div>
        <div class="stat-value blue" id="totalFiles">—</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Chunk Size</div>
        <div class="stat-value amber">2 MB</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Replication Factor</div>
        <div class="stat-value" style="color: var(--accent-purple);">2</div>
      </div>
    </div>

    <!-- Upload Row -->
    <div style="margin-bottom: 24px;">

      <!-- Upload -->
      <div class="card">
        <div class="card-header">
          <div class="card-icon upload-icon">📤</div>
          <h2>Upload File</h2>
        </div>
        <div class="card-body">
          <form id="uploadForm" enctype="multipart/form-data">
            <div class="file-input-wrapper" id="dropZone" onclick="document.getElementById('fileInput').click()">
              <input type="file" name="file" id="fileInput" required>
              <div class="upload-label">Click to <strong>browse</strong> or drag a file here</div>
              <div class="file-name" id="selectedFile"></div>
            </div>
            <button type="button" class="btn btn-upload" id="uploadBtn" onclick="uploadFile()">Upload to HDFS</button>
          </form>
          <div class="status-msg" id="uploadStatus"></div>
        </div>
      </div>
    </div>

    <!-- Nodes / Files Row -->
    <div class="grid-2col">

      <!-- Datanodes -->
      <div class="card">
        <div class="card-header">
          <div class="card-icon nodes-icon">🖥️</div>
          <h2>Datanode Health</h2>
        </div>
        <div id="nodesList" style="min-height:100px;">
          <div class="empty-state">Loading...</div>
        </div>
      </div>

      <!-- Files -->
      <div class="card">
        <div class="card-header">
          <div class="card-icon files-icon">📁</div>
          <h2>Stored Files</h2>
        </div>
        <div id="filesList" style="min-height:100px;">
          <div class="empty-state">Loading...</div>
        </div>
      </div>
    </div>

    <!-- Logs -->
    <div class="card" style="margin-bottom: 24px;">
      <div class="card-header">
        <div class="card-icon logs-icon">📋</div>
        <h2>System Logs</h2>
      </div>
      <div class="log-area" id="logArea">Connecting...</div>
    </div>

  </div>

  <script>
    // File input display
    const fileInput = document.getElementById('fileInput');
    const dropZone = document.getElementById('dropZone');
    fileInput.addEventListener('change', function() {
      const name = this.files[0] ? this.files[0].name : '';
      document.getElementById('selectedFile').textContent = name ? '📎 ' + name : '';
      dropZone.classList.toggle('active', !!name);
    });

    // Upload
    async function uploadFile() {
      const form = document.getElementById('uploadForm');
      const fd = new FormData(form);
      const status = document.getElementById('uploadStatus');
      const btn = document.getElementById('uploadBtn');
      if (!fileInput.files[0]) { status.textContent = 'Please select a file first.'; status.className = 'status-msg error'; return; }
      btn.disabled = true;
      status.textContent = 'Uploading...';
      status.className = 'status-msg pending';
      try {
        const res = await fetch('/upload', { method: 'POST', body: fd });
        const msg = await res.json();
        status.textContent = '✅ ' + msg.message;
        status.className = 'status-msg success';
        fileInput.value = '';
        document.getElementById('selectedFile').textContent = '';
        dropZone.classList.remove('active');
      } catch(e) {
        status.textContent = '❌ Upload failed: ' + e;
        status.className = 'status-msg error';
      }
      btn.disabled = false;
    }

    // Quick download from file list
    function quickDownload(file_id, fname) {
      window.location.href = '/download?file_id=' + encodeURIComponent(file_id) + '&filename=' + encodeURIComponent(fname);
    }

    // Refresh dashboard
    async function refreshDashboard() {
      try {
        const [statusRes, logsRes] = await Promise.all([
          fetch('/api/status'),
          fetch('/logs')
        ]);
        const data = await statusRes.json();
        const logs = await logsRes.text();

        // Nodes
        if (data.nodes) {
          let online = 0;
          let html = '';
          for (const [id, info] of Object.entries(data.nodes)) {
            const alive = info.alive;
            if (alive) online++;
            html += `
              <div class="node-item">
                <div class="node-left">
                  <div class="node-dot ${alive ? 'alive' : 'dead'}"></div>
                  <div>
                    <div class="node-name">${id.toUpperCase()}</div>
                    <div class="node-addr">${info.host}:${info.port}</div>
                  </div>
                </div>
                <div class="node-status ${alive ? 'online' : 'offline'}">${alive ? 'Online' : 'Offline'}</div>
              </div>`;
          }
          document.getElementById('nodesList').innerHTML = html || '<div class="empty-state">No datanodes configured</div>';
          document.getElementById('nodesOnline').textContent = online;
        }

        // Files
        if (data.files) {
          const integrity = data.integrity || {};
          let html = '';
          let count = 0;
          for (const [file_id, fileData] of Object.entries(data.files)) {
            if (integrity[file_id] === false) continue;
            count++;
            const fname = fileData.original_filename;
            const numChunks = fileData.chunks.length;
            html += `
              <div class="file-item">
                <div class="file-info">
                  <div class="file-icon">📄</div>
                  <div>
                    <div class="file-name-text">${fname}</div>
                    <div class="file-chunks">${numChunks} chunk${numChunks !== 1 ? 's' : ''}</div>
                  </div>
                </div>
                <button class="file-download-btn" onclick="quickDownload('${file_id}', '${fname.replace(/'/g, "\\'")}')">Download</button>
              </div>`;
          }
          document.getElementById('filesList').innerHTML = html || '<div class="empty-state">No files uploaded yet</div>';
          document.getElementById('totalFiles').textContent = count;
        }

        // Logs
        const logArea = document.getElementById('logArea');
        logArea.textContent = logs || 'No logs yet.';
        logArea.scrollTop = logArea.scrollHeight;

      } catch(e) {
        console.error('Dashboard refresh error:', e);
      }
    }

    setInterval(refreshDashboard, 3000);
    refreshDashboard();
  </script>
</body>
</html>
"""

@app.route("/")
def dashboard():
    return render_template_string(HTML_PAGE)

@app.route("/upload", methods=["POST"])
def api_upload():
    file = request.files["file"]
    path = os.path.join(".", file.filename)
    file.save(path)
    Thread(target=upload_file, args=(path,), daemon=True).start()
    return jsonify({"message": f"Uploading {file.filename}..."})

@app.route("/download", methods=["GET"])
def api_download():
    file_id = request.args.get("file_id")
    filename = request.args.get("filename")
    
    if not file_id or not filename:
        return "File ID and Filename are required", 400
        
    output_path = os.path.join(".", f"reconstructed_{filename}")
    error = download_file(file_id, output_path)
    
    if error:
        return f"❌ {error}", 503
    
    if os.path.exists(output_path):
        return send_file(output_path, as_attachment=True, download_name=filename)
    else:
        return "Error: File could not be reconstructed", 500

@app.route("/api/status")
def api_status_json():
    status = send_to_namenode({"action": "system_status"})
    if not status:
        return jsonify({"nodes": {}, "files": {}, "integrity": {}})
    return jsonify(status)

@app.route("/logs")
def api_logs():
    return "\n".join(LOG_BUFFER[-100:])

@app.route("/status")
def api_status():
    status = send_to_namenode({"action": "system_status"})
    if not status:
        return "Unable to retrieve system status."
    return jsonify(status)

def start_dashboard():
    app.run(host="0.0.0.0", port=8080, debug=False)

# --- Main driver ---
if __name__ == "__main__":
    Thread(target=start_dashboard, daemon=True).start()
    print("🌐 Dashboard running at http://localhost:8080")
    while True:
        time.sleep(1)
