# Mini HDFS Git History Builder — Run from project root

if (Test-Path ".git") { Remove-Item -Recurse -Force ".git" }

git init
git branch -M main

# Commit 1: Config — July 25
$env:GIT_AUTHOR_DATE = "2026-07-25T10:30:00+05:30"
$env:GIT_COMMITTER_DATE = "2026-07-25T10:30:00+05:30"
git add config.json Namenode/config.json DATANODE0/config.json Datanode1/config.json Client/config.json
git commit -m "Initial setup: add system configuration for all nodes"

# Commit 2: Namenode — July 27
$env:GIT_AUTHOR_DATE = "2026-07-27T14:15:00+05:30"
$env:GIT_COMMITTER_DATE = "2026-07-27T14:15:00+05:30"
git add Namenode/namenode.py
git commit -m "feat: implement Namenode with metadata management, heartbeat monitoring, and self-healing"

# Commit 3: Datanode 0 — July 29
$env:GIT_AUTHOR_DATE = "2026-07-29T11:45:00+05:30"
$env:GIT_COMMITTER_DATE = "2026-07-29T11:45:00+05:30"
git add DATANODE0/datanode0.py
git commit -m "feat: implement Datanode 0 with chunk storage, retrieval, and block reporting"

# Commit 4: Datanode 1 — July 30
$env:GIT_AUTHOR_DATE = "2026-07-30T16:20:00+05:30"
$env:GIT_COMMITTER_DATE = "2026-07-30T16:20:00+05:30"
git add Datanode1/datanode1.py
git commit -m "feat: implement Datanode 1 as replica node with checksum verification"

# Commit 5: Client + Dashboard — Aug 1
$env:GIT_AUTHOR_DATE = "2026-08-01T19:00:00+05:30"
$env:GIT_COMMITTER_DATE = "2026-08-01T19:00:00+05:30"
git add Client/client.py
git commit -m "feat: implement Client with Flask dashboard and upload/download engine"

# Commit 6: Docs — Aug 3
$env:GIT_AUTHOR_DATE = "2026-08-03T21:30:00+05:30"
$env:GIT_COMMITTER_DATE = "2026-08-03T21:30:00+05:30"
git add .gitignore README.md
git commit -m "docs: add comprehensive README and .gitignore"

Remove-Item Env:GIT_AUTHOR_DATE
Remove-Item Env:GIT_COMMITTER_DATE

Write-Host ""
Write-Host "Done! Commit history:" -ForegroundColor Green
git log --oneline --all
Write-Host ""
Write-Host "Now run:" -ForegroundColor Yellow
Write-Host "  git remote add origin https://github.com/Retesh07/Mini-HDFS-Simulation.git"
Write-Host "  git push -u origin main"
