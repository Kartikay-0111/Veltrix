# Veltrix: Complete Setup & Monitoring Guide

## 📋 Table of Contents
1. [Prerequisites](#prerequisites)
2. [Starting the Stack](#starting-the-stack)
3. [Monitoring & Logs](#monitoring--logs)
4. [Database Operations](#database-operations)
5. [API Testing](#api-testing)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### System Requirements
- Docker & Docker Compose (latest versions)
- Python 3.12+ (for local development, optional)
- curl or Postman (for testing APIs)

### Installation
```bash
# Verify Docker & Compose are installed
docker --version
docker compose version

# Clone/navigate to the Veltrix repo
cd ~/Desktop/Projects/iicpc/veltrix
```

### Environment Setup
The `.env` file is pre-configured with test credentials:
```bash
cat .env
```

Expected output:
```
POSTGRES_USER=iicpc
POSTGRES_PASSWORD=iicpc_secret
POSTGRES_DB=iicpc_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

REDIS_HOST=redis
REDIS_PORT=6379

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_HOST=minio
MINIO_PORT=9000
MINIO_BUCKET=submissions

SANDBOX_NETWORK=sandbox-net
```

---

## Starting the Stack

### 1. Full Stack Startup
```bash
# Build and start all services in detached mode
docker compose up -d --build

# Expected output:
# ✔ Network veltrix_backend                 Created
# ✔ Network veltrix_sandbox-net             Created
# ✔ Container veltrix-postgres-1            Started
# ✔ Container veltrix-redis-1               Started
# ✔ Container veltrix-minio-1               Started
# ✔ Container veltrix-submission-service-1  Started
# ✔ Container veltrix-sandbox-manager-1     Started
```

### 2. Check All Services Are Running
```bash
# List all running containers
docker compose ps

# Expected output:
# NAME                          STATUS           PORTS
# veltrix-postgres-1           Up (healthy)     0.0.0.0:5432->5432/tcp
# veltrix-redis-1              Up (healthy)     0.0.0.0:6379->6379/tcp
# veltrix-minio-1              Up (healthy)     0.0.0.0:9000->9000/tcp, 0.0.0.0:9001->9001/tcp
# veltrix-submission-service-1 Up               0.0.0.0:8080->8080/tcp
# veltrix-sandbox-manager-1    Up               (no ports exposed)
```

### 3. Start Individual Services (Optional)
```bash
# Start only specific services
docker compose up -d postgres redis minio

# Start submission service only
docker compose up -d submission-service

# Start sandbox manager only
docker compose up -d sandbox-manager
```

### 4. Rebuild Specific Services
```bash
# Rebuild and restart submission-service
docker compose up -d --build submission-service

# Rebuild and restart sandbox-manager
docker compose up -d --build sandbox-manager

# Rebuild everything
docker compose up -d --build
```

---

## Monitoring & Logs

### 🐘 PostgreSQL Logs

**View real-time logs:**
```bash
# Stream postgres logs (Ctrl+C to exit)
docker compose logs -f postgres

# View last 50 lines
docker compose logs postgres --tail=50

# Follow with timestamps
docker compose logs -f --timestamps postgres
```

**Health check:**
```bash
# Quick health check
docker compose exec postgres pg_isready -U iicpc -d iicpc_db

# Expected output: accepting connections
```

---

### 🔴 Redis Logs

**View real-time logs:**
```bash
# Stream redis logs
docker compose logs -f redis

# View last 50 lines
docker compose logs redis --tail=50
```

**Live monitoring (with commands):**
```bash
# Connect to redis CLI
docker compose exec redis redis-cli

# Once inside redis-cli:
# Monitor all commands in real-time
> MONITOR

# View all keys
> KEYS *

# Get submission queue jobs
> LLEN submission_queue
> LRANGE submission_queue 0 -1

# View Redis server info
> INFO

# Exit redis-cli
> EXIT
```

**Example workflow:**
```bash
# Terminal 1: Start monitoring
docker compose exec redis redis-cli MONITOR

# Terminal 2: Submit a job (see next section)
curl -X POST http://localhost:8080/submit -F "file=@submission.zip" \
  -H "x-api-key: test-api-key-1234"

# Terminal 1: Observe job appearing in redis
```

---

### 📦 MinIO (Object Storage) Logs

**View real-time logs:**
```bash
# Stream minio logs
docker compose logs -f minio

# View last 50 lines
docker compose logs minio --tail=50
```

**Web console access:**
```
URL: http://localhost:9001
Username: minioadmin
Password: minioadmin123
```

**MinIO CLI commands:**
```bash
# Connect to minio container
docker compose exec minio mc ls local/

# List all buckets
docker compose exec minio mc ls local/

# View bucket contents
docker compose exec minio mc ls local/submissions/

# Remove a submission
docker compose exec minio mc rm "local/submissions/team-id/submission-id/*"
```

---

### 🚀 Submission Service Logs

**View real-time logs:**
```bash
# Stream submission-service logs
docker compose logs -f submission-service

# View last 100 lines with timestamps
docker compose logs -f --timestamps submission-service --tail=100

# View only errors
docker compose logs submission-service 2>&1 | grep ERROR
```

**API health check:**
```bash
# Check if submission-service is healthy
curl http://localhost:8080/health

# Expected output:
# {"status":"ok"}
```

**Live API monitoring:**
```bash
# Terminal 1: Monitor logs
docker compose logs -f submission-service

# Terminal 2: Make an API call
curl -X POST http://localhost:8080/submit \
  -F "file=@test.zip" \
  -H "x-api-key: test-api-key-1234"

# Terminal 1: Observe the upload happening
```

---

### 🏗️ Sandbox Manager Logs

**View real-time logs:**
```bash
# Stream sandbox-manager logs
docker compose logs -f sandbox-manager

# View last 100 lines
docker compose logs sandbox-manager --tail=100

# Follow with full timestamps
docker compose logs -f --timestamps sandbox-manager
```

**Key log patterns to look for:**
```
[INFO] Sandbox Manager started. Waiting for jobs...
[INFO] Got job: <submission-id>
[INFO] Processing <submission-id> (cpp|rust|go)
[INFO] Waiting for sandbox to bind on port 9999...
[INFO] Sandbox READY → http://host.docker.internal:<port>
```

**Container monitoring:**
```bash
# View running sandbox containers
docker ps --filter "name=sandbox-"

# View logs from a specific sandbox
docker logs sandbox-<submission-id-first-8-chars>

# Inspect a sandbox container
docker inspect sandbox-<submission-id-first-8-chars>
```

---

### 📊 View All Logs at Once
```bash
# Stream all service logs with timestamps and color
docker compose logs -f --timestamps

# View all logs from the last 5 minutes
docker compose logs --since 5m

# Filter for ERROR level across all services
docker compose logs 2>&1 | grep -i error
```

---

## Database Operations

### PostgreSQL Connection

**Direct psql access:**
```bash
# Connect to the database
docker compose exec postgres psql -U iicpc -d iicpc_db

# Once connected, you're in psql prompt:
postgres=# 
```

### Common psql Commands

**View all tables:**
```sql
-- Inside psql:
\dt

-- Expected output:
--  public | teams          | table | iicpc
--  public | submissions    | table | iicpc
--  public | benchmark_jobs | table | iicpc
```

**Check teams:**
```sql
SELECT * FROM teams;

-- Expected output:
--  id                   | name      | email            | api_key
--  <uuid>               | Test Team | test@iicpc.dev   | test-api-key-1234
```

**Check submissions:**
```sql
SELECT id, team_id, language, status, endpoint_url FROM submissions;

-- Example output:
--  id                   | team_id              | language | status | endpoint_url
--  <submission-uuid>    | <team-uuid>          | cpp      | READY  | http://host.docker.internal:32768
```

**Check submission by status:**
```sql
SELECT id, status, error_message, created_at FROM submissions 
WHERE status = 'FAILED_STARTUP' 
ORDER BY created_at DESC;
```

**Count submissions by status:**
```sql
SELECT status, COUNT(*) as count FROM submissions GROUP BY status;
```

**Check benchmark jobs:**
```sql
SELECT * FROM benchmark_jobs;
```

**Monitor sandbox manager queries:**
```sql
-- View all PENDING submissions (what the sandbox manager is waiting for)
SELECT id, team_id, language, status FROM submissions WHERE status = 'PENDING';

-- View all READY sandboxes (ready for bot testing)
SELECT id, language, endpoint_url FROM submissions WHERE status = 'READY';

-- View all failed submissions
SELECT id, status, error_message FROM submissions 
WHERE status LIKE 'FAILED%';
```

**Exit psql:**
```sql
\q
```

### Full psql Connection Example

```bash
# One-liner to check if database is initialized
docker compose exec postgres psql -U iicpc -d iicpc_db -c "SELECT COUNT(*) as team_count FROM teams;"

# Full interactive session
docker compose exec postgres psql -U iicpc -d iicpc_db

# Then run queries:
SELECT * FROM teams;
SELECT COUNT(*) FROM submissions;
\dt
\q
```

---

## API Testing

### 1. Test Submission API

**Get API key (already set in .env):**
```
API_KEY: test-api-key-1234
```

**Create a test submission:**

First, create a simple test file:
```bash
# Create a minimal C++ server for testing
mkdir -p /tmp/test-submission
cd /tmp/test-submission
cat > main.cpp << 'EOF'
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

int main() {
    int server = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {AF_INET, htons(9999), INADDR_ANY};
    bind(server, (struct sockaddr*)&addr, sizeof(addr));
    listen(server, 1);
    std::cout << "Server listening on port 9999\n";
    while(true) {
        int client = accept(server, nullptr, nullptr);
        close(client);
    }
    return 0;
}
EOF

# Create CMakeLists.txt
cat > CMakeLists.txt << 'EOF'
cmake_minimum_required(VERSION 3.10)
project(server)
add_executable(server main.cpp)
EOF

# Create a zip archive
zip -r submission.zip CMakeLists.txt main.cpp
```

**Submit the file:**
```bash
curl -X POST http://localhost:8080/submit \
  -F "file=@/tmp/test-submission/submission.zip" \
  -F "language=cpp" \
  -H "x-api-key: test-api-key-1234"

# Expected response:
# {
#   "submission_id": "12345678-1234-1234-1234-123456789abc",
#   "status": "PENDING",
#   "message": "Submission received. Container will be ready shortly."
# }
```

**Poll for status:**
```bash
# Replace <submission_id> with the ID from above
SUBMISSION_ID="12345678-1234-1234-1234-123456789abc"

curl http://localhost:8080/submission/$SUBMISSION_ID \
  -H "x-api-key: test-api-key-1234"

# Keep polling (takes ~10-15 seconds):
# Status flow: PENDING → BUILDING → READY
```

### 2. Monitor Submission In Real-Time

**Terminal 1: Watch submission-service**
```bash
docker compose logs -f submission-service
```

**Terminal 2: Watch sandbox-manager**
```bash
docker compose logs -f sandbox-manager
```

**Terminal 3: Monitor Redis queue**
```bash
docker compose exec redis redis-cli MONITOR
```

**Terminal 4: Make the API call**
```bash
curl -X POST http://localhost:8080/submit \
  -F "file=@/tmp/test-submission/submission.zip" \
  -F "language=cpp" \
  -H "x-api-key: test-api-key-1234"
```

**Observe:**
- Terminal 1: File upload and storage
- Terminal 3: Job pushed to Redis queue
- Terminal 2: Job picked up and processed
- Terminal 2: Image built, container started, port opened

---

## Troubleshooting

### Check Overall Stack Health
```bash
# Show all containers and their status
docker compose ps

# Show only unhealthy containers
docker compose ps | grep -v healthy

# Show detailed stats
docker stats --all
```

### Common Issues

**1. Postgres won't start**
```bash
# Check logs
docker compose logs postgres

# If database doesn't exist:
docker compose exec postgres psql -U iicpc -d postgres \
  -c "CREATE DATABASE iicpc_db;"

# Re-apply schema
docker compose exec postgres psql -U iicpc -d iicpc_db < db/init.sql
```

**2. Redis connection error**
```bash
# Test redis connectivity
docker compose exec redis redis-cli ping
# Expected: PONG

# Check if queue has jobs
docker compose exec redis redis-cli LLEN submission_queue
```

**3. MinIO not accessible**
```bash
# Check minio health
curl http://localhost:9000/minio/health/live

# Check bucket creation
docker compose exec minio mc ls local/submissions/
```

**4. Submission service won't connect to database**
```bash
# Check network
docker network ls | grep veltrix

# Check if postgres is in the network
docker network inspect veltrix_backend
```

**5. Sandbox manager not picking up jobs**
```bash
# Check if sandbox-manager is running
docker compose ps sandbox-manager

# Check logs for errors
docker compose logs sandbox-manager --tail=100

# Manually trigger a job (from terminal 1 of monitoring section)
# and check if sandbox-manager picks it up
```

### Nuclear Option: Fresh Start
```bash
# Stop all containers
docker compose down

# Remove all volumes (WARNING: deletes all data)
docker compose down -v

# Remove images
docker compose down --rmi all

# Start fresh
docker compose up -d --build
```

---

## Complete Monitoring Dashboard

Create this shell script to monitor everything:

```bash
#!/bin/bash
# save as: monitor.sh
# usage: chmod +x monitor.sh && ./monitor.sh

echo "🟢 Container Status:"
docker compose ps

echo -e "\n🔴 Recent Errors:"
docker compose logs --tail=20 2>&1 | grep -i error || echo "No errors"

echo -e "\n📊 Submissions:"
docker compose exec postgres psql -U iicpc -d iicpc_db -c \
  "SELECT status, COUNT(*) FROM submissions GROUP BY status;"

echo -e "\n⏳ Pending jobs in Redis:"
docker compose exec redis redis-cli LLEN submission_queue

echo -e "\n✅ All systems operational!"
```

Run it:
```bash
chmod +x monitor.sh
./monitor.sh
```

---

## Quick Reference Commands

| Component | View Logs | Health Check | CLI Access |
|-----------|-----------|--------------|------------|
| **Postgres** | `docker compose logs -f postgres` | `docker compose exec postgres pg_isready` | `docker compose exec postgres psql -U iicpc -d iicpc_db` |
| **Redis** | `docker compose logs -f redis` | `docker compose exec redis redis-cli ping` | `docker compose exec redis redis-cli` |
| **MinIO** | `docker compose logs -f minio` | `curl localhost:9000/minio/health/live` | `docker compose exec minio mc ls local/` |
| **Submission Service** | `docker compose logs -f submission-service` | `curl localhost:8080/health` | `docker compose exec submission-service bash` |
| **Sandbox Manager** | `docker compose logs -f sandbox-manager` | Check logs | `docker compose exec sandbox-manager bash` |

---

## Next Steps

1. **Start the stack**: `docker compose up -d --build`
2. **Verify health**: `docker compose ps`
3. **Test API**: Follow API Testing section
4. **Monitor in real-time**: Use the Monitoring section
5. **Inspect database**: Use psql commands to verify data

---

**Happy benchmarking! 🚀**
