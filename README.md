# QueueCTL - Backend Developer Internship Assignment

`queuectl` is a minimal, production-grade background job queue system built in Java.  
It manages background jobs with worker processes, handles retries using exponential backoff,  
and maintains a Dead Letter Queue (DLQ) for permanently failed jobs.

All operations are accessible through a CLI, and job data is persisted in an SQLite database (`queue.db`).

**Demo Video:** https://drive.google.com/file/d/12HWfYGThOnfwJd0gQdGFdHKKJ7vsclg0/view?usp=drive_link

---

## Tech Stack

- **Language:** Java (JDK 11+)
- **Build Tool:** Gradle
- **CLI Library:** Picocli
- **Database:** SQLite (with WAL mode for concurrency)
- **Concurrency:** Java `ExecutorService` (Thread Pools)

---

## Features

- **Persistent Job Storage:** Jobs are stored in a `queue.db` SQLite file and survive restarts.
- **Multiple Worker Support:** Can run multiple worker threads in parallel (`worker --count N`).
- **Concurrency Safe:** Uses an atomic database transaction (`SELECT` → `UPDATE` → `FETCH`) to claim jobs safely.
- **Retry & Backoff:** Failed jobs retry automatically with exponential backoff (`base ^ attempts`).
- **Dead Letter Queue (DLQ):** Jobs are moved to the DLQ after exhausting all retries.
- **Crash Recovery:** A "reaper" automatically recovers stale jobs stuck in the `processing` state.
- **Configurable:** All parameters (`max_retries`, `backoff_base`, `job_timeout_seconds`, `lock_timeout_seconds`) are stored in a `config.properties` file.
- **Graceful Shutdown:** Workers finish active jobs before stopping.

---

## Setup & Run Instructions

### 1. Prerequisites

- Java JDK 11 or newer
- Gradle (or use the included Gradle wrapper)

---

### 2. Build the Project

Open your terminal (inside the project folder) and run:

```bash
# Build the project
./gradlew build
```

---

### 3. Run the Application

You can run `queuectl` using Gradle and pass arguments using `--args`.

#### Show Help Menu
```bash
./gradlew run --args="--help"
```

#### Enqueue Jobs
```bash
# Enqueue a successful job
./gradlew run --args="enqueue 'echo Job success'"

# Enqueue a job that will fail (invalid command)
./gradlew run --args="enqueue 'badcommand'"
```

#### Start Workers
```bash
# Start 3 workers
./gradlew run --args="worker --count 3"
```

#### List Jobs
```bash
# List all pending jobs
./gradlew run --args="list --state pending"

# List all completed jobs
./gradlew run --args="list --state completed" 
```

#### Check Job Status
```bash
./gradlew run --args="status"
```

#### Manage Dead Letter Queue
```bash
# List all jobs in DLQ
./gradlew run --args="dlq list"

# Retry a failed job
./gradlew run --args="dlq retry [job-id-here]"
```

#### Configure System Settings
```bash
# Set max retry count to 5
./gradlew run --args="config set max_retries 5"

# Get current config value
./gradlew run --args="config get max_retries"
```

---

## Configuration

The system auto-creates a `config.properties` file with defaults:

| Key | Default | Description |
|------|----------|-------------|
| `max_retries` | 3 | Retry attempts before job moves to DLQ. |
| `backoff_base` | 2 | The base for exponential backoff (`base ^ attempts`). |
| `job_timeout_seconds` | 300 | Max time a job can run before being killed. |
| `lock_timeout_seconds` | 60 | Time before a `processing` job is considered "stale" and recovered. |
