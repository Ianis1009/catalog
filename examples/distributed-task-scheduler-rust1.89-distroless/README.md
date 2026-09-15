# Distributed Task Scheduler — Rust 1.89 Distroless

A distributed task scheduler written in Rust 1.89 and packaged as a minimal distroless Unikraft image.

The example demonstrates a coordinator/worker architecture where the coordinator maintains a task queue, tracks workers through heartbeats, assigns tasks to available workers, and requeues tasks when a worker becomes unavailable.

## Features

* Rust 1.89 with no external crates
* Minimal distroless runtime image
* Coordinator/worker architecture using the same binary
* Dynamic worker registration
* Worker heartbeat monitoring
* Task queue and scheduling
* Concurrent task execution
* Task reassignment after worker failure
* Simple HTTP API
* No JSON or external runtime dependencies
* Runs with Unikraft/QEMU

## Architecture

The same `task-scheduler` binary can run in two roles:

```text
                    +----------------------+
                    |     Coordinator      |
                    |       :8080          |
                    |                      |
                    |  Task queue           |
                    |  Worker registry      |
                    |  Scheduler            |
                    |  Heartbeat monitor    |
                    +----------+-----------+
                               |
                 +-------------+-------------+
                 |                           |
                 v                           v
        +----------------+          +----------------+
        |    Worker 1    |          |    Worker 2    |
        |     :9001      |          |     :9002      |
        +----------------+          +----------------+
```

The coordinator assigns queued tasks to idle workers. Workers periodically send heartbeats so the coordinator can detect failures.

If a worker disappears while executing a task, the coordinator removes the worker, requeues the task, and assigns it to another available worker.

## Task Types

The scheduler supports four task types:

| Type    | Input           | Description                       |
| ------- | --------------- | --------------------------------- |
| `echo`  | string          | Returns the input unchanged       |
| `add`   | `number,number` | Adds two integers                 |
| `fib`   | integer         | Calculates Fibonacci recursively  |
| `sleep` | seconds         | Sleeps for the requested duration |

Limits:

* `fib` accepts values from `0` to `45`
* `sleep` accepts values from `0` to `30` seconds
* `add` requires exactly two integers

## HTTP API

### Health

```bash
curl http://localhost:8080/health
```

Response:

```text
coordinator: healthy
```

### List workers

```bash
curl http://localhost:8080/workers
```

Example:

```text
worker-1 worker-1:9001 idle last_seen=1s_ago
worker-2 worker-2:9002 idle last_seen=1s_ago
```

### Create a task

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=echo&value=hello-unikraft'
```

Response:

```text
task_id=1
status=queued
```

### Get task status

```bash
curl http://localhost:8080/tasks/1
```

Example:

```text
task_id=1
type=echo
value=hello-unikraft
status=completed
result=hello-unikraft
worker=none
attempts=1
```

## Building

Build the Unikraft image with:

```bash
kraft build --plat qemu --arch x86_64 .
```

A successful build produces an x86_64 initramfs under `.unikraft/build/`.

## Running with Unikraft

The coordinator can be started with:

```bash
kraft run --plat qemu --arch x86_64 . \
  -p 8080:8080 \
  -e SCHEDULER_ROLE=coordinator \
  -e SCHEDULER_PORT=8080
```

Verify the coordinator from another terminal:

```bash
curl http://localhost:8080/health
```

Expected:

```text
coordinator: healthy
```

## Running with Docker

The example can also be tested as a multi-container distributed system.

Create a Docker network:

```bash
docker network create scheduler-net 2>/dev/null || true
```

Build the image:

```bash
docker build -t distributed-task-scheduler-rust1.89-distroless .
```

Start the coordinator:

```bash
docker run -d \
  --name scheduler \
  --network scheduler-net \
  -p 8080:8080 \
  -e SCHEDULER_ROLE=coordinator \
  -e SCHEDULER_PORT=8080 \
  distributed-task-scheduler-rust1.89-distroless
```

Start worker 1:

```bash
docker run -d \
  --name worker-1 \
  --network scheduler-net \
  -e SCHEDULER_ROLE=worker \
  -e WORKER_ID=worker-1 \
  -e WORKER_PORT=9001 \
  -e WORKER_ADDRESS=worker-1:9001 \
  -e COORDINATOR_ADDRESS=scheduler:8080 \
  distributed-task-scheduler-rust1.89-distroless
```

Start worker 2:

```bash
docker run -d \
  --name worker-2 \
  --network scheduler-net \
  -e SCHEDULER_ROLE=worker \
  -e WORKER_ID=worker-2 \
  -e WORKER_PORT=9002 \
  -e WORKER_ADDRESS=worker-2:9002 \
  -e COORDINATOR_ADDRESS=scheduler:8080 \
  distributed-task-scheduler-rust1.89-distroless
```

Check the workers:

```bash
curl http://localhost:8080/workers
```

## Example Tasks

### Echo

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=echo&value=hello-unikraft'
```

### Add

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=add&value=10,32'
```

Expected result:

```text
result=42
```

### Fibonacci

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=fib&value=20'
```

Expected result:

```text
result=6765
```

### Sleep

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=sleep&value=5'
```

The task initially reports:

```text
status=running
```

and completes after the requested delay:

```text
status=completed
result=slept for 5 seconds
```

## Concurrent Scheduling

Multiple tasks can be queued at the same time:

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=sleep&value=5'

curl -X POST http://localhost:8080/tasks \
  -d 'type=sleep&value=5'
```

The scheduler can assign the tasks to different idle workers, allowing them to execute concurrently.

## Error Handling

Unsupported task types are rejected by the coordinator:

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=unknown&value=test'
```

Response:

```text
unsupported task type
```

Invalid task input is reported by the worker:

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=add&value=abc,10'
```

The resulting task is marked as failed:

```text
status=failed
result=invalid number: abc
```

Missing parameters are also rejected:

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'value=test'
```

```text
missing type
```

and:

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=echo'
```

```text
missing value
```

## Worker Failure Recovery

Workers send heartbeats to the coordinator every second.

A worker is considered unavailable after missing heartbeats for more than five seconds.

When a worker executing a task becomes unavailable:

1. The coordinator detects the missed heartbeat.
2. The worker is removed from the registry.
3. The running task is requeued.
4. Another available worker receives the task.
5. The task continues from the beginning on the replacement worker.

For example, a long-running task can be created with:

```bash
curl -X POST http://localhost:8080/tasks \
  -d 'type=sleep&value=20'
```

If the worker executing the task is stopped, the task is reassigned to another worker.

The resulting task status shows the additional attempt:

```text
status=completed
result=slept for 20 seconds
attempts=2
```

## Configuration

The role and network configuration are controlled through environment variables.

### Coordinator

| Variable         | Default       | Description           |
| ---------------- | ------------- | --------------------- |
| `SCHEDULER_ROLE` | `coordinator` | Process role          |
| `SCHEDULER_PORT` | `8080`        | Coordinator HTTP port |

### Worker

| Variable              | Default          | Description                         |
| --------------------- | ---------------- | ----------------------------------- |
| `SCHEDULER_ROLE`      | `coordinator`    | Set to `worker`                     |
| `WORKER_ID`           | `worker-1`       | Worker identifier                   |
| `WORKER_PORT`         | `9001`           | Worker HTTP port                    |
| `WORKER_ADDRESS`      | `127.0.0.1:9001` | Address registered with coordinator |
| `COORDINATOR_ADDRESS` | `127.0.0.1:8080` | Coordinator address                 |

## Implementation

The implementation uses only the Rust standard library.

The scheduler maintains:

* a task map
* a worker registry
* task states
* worker heartbeat timestamps
* worker busy/idle state
* task attempt counters

Task states are:

```text
queued → running → completed
                 ↘ failed
```

If a worker fails while a task is running:

```text
running
   ↓
worker failure
   ↓
queued
   ↓
running
   ↓
completed
```

## Files

```text
.
├── Dockerfile
├── Kraftfile
├── README.md
└── src
    └── main.rs
```

## Cleanup

Stop the Docker containers:

```bash
docker rm -f scheduler worker-1 worker-2
```

Remove the Docker network:

```bash
docker network rm scheduler-net
```
