
# Supervised Multi-Container Runtime with Kernel Memory Monitor
 
## Team Members

* PES1UG24CS617 – Satvik Kalyanpur
* PES1UG24CS619 – Shahid Tahsildar
* Section: K

---

## Overview

This project implements a lightweight container runtime in user space along with a Linux kernel module for monitoring and enforcing memory limits on containers.

The runtime supports container creation and isolation using the `clone()` system call and Linux namespaces. It also provides container lifecycle management, logging, and inter-process communication between client and supervisor.

The system consists of:

* User-space runtime (`engine.c`)
* Kernel module (`monitor.c`)

---

## Project Structure

* `engine.c` – User-space container runtime
* `monitor.c` – Kernel module for memory monitoring
* `monitor_ioctl.h` – IOCTL interface definitions
* `Makefile` – Build configuration
* `logs/` – Directory for container logs
* `rootfs/` – Minimal filesystem used for containers

---

## Architecture

### User Space (engine.c)

The user-space runtime is responsible for managing containers and handling client requests.

Key components:

* Supervisor process using UNIX domain sockets
* Client-server request/response communication
* Container lifecycle management:

  * start
  * stop
  * ps (list containers)
  * logs
* Logging system:

  * Bounded buffer (producer-consumer model)
  * Dedicated logging thread
  * File-based logs (`logs/<container>.log`)
* Graceful shutdown using signals (SIGTERM followed by SIGKILL if required)

---

### Containerization

Each container is created using:

* `clone()` system call
* Linux namespaces:

  * PID namespace (isolated process IDs)
  * UTS namespace (separate hostname)
  * Mount namespace (independent filesystem view)

Additional setup inside the container:

* `chroot()` for filesystem isolation
* Minimal root filesystem (`rootfs`)
* Mounting `/proc` to access system information within the container

---

### Kernel Space (monitor.c)

The kernel module is responsible for monitoring container memory usage.

Key components:

* Character device: `/dev/container_monitor`
* Linked list to track monitored containers
* Periodic monitoring using a kernel timer
* Memory limits:

  * Soft limit: generates warnings (visible via `dmesg`)
  * Hard limit: terminates the container process
* Synchronization using mutexes
* Communication with user space via `ioctl`

---

## Features

* Multi-container support
* Container isolation using namespaces (PID, UTS, mount)
* Process creation using `clone()`
* Filesystem isolation using `chroot()`
* `/proc` mounting inside containers
* Memory monitoring and enforcement (kernel module)
* Logging system with thread and bounded buffer
* File-based persistent logs
* Inter-process communication using UNIX domain sockets
* Structured request-response protocol between client and supervisor
* Graceful container shutdown

---

## How to Run

**Environment Setup:** Ubuntu 22.04/24.04 VM. 
Secure boot must be disabled.

**Step 1: Build the Project and Rootfs**
```bash
# Build the engine, workloads, and kernel module
make all

# Setup the Alpine mini root filesystem
mkdir rootfs-base
wget [https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz](https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz)
# Note: For ARM64 Macs, use the aarch64 tarball instead
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base

# Create isolated, writable rootfs directories for the containers
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
```

**Step 2: Load the Kernel Monitor**
```bash
sudo insmod monitor.ko
ls -l /dev/container_monitor # Verify device creation
```

**Step 3: Run the Supervisor**
Open a new terminal (Terminal 1) and start the long-running daemon:
```bash
sudo ./engine supervisor ./rootfs-base
```

**Step 4: Launch and Manage Containers**
Open another terminal (Terminal 2) to use the CLI client:
```bash
# Start containers (using 'exec' so the workload becomes PID 1)
sudo ./engine start alpha ./rootfs-alpha "exec /bin/sleep 300" --soft-mib 40 --hard-mib 64
sudo ./engine start beta ./rootfs-beta "exec /bin/sleep 300"

# View running containers and their states
sudo ./engine ps

# View bounded-buffer logs
sudo ./engine logs alpha

# Stop containers gracefully via SIGTERM
sudo ./engine stop alpha
sudo ./engine stop beta

# For soft and hard limits
In terminal 3:
sudo dmesg -c > /dev/null
sudo dmesg  -w
In terminal 2:
sudo ./engine start hog2 ./rootfs-alpha "/memory_hog 8 1000" --soft-mib 15 --hard-mib 30
sudo ./engine ps
```

**Step 5: Clean Teardown**
```bash
# In Terminal 1, stop the supervisor cleanly
# Press Ctrl+C

# Verify no ghost processes remain
ps aux | grep engine

# Unload the kernel module
sudo rmmod monitor
sudo make clean
```

---
## Screenshots

<img width="1280" height="800" alt="1" src="https://github.com/user-attachments/assets/fc5de3e1-6abb-43f4-925e-b7498c14a30f" />
<img width="1280" height="800" alt="2" src="https://github.com/user-attachments/assets/c2a47d41-16f0-4277-9805-364ff04017cc" />
<img width="1280" height="800" alt="3" src="https://github.com/user-attachments/assets/b4c7ffee-d344-4930-861f-89736d09520e" />
<img width="1280" height="800" alt="4" src="https://github.com/user-attachments/assets/f8370d88-67d6-4f0e-89d4-0985510bc48f" />
<img width="1280" height="800" alt="5" src="https://github.com/user-attachments/assets/e3647ca1-714c-4eab-82d7-d12220853f43" />
<img width="1280" height="800" alt="6" src="https://github.com/user-attachments/assets/43e1e5ea-5933-439c-aa59-5b4b075e3192" />
<img width="1280" height="800" alt="7" src="https://github.com/user-attachments/assets/3aa6da5d-2e6b-4ad1-86a4-b55ac5fe0c7a" />


---

## Engineering Analysis

1. Isolation Mechanisms

The runtime uses Linux namespaces and chroot() for isolation.
PID namespaces (CLONE_NEWPID) give each container its own process tree, so processes inside cannot see host processes. UTS namespaces (CLONE_NEWUTS) isolate system identity, allowing each container to have its own hostname. Mount namespaces (CLONE_NEWNS) isolate filesystem changes.

chroot() restricts the container’s filesystem view to its rootfs, and /proc is mounted inside for process visibility within the namespace.

However, all containers still share the same host kernel, including the scheduler, memory management, and system calls.

2. Supervisor and Process Lifecycle

A long-running supervisor manages all containers. It creates them using clone(), tracks metadata (ID, PID, state), and handles lifecycle events.

Containers are child processes of the supervisor. When they exit, the supervisor reaps them using waitpid() to avoid zombies. It also handles signals—sending SIGTERM for graceful shutdown and SIGKILL if needed.

This centralized control simplifies management and cleanup.

3. IPC, Threads, and Synchronization

The runtime uses a UNIX domain socket for communication between client and supervisor (control plane).

For logging, it uses a bounded buffer with a producer-consumer model. Multiple producers (container events) and a consumer thread (logger) share this buffer.

Race conditions are avoided using a mutex for mutual exclusion and condition variables to handle full/empty buffer states. This ensures safe and efficient synchronization without busy waiting.

4. Memory Management and Enforcement

RSS (Resident Set Size) measures the physical memory a process is using in RAM, but not swapped-out memory.

Soft limits act as thresholds for warning or pressure, while hard limits are strict caps that trigger enforcement.

Enforcement must be in kernel space because only the kernel has full control over memory usage and can reliably enforce limits. User-space alone cannot guarantee this.

5. Scheduling Behavior

Linux’s Completely Fair Scheduler distributes CPU time fairly among processes. CPU-bound workloads share CPU evenly, while processes with lower nice values get higher priority.

I/O-bound processes are scheduled quickly to maintain responsiveness.


---
## Design Decisions and Tradeoffs
Namespace Isolation: We used Linux namespaces (PID, mount, UTS, etc.) so each container gets its own isolated view of processes and filesystem. This ensures strong separation between host and container environments. The main tradeoff is that debugging becomes more difficult because processes and resources are hidden within separate namespace contexts. We chose this approach because it is lightweight, secure, and the standard mechanism used by Linux containers, avoiding the need for heavier virtualization.

Supervisor Architecture: We implemented a long-running supervisor process that is responsible for creating, tracking, and cleaning up child container processes. This centralizes lifecycle management and ensures controlled execution. The tradeoff is that the supervisor becomes a single point of failure and must be kept reliable. We chose this design because it simplifies orchestration and gives a single control layer for all container operations.

IPC/Logging: We used a simple inter-process communication and centralized logging mechanism between the supervisor and containers. This makes it easy to trace execution flow and debug issues since all logs are collected in one place. The tradeoff is reduced performance and flexibility compared to lower-level IPC mechanisms like shared memory or advanced message queues. We chose this design because clarity and debuggability were more important than raw performance for this project.

Kernel Monitor: We built a lightweight monitoring system that observes process and resource usage externally rather than modifying the kernel. This provides visibility into system behavior without deep kernel integration. The tradeoff is that the monitoring is approximate and may not capture fine-grained or real-time kernel-level metrics. We chose this approach because it avoids kernel complexity while still providing useful runtime insights.

Scheduling Experiments: We experimented with Linux scheduling policies by adjusting process priorities and observing behavior under different workloads. This allowed us to study scheduling without modifying the kernel itself. The tradeoff is that results can vary depending on host system load and are not fully deterministic. We chose this approach because it is safe, portable, and sufficient for demonstrating scheduling concepts without kernel-level modifications.


---

## Scheduler Experiment Results

To observe Linux CFS behavior, we ran a "cage match" on a single CPU core. We executed two `cpu_hog` workloads simultaneously, each running for 15 seconds. 

* `fast_hog`: nice `-20` (Maximum priority)
* `slow_hog`: nice `19` (Minimum priority)

**Raw Accumulator Results:**
| Container | Priority (`nice`) | Final Accumulator Value | Time Elapsed |
| :--- | :--- | :--- | :--- |
| **`fast_hog`** | -20 | `14053102039526523489` | 15s |
| **`slow_hog`** |  19 | `813103959654799625` | 15s (Lagged) |
