
# Supervised Multi-Container Runtime with Kernel Memory Monitor

## Team Members

* PES1UG24CS617 – Satvik Kalyanpur
* PES1UG24CS619 – Shahid Tahsildar

---

## Overview

This project implements a lightweight container runtime in user space along with a Linux kernel module for monitoring and enforcing memory limits on containers.

The runtime supports container creation and isolation using the `clone()` system call and Linux namespaces. It also provides container lifecycle management, logging, and inter-process communication between client and supervisor.

The system consists of:

* User-space runtime (`engine.c`)
* Kernel module (`monitor.c`)

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

### Build

```bash
make
```

### Load Kernel Module

```bash
sudo insmod monitor.ko
```

### Start Supervisor

```bash
sudo ./engine supervisor rootfs
```

### Start Container

```bash
sudo ./engine start c1 rootfs /bin/bash
```

### Inside Container (Example)

```bash
echo $$
echo $(</proc/sys/kernel/hostname)
```

### List Containers

```bash
sudo ./engine ps
```

### View Logs

```bash
sudo ./engine logs c1
```

### Stop Container

```bash
sudo ./engine stop c1
```

---

## Expected Output

* Container starts successfully with a unique PID (host side)
* Inside the container:

  * PID appears as 1 (PID namespace isolation)
  * Hostname matches container ID (UTS namespace isolation)
* Logs are printed via the logging system
* Log files are created under `logs/`
* Kernel messages related to monitoring are visible using `dmesg`

---

## Project Structure

* `engine.c` – User-space container runtime
* `monitor.c` – Kernel module for memory monitoring
* `monitor_ioctl.h` – IOCTL interface definitions
* `Makefile` – Build configuration
* `logs/` – Directory for container logs
* `rootfs/` – Minimal filesystem used for containers

---
## Screenshots

<img width="1280" height="800" alt="task1" src="https://github.com/user-attachments/assets/72da0cba-bac7-4f90-af33-507f600fa1b2" />
<img width="1280" height="800" alt="task2" src="https://github.com/user-attachments/assets/de946c90-e336-4b9f-9343-bcffa0a7bae8" />
<img width="1280" height="800" alt="task3" src="https://github.com/user-attachments/assets/883f1f87-718e-48b8-b2d8-30ab6b07b91a" />
<img width="1280" height="800" alt="c1_logs" src="https://github.com/user-attachments/assets/b9955117-34ef-4421-846e-34ba94317e26" />
<img width="1280" height="800" alt="hostname_namespace_proof" src="https://github.com/user-attachments/assets/200d99fe-339d-4a5d-ac4c-04f869b1af28" />
<img width="1280" height="800" alt="ps_after_stop" src="https://github.com/user-attachments/assets/fd0a9fa1-4e70-426d-9f7b-68e8d85068b0" />
<img width="1280" height="800" alt="ps_command" src="https://github.com/user-attachments/assets/d0a6778f-68f1-4b3d-997d-78766eb6b802" />
<img width="1280" height="800" alt="supervisor_running" src="https://github.com/user-attachments/assets/9f1c0e2f-0d90-4eed-bd3a-469a207697eb" />

---
## Conclusion

This project demonstrates the design and implementation of a simplified container runtime using Linux system programming concepts.

It integrates:

* Process creation with `clone()`
* Namespace-based isolation
* Filesystem isolation using `chroot()`
* Kernel-user communication
* Memory monitoring via a kernel module
* IPC and concurrent logging mechanisms

The system provides a basic Docker-like environment implemented from scratch, highlighting core operating system concepts in practice.
