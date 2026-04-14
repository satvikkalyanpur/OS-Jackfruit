
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
