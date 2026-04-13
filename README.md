# Multi-Container Runtime

## Team Information

* Name: Likhith Bommisetty
* SRN: PES1UG24CS249

---

## Project Overview

This project implements a lightweight Linux container runtime in C with:

* Multi-container supervision
* CLI-based control using UNIX domain sockets
* Bounded-buffer logging system
* Kernel module for memory monitoring
* Scheduling experiments using nice values

---

## Build Instructions

```bash
cd boilerplate
make
```

---

## Run Instructions

### Start Supervisor

```bash
sudo ./engine supervisor ./rootfs-base
```

### Create Root Filesystems

```bash
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
```

### Start Containers

```bash
sudo ./engine start alpha ./rootfs-alpha "/bin/sh"
sudo ./engine start beta ./rootfs-beta "/bin/sh"
```

### List Containers

```bash
sudo ./engine ps
```

### View Logs

```bash
sudo ./engine logs <id>
```

### Stop Containers

```bash
sudo ./engine stop <id>
```

---

## Kernel Module

### Load Module

```bash
sudo insmod monitor.ko
```

### Verify

```bash
ls -l /dev/container_monitor
```

### Check Logs

```bash
sudo dmesg | tail
```

### Unload

```bash
sudo rmmod monitor
```

---

## Demo Screenshots

### 1. Multi-container supervision

Two containers running under one supervisor.

### 2. Metadata tracking

Output of `engine ps` showing container metadata.

### 3. Logging pipeline

Container output captured using bounded buffer.

### 4. CLI and IPC

CLI command communicating with supervisor via UNIX socket.

### 5. Soft limit warning

Kernel logs showing soft memory limit exceeded.

### 6. Hard limit enforcement

Container killed after exceeding hard memory limit.

### 7. Scheduling experiment

CPU-bound workloads with different nice values showing different CPU usage.

### 8. Clean teardown

Containers stopped with no zombie processes remaining.

---

## Engineering Analysis

### Isolation Mechanisms

Containers use PID, UTS, and mount namespaces with `chroot` to isolate filesystem and process views. The kernel is shared across all containers.

### Supervisor and Lifecycle

A long-running supervisor manages containers, tracks metadata, and handles signals. It ensures proper child reaping and prevents zombie processes.

### IPC and Synchronization

Two IPC mechanisms are used:

* Pipes for logging
* UNIX socket for CLI communication
  Mutexes and condition variables ensure safe access to shared buffers.

### Memory Management

RSS (Resident Set Size) measures actual physical memory used.
Soft limit logs a warning, while hard limit enforces termination.
Kernel-space enforcement ensures accuracy and reliability.

### Scheduling Behavior

Lower nice value (-5) receives higher CPU priority than higher nice value (10).
This results in observable differences in CPU usage.

---

## Design Decisions and Tradeoffs

* Used `chroot` instead of `pivot_root` for simplicity
* Used UNIX sockets for reliable IPC
* Used mutex + condition variables for bounded buffer
* Kernel module chosen for accurate memory tracking

---

## Scheduler Experiment Results

Two CPU-bound containers were run:

* One with nice = 10
* One with nice = -5

The container with lower nice value received more CPU time, demonstrating Linux scheduler priority behavior.

---

## Conclusion

This project demonstrates core OS concepts including:

* Process isolation
* IPC mechanisms
* Memory management
* Scheduling behavior
* Kernel-user space interaction
