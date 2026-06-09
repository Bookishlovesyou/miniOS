# MiniOS — Real-Time OS Simulator

A command-line mini operating system simulator written in **C**, featuring a round-robin process scheduler with multithreading, a fixed-partition memory manager, and a fully in-memory virtual filesystem — all accessible through an interactive shell.



---

## Features

### Process Scheduler
- Create processes with custom burst time and memory requirements
- **Round-Robin scheduling** with a configurable time quantum (default: 4)
- Multithreaded execution using `pthreads` — each process runs on its own thread
- Kill processes mid-execution with `kill <pid>`
- Real-time execution log viewable with `schedule`

### Memory Manager
- Fixed-partition memory allocation (1000 units total, 100 per partition = 10 partitions)
- First-fit allocation strategy
- Automatic memory release on process termination or kill
- View partition status with `mem`

### Virtual Filesystem (in-memory)
- Boots with a pre-built directory tree: `/home`, `/usr`, `/etc`, `/tmp`, `/bin`
- Full path navigation with `cd` (supports `.`, `..`, `/`, and absolute paths)
- Shell prompt shows your current path: `MiniOS:/home/user>`
- File read/write via `echo` redirects (`>` overwrite, `>>` append)

---

## Getting Started

### Requirements
- GCC (or any C99-compatible compiler)
- POSIX-compliant OS (Linux / macOS)
- `pthreads` library

### Compile
```bash
gcc -Wall -o minios minios.c -lpthread
```

### Run
```bash
./minios
```

---

## Commands

### Process & Scheduler

| Command | Description |
|---|---|
| `run` | Create and schedule a new process (prompts for burst time and memory) |
| `status` | Show all processes and their current state |
| `kill <pid>` | Send a kill signal to a running or ready process |
| `mem` | Show memory partition map |
| `schedule` | Print the full execution log |

### Filesystem

| Command | Description |
|---|---|
| `pwd` | Print current working directory |
| `ls [path]` | List directory contents |
| `cd <path>` | Change directory |
| `mkdir <name>` | Create a new directory |
| `touch <name>` | Create an empty file |
| `echo <text>` | Print text to screen |
| `echo <text> > <file>` | Write text to file (overwrite) |
| `echo <text> >> <file>` | Append text to file |
| `cat <file>` | Print file contents |
| `rm <file>` | Remove a file |
| `rmdir <dir>` | Remove an empty directory |
| `help` | Show all available commands |
| `exit` | Exit MiniOS (cleans up all threads) |

---

## Example Session

```
MiniOS:/> mkdir projects
Directory 'projects' created.

MiniOS:/> cd projects
MiniOS:/projects> touch notes.txt
MiniOS:/projects> echo hello world > notes.txt
MiniOS:/projects> cat notes.txt
hello world

MiniOS:/projects> cd ..
MiniOS:/> run
Enter burst time: 10
Enter memory needed: 150
Process 1 created and added to the queue.

MiniOS:/> status
PID: 1 | Burst: 10 | Remaining: 6 | Memory: 150 | Partition: 0 | Status: RUNNING

MiniOS:/> kill 1
Kill signal sent to Process 1.
```

---

## Technical Notes

- **Mutex locks** protect the CPU, process list, and log from concurrent thread access
- **Kill flag** pattern used instead of `pthread_cancel` for safe, cooperative thread termination
- The virtual filesystem is entirely in-memory — nothing is written to disk
- Max 100 processes, 256 filesystem nodes, 1024 bytes per file

---
