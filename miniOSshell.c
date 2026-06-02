#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdarg.h>

 
//  OS / Scheduler constants
 
#define MAX_PROCESSES   100
#define MEMORY_SIZE     1000
#define PARTITION_SIZE  100
#define NUM_PARTITIONS  (MEMORY_SIZE / PARTITION_SIZE)
#define TIME_QUANTUM    4
#define MAX_LOGS        1000

 
//  Virtual Filesystem constants
 
#define MAX_FS_NODES      256
#define MAX_NAME_LEN       64
#define MAX_PATH_LEN      512
#define MAX_FILE_CONTENT 1024

typedef enum { FS_DIR, FS_FILE } NodeType;

typedef struct FSNode {
    int       id;
    NodeType  type;
    char      name[MAX_NAME_LEN];
    char      content[MAX_FILE_CONTENT]; // only used by files
    int       parent_id;                 // -1 = root
} FSNode;

FSNode   fs_nodes[MAX_FS_NODES];
int      fs_node_count = 0;
int      cwd_id        = 0; // current working directory node id

 
//  Process scheduler types
 
typedef enum { READY, RUNNING, TERMINATED, KILLED } Status;

typedef struct {
    int      pid;
    int      bursttime;
    int      remainingtime;
    int      memoryneeded;
    int      partitionidx;
    Status   status;
    int      killed;
    pthread_t thread;
} Process;

 
//  Globals
 
Process         processes[MAX_PROCESSES];
int             process_count = 0;
int             mem_map[NUM_PARTITIONS]; // 0=free 1=occupied
int             pid_counter = 1;
pthread_mutex_t cpu_lock  = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t proc_lock = PTHREAD_MUTEX_INITIALIZER;

char            logentry[MAX_LOGS][128];
int             logindex = 0;
pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;

 
//  LOGGING
 
void add_log(const char *fmt, ...) {
    pthread_mutex_lock(&log_lock);
    if (logindex < MAX_LOGS) {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(logentry[logindex], sizeof(logentry[logindex]), fmt, ap);
        va_end(ap);
        logindex++;
    }
    pthread_mutex_unlock(&log_lock);
}

 
//  MEMORY MANAGEMENT
 
int allocate_memory(int size) {
    int needed = (size + PARTITION_SIZE - 1) / PARTITION_SIZE;
    for (int i = 0; i <= NUM_PARTITIONS - needed; i++) {
        int ok = 1;
        for (int j = 0; j < needed; j++) if (mem_map[i+j]) { ok=0; break; }
        if (ok) {
            for (int j = 0; j < needed; j++) mem_map[i+j] = 1;
            return i;
        }
    }
    return -1;
}

void free_memory(int index, int size) {
    int needed = (size + PARTITION_SIZE - 1) / PARTITION_SIZE;
    for (int i = 0; i < needed; i++) mem_map[index+i] = 0;
}

 
//  PROCESS EXECUTION THREAD
 
void *process_execution(void *arg) {
    Process *p = (Process *)arg;
    while (p->remainingtime > 0) {
        if (p->killed) {
            pthread_mutex_lock(&cpu_lock);
            free_memory(p->partitionidx, p->memoryneeded);
            p->status = KILLED;
            add_log("Process %d was KILLED. Memory freed (Partition %d)", p->pid, p->partitionidx);
            pthread_mutex_unlock(&cpu_lock);
            return NULL;
        }
        pthread_mutex_lock(&cpu_lock);
        if (p->killed) {
            free_memory(p->partitionidx, p->memoryneeded);
            p->status = KILLED;
            add_log("Process %d was KILLED. Memory freed (Partition %d)", p->pid, p->partitionidx);
            pthread_mutex_unlock(&cpu_lock);
            return NULL;
        }
        p->status = RUNNING;
        add_log("Process %d started using CPU (Remaining: %d)", p->pid, p->remainingtime);
        int exec_time = (p->remainingtime >= TIME_QUANTUM) ? TIME_QUANTUM : p->remainingtime;
        p->remainingtime -= exec_time;
        usleep(exec_time * 500000);
        if (p->remainingtime <= 0) {
            p->status = TERMINATED;
            free_memory(p->partitionidx, p->memoryneeded);
            add_log("Process %d TERMINATED. Memory freed (Partition %d)", p->pid, p->partitionidx);
        } else {
            p->status = READY;
            add_log("Process %d finished quantum (Remaining: %d)", p->pid, p->remainingtime);
        }
        pthread_mutex_unlock(&cpu_lock);
        sleep(1);
    }
    return NULL;
}

 
//  SCHEDULER COMMANDS
 
void cmd_run() {
    if (process_count >= MAX_PROCESSES) { printf("Max process limit reached.\n"); return; }
    int burst, mem;
    printf("Enter burst time: ");  scanf("%d", &burst);
    printf("Enter memory needed: "); scanf("%d", &mem);
    if (burst <= 0 || mem <= 0) { printf("Values must be positive.\n"); return; }
    int partition = allocate_memory(mem);
    if (partition == -1) { printf("Not enough memory.\n"); return; }
    pthread_mutex_lock(&proc_lock);
    Process *p   = &processes[process_count];
    p->pid          = pid_counter++;
    p->bursttime    = burst;
    p->remainingtime = burst;
    p->memoryneeded = mem;
    p->partitionidx = partition;
    p->status       = READY;
    p->killed       = 0;
    add_log("Process %d created | Burst: %d | Memory: %d | Partition: %d",
            p->pid, p->bursttime, p->memoryneeded, p->partitionidx);
    pthread_create(&p->thread, NULL, process_execution, p);
    printf("Process %d created and added to the queue.\n", p->pid);
    process_count++;
    pthread_mutex_unlock(&proc_lock);
}

void cmd_kill(int target_pid) {
    for (int i = 0; i < process_count; i++) {
        Process *p = &processes[i];
        if (p->pid == target_pid) {
            if (p->status == TERMINATED || p->status == KILLED) {
                printf("Process %d is already %s.\n", target_pid,
                       p->status == TERMINATED ? "TERMINATED" : "KILLED");
                return;
            }
            p->killed = 1;
            printf("Kill signal sent to Process %d.\n", target_pid);
            add_log("Kill signal sent to Process %d", target_pid);
            return;
        }
    }
    printf("No process with PID %d.\n", target_pid);
}

void cmd_status() {
    printf("\n--- Process Status ---\n");
    if (process_count == 0) { printf("No processes.\n"); return; }
    for (int i = 0; i < process_count; i++) {
        Process *p = &processes[i];
        const char *s;
        switch (p->status) {
            case READY:      s="READY";      break;
            case RUNNING:    s="RUNNING";    break;
            case TERMINATED: s="TERMINATED"; break;
            case KILLED:     s="KILLED";     break;
            default:         s="UNKNOWN";    break;
        }
        printf("PID: %d | Burst: %d | Remaining: %d | Memory: %d | Partition: %d | Status: %s\n",
               p->pid, p->bursttime, p->remainingtime, p->memoryneeded, p->partitionidx, s);
    }
}

void cmd_mem() {
    printf("\n--- Memory Partitions ---\n");
    for (int i = 0; i < NUM_PARTITIONS; i++)
        printf("Partition %d: %s\n", i, mem_map[i] ? "Occupied" : "Free");
}

void cmd_schedule() {
    printf("\n--- Execution Log ---\n");
    if (logindex == 0) { printf("No log entries yet.\n"); return; }
    for (int i = 0; i < logindex; i++)
        printf("[%03d] %s\n", i+1, logentry[i]);
}

 
//  VIRTUAL FILESYSTEM HELPERS
 

// Find a node by name inside a given parent directory (-1 = not found)
int fs_find_child(int parent_id, const char *name) {
    for (int i = 0; i < fs_node_count; i++)
        if (fs_nodes[i].parent_id == parent_id &&
            strcmp(fs_nodes[i].name, name) == 0)
            return i;
    return -1;
}

// Create a new node; returns index or -1 on failure
int fs_create_node(NodeType type, const char *name, int parent_id) {
    if (fs_node_count >= MAX_FS_NODES) return -1;
    if (strlen(name) == 0 || strlen(name) >= MAX_NAME_LEN) return -1;
    // Disallow duplicate names in same directory
    if (fs_find_child(parent_id, name) != -1) return -2; // already exists
    FSNode *n = &fs_nodes[fs_node_count];
    n->id        = fs_node_count;
    n->type      = type;
    n->parent_id = parent_id;
    strncpy(n->name, name, MAX_NAME_LEN - 1);
    n->name[MAX_NAME_LEN-1] = '\0';
    n->content[0] = '\0';
    return fs_node_count++;
}

// Build absolute path string for a node (for pwd)
void fs_build_path(int node_id, char *buf, int buflen) {
    if (node_id == 0) { strncpy(buf, "/", buflen); return; }
    // Walk up the tree collecting names
    char parts[64][MAX_NAME_LEN];
    int  depth = 0;
    int  cur   = node_id;
    while (cur != 0 && depth < 64) {
        strncpy(parts[depth++], fs_nodes[cur].name, MAX_NAME_LEN);
        cur = fs_nodes[cur].parent_id;
    }
    buf[0] = '\0';
    for (int i = depth-1; i >= 0; i--) {
        strncat(buf, "/", buflen - strlen(buf) - 1);
        strncat(buf, parts[i], buflen - strlen(buf) - 1);
    }
}

// Resolve a path (absolute or relative) to a node index; -1 = not found
int fs_resolve(const char *path) {
    int cur;
    const char *p = path;
    if (path[0] == '/') { cur = 0; p++; } // absolute
    else                { cur = cwd_id; } // relative

    char token[MAX_NAME_LEN];
    while (*p) {
        // grab next component
        int len = 0;
        while (*p && *p != '/') token[len++] = *p++;
        token[len] = '\0';
        if (*p == '/') p++;
        if (len == 0 || strcmp(token, ".") == 0) continue;
        if (strcmp(token, "..") == 0) {
            if (cur != 0) cur = fs_nodes[cur].parent_id;
            continue;
        }
        int child = fs_find_child(cur, token);
        if (child == -1) return -1;
        cur = child;
    }
    return cur;
}

 
//  VIRTUAL FILESYSTEM COMMANDS
 

void cmd_pwd() {
    char path[MAX_PATH_LEN];
    fs_build_path(cwd_id, path, sizeof(path));
    printf("%s\n", path);
}

void cmd_ls(const char *target) {
    int dir_id = cwd_id;
    if (target && strlen(target) > 0) {
        dir_id = fs_resolve(target);
        if (dir_id == -1) { printf("ls: cannot access '%s': No such file or directory\n", target); return; }
        if (fs_nodes[dir_id].type != FS_DIR) { printf("ls: '%s': Not a directory\n", target); return; }
    }
    int found = 0;
    for (int i = 0; i < fs_node_count; i++) {
        if (fs_nodes[i].parent_id == dir_id) {
            printf("%s%s\n", fs_nodes[i].name,
                   fs_nodes[i].type == FS_DIR ? "/" : "");
            found++;
        }
    }
    if (!found) printf("(empty)\n");
}

void cmd_cd(const char *path) {
    if (!path || strlen(path) == 0 || strcmp(path, "~") == 0) {
        cwd_id = 0; return; // go to root
    }
    int target = fs_resolve(path);
    if (target == -1) { printf("cd: '%s': No such file or directory\n", path); return; }
    if (fs_nodes[target].type != FS_DIR) { printf("cd: '%s': Not a directory\n", path); return; }
    cwd_id = target;
}

void cmd_mkdir(const char *name) {
    if (!name || strlen(name) == 0) { printf("mkdir: missing operand\n"); return; }
    int r = fs_create_node(FS_DIR, name, cwd_id);
    if (r == -1) printf("mkdir: cannot create directory '%s': filesystem full\n", name);
    else if (r == -2) printf("mkdir: cannot create directory '%s': File exists\n", name);
    else printf("Directory '%s' created.\n", name);
}

void cmd_touch(const char *name) {
    if (!name || strlen(name) == 0) { printf("touch: missing file operand\n"); return; }
    // If already exists, silently succeed (like real touch)
    if (fs_find_child(cwd_id, name) != -1) return;
    int r = fs_create_node(FS_FILE, name, cwd_id);
    if (r == -1) printf("touch: cannot create '%s': filesystem full\n", name);
}

void cmd_echo(const char *line) {
    // Supports:  echo hello world
    //            echo hello > file.txt   (write)
    //            echo hello >> file.txt  (append)
    char text[512] = {0};
    char fname[MAX_NAME_LEN] = {0};
    int  append = 0;

    // Look for >> or > redirect
    const char *redir_dbl = strstr(line, ">>");
    const char *redir_sgl = NULL;
    if (!redir_dbl) redir_sgl = strchr(line, '>');

    if (redir_dbl) {
        append = 1;
        int tlen = (int)(redir_dbl - line);
        strncpy(text, line, tlen);
        // trim trailing spaces
        while (tlen > 0 && text[tlen-1] == ' ') tlen--;
        text[tlen] = '\0';
        const char *fn = redir_dbl + 2;
        while (*fn == ' ') fn++;
        strncpy(fname, fn, MAX_NAME_LEN-1);
        // trim trailing newline/space
        int fl = strlen(fname);
        while (fl > 0 && (fname[fl-1]==' '||fname[fl-1]=='\n')) fl--;
        fname[fl] = '\0';
    } else if (redir_sgl) {
        int tlen = (int)(redir_sgl - line);
        strncpy(text, line, tlen);
        while (tlen > 0 && text[tlen-1] == ' ') tlen--;
        text[tlen] = '\0';
        const char *fn = redir_sgl + 1;
        while (*fn == ' ') fn++;
        strncpy(fname, fn, MAX_NAME_LEN-1);
        int fl = strlen(fname);
        while (fl > 0 && (fname[fl-1]==' '||fname[fl-1]=='\n')) fl--;
        fname[fl] = '\0';
    } else {
        strncpy(text, line, sizeof(text)-1);
    }

    if (strlen(fname) == 0) {
        // Plain echo — just print
        printf("%s\n", text);
        return;
    }

    // Write/append to file
    int idx = fs_find_child(cwd_id, fname);
    if (idx == -1) {
        // create file
        idx = fs_create_node(FS_FILE, fname, cwd_id);
        if (idx < 0) { printf("echo: cannot create '%s'\n", fname); return; }
    }
    if (fs_nodes[idx].type == FS_DIR) { printf("echo: '%s': Is a directory\n", fname); return; }

    if (append) {
        int existing = strlen(fs_nodes[idx].content);
        int space    = MAX_FILE_CONTENT - existing - 1;
        if (space > 0) {
            if (existing > 0) strncat(fs_nodes[idx].content, "\n", space--);
            strncat(fs_nodes[idx].content, text, space);
        } else {
            printf("echo: '%s': File content limit reached\n", fname);
        }
    } else {
        strncpy(fs_nodes[idx].content, text, MAX_FILE_CONTENT-1);
        fs_nodes[idx].content[MAX_FILE_CONTENT-1] = '\0';
    }
}

void cmd_cat(const char *name) {
    if (!name || strlen(name) == 0) { printf("cat: missing operand\n"); return; }
    int idx = fs_resolve(name);
    if (idx == -1) { printf("cat: '%s': No such file or directory\n", name); return; }
    if (fs_nodes[idx].type == FS_DIR) { printf("cat: '%s': Is a directory\n", name); return; }
    if (strlen(fs_nodes[idx].content) == 0) printf("(empty file)\n");
    else printf("%s\n", fs_nodes[idx].content);
}

void cmd_rm(const char *name) {
    if (!name || strlen(name) == 0) { printf("rm: missing operand\n"); return; }
    int idx = fs_resolve(name);
    if (idx == -1) { printf("rm: cannot remove '%s': No such file or directory\n", name); return; }
    if (fs_nodes[idx].type == FS_DIR) { printf("rm: cannot remove '%s': Is a directory (use rmdir)\n", name); return; }
    // Remove by shifting array
    for (int i = idx; i < fs_node_count - 1; i++) {
        fs_nodes[i] = fs_nodes[i+1];
        fs_nodes[i].id = i;
    }
    fs_node_count--;
    printf("'%s' removed.\n", name);
}

void cmd_rmdir(const char *name) {
    if (!name || strlen(name) == 0) { printf("rmdir: missing operand\n"); return; }
    int idx = fs_resolve(name);
    if (idx == -1) { printf("rmdir: failed to remove '%s': No such file or directory\n", name); return; }
    if (fs_nodes[idx].type != FS_DIR) { printf("rmdir: failed to remove '%s': Not a directory\n", name); return; }
    // Check empty
    for (int i = 0; i < fs_node_count; i++)
        if (fs_nodes[i].parent_id == idx) { printf("rmdir: failed to remove '%s': Directory not empty\n", name); return; }
    if (cwd_id == idx) { printf("rmdir: cannot remove current working directory\n"); return; }
    for (int i = idx; i < fs_node_count - 1; i++) {
        fs_nodes[i] = fs_nodes[i+1];
        fs_nodes[i].id = i;
        // fix parent references
        if (fs_nodes[i].parent_id > idx) fs_nodes[i].parent_id--;
    }
    fs_node_count--;
    printf("Directory '%s' removed.\n", name);
}

 
//  HELP
 
void cmd_help() {
    printf("\n─────────────────────────────────────────\n");
    printf("  MiniOS Commands\n");
    printf("─────────────────────────────────────────\n");
    printf("  PROCESS & SCHEDULER\n");
    printf("    run              Create and schedule a new process\n");
    printf("    status           Show all process statuses\n");
    printf("    kill <pid>       Send kill signal to a process\n");
    printf("    mem              Show memory partition map\n");
    printf("    schedule         Show execution log\n");
    printf("\n  FILESYSTEM\n");
    printf("    pwd              Print current directory\n");
    printf("    ls [path]        List directory contents\n");
    printf("    cd <path>        Change directory (supports . .. /)\n");
    printf("    mkdir <name>     Create a new directory\n");
    printf("    touch <name>     Create an empty file\n");
    printf("    echo <text>      Print text\n");
    printf("    echo <t> > <f>   Write text to file (overwrite)\n");
    printf("    echo <t> >> <f>  Append text to file\n");
    printf("    cat <file>       Print file contents\n");
    printf("    rm <file>        Remove a file\n");
    printf("    rmdir <dir>      Remove an empty directory\n");
    printf("\n  OTHER\n");
    printf("    help             Show this help message\n");
    printf("    exit             Exit MiniOS\n");
    printf("─────────────────────────────────────────\n");
}

 
//  FILESYSTEM INIT
 
void fs_init() {
    // Create root node (id=0, parent=-1)
    FSNode *root = &fs_nodes[0];
    root->id        = 0;
    root->type      = FS_DIR;
    root->parent_id = -1;
    strncpy(root->name, "", MAX_NAME_LEN);
    root->content[0] = '\0';
    fs_node_count   = 1;
    cwd_id          = 0;

    // Pre-create some starter dirs like a real OS
    int home = fs_create_node(FS_DIR, "home",  0);
    int usr  = fs_create_node(FS_DIR, "usr",   0);
               fs_create_node(FS_DIR, "etc",   0);
               fs_create_node(FS_DIR, "tmp",   0);
               fs_create_node(FS_DIR, "bin",   0);
               fs_create_node(FS_DIR, "user",  home);
    (void)usr;
}

 
//  SHELL — main input loop
 
void shell() {
    char line[512];

    printf("╔══════════════════════════════════════╗\n");
    printf("║     MiniOS Real-Time Simulator       ║\n");
    printf("║     Type 'help' for commands         ║\n");
    printf("╚══════════════════════════════════════╝\n");

    while (1) {
        // Print prompt with current path
        char path[MAX_PATH_LEN];
        fs_build_path(cwd_id, path, sizeof(path));
        printf("\nMiniOS:%s> ", path);
        fflush(stdout);

        if (!fgets(line, sizeof(line), stdin)) break;

        // Strip trailing newline
        line[strcspn(line, "\n")] = '\0';
        // Strip leading spaces
        char *input = line;
        while (*input == ' ') input++;
        if (strlen(input) == 0) continue;

        // Tokenise: first word = command, rest = args
        char cmd[64]  = {0};
        char args[448] = {0};
        sscanf(input, "%63s", cmd);
        // args = everything after the command word
        const char *after = input + strlen(cmd);
        while (*after == ' ') after++;
        strncpy(args, after, sizeof(args)-1);

        // ── Dispatch ──────────────────────────
        if      (strcmp(cmd, "run")      == 0) cmd_run();
        else if (strcmp(cmd, "status")   == 0) cmd_status();
        else if (strcmp(cmd, "mem")      == 0) cmd_mem();
        else if (strcmp(cmd, "schedule") == 0) cmd_schedule();
        else if (strcmp(cmd, "kill")     == 0) {
            int pid;
            if (sscanf(args, "%d", &pid) == 1) cmd_kill(pid);
            else printf("Usage: kill <pid>\n");
        }
        else if (strcmp(cmd, "pwd")      == 0) cmd_pwd();
        else if (strcmp(cmd, "ls")       == 0) cmd_ls(strlen(args) ? args : NULL);
        else if (strcmp(cmd, "cd")       == 0) cmd_cd(args);
        else if (strcmp(cmd, "mkdir")    == 0) cmd_mkdir(args);
        else if (strcmp(cmd, "touch")    == 0) cmd_touch(args);
        else if (strcmp(cmd, "echo")     == 0) cmd_echo(args);
        else if (strcmp(cmd, "cat")      == 0) cmd_cat(args);
        else if (strcmp(cmd, "rm")       == 0) cmd_rm(args);
        else if (strcmp(cmd, "rmdir")    == 0) cmd_rmdir(args);
        else if (strcmp(cmd, "help")     == 0) cmd_help();
        else if (strcmp(cmd, "exit")     == 0) {
            printf("Exiting MiniOS...\n");
            for (int i = 0; i < process_count; i++)
                if (processes[i].status != TERMINATED && processes[i].status != KILLED)
                    processes[i].killed = 1;
            break;
        }
        else printf("Unknown command: '%s'. Type 'help' for commands.\n", cmd);
    }

    for (int i = 0; i < process_count; i++)
        pthread_join(processes[i].thread, NULL);
}

 
//  MAIN
 
int main() {
    memset(mem_map,   0, sizeof(mem_map));
    memset(processes, 0, sizeof(processes));
    fs_init();
    shell();
    return 0;
}