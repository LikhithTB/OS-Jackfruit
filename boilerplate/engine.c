/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ------------------------------------------------------------------ */
/*  Constants                                                           */
/* ------------------------------------------------------------------ */
#define STACK_SIZE           (1024 * 1024)
#define CONTAINER_ID_LEN     32
#define CONTROL_PATH         "/tmp/mini_runtime.sock"
#define LOG_DIR              "/tmp/mini_runtime_logs"
#define CONTROL_MESSAGE_LEN  512
#define CHILD_COMMAND_LEN    256
#define LOG_CHUNK_SIZE       4096
#define LOG_BUFFER_CAPACITY  64
#define DEFAULT_SOFT_LIMIT   (40UL << 20)
#define DEFAULT_HARD_LIMIT   (64UL << 20)
#define MAX_CONTAINERS       32
#define MONITOR_DEV          "/dev/container_monitor"

/* ------------------------------------------------------------------ */
/*  Enumerations                                                        */
/* ------------------------------------------------------------------ */
typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED,
    CONTAINER_HARD_LIMIT_KILLED
} container_state_t;

/* ------------------------------------------------------------------ */
/*  Data Structures                                                     */
/* ------------------------------------------------------------------ */
typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;          /* set before sending SIGTERM/SIGKILL */
    char log_path[PATH_MAX];
    int pipe_stdout[2];
    int pipe_stderr[2];
    pthread_t producer_thread;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
    int is_eof;                  /* sentinel: producer signals EOF */
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    int is_run;                  /* CMD_RUN: keep client socket open */
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

/* config passed into clone child via void* */
typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int stdout_write_fd;
    int stderr_write_fd;
} child_config_t;

/* producer thread arg */
typedef struct {
    container_record_t *record;
    bounded_buffer_t   *log_buffer;
} producer_arg_t;

/* handle_client thread arg */
typedef struct {
    int client_fd;
    void *ctx;                   /* supervisor_ctx_t* — forward decl trick */
} client_arg_t;

typedef struct supervisor_ctx {
    int server_fd;
    int monitor_fd;
    volatile int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    /* self-pipe for safe signal → main-loop wakeup */
    int sig_pipe[2];
} supervisor_ctx_t;

/* ------------------------------------------------------------------ */
/*  Global supervisor pointer (for signal handlers)                    */
/* ------------------------------------------------------------------ */
static supervisor_ctx_t *g_ctx = NULL;

/* ------------------------------------------------------------------ */
/*  Usage / helpers                                                     */
/* ------------------------------------------------------------------ */
static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static const char *state_to_string(container_state_t s)
{
    switch (s) {
    case CONTAINER_STARTING:          return "starting";
    case CONTAINER_RUNNING:           return "running";
    case CONTAINER_STOPPED:           return "stopped";
    case CONTAINER_KILLED:            return "killed";
    case CONTAINER_EXITED:            return "exited";
    case CONTAINER_HARD_LIMIT_KILLED: return "hard_limit_killed";
    default:                          return "unknown";
    }
}

static int parse_mib_flag(const char *flag, const char *value,
                           unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc,
                                 char *argv[], int start_index)
{
    for (int i = start_index; i < argc; i += 2) {
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1],
                               &req->soft_limit_bytes) != 0)
                return -1;
        } else if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1],
                               &req->hard_limit_bytes) != 0)
                return -1;
        } else if (strcmp(argv[i], "--nice") == 0) {
            char *end = NULL;
            errno = 0;
            long nv = strtol(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0' ||
                nv < -20 || nv > 19) {
                fprintf(stderr, "Invalid --nice value: %s\n", argv[i+1]);
                return -1;
            }
            req->nice_value = (int)nv;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return -1;
        }
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Bounded buffer                                                      */
/* ------------------------------------------------------------------ */
static int bounded_buffer_init(bounded_buffer_t *b)
{
    memset(b, 0, sizeof(*b));
    int rc;
    if ((rc = pthread_mutex_init(&b->mutex, NULL)) != 0) return rc;
    if ((rc = pthread_cond_init(&b->not_empty, NULL)) != 0) {
        pthread_mutex_destroy(&b->mutex); return rc;
    }
    if ((rc = pthread_cond_init(&b->not_full, NULL)) != 0) {
        pthread_cond_destroy(&b->not_empty);
        pthread_mutex_destroy(&b->mutex); return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *b)
{
    pthread_cond_destroy(&b->not_full);
    pthread_cond_destroy(&b->not_empty);
    pthread_mutex_destroy(&b->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *b)
{
    pthread_mutex_lock(&b->mutex);
    b->shutting_down = 1;
    pthread_cond_broadcast(&b->not_empty);
    pthread_cond_broadcast(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
}

/* Returns 0 on success, -1 if shutting down */
static int bounded_buffer_push(bounded_buffer_t *b, const log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);
    while (b->count == LOG_BUFFER_CAPACITY && !b->shutting_down)
        pthread_cond_wait(&b->not_full, &b->mutex);

    if (b->shutting_down && b->count == LOG_BUFFER_CAPACITY) {
        pthread_mutex_unlock(&b->mutex);
        return -1;
    }

    b->items[b->tail] = *item;
    b->tail = (b->tail + 1) % LOG_BUFFER_CAPACITY;
    b->count++;
    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/* Returns 0 on success, -1 if shutting down and buffer empty */
static int bounded_buffer_pop(bounded_buffer_t *b, log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);
    while (b->count == 0 && !b->shutting_down)
        pthread_cond_wait(&b->not_empty, &b->mutex);

    if (b->count == 0) {
        /* shutting_down and empty */
        pthread_mutex_unlock(&b->mutex);
        return -1;
    }

    *item = b->items[b->head];
    b->head = (b->head + 1) % LOG_BUFFER_CAPACITY;
    b->count--;
    pthread_cond_signal(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Consumer (logger) thread                                            */
/* ------------------------------------------------------------------ */
static void *logging_thread(void *arg)
{
    bounded_buffer_t *buf = (bounded_buffer_t *)arg;
    log_item_t item;

    while (1) {
        int rc = bounded_buffer_pop(buf, &item);
        if (rc != 0) break; /* shutdown + empty */

        if (item.length == 0) continue; /* empty chunk, skip */

        /* Build log path: LOG_DIR/<container_id>.log */
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            fprintf(stderr, "[logger] cannot open %s: %s\n",
                    path, strerror(errno));
            continue;
        }
        ssize_t written = 0;
        while (written < (ssize_t)item.length) {
            ssize_t n = write(fd, item.data + written,
                              item.length - written);
            if (n <= 0) break;
            written += n;
        }
        close(fd);
    }

    fprintf(stderr, "[logger] consumer thread exiting\n");
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Producer thread — one per container                                 */
/* ------------------------------------------------------------------ */
static void *producer_thread(void *arg)
{
    producer_arg_t *pa = (producer_arg_t *)arg;
    container_record_t *rec = pa->record;
    bounded_buffer_t *buf = pa->log_buffer;
    free(pa);

    int fd_out = rec->pipe_stdout[0];
    int fd_err = rec->pipe_stderr[0];

    char tmp[LOG_CHUNK_SIZE];
    int done_out = 0, done_err = 0;

    while (!done_out || !done_err) {
        fd_set rfds;
        FD_ZERO(&rfds);
        if (!done_out) FD_SET(fd_out, &rfds);
        if (!done_err) FD_SET(fd_err, &rfds);
        int maxfd = (fd_out > fd_err ? fd_out : fd_err) + 1;

        int rc = select(maxfd, &rfds, NULL, NULL, NULL);
        if (rc < 0) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < 2; i++) {
            int fd = (i == 0) ? fd_out : fd_err;
            int *done = (i == 0) ? &done_out : &done_err;
            if (*done) continue;
            if (!FD_ISSET(fd, &rfds)) continue;

            ssize_t n = read(fd, tmp, sizeof(tmp));
            if (n <= 0) {
                *done = 1;
                continue;
            }

            log_item_t item;
            memset(&item, 0, sizeof(item));
            strncpy(item.container_id, rec->id,
                    sizeof(item.container_id) - 1);
            item.length = (size_t)n;
            memcpy(item.data, tmp, (size_t)n);
            bounded_buffer_push(buf, &item);
        }
    }

    close(fd_out);
    close(fd_err);

    fprintf(stderr, "[producer:%s] exiting\n", rec->id);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Clone child entrypoint                                              */
/* ------------------------------------------------------------------ */
static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Set container hostname */
    sethostname(cfg->id, strlen(cfg->id));

    /* Redirect stdout/stderr to supervisor pipes */
    dup2(cfg->stdout_write_fd, STDOUT_FILENO);
    dup2(cfg->stderr_write_fd, STDERR_FILENO);
    close(cfg->stdout_write_fd);
    close(cfg->stderr_write_fd);

    /* chroot into container rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir /");
        return 1;
    }

    /* Mount /proc so ps/top work inside container */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        /* non-fatal: already mounted sometimes */
    }

    /* Apply nice value */
    if (cfg->nice_value != 0)
        setpriority(PRIO_PROCESS, 0, cfg->nice_value);

    /* Build argv: split command on spaces (simple tokenisation) */
    char cmd_copy[CHILD_COMMAND_LEN];
    strncpy(cmd_copy, cfg->command, sizeof(cmd_copy) - 1);

    char *argv_arr[64];
    int argc = 0;
    char *tok = strtok(cmd_copy, " \t");
    while (tok && argc < 63) {
        argv_arr[argc++] = tok;
        tok = strtok(NULL, " \t");
    }
    argv_arr[argc] = NULL;

    if (argc == 0) {
        fprintf(stderr, "Empty command\n");
        return 1;
    }

    execvp(argv_arr[0], argv_arr);
    perror("execvp");
    return 127;
}

/* ------------------------------------------------------------------ */
/*  Monitor registration helpers                                        */
/* ------------------------------------------------------------------ */
static int register_with_monitor(int monitor_fd,
                                  const char *container_id,
                                  pid_t host_pid,
                                  unsigned long soft_bytes,
                                  unsigned long hard_bytes)
{
    if (monitor_fd < 0) return 0;
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_bytes;
    req.hard_limit_bytes = hard_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0) {
        perror("ioctl MONITOR_REGISTER");
        return -1;
    }
    return 0;
}

static int unregister_from_monitor(int monitor_fd,
                                    const char *container_id,
                                    pid_t host_pid)
{
    if (monitor_fd < 0) return 0;
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Container launch                                                    */
/* ------------------------------------------------------------------ */
static container_record_t *launch_container(supervisor_ctx_t *ctx,
                                             const control_request_t *req)
{
    /* Check for duplicate ID */
    pthread_mutex_lock(&ctx->metadata_lock);
    for (container_record_t *c = ctx->containers; c; c = c->next) {
        if (strcmp(c->id, req->container_id) == 0 &&
            c->state == CONTAINER_RUNNING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            fprintf(stderr, "Container '%s' already running\n",
                    req->container_id);
            return NULL;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Allocate record */
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) return NULL;

    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->state = CONTAINER_STARTING;
    rec->started_at = time(NULL);
    snprintf(rec->log_path, sizeof(rec->log_path),
             "%s/%s.log", LOG_DIR, rec->id);

    /* Create pipes: parent reads, child writes */
    if (pipe(rec->pipe_stdout) < 0 || pipe(rec->pipe_stderr) < 0) {
        perror("pipe");
        free(rec);
        return NULL;
    }

    /* Build child config on heap so clone child can safely read it */
    child_config_t *cfg = malloc(sizeof(*cfg));
    if (!cfg) { free(rec); return NULL; }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id,      rec->id,       sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs,  req->rootfs,   sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command,  sizeof(cfg->command) - 1);
    cfg->nice_value      = req->nice_value;
    cfg->stdout_write_fd = rec->pipe_stdout[1];
    cfg->stderr_write_fd = rec->pipe_stderr[1];

    /* Allocate stack for clone */
    char *stack = malloc(STACK_SIZE);
    if (!stack) { free(cfg); free(rec); return NULL; }

    int clone_flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t pid = clone(child_fn, stack + STACK_SIZE, clone_flags, cfg);

    free(stack); /* child has its own stack; parent can free template */
    free(cfg);

    if (pid < 0) {
        perror("clone");
        close(rec->pipe_stdout[0]); close(rec->pipe_stdout[1]);
        close(rec->pipe_stderr[0]); close(rec->pipe_stderr[1]);
        free(rec);
        return NULL;
    }

    /* Close write ends in parent */
    close(rec->pipe_stdout[1]);
    close(rec->pipe_stderr[1]);

    rec->host_pid = pid;
    rec->state    = CONTAINER_RUNNING;

    /* Register with kernel monitor */
    register_with_monitor(ctx->monitor_fd, rec->id, pid,
                          rec->soft_limit_bytes,
                          rec->hard_limit_bytes);

    /* Start producer thread */
    producer_arg_t *pa = malloc(sizeof(*pa));
    pa->record     = rec;
    pa->log_buffer = &ctx->log_buffer;
    pthread_create(&rec->producer_thread, NULL, producer_thread, pa);

    /* Insert into linked list */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    fprintf(stderr, "[supervisor] started container '%s' pid=%d\n",
            rec->id, pid);
    return rec;
}

/* ------------------------------------------------------------------ */
/*  SIGCHLD handler (writes to self-pipe, main loop does the work)     */
/* ------------------------------------------------------------------ */
static void sigchld_handler(int sig)
{
    (void)sig;
    if (g_ctx) {
        char b = 'C';
        write(g_ctx->sig_pipe[1], &b, 1);
    }
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx) {
        g_ctx->should_stop = 1;
        char b = 'T';
        write(g_ctx->sig_pipe[1], &b, 1);
    }
}

/* Reap all exited children — called from main loop */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            if (c->host_pid != pid) continue;
            if (WIFEXITED(status)) {
                c->exit_code = WEXITSTATUS(status);
                c->state = CONTAINER_EXITED;
            } else if (WIFSIGNALED(status)) {
                c->exit_signal = WTERMSIG(status);
                if (c->stop_requested)
                    c->state = CONTAINER_STOPPED;
                else if (WTERMSIG(status) == SIGKILL)
                    c->state = CONTAINER_HARD_LIMIT_KILLED;
                else
                    c->state = CONTAINER_KILLED;
            }
            unregister_from_monitor(ctx->monitor_fd, c->id, c->host_pid);
            fprintf(stderr, "[supervisor] container '%s' exited state=%s\n",
                    c->id, state_to_string(c->state));
            break;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* ------------------------------------------------------------------ */
/*  Handle one CLI client connection                                    */
/* ------------------------------------------------------------------ */
static void handle_client(int cfd, supervisor_ctx_t *ctx)
{
    control_request_t req;
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    /* Read the request struct */
    ssize_t n = recv(cfd, &req, sizeof(req), MSG_WAITALL);
    if (n != (ssize_t)sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Bad request");
        send(cfd, &resp, sizeof(resp), 0);
        return;
    }

    switch (req.kind) {

    case CMD_START: {
        container_record_t *rec = launch_container(ctx, &req);
        if (rec) {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "started %s pid=%d", rec->id, rec->host_pid);
        } else {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "failed to start %s", req.container_id);
        }
        send(cfd, &resp, sizeof(resp), 0);
        break;
    }

    case CMD_RUN: {
        container_record_t *rec = launch_container(ctx, &req);
        if (!rec) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "failed to start %s", req.container_id);
            send(cfd, &resp, sizeof(resp), 0);
            break;
        }
        /* Send ack so client knows it started */
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "running %s pid=%d", rec->id, rec->host_pid);
        send(cfd, &resp, sizeof(resp), 0);

        /* Block until container exits */
        while (1) {
            sleep(1);
            pthread_mutex_lock(&ctx->metadata_lock);
            int done = (rec->state != CONTAINER_RUNNING &&
                        rec->state != CONTAINER_STARTING);
            int code = rec->exit_code;
            int sig  = rec->exit_signal;
            container_state_t st = rec->state;
            pthread_mutex_unlock(&ctx->metadata_lock);
            if (done) {
                memset(&resp, 0, sizeof(resp));
                if (st == CONTAINER_EXITED)
                    resp.status = code;
                else
                    resp.status = 128 + sig;
                snprintf(resp.message, sizeof(resp.message),
                         "exited state=%s code=%d",
                         state_to_string(st), resp.status);
                send(cfd, &resp, sizeof(resp), 0);
                break;
            }
        }
        break;
    }

    case CMD_PS: {
        char buf[4096];
        int pos = 0;
        pos += snprintf(buf + pos, sizeof(buf) - pos,
                        "%-16s %-8s %-22s %-20s %-10s %-10s\n",
                        "ID", "PID", "STARTED",
                        "STATE", "SOFT(MiB)", "HARD(MiB)");
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            char tstr[32];
            struct tm *tm = localtime(&c->started_at);
            strftime(tstr, sizeof(tstr), "%Y-%m-%d %H:%M:%S", tm);
            pos += snprintf(buf + pos, sizeof(buf) - pos,
                            "%-16s %-8d %-22s %-20s %-10lu %-10lu\n",
                            c->id, c->host_pid, tstr,
                            state_to_string(c->state),
                            c->soft_limit_bytes >> 20,
                            c->hard_limit_bytes >> 20);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 0;
        strncpy(resp.message, buf, sizeof(resp.message) - 1);
        send(cfd, &resp, sizeof(resp), 0);
        break;
    }

    case CMD_LOGS: {
        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path),
                 "%s/%s.log", LOG_DIR, req.container_id);
        int lfd = open(log_path, O_RDONLY);
        if (lfd < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "No log for '%s'", req.container_id);
            send(cfd, &resp, sizeof(resp), 0);
        } else {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "LOG:%s", log_path);
            send(cfd, &resp, sizeof(resp), 0);
            /* Stream file content */
            char fbuf[4096];
            ssize_t r;
            while ((r = read(lfd, fbuf, sizeof(fbuf))) > 0)
                write(cfd, fbuf, r);
            close(lfd);
        }
        break;
    }

    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *found = NULL;
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            if (strcmp(c->id, req.container_id) == 0) {
                found = c;
                break;
            }
        }
        if (found && (found->state == CONTAINER_RUNNING ||
                      found->state == CONTAINER_STARTING)) {
            found->stop_requested = 1;
            kill(found->host_pid, SIGTERM);
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "sent SIGTERM to '%s'", req.container_id);
        } else {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' not found or not running",
                     req.container_id);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        send(cfd, &resp, sizeof(resp), 0);
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Unknown command");
        send(cfd, &resp, sizeof(resp), 0);
        break;
    }
}

typedef struct { int cfd; supervisor_ctx_t *ctx; } hc_arg_t;
static void *handle_client_thread(void *arg)
{
    hc_arg_t *a = (hc_arg_t *)arg;
    handle_client(a->cfd, a->ctx);
    close(a->cfd);
    free(a);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Supervisor main                                                     */
/* ------------------------------------------------------------------ */
static int run_supervisor(const char *rootfs)
{
    (void)rootfs; /* base rootfs noted, per-container rootfs passed in start */

    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    /* Create log directory */
    mkdir(LOG_DIR, 0755);

    /* Init metadata lock */
    if (pthread_mutex_init(&ctx.metadata_lock, NULL) != 0) {
        perror("pthread_mutex_init");
        return 1;
    }

    /* Init bounded buffer */
    if (bounded_buffer_init(&ctx.log_buffer) != 0) {
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* Self-pipe for signals */
    if (pipe(ctx.sig_pipe) < 0) {
        perror("pipe(sig_pipe)");
        return 1;
    }
    fcntl(ctx.sig_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(ctx.sig_pipe[1], F_SETFL, O_NONBLOCK);

    /* Install signal handlers */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    /* Start consumer (logger) thread */
    if (pthread_create(&ctx.logger_thread, NULL,
                       logging_thread, &ctx.log_buffer) != 0) {
        perror("pthread_create logger");
        return 1;
    }

    /* Open kernel monitor (optional — skip if not loaded) */
    ctx.monitor_fd = open(MONITOR_DEV, O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] kernel monitor not available (%s)\n",
                strerror(errno));

    /* Create UNIX domain socket */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        return 1;
    }
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    unlink(CONTROL_PATH);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(ctx.server_fd, 16) < 0) {
        perror("listen");
        return 1;
    }
    fprintf(stderr, "[supervisor] listening on %s\n", CONTROL_PATH);

    /* Event loop: select on server_fd + sig_pipe */
    while (!ctx.should_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        FD_SET(ctx.sig_pipe[0], &rfds);
        int maxfd = ctx.server_fd > ctx.sig_pipe[0]
                    ? ctx.server_fd : ctx.sig_pipe[0];

        int rc = select(maxfd + 1, &rfds, NULL, NULL, NULL);
        if (rc < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        /* Drain self-pipe */
        if (FD_ISSET(ctx.sig_pipe[0], &rfds)) {
            char tmp[64];
            read(ctx.sig_pipe[0], tmp, sizeof(tmp));
            reap_children(&ctx);
        }

        /* Accept new CLI client */
        if (!ctx.should_stop && FD_ISSET(ctx.server_fd, &rfds)) {
            int cfd = accept(ctx.server_fd, NULL, NULL);
            if (cfd >= 0) {
                hc_arg_t *a = malloc(sizeof(*a));
                a->cfd = cfd;
                a->ctx = &ctx;
                pthread_t t;
                pthread_create(&t, NULL, handle_client_thread, a);
                pthread_detach(t);
            }
        }
    }

    fprintf(stderr, "[supervisor] shutting down...\n");

    /* Stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *c = ctx.containers; c; c = c->next) {
        if (c->state == CONTAINER_RUNNING ||
            c->state == CONTAINER_STARTING) {
            c->stop_requested = 1;
            kill(c->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Give containers a moment to exit, then reap */
    sleep(1);
    reap_children(&ctx);

    /* Join all producer threads */
    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *c = ctx.containers; c; c = c->next) {
        if (c->producer_thread) {
            pthread_mutex_unlock(&ctx.metadata_lock);
            pthread_join(c->producer_thread, NULL);
            pthread_mutex_lock(&ctx.metadata_lock);
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Shutdown consumer thread */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    /* Free container list */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        container_record_t *next = c->next;
        free(c);
        c = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Cleanup */
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    close(ctx.sig_pipe[0]);
    close(ctx.sig_pipe[1]);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    fprintf(stderr, "[supervisor] clean exit\n");
    return 0;
}

/* ------------------------------------------------------------------ */
/*  CLI client — send request, print response                           */
/* ------------------------------------------------------------------ */
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s: %s\n"
                        "Is the supervisor running?\n",
                CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    /* Send full request struct */
    if (send(fd, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    /* Read first response */
    control_response_t resp;
    ssize_t n = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
    if (n != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "Short response from supervisor\n");
        close(fd);
        return 1;
    }
    printf("%s\n", resp.message);

    /* For CMD_RUN: wait for second response (exit status) */
    if (req->kind == CMD_RUN && resp.status == 0) {
        control_response_t final;
        n = recv(fd, &final, sizeof(final), MSG_WAITALL);
        if (n == (ssize_t)sizeof(final)) {
            printf("%s\n", final.message);
            close(fd);
            return final.status;
        }
    }

    /* For CMD_LOGS: stream remaining data */
    if (req->kind == CMD_LOGS && resp.status == 0) {
        char buf[4096];
        while ((n = read(fd, buf, sizeof(buf))) > 0)
            fwrite(buf, 1, n, stdout);
    }

    close(fd);
    return (resp.status == 0) ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/*  CLI command handlers                                                */
/* ------------------------------------------------------------------ */
static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ------------------------------------------------------------------ */
/*  main                                                                */
/* ------------------------------------------------------------------ */
int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
