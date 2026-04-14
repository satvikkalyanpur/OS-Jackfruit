/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
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
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

void write_log_to_file(const char *container_id, const char *msg)
{
    char path[256];
    snprintf(path, sizeof(path), "logs/%s.log", container_id);

    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return;

    write(fd, msg, strlen(msg));
    write(fd, "\n", 1);

    close(fd);
}
#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

#define MAX_CONTAINERS 32
char child_stack[STACK_SIZE];
typedef struct {
    char id[64];
    pid_t pid;
} container_entry;

static container_entry containers[MAX_CONTAINERS];
static int container_count = 0;
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
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
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
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
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

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
// (keeping your includes and definitions SAME — unchanged)


// ================= BUFFER PUSH =================
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

// ================= BUFFER POP =================
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

// ================= LOG THREAD =================
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) < 0)
            break;

        printf("[LOG][%s]: %s", item.container_id, item.data);
    }

    return NULL;
}

// ================= CHILD FUNCTION =================
int child_fn(void *arg)
{
    control_request_t *req = (control_request_t *)arg;

    printf(" Container started (PID namespace)\n");

    // 🔹 Hostname isolation
    sethostname(req->container_id, strlen(req->container_id));

    // 🔹 Filesystem isolation
    if (chroot(req->rootfs) != 0) {
        perror("chroot failed");
        exit(1);
    }
    chdir("/");
    mkdir("/proc", 0555);

if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
    perror("mount /proc failed");
}

    // 🔹 Execute command
    execl(req->command, req->command, NULL);

    perror("exec failed");
    exit(1);
}
int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;

    strncpy(req.container_id,
            container_id,
            sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0) {
        return -1;
    }

    return 0;
}

void handle_sigchld(int sig)
{
    while (waitpid(-1, NULL, WNOHANG) > 0);
}

// ================= SUPERVISOR =================
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
mkdir("logs", 0755);
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    pthread_mutex_init(&ctx.metadata_lock, NULL);
    bounded_buffer_init(&ctx.log_buffer);

signal(SIGCHLD, handle_sigchld);
    //  START LOG THREAD
    pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);

    // ===== SOCKET SETUP =====
    struct sockaddr_un addr;

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    unlink(CONTROL_PATH);

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(ctx.server_fd, 5);

    printf("Supervisor listening on %s\n", CONTROL_PATH);

while (1) {
    int client_fd;
    control_request_t req;
    control_response_t res;

    client_fd = accept(ctx.server_fd, NULL, NULL);
    if (client_fd < 0) continue;

    memset(&req, 0, sizeof(req));
    memset(&res, 0, sizeof(res));

    if (read(client_fd, &req, sizeof(req)) <= 0) {
        close(client_fd);
        continue;
    }

    printf("Received request: kind=%d id=%s\n",
           req.kind, req.container_id);

    // ===== RUN / START =====
    if (req.kind == CMD_RUN || req.kind == CMD_START) {

        int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS;

        control_request_t *child_req = malloc(sizeof(control_request_t));
        if (!child_req) {
            perror("malloc failed");
            res.status = -1;
            strcpy(res.message, "malloc failed");
            write(client_fd, &res, sizeof(res));
            close(client_fd);
            continue;
        }

        *child_req = req;

        pid_t pid = clone(child_fn,
                          child_stack + STACK_SIZE,
                          flags | SIGCHLD,
                          child_req);

        if (pid < 0) {
            perror("clone failed");
            free(child_req);
            res.status = -1;
            strcpy(res.message, "clone failed");
        }
        else {
            printf("Started container %s (PID %d)\n",
                   req.container_id, pid);

            // store container
            if (container_count < MAX_CONTAINERS) {
                strncpy(containers[container_count].id,
                        req.container_id,
                        sizeof(containers[0].id));
                containers[container_count].pid = pid;
                container_count++;
            }

            // logging
            char log_msg[256];
            snprintf(log_msg, sizeof(log_msg),
                     "STARTED container=%s pid=%d",
                     req.container_id, pid);

            write_log_to_file(req.container_id, log_msg);

            log_item_t item;
            memset(&item, 0, sizeof(item));
            strncpy(item.container_id, req.container_id, sizeof(item.container_id));
            snprintf(item.data, sizeof(item.data),
                     "Container %s started with PID %d\n",
                     req.container_id, pid);
            item.length = strlen(item.data);

            bounded_buffer_push(&ctx.log_buffer, &item);

            res.status = 0;
            snprintf(res.message, sizeof(res.message),
                     "Container %s started (PID %d)",
                     req.container_id, pid);
        }
    }

    // ===== STOP =====
    else if (req.kind == CMD_STOP) {
        int found = 0;

        for (int i = 0; i < container_count; i++) {
            if (strcmp(containers[i].id, req.container_id) == 0) {
                pid_t pid = containers[i].pid;

                kill(pid, SIGTERM);
                sleep(2);

                if (kill(pid, 0) == 0) {
                    kill(pid, SIGKILL);
                }

                containers[i] = containers[container_count - 1];
                container_count--;

                found = 1;
                break;
            }
        }

        if (found) {
            res.status = 0;
            snprintf(res.message, sizeof(res.message),
                     "Container %s stopped", req.container_id);
        } else {
            res.status = -1;
            strcpy(res.message, "Container not found");
        }
    }

    // ===== PS =====
    else if (req.kind == CMD_PS) {

        printf("\n%-10s %-10s\n", "CONTAINER", "PID");
        printf("----------------------\n");

        for (int i = 0; i < container_count; i++) {
            printf("%-10s %-10d\n",
                   containers[i].id,
                   containers[i].pid);
        }

        res.status = 0;
        snprintf(res.message, sizeof(res.message),
                 "Listed %d containers", container_count);
    }

    // ===== LOGS =====
    else if (req.kind == CMD_LOGS) {

        char path[256];
        snprintf(path, sizeof(path), "logs/%s.log", req.container_id);

        int fd = open(path, O_RDONLY);
        if (fd < 0) {
            res.status = -1;
            strcpy(res.message, "Log file not found");
        } else {
            char buf[512];
            int n;

            while ((n = read(fd, buf, sizeof(buf))) > 0) {
                write(STDOUT_FILENO, buf, n);
            }

            close(fd);
            res.status = 0;
            strcpy(res.message, "Logs printed");
        }
    }

    // ===== UNKNOWN =====
    else {
        res.status = -1;
        strcpy(res.message, "Unknown command");
    }

    //  SEND RESPONSE BACK
    write(client_fd, &res, sizeof(res));

    close(client_fd);
}
    // ===== CLEANUP (never reached but must be inside function) =====
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    return 0;
}
/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t res;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) < 0) {
        perror("write");
        close(fd);
        return 1;
    }

    //  READ RESPONSE FROM SERVER
    if (read(fd, &res, sizeof(res)) > 0) {
        printf("[SERVER]: %s\n", res.message);
    } else {
        printf("[SERVER]: No response received\n");
    }

    close(fd);
    return 0;
}
static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
