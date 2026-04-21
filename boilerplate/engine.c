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
#include <sys/socket.h>
#include <sys/un.h>
#include <sched.h> // For clone()

#include "monitor_ioctl.h"

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

static volatile sig_atomic_t g_supervisor_stop = 0;

static void handle_sigint(int sig) {
    (void)sig;
    g_supervisor_stop = 1;
}

static void handle_sigchld(int sig) {
    (void)sig;
    int saved_errno = errno;
    // Reap all dead children immediately to prevent zombies
    while (waitpid(-1, NULL, WNOHANG) > 0);
    errno = saved_errno;
}
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
/*
 * Bounded Buffer Push (Producer Side)
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    // Wait while the buffer is full, unless we are shutting down
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    // If shutdown started while we were waiting, abort the push
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1; 
    }

    // Insert the item at the tail
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    // Wake up any sleeping consumers
    pthread_cond_signal(&buffer->not_empty);

    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
/*
 * Bounded Buffer Pop (Consumer Side)
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    // Wait while the buffer is empty, unless we are shutting down
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    // If we are shutting down AND the buffer is empty, it's time to exit
    if (buffer->shutting_down && buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    // Extract the item from the head
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    // Wake up any sleeping producers
    pthread_cond_signal(&buffer->not_full);

    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}




/* --- Producer Thread Definition --- */
typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *log_buffer;
} producer_ctx_t;

/* --- Improved Producer Thread --- */
void *producer_thread(void *arg) {
    producer_ctx_t *pctx = (producer_ctx_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    ssize_t bytes_read;

    while (1) {
        bytes_read = read(pctx->read_fd, buf, sizeof(buf));
        if (bytes_read < 0) {
            if (errno == EINTR) continue; 
            break; 
        }
        if (bytes_read == 0) break; // EOF: Container closed the pipe

        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, pctx->container_id, CONTAINER_ID_LEN - 1);
        item.length = bytes_read;
        memcpy(item.data, buf, bytes_read);

        // This relies on your newly implemented push function!
        if (bounded_buffer_push(pctx->log_buffer, &item) != 0) {
            break; // Buffer is shutting down
        }
    }
    
    close(pctx->read_fd);
    free(pctx);
    return NULL;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    char path[PATH_MAX];
    int fd;

    mkdir(LOG_DIR, 0755);

    while (1) {
        // This relies on your newly implemented pop function!
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0) {
            break;
        }

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            // Write silently to disk
            write(fd, item.data, item.length);
            close(fd);
        }
    }
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
/* --- CLEAN child_fn (with exec fix) --- */
int child_fn(void *arg)
{
    child_config_t *config = (child_config_t *)arg;
    
    // --- THE FIX: Prepend 'exec' so the payload replaces the shell as PID 1 ---
    char smart_cmd[CHILD_COMMAND_LEN + 10];
    snprintf(smart_cmd, sizeof(smart_cmd), "exec %s", config->command);
    char *exec_args[] = { "/bin/sh", "-c", smart_cmd, NULL };

    // Keep this at the top so early setup errors are logged
    if (config->log_write_fd >= 0) {
        dup2(config->log_write_fd, STDOUT_FILENO);
        dup2(config->log_write_fd, STDERR_FILENO);
        close(config->log_write_fd);
    }

    if (sethostname(config->id, strlen(config->id)) != 0) {
        perror("sethostname failed");
        return 1;
    }

    if (chroot(config->rootfs) != 0) {
        perror("chroot failed");
        return 1;
    }
    
    if (chdir("/") != 0) {
        perror("chdir failed");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc failed");
        return 1;
    }

    if (execvp(exec_args[0], exec_args) == -1) {
        perror("execvp failed");
        return 1;
    }

    return 0; 
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
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    struct sockaddr_un addr;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) return 1;

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) return 1;

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) perror("Warning: Could not open /dev/container_monitor");

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) return 1;

    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) return 1;
    if (listen(ctx.server_fd, 5) == -1) return 1;

    printf("Supervisor listening on %s with base rootfs: %s\n", CONTROL_PATH, rootfs);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_sigchld;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_flags = 0; 
    sa.sa_handler = handle_sigint;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) return 1;

    while (!ctx.should_stop && !g_supervisor_stop) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            break;
        }

        control_request_t req;
        ssize_t bytes_read = read(client_fd, &req, sizeof(req));
        
        if (bytes_read == sizeof(req)) {
            control_response_t res;
            memset(&res, 0, sizeof(res));

            if (req.kind == CMD_START) {
                printf("Received START command for container: %s\n", req.container_id);
                int pipefd[2];
                if (pipe(pipefd) != 0) continue;

                char *stack = malloc(STACK_SIZE);
                if (stack) {
                    char *stack_top = stack + STACK_SIZE;
                    stack_top = (char *)((uintptr_t)stack_top & ~0xF);

                    child_config_t *config = malloc(sizeof(child_config_t));
                    strncpy(config->id, req.container_id, CONTAINER_ID_LEN - 1);
                    strncpy(config->rootfs, req.rootfs, PATH_MAX - 1);
                    strncpy(config->command, req.command, CHILD_COMMAND_LEN - 1);
                    config->nice_value = req.nice_value;
                    config->log_write_fd = pipefd[1];

                    int clone_flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
                    pid_t child_pid = clone(child_fn, stack_top, clone_flags, config);

                    close(pipefd[1]);

                    if (child_pid != -1) {
                        producer_ctx_t *pctx = malloc(sizeof(producer_ctx_t));
                        pctx->read_fd = pipefd[0];
                        pctx->log_buffer = &ctx.log_buffer;
                        strncpy(pctx->container_id, req.container_id, CONTAINER_ID_LEN - 1);

                        pthread_t prod_tid;
                        pthread_create(&prod_tid, NULL, producer_thread, pctx);
                        pthread_detach(prod_tid);

                        // --- NEW: Register with Kernel Monitor ---
                        if (ctx.monitor_fd >= 0) {
                            struct monitor_request mreq;
                            memset(&mreq, 0, sizeof(mreq));
                            mreq.pid = child_pid;
                            mreq.soft_limit_bytes = req.soft_limit_bytes;
                            mreq.hard_limit_bytes = req.hard_limit_bytes;
                            strncpy(mreq.container_id, req.container_id, MONITOR_NAME_LEN - 1);
                            
                            if (ioctl(ctx.monitor_fd, MONITOR_REGISTER, &mreq) < 0) {
                                perror("ioctl MONITOR_REGISTER failed");
                            }
                        }

                        container_record_t *rec = malloc(sizeof(container_record_t));
                        memset(rec, 0, sizeof(*rec));
                        strncpy(rec->id, req.container_id, CONTAINER_ID_LEN - 1);
                        rec->host_pid = child_pid;
                        rec->state = CONTAINER_RUNNING;
                        rec->started_at = time(NULL);
                        rec->soft_limit_bytes = req.soft_limit_bytes;
                        rec->hard_limit_bytes = req.hard_limit_bytes;

                        pthread_mutex_lock(&ctx.metadata_lock);
                        rec->next = ctx.containers;
                        ctx.containers = rec;
                        pthread_mutex_unlock(&ctx.metadata_lock);

                        res.status = 0;
                        snprintf(res.message, sizeof(res.message), "Container %s started with Host PID %d", req.container_id, child_pid);
                    }
                }
            } else if (req.kind == CMD_PS) {
                res.status = 0;
                char *ptr = res.message;
                size_t left = sizeof(res.message);
                int n = snprintf(ptr, left, "\n%-10s %-8s %-10s", "CONTAINER", "PID", "STATE");
                if (n > 0 && (size_t)n < left) { ptr += n; left -= n; }

                pthread_mutex_lock(&ctx.metadata_lock);
                container_record_t *curr = ctx.containers;
                while (curr && left > 0) {
                    
                    // --- NEW: THE GHOST BUSTER FIX ---
                    // If our list says it's running, double-check with the OS!
                    if (curr->state == CONTAINER_RUNNING) {
                        // kill(pid, 0) returns -1 with ESRCH if the process is completely dead
                        if (kill(curr->host_pid, 0) == -1 && errno == ESRCH) {
                            curr->state = CONTAINER_EXITED; 
                        }
                    }

                    n = snprintf(ptr, left, "\n%-10s %-8d %-10s", curr->id, curr->host_pid, state_to_string(curr->state));
                    if (n > 0 && (size_t)n < left) { ptr += n; left -= n; }
                    curr = curr->next;
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
            
            } else if (req.kind == CMD_STOP) {
                pthread_mutex_lock(&ctx.metadata_lock);
                container_record_t *curr = ctx.containers;
                int found = 0;
                while (curr) {
                    if (strcmp(curr->id, req.container_id) == 0 && curr->state == CONTAINER_RUNNING) {
                        kill(curr->host_pid, SIGTERM);
                        curr->state = CONTAINER_STOPPED;
                        found = 1;
                        
                        // --- NEW: Unregister from Kernel Monitor ---
                        if (ctx.monitor_fd >= 0) {
                            struct monitor_request mreq;
                            memset(&mreq, 0, sizeof(mreq));
                            mreq.pid = curr->host_pid;
                            strncpy(mreq.container_id, curr->id, MONITOR_NAME_LEN - 1);
                            ioctl(ctx.monitor_fd, MONITOR_UNREGISTER, &mreq);
                        }
                        break;
                    }
                    curr = curr->next;
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
                res.status = found ? 0 : -1;
            }
            write(client_fd, &res, sizeof(res));
        }
        close(client_fd);
    }

    printf("\n[Supervisor] Shutting down. Cleaning up resources...\n");

    unlink(CONTROL_PATH);
    if (ctx.server_fd >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);

    printf("[Supervisor] Joining logger thread and flushing buffer...\n");
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);
    
    pthread_mutex_destroy(&ctx.metadata_lock);
    printf("[Supervisor] Teardown complete. Exiting gracefully.\n");
    
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
    int sock_fd;
    struct sockaddr_un addr;
    control_response_t res;

    sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("connect");
        fprintf(stderr, "Is the supervisor running?\n");
        close(sock_fd);
        return 1;
    }

    // Send the request
    if (write(sock_fd, req, sizeof(*req)) != sizeof(*req)) {
        perror("write");
        close(sock_fd);
        return 1;
    }

    // Wait for the response
    if (read(sock_fd, &res, sizeof(res)) != sizeof(res)) {
        perror("read");
        close(sock_fd);
        return 1;
    }

    printf("Supervisor response [%d]: %s\n", res.status, res.message);

    close(sock_fd);
    return res.status;
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
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, argv[2]);

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "No logs found for container %s\n", argv[2]);
        return 1;
    }

    char buf[4096];
    ssize_t bytes_read;
    while ((bytes_read = read(fd, buf, sizeof(buf))) > 0) {
        write(STDOUT_FILENO, buf, bytes_read);
    }

    close(fd);
    return 0;
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
