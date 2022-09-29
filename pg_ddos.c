#include <postgresql/libpq-fe.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <uv.h>

#define DEBUG(fmt, ...) fprintf(stderr, "DEBUG:%lu:%s:%d:%s:" fmt, syscall(SYS_gettid), __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR(fmt, ...) fprintf(stderr, "ERROR:%lu:%s:%d:%s:" fmt, syscall(SYS_gettid), __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define FATAL(fmt, ...) fprintf(stderr, "FATAL:%lu:%s:%d:%s:" fmt, syscall(SYS_gettid), __FILE__, __LINE__, __func__, ##__VA_ARGS__)

typedef struct {
    uv_poll_t poll;
    PGconn *conn;
} ddos_t;

static void ddos_notice_processor(void *arg, const char *message) {
    DEBUG("PGRES_NONFATAL_ERROR: %s", message);
}

static void ddos_finish(ddos_t *ddos) {
    PQfinish(ddos->conn);
    free(ddos);
}

static void ddos_on_poll(uv_poll_t *handle, int status, int events);

static void ddos_error(ddos_t *ddos, PGresult *res) {
    ERROR("PGRES_FATAL_ERROR: %s", PQresultErrorMessage(res));
}

static void ddos_select(ddos_t *ddos) {
    if (!PQsendQuery(ddos->conn, "select now()")) { ERROR("!PQsendQuery and %s", PQerrorMessage(ddos->conn)); ddos_finish(ddos); return; }
    int rc;
    if ((rc = uv_poll_start(&ddos->poll, UV_WRITABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s\n", uv_strerror(rc)); ddos_finish(ddos); return; }
}

static void ddos_success(ddos_t *ddos, PGresult *res) {
    static const PQprintOpt po = {
        .header = true,
        .align = true,
        .standard = false,
        .html3 = false,
        .expanded = false,
        .pager = false,
        .fieldSep = "\t",
        .tableOpt = NULL,
        .caption = NULL,
        .fieldName = NULL,
    };
    PQprint(stdout, res, &po);
}

static void ddos_reset(ddos_t *ddos) {
    int rc;
    if (uv_is_active((uv_handle_t *)&ddos->poll)) if ((rc = uv_poll_stop(&ddos->poll))) { ERROR("uv_poll_stop = %s\n", uv_strerror(rc)); ddos_finish(ddos); return; }
    if (!PQresetStart(ddos->conn)) { ERROR("!PQresetStart and %s", PQerrorMessage(ddos->conn)); ddos_finish(ddos); return; }
    if ((ddos->poll.io_watcher.fd = PQsocket(ddos->conn)) < 0) { ERROR("PQsocket < 0\n"); ddos_finish(ddos); ddos_finish(ddos); return; }
    if ((rc = uv_poll_start(&ddos->poll, UV_WRITABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s\n", uv_strerror(rc)); ddos_finish(ddos); return; }
}

static void ddos_on_poll(uv_poll_t *handle, int status, int events) {
    ddos_t *ddos = handle->data;
    if (PQsocket(ddos->conn) < 0) { ERROR("PQsocket < 0\n"); ddos_reset(ddos); return; }
    int rc;
    switch (PQstatus(ddos->conn)) {
        case CONNECTION_OK: break;
        case CONNECTION_BAD: ERROR("PQstatus = CONNECTION_BAD and %s", PQerrorMessage(ddos->conn)); ddos_reset(ddos); return;
        default: switch (PQconnectPoll(ddos->conn)) {
            case PGRES_POLLING_ACTIVE: return;
            case PGRES_POLLING_FAILED: ERROR("PGRES_POLLING_FAILED and %s", PQerrorMessage(ddos->conn)); ddos_finish(ddos); return;
            case PGRES_POLLING_OK: break;
            case PGRES_POLLING_READING: if ((rc = uv_poll_start(&ddos->poll, UV_READABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s\n", uv_strerror(rc)); ddos_finish(ddos); } return;
            case PGRES_POLLING_WRITING: if ((rc = uv_poll_start(&ddos->poll, UV_WRITABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s\n", uv_strerror(rc)); ddos_finish(ddos); } return;
        }
    }
    if (!PQconsumeInput(ddos->conn)) { FATAL("!PQconsumeInput and %s", PQerrorMessage(ddos->conn)); ddos_reset(ddos); return; }
    for (PGresult *res; (res = PQgetResult(ddos->conn)); PQclear(res)) switch (PQresultStatus(res)) {
        case PGRES_TUPLES_OK: ddos_success(ddos, res); break;
        case PGRES_FATAL_ERROR: ddos_error(ddos, res); break;
        default: break;
    }
    for (PGnotify *notify; (notify = PQnotifies(ddos->conn)); PQfreemem(notify)) {
        DEBUG("Asynchronous notification \"%s\" with payload \"%s\" received from server process with PID %d.\n", notify->relname, notify->extra, notify->be_pid);
    }
    ddos_select(ddos);
}

static void ddos_on_start(void *arg) {
    char *conninfo = getenv("DDOS_CONNINFO");
    if (!conninfo) conninfo = "";
    char *ddos_ddos_count = getenv("DDOS_COUNT");
    int count = 1;
    if (ddos_ddos_count) count = atoi(ddos_ddos_count);
    if (count < 1) count = 1;
    ddos_t *ddos;
    int rc;
    uv_loop_t loop;
    if ((rc = uv_loop_init(&loop))) { FATAL("uv_loop_init = %s\n", uv_strerror(rc)); return; }
    for (int i = 0; i < count; i++) {
        if (!(ddos = malloc(sizeof(*ddos)))) { ERROR("!malloc\n"); continue; }
        if (PQstatus(ddos->conn = PQconnectStart(conninfo)) == CONNECTION_BAD) { ERROR("PQstatus = CONNECTION_BAD and %s", PQerrorMessage(ddos->conn)); ddos_finish(ddos); continue; }
        if (PQsetnonblocking(ddos->conn, 1) == -1) { ERROR("PQsetnonblocking == -1 and %s", PQerrorMessage(ddos->conn)); ddos_finish(ddos); continue; }
        if ((ddos->poll.io_watcher.fd = PQsocket(ddos->conn)) < 0) { ERROR("PQsocket < 0\n"); ddos_finish(ddos); ddos_finish(ddos); continue; }
        (void)PQsetNoticeProcessor(ddos->conn, ddos_notice_processor, ddos);
        if ((rc = uv_poll_init_socket(&loop, &ddos->poll, ddos->poll.io_watcher.fd))) { ERROR("uv_poll_init_socket = %s\n", uv_strerror(rc)); ddos_finish(ddos); continue; }
        ddos->poll.data = ddos;
        if ((rc = uv_poll_start(&ddos->poll, UV_WRITABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s\n", uv_strerror(rc)); ddos_finish(ddos); continue; }
    }
    if ((rc = uv_run(&loop, UV_RUN_DEFAULT))) { FATAL("uv_run = %s\n", uv_strerror(rc)); return; }
//    if ((rc = uv_loop_close(&loop))) { FATAL("uv_loop_close = %s\n", uv_strerror(rc)); return; }
}

int main(int argc, char **argv) {
    for (int i = 0; i < argc; i++) DEBUG("argv[%i]=%s\n", i, argv[i]);
    int rc = 0;
    int count;
    uv_cpu_info_t *cpu_infos;
    if ((rc = uv_cpu_info(&cpu_infos, &count))) { FATAL("uv_cpu_info = %s\n", uv_strerror(rc)); return rc; }
    uv_free_cpu_info(cpu_infos, count);
    uv_loop_t loop;
    if ((rc = uv_loop_init(&loop))) { FATAL("uv_loop_init = %s\n", uv_strerror(rc)); return rc; }
    int thread_count = count;
    char *ddos_thread_count = getenv("DDOS_THREAD_COUNT");
    if (ddos_thread_count) thread_count = atoi(ddos_thread_count);
    if (thread_count < 1) thread_count = count;
    if (thread_count == 1) ddos_on_start((void *)&loop); else {
        uv_thread_t tid[thread_count];
        for (int i = 0; i < thread_count; i++) if ((rc = uv_thread_create(&tid[i], ddos_on_start, (void *)&loop))) { FATAL("uv_thread_create = %s\n", uv_strerror(rc)); return rc; }
        for (int i = 0; i < thread_count; i++) if ((rc = uv_thread_join(&tid[i]))) { FATAL("uv_thread_join = %s\n", uv_strerror(rc)); return rc; }
    }
//    if ((rc = uv_run(&loop, UV_RUN_DEFAULT))) { FATAL("uv_run = %s\n", uv_strerror(rc)); return rc; }
//    if ((rc = uv_loop_close(&loop))) { FATAL("uv_loop_close = %s\n", uv_strerror(rc)); return rc; }
    return rc;
}
