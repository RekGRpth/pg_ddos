#include <errno.h> // errno
#include <postgresql/libpq-fe.h> // PQ*, PG*
#include <stdbool.h>
#include <stdio.h> // fprintf, stdout
#include <stdlib.h> // malloc, realloc, calloc, free, getenv, setenv, atoi, size_t
#include <string.h> // strerror
#include <sys/syscall.h> // SYS_gettid
#include <unistd.h> // syscall
#include <uv.h> // uv_*

#define DEBUG(fmt, ...) fprintf(stderr, "DEBUG:%lu:%s:%d:%s:" fmt "\n", syscall(SYS_gettid), __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR(fmt, ...) fprintf(stderr, "ERROR:%lu:%s:%d:%s:" fmt "\n", syscall(SYS_gettid), __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define FATAL(fmt, ...) fprintf(stderr, "FATAL:%lu:%s:%d:%s:" fmt "\n", syscall(SYS_gettid), __FILE__, __LINE__, __func__, ##__VA_ARGS__)
//#define FATAL(fmt, ...) fprintf(stderr, "FATAL:%lu:%s:%d:%s(%i)%s:" fmt, syscall(SYS_gettid), __FILE__, __LINE__, __func__, errno, strerror(errno), ##__VA_ARGS__)

typedef struct PQExpBufferData
{
	char	   *data;
	size_t		len;
	size_t		maxlen;
} PQExpBufferData;

typedef PQExpBufferData *PQExpBuffer;

extern void initPQExpBuffer(PQExpBuffer str);
extern void termPQExpBuffer(PQExpBuffer str);
extern void appendPQExpBuffer(PQExpBuffer str, const char *fmt,...);

#define PQExpBufferDataBroken(buf)	\
	((buf).maxlen == 0)

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
    //char *message = PQresultErrorMessage(res); // char *PQresultErrorMessage(const PGresult *res)
    ERROR("PGRES_FATAL_ERROR: %s", PQresultErrorMessage(res));
    //if (ddos_socket(ddos)) { FATAL("ddos_socket\n"); return; }
    //char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE); // char *PQresultErrorField(const PGresult *res, int fieldcode)
    //if (ddos_connection_error(sqlstate)) return;
    //request_t *request = ddos->request;
//    DEBUG("sqlstate=%s\n", sqlstate);
    //if (ddos_code_body(request, ddos_sqlstate_to_code(sqlstate), message, strlen(message))) FATAL("ddos_code_body\n");
}

static void ddos_select(ddos_t *ddos) {
//    DEBUG("postgres=%p\n", postgres);
    //if (!PQisnonblocking(postgres->conn) && PQsetnonblocking(postgres->conn, 1)) FATAL("PQsetnonblocking:%s", PQerrorMessage(postgres->conn)); // int PQisnonblocking(const PGconn *conn); int PQsetnonblocking(PGconn *conn, int arg); char *PQerrorMessage(const PGconn *conn)
    if (!PQsendQuery(ddos->conn, "select now()")) { ERROR("!PQsendQuery and %s", PQerrorMessage(ddos->conn)); ddos_finish(ddos); return; }// int PQsendQuery(PGconn *conn, const char *command); char *PQerrorMessage(const PGconn *conn)
    int error;
    if ((error = uv_poll_start(&ddos->poll, UV_WRITABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s", uv_strerror(error)); ddos_finish(ddos); return; } // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
}

static void ddos_success(ddos_t *ddos, PGresult *res) {
//    DEBUG("res=%p, ddos=%p\n", res, ddos);
    //char *value;
    //if ((value = PQcmdStatus(res)) && strlen(value)) DEBUG("PGRES_TUPLES_OK and %s", value);
    //else DEBUG("PGRES_TUPLES_OK");
    static PQprintOpt po = {
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
    //for (int col = 0; col < PQnfields(res); col++) {
    //    if (col > 0) fprintf(stdout, "\t");
    //}
//    request_t *request = ddos->request;
//    char *error = NULL;
//    if (PQntuples(res) != 1 || PQnfields(res) != 2) error = "1 row and 2 cols expected"; // int PQntuples(const PGresult *res); int PQnfields(const PGresult *res);
//    int info = PQfnumber(res, "info");
//    if (info == -1) error = "info col expected";
//    if (PQftype(res, info) != TEXTOID) error = "info col must be text"; // Oid PQftype(const PGresult *res, int column_number);
//    int body = PQfnumber(res, "body");
//    if (body == -1) error = "body col expected";
//    if (PQftype(res, body) != BYTEAOID) error = "body col must be bytea"; // Oid PQftype(const PGresult *res, int column_number);
//    if (error) { if (request) ddos_error_code_message_length(ddos, HTTP_STATUS_NO_RESPONSE, error, strlen(error)); return; }
//    if (ddos_info_body(request, PQgetvalue(res, 0, info), PQgetlength(res, 0, info), PQgetvalue(res, 0, body), PQgetlength(res, 0, body))) FATAL("ddos_info_body\n");
}

static void ddos_reset(ddos_t *ddos) {
//    DEBUG("ddos=%p, ddos->request=%p\n", ddos, ddos->request);
    int error;
    if (uv_is_active((uv_handle_t *)&ddos->poll)) if ((error = uv_poll_stop(&ddos->poll))) { ERROR("uv_poll_stop = %s", uv_strerror(error)); ddos_finish(ddos); return; } // int uv_is_active(const uv_handle_t* handle); int uv_poll_stop(uv_poll_t* poll)
    //if (ddos->request) if (request_push(ddos->request)) FATAL("request_push\n");
    //ddos->request = NULL;
    //pointer_remove(&ddos->server_pointer);
//    PQfinish(ddos->conn);
//    if ((error = ddos_connect(ddos->poll.loop, ddos))) { FATAL("ddos_connect\n"); ddos_reset(ddos); return error; }
    if (!PQresetStart(ddos->conn)) { ERROR("!PQresetStart and %s", PQerrorMessage(ddos->conn)); ddos_finish(ddos); return; } // int PQresetStart(PGconn *conn);
    if ((ddos->poll.io_watcher.fd = PQsocket(ddos->conn)) < 0) { ERROR("PQsocket < 0"); ddos_finish(ddos); ddos_finish(ddos); return; }
    if ((error = uv_poll_start(&ddos->poll, UV_WRITABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s", uv_strerror(error)); ddos_finish(ddos); return; } // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
}

static void ddos_on_poll(uv_poll_t *handle, int status, int events) { // void (*uv_poll_cb)(uv_poll_t* handle, int status, int events)
//    DEBUG("handle=%p, status=%i, events=%i\n", handle, status, events);
    ddos_t *ddos = handle->data;
    if (status) { ERROR("status = %i", status); ddos_finish(ddos); return; }
    if (PQsocket(ddos->conn) < 0) { ERROR("PQsocket < 0"); ddos_reset(ddos); return; } // int PQsocket(const PGconn *conn)
//    DEBUG("PQstatus(ddos->conn)=%i\n", PQstatus(ddos->conn));
    int error;
    switch (PQstatus(ddos->conn)) { // ConnStatusType PQstatus(const PGconn *conn)
        case CONNECTION_OK: /*DEBUG("CONNECTION_OK\n"); */break;
        case CONNECTION_BAD: ERROR("PQstatus = CONNECTION_BAD and %s", PQerrorMessage(ddos->conn)); ddos_reset(ddos); return; // char *PQerrorMessage(const PGconn *conn)
        default: switch (PQconnectPoll(ddos->conn)) { // PostgresPollingStatusType PQconnectPoll(PGconn *conn)
            case PGRES_POLLING_ACTIVE: return;
            case PGRES_POLLING_FAILED: ERROR("PGRES_POLLING_FAILED"); ddos_reset(ddos); return;
            case PGRES_POLLING_OK: ddos_select(ddos); break;
            case PGRES_POLLING_READING: if ((error = uv_poll_start(&ddos->poll, UV_READABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s", uv_strerror(error)); ddos_finish(ddos); } return; // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
            case PGRES_POLLING_WRITING: if ((error = uv_poll_start(&ddos->poll, UV_WRITABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s", uv_strerror(error)); ddos_finish(ddos); } return; // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
        }
    }
    if (events & UV_READABLE) {
        if (!PQconsumeInput(ddos->conn)) { FATAL("!PQconsumeInput and %s", PQerrorMessage(ddos->conn)); ddos_reset(ddos); return; } // int PQconsumeInput(PGconn *conn); char *PQerrorMessage(const PGconn *conn)
        //if (PQisBusy(ddos->conn)) return; // int PQisBusy(PGconn *conn)
        for (PGresult *res; (res = PQgetResult(ddos->conn)); PQclear(res)) switch (PQresultStatus(res)) { // PGresult *PQgetResult(PGconn *conn); void PQclear(PGresult *res); ExecStatusType PQresultStatus(const PGresult *res)
            case PGRES_TUPLES_OK: ddos_success(ddos, res); break;
            case PGRES_FATAL_ERROR: ddos_error(ddos, res); break;
            default: break;
        }
        for (PGnotify *notify; (notify = PQnotifies(ddos->conn)); PQfreemem(notify)) { // PGnotify *PQnotifies(PGconn *conn); void PQfreemem(void *ptr)
            DEBUG("Asynchronous notification \"%s\" with payload \"%s\" received from server process with PID %d.", notify->relname, notify->extra, notify->be_pid);
        }
        ddos_select(ddos);
        //if (ddos_push(ddos)) FATAL("ddos_push\n");
    }
    //if (events & UV_WRITABLE) switch (PQflush(ddos->conn)) { // int PQflush(PGconn *conn);
    //    case 0: /*DEBUG("No data left to send\n"); */if ((error = uv_poll_start(&ddos->poll, UV_READABLE, ddos_on_poll))) FATAL("uv_poll_start = %s", uv_strerror(error)); break; // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
    //    case 1: DEBUG("More data left to send"); break;
    //    default: FATAL("error sending query"); break;
    //}
}

static void ddos_on_start(void *arg) { // void (*uv_thread_cb)(void* arg)
    //DEBUG("arg");
    //uv_loop_t *loop = arg;
    char *conninfo = getenv("DDOS_ddos_CONNINFO"); // char *getenv(const char *name)
    if (!conninfo) conninfo = "postgresql://";
    char *ddos_ddos_count = getenv("DDOS_ddos_COUNT"); // char *getenv(const char *name);
    int count = 1;
    if (ddos_ddos_count) count = atoi(ddos_ddos_count);
    if (count < 1) count = 1;
    ddos_t *ddos;
    //uv_os_sock_t ddos_sock;
    int error;
    uv_loop_t loop;
    if ((error = uv_loop_init(&loop))) { FATAL("uv_loop_init = %s", uv_strerror(error)); return; } // int uv_loop_init(uv_loop_t* loop)
    for (int i = 0; i < count; i++) {
        if (!(ddos = malloc(sizeof(*ddos)))) { ERROR("!malloc"); continue; }
        if (PQstatus(ddos->conn = PQconnectStart(conninfo)) == CONNECTION_BAD) { ERROR("PQstatus = CONNECTION_BAD and %s", PQerrorMessage(ddos->conn)); ddos_finish(ddos); continue; }
        if (PQsetnonblocking(ddos->conn, 1) == -1) { ERROR("PQsetnonblocking == -1 and %s", PQerrorMessage(ddos->conn)); ddos_finish(ddos); continue; }
        //if ((ddos_sock = PQsocket(ddos->conn)) < 0) { ERROR("PQsocket < 0"); ddos_finish(ddos); continue; }
        if ((ddos->poll.io_watcher.fd = PQsocket(ddos->conn)) < 0) { ERROR("PQsocket < 0"); ddos_finish(ddos); ddos_finish(ddos); continue; }
        (void)PQsetNoticeProcessor(ddos->conn, ddos_notice_processor, ddos);
        if ((error = uv_poll_init_socket(&loop, &ddos->poll, ddos->poll.io_watcher.fd))) { ERROR("uv_poll_init_socket = %s", uv_strerror(error)); ddos_finish(ddos); continue; } // int uv_poll_init_socket(uv_loop_t* loop, uv_poll_t* handle, uv_os_sock_t socket)
        ddos->poll.data = ddos;
        if ((error = uv_poll_start(&ddos->poll, UV_WRITABLE, ddos_on_poll))) { ERROR("uv_poll_start = %s", uv_strerror(error)); ddos_finish(ddos); continue; } // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
    }
    if ((error = uv_run(&loop, UV_RUN_DEFAULT))) { FATAL("uv_run = %s", uv_strerror(error)); return; } // int uv_run(uv_loop_t* loop, uv_run_mode mode)
//    if ((error = uv_loop_close(&loop))) { FATAL("uv_loop_close = %s", uv_strerror(error)); return; } // int uv_loop_close(uv_loop_t* loop)
}

int main(int argc, char **argv) {
    for (int i = 0; i < argc; i++) DEBUG("argv[%i]=%s", i, argv[i]);
    int error = 0;
    int count;
    uv_cpu_info_t *cpu_infos;
    if ((error = uv_cpu_info(&cpu_infos, &count))) { FATAL("uv_cpu_info = %s", uv_strerror(error)); return error; } // int uv_cpu_info(uv_cpu_info_t** cpu_infos, int* count)
    uv_free_cpu_info(cpu_infos, count); // void uv_free_cpu_info(uv_cpu_info_t* cpu_infos, int count)
    char *uv_threadpool_size = getenv("UV_THREADPOOL_SIZE"); // char *getenv(const char *name);
    if (!uv_threadpool_size) {
        PQExpBufferData str;
        initPQExpBuffer(&str);
        appendPQExpBuffer(&str, "%d", count);
        if (PQExpBufferDataBroken(str)) { FATAL("PQExpBufferDataBroken"); termPQExpBuffer(&str); return -1; }
        if ((error = setenv("UV_THREADPOOL_SIZE", str.data, 1))) { FATAL("setenv = %s", strerror(error)); termPQExpBuffer(&str); return error; } // int setenv(const char *name, const char *value, int overwrite)
        termPQExpBuffer(&str);
    }
    uv_loop_t loop;
    if ((error = uv_loop_init(&loop))) { FATAL("uv_loop_init = %s", uv_strerror(error)); return error; } // int uv_loop_init(uv_loop_t* loop)
    int thread_count = count;
    char *ddos_thread_count = getenv("DDOS_THREAD_COUNT"); // char *getenv(const char *name);
    if (ddos_thread_count) thread_count = atoi(ddos_thread_count);
    if (thread_count < 1) thread_count = count;
    if (thread_count == 1) ddos_on_start((void *)&loop);
    else {
        uv_thread_t tid[thread_count];
        for (int i = 0; i < thread_count; i++) if ((error = uv_thread_create(&tid[i], ddos_on_start, (void *)&loop))) { FATAL("uv_thread_create = %s", uv_strerror(error)); return error; } // int uv_thread_create(uv_thread_t* tid, uv_thread_cb entry, void* arg)
        for (int i = 0; i < thread_count; i++) if ((error = uv_thread_join(&tid[i]))) { FATAL("uv_thread_join = %s", uv_strerror(error)); return error; } // int uv_thread_join(uv_thread_t *tid)
    }
//    if ((error = uv_run(&loop, UV_RUN_DEFAULT))) { FATAL("uv_run = %s", uv_strerror(error)); return error; } // int uv_run(uv_loop_t* loop, uv_run_mode mode)
//    if ((error = uv_loop_close(&loop))) { FATAL("uv_loop_close = %s", uv_strerror(error)); return error; } // int uv_loop_close(uv_loop_t* loop)
    return error;
}
