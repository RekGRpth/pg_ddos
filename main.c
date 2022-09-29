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
} postgres_t;

static void ddos_notice_processor(void *arg, const char *message) {
    DEBUG("PGRES_NONFATAL_ERROR: %s", message);
}

static void postgres_finish(postgres_t *postgres) {
    PQfinish(postgres->conn);
    free(postgres);
}

static void postgres_reset(postgres_t *postgres);

static void postgres_error(postgres_t *postgres, PGresult *res) {
    //char *message = PQresultErrorMessage(res); // char *PQresultErrorMessage(const PGresult *res)
    ERROR("PGRES_FATAL_ERROR: %s", PQresultErrorMessage(res));
    //if (postgres_socket(postgres)) { FATAL("postgres_socket\n"); return; }
    //char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE); // char *PQresultErrorField(const PGresult *res, int fieldcode)
    //if (postgres_connection_error(sqlstate)) return;
    //request_t *request = postgres->request;
//    DEBUG("sqlstate=%s\n", sqlstate);
    //if (postgres_code_body(request, postgres_sqlstate_to_code(sqlstate), message, strlen(message))) FATAL("postgres_code_body\n");
}

static void postgres_success(postgres_t *postgres, PGresult *res) {
//    DEBUG("res=%p, postgres=%p\n", res, postgres);
    char *value;
    if ((value = PQcmdStatus(res)) && strlen(value)) DEBUG("PGRES_TUPLES_OK and %s", value);
    else DEBUG("PGRES_TUPLES_OK");
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
//    request_t *request = postgres->request;
//    char *error = NULL;
//    if (PQntuples(res) != 1 || PQnfields(res) != 2) error = "1 row and 2 cols expected"; // int PQntuples(const PGresult *res); int PQnfields(const PGresult *res);
//    int info = PQfnumber(res, "info");
//    if (info == -1) error = "info col expected";
//    if (PQftype(res, info) != TEXTOID) error = "info col must be text"; // Oid PQftype(const PGresult *res, int column_number);
//    int body = PQfnumber(res, "body");
//    if (body == -1) error = "body col expected";
//    if (PQftype(res, body) != BYTEAOID) error = "body col must be bytea"; // Oid PQftype(const PGresult *res, int column_number);
//    if (error) { if (request) postgres_error_code_message_length(postgres, HTTP_STATUS_NO_RESPONSE, error, strlen(error)); return; }
//    if (postgres_info_body(request, PQgetvalue(res, 0, info), PQgetlength(res, 0, info), PQgetvalue(res, 0, body), PQgetlength(res, 0, body))) FATAL("postgres_info_body\n");
}

static void postgres_on_poll(uv_poll_t *handle, int status, int events) { // void (*uv_poll_cb)(uv_poll_t* handle, int status, int events)
//    DEBUG("handle=%p, status=%i, events=%i\n", handle, status, events);
    postgres_t *postgres = handle->data;
    if (status) { ERROR("status = %i", status); postgres_reset(postgres); return; }
    if (PQsocket(postgres->conn) < 0) { ERROR("PQsocket < 0"); postgres_reset(postgres); return; } // int PQsocket(const PGconn *conn)
//    DEBUG("PQstatus(postgres->conn)=%i\n", PQstatus(postgres->conn));
    int error;
    switch (PQstatus(postgres->conn)) { // ConnStatusType PQstatus(const PGconn *conn)
        case CONNECTION_OK: /*DEBUG("CONNECTION_OK\n"); */break;
        case CONNECTION_BAD: ERROR("PQstatus = CONNECTION_BAD and %s", PQerrorMessage(postgres->conn)); postgres_reset(postgres); return; // char *PQerrorMessage(const PGconn *conn)
        default: switch (PQconnectPoll(postgres->conn)) { // PostgresPollingStatusType PQconnectPoll(PGconn *conn)
            case PGRES_POLLING_ACTIVE: return;
            case PGRES_POLLING_FAILED: ERROR("PGRES_POLLING_FAILED"); postgres_reset(postgres); return;
            case PGRES_POLLING_OK: postgres_select(postgres); break;
            case PGRES_POLLING_READING: if ((error = uv_poll_start(&postgres->poll, UV_READABLE, postgres_on_poll))) { ERROR("uv_poll_start = %s", uv_strerror(error)); postgres_finish(postgres); } return; // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
            case PGRES_POLLING_WRITING: if ((error = uv_poll_start(&postgres->poll, UV_WRITABLE, postgres_on_poll))) { ERROR("uv_poll_start = %s", uv_strerror(error)); postgres_finish(postgres); } return; // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
        }
    }
    if (events & UV_READABLE) {
        if (!PQconsumeInput(postgres->conn)) { FATAL("!PQconsumeInput and %s", PQerrorMessage(postgres->conn)); postgres_reset(postgres); return; } // int PQconsumeInput(PGconn *conn); char *PQerrorMessage(const PGconn *conn)
        //if (PQisBusy(postgres->conn)) return; // int PQisBusy(PGconn *conn)
        for (PGresult *res; (res = PQgetResult(postgres->conn)); PQclear(res)) switch (PQresultStatus(res)) { // PGresult *PQgetResult(PGconn *conn); void PQclear(PGresult *res); ExecStatusType PQresultStatus(const PGresult *res)
            case PGRES_TUPLES_OK: postgres_success(postgres, res); break;
            case PGRES_FATAL_ERROR: postgres_error(postgres, res); break;
            default: break;
        }
        for (PGnotify *notify; (notify = PQnotifies(postgres->conn)); PQfreemem(notify)) { // PGnotify *PQnotifies(PGconn *conn); void PQfreemem(void *ptr)
            DEBUG("Asynchronous notification \"%s\" with payload \"%s\" received from server process with PID %d.", notify->relname, notify->extra, notify->be_pid);
        }
        //if (postgres_push(postgres)) FATAL("postgres_push\n");
    }
    //if (events & UV_WRITABLE) switch (PQflush(postgres->conn)) { // int PQflush(PGconn *conn);
    //    case 0: /*DEBUG("No data left to send\n"); */if ((error = uv_poll_start(&postgres->poll, UV_READABLE, postgres_on_poll))) FATAL("uv_poll_start = %s", uv_strerror(error)); break; // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
    //    case 1: DEBUG("More data left to send"); break;
    //    default: FATAL("error sending query"); break;
    //}
}

static void postgres_reset(postgres_t *postgres) {
//    DEBUG("postgres=%p, postgres->request=%p\n", postgres, postgres->request);
    int error;
    if (uv_is_active((uv_handle_t *)&postgres->poll)) if ((error = uv_poll_stop(&postgres->poll))) { ERROR("uv_poll_stop = %s", uv_strerror(error)); postgres_finish(postgres); return; } // int uv_is_active(const uv_handle_t* handle); int uv_poll_stop(uv_poll_t* poll)
    //if (postgres->request) if (request_push(postgres->request)) FATAL("request_push\n");
    //postgres->request = NULL;
    //pointer_remove(&postgres->server_pointer);
//    PQfinish(postgres->conn);
//    if ((error = postgres_connect(postgres->poll.loop, postgres))) { FATAL("postgres_connect\n"); postgres_reset(postgres); return error; }
    if (!PQresetStart(postgres->conn)) { ERROR("!PQresetStart and %s", PQerrorMessage(postgres->conn)); postgres_finish(postgres); return; } // int PQresetStart(PGconn *conn);
    if ((postgres->poll.io_watcher.fd = PQsocket(postgres->conn)) < 0) { ERROR("PQsocket < 0"); postgres_finish(postgres); postgres_finish(postgres); return; }
    if ((error = uv_poll_start(&postgres->poll, UV_WRITABLE, postgres_on_poll))) { ERROR("uv_poll_start = %s", uv_strerror(error)); postgres_finish(postgres); return; } // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
}

static void ddos_on_start(void *arg) { // void (*uv_thread_cb)(void* arg)
    //DEBUG("arg");
    uv_loop_t *loop = arg;
    char *conninfo = getenv("DDOS_POSTGRES_CONNINFO"); // char *getenv(const char *name)
    if (!conninfo) conninfo = "postgresql://localhost?application_name=ddos";
    char *ddos_postgres_count = getenv("DDOS_POSTGRES_COUNT"); // char *getenv(const char *name);
    int count = 1;
    if (ddos_postgres_count) count = atoi(ddos_postgres_count);
    if (count < 1) count = 1;
    postgres_t *postgres;
    //uv_os_sock_t postgres_sock;
    int error;
    for (int i = 0; i < count; i++) {
        if (!(postgres = malloc(sizeof(*postgres)))) { ERROR("!malloc"); continue; }
        if (PQstatus(postgres->conn = PQconnectStart(conninfo)) == CONNECTION_BAD) { ERROR("PQstatus = CONNECTION_BAD and %s", PQerrorMessage(postgres->conn)); postgres_finish(postgres); continue; }
        if (PQsetnonblocking(postgres->conn, 1) == -1) { ERROR("PQsetnonblocking == -1 and %s", PQerrorMessage(postgres->conn)); postgres_finish(postgres); continue; }
        //if ((postgres_sock = PQsocket(postgres->conn)) < 0) { ERROR("PQsocket < 0"); postgres_finish(postgres); continue; }
        if ((postgres->poll.io_watcher.fd = PQsocket(postgres->conn)) < 0) { ERROR("PQsocket < 0"); postgres_finish(postgres); postgres_finish(postgres); continue; }
        (void)PQsetNoticeProcessor(postgres->conn, ddos_notice_processor, postgres);
        if ((error = uv_poll_init_socket(loop, &postgres->poll, postgres->poll.io_watcher.fd))) { ERROR("uv_poll_init_socket = %s", uv_strerror(error)); postgres_finish(postgres); continue; } // int uv_poll_init_socket(uv_loop_t* loop, uv_poll_t* handle, uv_os_sock_t socket)
        postgres->poll.data = postgres;
        if ((error = uv_poll_start(&postgres->poll, UV_WRITABLE, postgres_on_poll))) { ERROR("uv_poll_start = %s", uv_strerror(error)); postgres_finish(postgres); continue; } // int uv_poll_start(uv_poll_t* handle, int events, uv_poll_cb cb)
    }
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
    if ((error = uv_run(&loop, UV_RUN_DEFAULT))) { FATAL("uv_run = %s", uv_strerror(error)); return error; } // int uv_run(uv_loop_t* loop, uv_run_mode mode)
    if ((error = uv_loop_close(&loop))) { FATAL("uv_loop_close = %s", uv_strerror(error)); return error; } // int uv_loop_close(uv_loop_t* loop)
    return error;
}
