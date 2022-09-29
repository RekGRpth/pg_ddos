#include <errno.h> // errno
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

static void server_on_start(void *arg) { // void (*uv_thread_cb)(void* arg)
    DEBUG("arg");
    /*uv_loop_t loop;
    int error;
    if ((error = uv_loop_init(&loop))) { FATAL("uv_loop_init = %s", uv_strerror(error)); return; } // int uv_loop_init(uv_loop_t* loop)
    if ((error = uv_run(&loop, UV_RUN_DEFAULT))) { FATAL("uv_run = %s", uv_strerror(error)); return; } // int uv_run(uv_loop_t* loop, uv_run_mode mode)
    if ((error = uv_loop_close(&loop))) { FATAL("uv_loop_close = %s", uv_strerror(error)); return; } // int uv_loop_close(uv_loop_t* loop)
    */
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
    if (thread_count == 1) server_on_start((void *)&loop);
    else {
        uv_thread_t tid[thread_count];
        for (int i = 0; i < thread_count; i++) if ((error = uv_thread_create(&tid[i], server_on_start, (void *)&loop))) { FATAL("uv_thread_create = %s", uv_strerror(error)); return error; } // int uv_thread_create(uv_thread_t* tid, uv_thread_cb entry, void* arg)
        for (int i = 0; i < thread_count; i++) if ((error = uv_thread_join(&tid[i]))) { FATAL("uv_thread_join = %s", uv_strerror(error)); return error; } // int uv_thread_join(uv_thread_t *tid)
    }
    if ((error = uv_run(&loop, UV_RUN_DEFAULT))) { FATAL("uv_run = %s", uv_strerror(error)); return error; } // int uv_run(uv_loop_t* loop, uv_run_mode mode)
    if ((error = uv_loop_close(&loop))) { FATAL("uv_loop_close = %s", uv_strerror(error)); return error; } // int uv_loop_close(uv_loop_t* loop)
    return error;
}
