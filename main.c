#include <errno.h>  // errno
#include <stdio.h> // fprintf, stdout
#include <stdlib.h> // malloc, realloc, calloc, free, getenv, setenv, atoi, size_t
#include <string.h> // strerror
#include <sys/syscall.h> // SYS_gettid
#include <unistd.h> // syscall
#include <uv.h> // uv_*

#define DEBUG(fmt, ...) fprintf(stderr, "DEBUG:%lu:%s:%d:%s:" fmt, syscall(SYS_gettid), __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR(fmt, ...) fprintf(stderr, "ERROR:%lu:%s:%d:%s:" fmt, syscall(SYS_gettid), __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define FATAL(fmt, ...) fprintf(stderr, "FATAL:%lu:%s:%d:%s(%i)%s:" fmt, syscall(SYS_gettid), __FILE__, __LINE__, __func__, errno, strerror(errno), ##__VA_ARGS__)

static void server_on_start(void *arg) { // void (*uv_thread_cb)(void* arg)
    uv_loop_t loop;
    if (uv_loop_init(&loop)) { FATAL("uv_loop_init\n"); return; } // int uv_loop_init(uv_loop_t* loop)
    /*server_t *server = server_init(&loop);
    if (!server) { FATAL("server_init\n"); return; }
    uv_tcp_t tcp;
    if (uv_tcp_init(&loop, &tcp)) { FATAL("uv_tcp_init\n"); server_free(server); return; } // int uv_tcp_init(uv_loop_t* loop, uv_tcp_t* handle)
    uv_os_sock_t client_sock = *((uv_os_sock_t *)arg);
    if (uv_tcp_open(&tcp, client_sock)) { FATAL("uv_tcp_open\n"); server_free(server); return; } // int uv_tcp_open(uv_tcp_t* handle, uv_os_sock_t sock)
    int name[] = {CTL_NET, NET_CORE, NET_CORE_SOMAXCONN}, nlen = sizeof(name), oldval[nlen]; size_t oldlenp = sizeof(oldval);
    if (sysctl(name, nlen / sizeof(int), (void *)oldval, &oldlenp, NULL, 0)) { FATAL("sysctl\n"); server_free(server); return; } // int sysctl (int *name, int nlen, void *oldval, size_t *oldlenp, void *newval, size_t newlen)
    int backlog = SOMAXCONN; if (oldlenp > 0) backlog = oldval[0];
    if (uv_listen((uv_stream_t *)&tcp, backlog, client_on_connect)) { FATAL("uv_listen\n"); server_free(server); return; } // int uv_listen(uv_stream_t* stream, int backlog, uv_connection_cb cb)
    */if (uv_run(&loop, UV_RUN_DEFAULT)) { FATAL("uv_run\n"); goto server_free; } // int uv_run(uv_loop_t* loop, uv_run_mode mode)
    if (uv_loop_close(&loop)) { FATAL("uv_loop_close\n"); goto server_free; } // int uv_loop_close(uv_loop_t* loop)
server_free:
    //server_free(server);
}

int main(int argc, char **argv) {
    for (int i = 0; i < argc; i++) DEBUG("argv[%i]=%s\n", i, argv[i]);
    int error = 0;
    if ((error = uv_replace_allocator(malloc, realloc, calloc, free))) { FATAL("uv_replace_allocator\n"); return error; } // int uv_replace_allocator(uv_malloc_func malloc_func, uv_realloc_func realloc_func, uv_calloc_func calloc_func, uv_free_func free_func)
    int cpu_count; uv_cpu_info_t *cpu_infos;
    if ((error = uv_cpu_info(&cpu_infos, &cpu_count))) { FATAL("uv_cpu_info\n"); return error; } // int uv_cpu_info(uv_cpu_info_t** cpu_infos, int* count)
    uv_free_cpu_info(cpu_infos, cpu_count); // void uv_free_cpu_info(uv_cpu_info_t* cpu_infos, int count)
    char *uv_threadpool_size = getenv("UV_THREADPOOL_SIZE"); // char *getenv(const char *name);
    if (!uv_threadpool_size) {
        int length = sizeof("%d") - 2;
        for (int number = cpu_count; number /= 10; length++);
        char str[length + 1];
        if ((error = snprintf(str, length + 1, "%d", cpu_count) - length)) { FATAL("snprintf\n"); return error; } // int snprintf(char *str, size_t size, const char *format, ...)
        if ((error = setenv("UV_THREADPOOL_SIZE", str, 1))) { FATAL("setenv\n"); return error; } // int setenv(const char *name, const char *value, int overwrite)
    }
    uv_loop_t loop;
    if ((error = uv_loop_init(&loop))) { FATAL("uv_loop_init\n"); return error; } // int uv_loop_init(uv_loop_t* loop)
    /*uv_tcp_t tcp;
    if ((error = uv_tcp_init(&loop, &tcp))) { FATAL("uv_tcp_init\n"); return error; } // int uv_tcp_init(uv_loop_t* loop, uv_tcp_t* handle)
    char *webserver_port = getenv("WEBSERVER_PORT"); // char *getenv(const char *name);
    int port = 8080;
    if (webserver_port) port = atoi(webserver_port);
    struct sockaddr_in addr;
    const char *ip = "0.0.0.0";
    if ((error = uv_ip4_addr(ip, port, &addr))) { FATAL("uv_ip4_addr(%s:%i)\n", ip, port); return error; } // int uv_ip4_addr(const char* ip, int port, struct sockaddr_in* addr)
    if ((error = uv_tcp_bind(&tcp, (const struct sockaddr *)&addr, 0))) { FATAL("uv_tcp_bind(%s:%i)\n", ip, port); return error; } // int uv_tcp_bind(uv_tcp_t* handle, const struct sockaddr* addr, unsigned int flags)
    uv_os_sock_t sock;
    if ((error = uv_fileno((const uv_handle_t*)&tcp, (uv_os_fd_t *)&sock))) { FATAL("uv_fileno\n"); return error; } // int uv_fileno(const uv_handle_t* handle, uv_os_fd_t* fd)
    */int thread_count = cpu_count;
    char *webserver_thread_count = getenv("WEBSERVER_THREAD_COUNT"); // char *getenv(const char *name);
    if (webserver_thread_count) thread_count = atoi(webserver_thread_count);
    if (thread_count < 1) thread_count = cpu_count;
    if (thread_count == 1) server_on_start((void *)&loop);
    else {
        uv_thread_t tid[thread_count];
        for (int i = 0; i < thread_count; i++) if ((error = uv_thread_create(&tid[i], server_on_start, (void *)&loop))) { FATAL("uv_thread_create\n"); return error; } // int uv_thread_create(uv_thread_t* tid, uv_thread_cb entry, void* arg)
        for (int i = 0; i < thread_count; i++) if ((error = uv_thread_join(&tid[i]))) { FATAL("uv_thread_join\n"); return error; } // int uv_thread_join(uv_thread_t *tid)
    }
    if ((error = uv_loop_close(&loop))) { FATAL("uv_loop_close\n"); return error; } // int uv_loop_close(uv_loop_t* loop)
    return error;
}
