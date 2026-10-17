# Building a Layer 7 (HTTP) Load Balancer in C: Step-by-Step Implementation Guide

This guide walks you through designing, implementing, and testing a production-style **Layer 7 (Application Layer) HTTP Reverse Proxy and Load Balancer** in standard C (`C11`) on Linux using POSIX sockets and `epoll`.

---

## 1. Architecture & How L7 Differs from L4

Unlike a Layer 4 (TCP) load balancer that blindly forwards raw TCP byte streams based only on IP addresses and ports, a **Layer 7 Load Balancer** terminates the client TCP connection, parses the application-layer HTTP protocol, inspects headers and URI paths, and routes each HTTP request to an appropriate upstream backend pool.

| Capability | Layer 4 (TCP) Load Balancer | Layer 7 (HTTP) Load Balancer |
| :--- | :--- | :--- |
| **Connection Model** | Single passthrough or NAT tuple mapping | Dual TCP termination (Client ↔ Proxy and Proxy ↔ Backend) |
| **Routing Granularity** | Source/Destination IP and TCP Port | HTTP `Host` header, URI path (`/api`, `/static`), HTTP method, headers, or cookies |
| **Header Manipulation** | Cannot inspect or modify HTTP payload | Injects `X-Forwarded-For`, `X-Forwarded-Proto`, rewrites `Host` and `Connection` |
| **Health Awareness** | TCP 3-way handshake check (`SYN`/`ACK`) | HTTP status code validation (`GET /health` expecting `200 OK`) |
| **Session Persistence** | Source IP hash only | HTTP Cookie insertion/inspection or custom header hashing |

### High-Level Request Lifecycle

1. **Accept**: Client connects to the load balancer's listening socket (`:8080`). The socket is set to non-blocking mode and registered with `epoll`.
2. **Buffer & Parse HTTP Headers**: The load balancer reads bytes into a per-session request buffer until the end-of-headers delimiter (`\r\n\r\n`) arrives.
3. **L7 Route Match**: The router inspects the parsed `Host` header and URI path (e.g., `/api/v1/users`) to select an **Upstream Pool**.
4. **Backend Selection**: The load balancing scheduler (Round Robin, Weighted Round Robin, or Least Connections) selects a healthy **Backend Server** from the matched pool.
5. **Async Upstream Connect**: The load balancer initiates a non-blocking TCP `connect()` to the chosen backend (`EINPROGRESS`) and registers the backend file descriptor with `epoll`.
6. **Header Rewriting & Forwarding**: Once the backend socket becomes writable, the load balancer injects proxy headers (`X-Forwarded-For`, `Connection: close`) and streams the request headers and body.
7. **Response Relay & Teardown**: The load balancer reads the HTTP response stream from the backend, writes it back to the client handling partial writes (`EAGAIN`), and decrements active connection counters upon completion.

---

## 2. Project Directory Structure

Organize your C workspace into modular headers and implementation files before writing code:

```text
l7-load-balancer/
├── Makefile
├── include/
│   ├── common.h          # Constants, logging macros, error codes
│   ├── http_parser.h     # HTTP request parser structs and prototypes
│   ├── backend.h         # Backend, pool, route, and scheduler structs
│   ├── session.h         # Client-to-backend proxy session state machine
│   └── health.h          # Active (timerfd) and passive health check APIs
└── src/
    ├── main.c            # CLI parsing, signal setup, event loop entry
    ├── net.c             # Socket creation, non-blocking helpers, epoll wrappers
    ├── http_parser.c     # State-machine HTTP/1.1 header parser & header rewriter
    ├── balancer.c        # L7 routing table and RR / WRR / Least-Conn algorithms
    ├── session.c         # Bidirectional buffer relay and state transitions
    └── health.c          # Periodic HTTP health probe implementation
```

---

## 3. Core Data Structures (`include/backend.h` & `include/session.h`)

### Step 3.1: Define Backend Pools and L7 Routes

Create `include/backend.h` to represent individual upstream servers, upstream pools, and L7 routing rules.

```c
#ifndef BACKEND_H
#define BACKEND_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#define MAX_BACKENDS_PER_POOL 16
#define MAX_POOLS 8
#define MAX_ROUTES 16

typedef enum {
    LB_ALGO_ROUND_ROBIN = 0,
    LB_ALGO_WEIGHTED_RR,
    LB_ALGO_LEAST_CONN,
    LB_ALGO_IP_HASH
} lb_algo_t;

typedef struct {
    char ip[64];
    uint16_t port;
    int weight;                 // Static weight (for WRR)
    int current_weight;         // Dynamic weight counter (for smooth WRR)
    int active_connections;     // Tracked for Least Connections
    int consecutive_failures;   // Tracked for Passive Health Checks
    time_t marked_down_until;   // Cooldown timestamp if unhealthy
    bool is_healthy;
} backend_t;

typedef struct {
    char name[64];
    lb_algo_t algorithm;
    backend_t backends[MAX_BACKENDS_PER_POOL];
    size_t backend_count;
    size_t rr_index;            // Cursor for standard Round Robin
} upstream_pool_t;

typedef struct {
    char host_match[128];       // e.g., "api.example.com" or "*" for wildcard
    char path_prefix[128];      // e.g., "/api/" or "/"
    upstream_pool_t *pool;      // Target pool when Host and Path match
} l7_route_t;

#endif
```

### Step 3.2: Define the Proxy Session State Machine

Because `epoll` notifications are asynchronous and associate a single `void *ptr` (`ev.data.ptr`) with each file descriptor, wrap your descriptor alongside a pointer to its parent `proxy_session_t` so you immediately know whether an event came from the **client socket** or the **backend socket**.

```c
#ifndef SESSION_H
#define SESSION_H

#include <stddef.h>
#include "backend.h"

#define BUFFER_CAPACITY 65536

typedef enum {
    SESSION_READ_CLIENT_HEADERS = 0,
    SESSION_CONNECTING_BACKEND,
    SESSION_FORWARDING_REQUEST,
    SESSION_RELAYING_RESPONSE,
    SESSION_CLOSING
} session_state_t;

typedef enum {
    FD_ROLE_LISTENER = 0,
    FD_ROLE_CLIENT,
    FD_ROLE_BACKEND,
    FD_ROLE_HEALTH_TIMER
} fd_role_t;

struct proxy_session;

typedef struct {
    int fd;
    fd_role_t role;
    struct proxy_session *session;
} epoll_context_t;

typedef struct {
    char method[16];
    char uri[1024];
    char version[16];
    char host[256];
    size_t header_len;          // Byte offset where \r\n\r\n ends
    size_t content_length;      // Parsed Content-Length (0 if none)
} http_request_meta_t;

typedef struct proxy_session {
    session_state_t state;
    epoll_context_t client_ctx;
    epoll_context_t backend_ctx;
    char client_ip[64];

    backend_t *assigned_backend;
    http_request_meta_t req_meta;

    // Request buffer (Client -> Backend)
    char req_buf[BUFFER_CAPACITY];
    size_t req_buf_len;
    size_t req_buf_sent;

    // Response buffer (Backend -> Client)
    char resp_buf[BUFFER_CAPACITY];
    size_t resp_buf_len;
    size_t resp_buf_sent;
} proxy_session_t;

#endif
```

---

## 4. Milestone 1: Non-Blocking Network Core & `epoll` Loop

### Step 4.1: Socket Utility Functions (`src/net.c`)

Every socket in an event-driven load balancer **must** be non-blocking (`O_NONBLOCK`). If a socket blocks on `read()` or `write()`, a single slow client will freeze the entire load balancer for all other clients.

```c
// Pseudocode & C implementation for src/net.c
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int create_listen_socket(uint16_t port) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) return -1;

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    if (set_nonblocking(listen_fd) < 0) {
        close(listen_fd);
        return -1;
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(listen_fd);
        return -1;
    }

    if (listen(listen_fd, SOMAXCONN) < 0) {
        close(listen_fd);
        return -1;
    }

    return listen_fd;
}
```

### Step 4.2: Initiating a Non-Blocking Backend Connection

When connecting to an upstream backend, `connect()` on a non-blocking socket immediately returns `-1` with `errno == EINPROGRESS`. You must register the socket with `epoll` for `EPOLLOUT` and verify connection success via `getsockopt(..., SO_ERROR, ...)` when `EPOLLOUT` triggers.

```c
int connect_to_backend_async(const char *ip, uint16_t port) {
    int backend_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (backend_fd < 0) return -1;

    set_nonblocking(backend_fd);

    // Disable Nagle's algorithm for low-latency proxy relay
    int flag = 1;
    setsockopt(backend_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    int rc = connect(backend_fd, (struct sockaddr *)&addr, sizeof(addr));
    if (rc == 0 || (rc < 0 && errno == EINPROGRESS)) {
        return backend_fd; // Connection in progress or immediately ready
    }

    close(backend_fd);
    return -1;
}
```

### Step 4.3: Main Event Loop Pseudocode (`src/main.c`)

First ignore `SIGPIPE` so writing to a abruptly closed client socket returns `-1` (`EPIPE`) instead of killing the load balancer process.

```c
// Event Loop Structure in src/main.c
signal(SIGPIPE, SIG_IGN);

int epoll_fd = epoll_create1(0);
int listen_fd = create_listen_socket(8080);

epoll_context_t listener_ctx = { .fd = listen_fd, .role = FD_ROLE_LISTENER, .session = NULL };
register_epoll(epoll_fd, listen_fd, EPOLLIN, &listener_ctx);

struct epoll_event events[1024];

while (running) {
    int n = epoll_wait(epoll_fd, events, 1024, 1000);
    for (int i = 0; i < n; i++) {
        epoll_context_t *ctx = (epoll_context_t *)events[i].data.ptr;
        uint32_t revents = events[i].events;

        if (ctx->role == FD_ROLE_LISTENER) {
            accept_new_clients(epoll_fd, listen_fd);
        } else if (ctx->role == FD_ROLE_HEALTH_TIMER) {
            run_active_health_checks(epoll_fd, ctx->fd);
        } else if (ctx->role == FD_ROLE_CLIENT) {
            handle_client_event(epoll_fd, ctx->session, revents);
        } else if (ctx->role == FD_ROLE_BACKEND) {
            handle_backend_event(epoll_fd, ctx->session, revents);
        }
    }
}
```

---

## 5. Milestone 2: L7 HTTP/1.1 Parser & Header Rewriter

### Step 5.1: Parsing Request Line and Headers (`src/http_parser.c`)

Your L7 load balancer does not need to parse every obscure HTTP header. It only needs to:
1. Detect whether the complete HTTP header block (`\r\n\r\n`) has arrived.
2. Parse the Request Line (`GET /api/items HTTP/1.1`).
3. Extract `Host:` and `Content-Length:` (case-insensitively using `strncasecmp`).
4. Rewrite the outgoing header block to inject `X-Forwarded-For: <client_ip>` and `Connection: close` (to simplify 1-request-per-upstream-connection lifecycle before you add keep-alive pooling).

```c
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdlib.h>
#include "session.h"

// Returns:
//  1 -> Complete headers parsed successfully
//  0 -> Incomplete headers, wait for more EPOLLIN data
// -1 -> Malformed HTTP request (send 400 Bad Request)
int parse_http_headers(const char *buf, size_t len, http_request_meta_t *meta) {
    const char *header_end = strstr(buf, "\r\n\r\n");
    if (!header_end) {
        if (len >= BUFFER_CAPACITY - 1) return -1; // Header too large (431)
        return 0; // Need more bytes
    }

    meta->header_len = (size_t)(header_end - buf) + 4;

    // 1. Parse Request Line: METHOD URI VERSION\r\n
    if (sscanf(buf, "%15s %1023s %15s", meta->method, meta->uri, meta->version) != 3) {
        return -1;
    }

    // 2. Walk header lines
    const char *line = strstr(buf, "\r\n");
    if (!line) return -1;
    line += 2; // Skip request line

    meta->host[0] = '\0';
    meta->content_length = 0;

    while (line < header_end) {
        const char *next_line = strstr(line, "\r\n");
        if (!next_line) break;

        size_t line_len = (size_t)(next_line - line);

        if ( line_len > 5 && strncasecmp(line, "Host:", 5) == 0 ) {
            const char *val = line + 5;
            while (*val == ' ' || *val == '\t') val++;
            size_t vlen = (size_t)(next_line - val);
            if (vlen >= sizeof(meta->host)) vlen = sizeof(meta->host) - 1;
            memcpy(meta->host, val, vlen);
            meta->host[vlen] = '\0';
        } else if ( line_len > 15 && strncasecmp(line, "Content-Length:", 15) == 0 ) {
            meta->content_length = (size_t)strtoul(line + 15, NULL, 10);
        }

        line = next_line + 2;
    }

    return 1;
}
```

### Step 5.2: Rewriting Headers for Upstream (`X-Forwarded-For`)

When forwarding the client request to the backend, rebuild the header block so existing `Connection:` headers are replaced with `Connection: close` and `X-Forwarded-For` is appended before the final `\r\n\r\n` and any request body bytes already read.

```c
int rewrite_request_for_backend(proxy_session_t *s) {
    char rewritten[BUFFER_CAPACITY];
    size_t offset = 0;

    // 1. Write Request Line
    int n = snprintf(rewritten + offset, sizeof(rewritten) - offset,
                     "%s %s %s\r\n",
                     s->req_meta.method, s->req_meta.uri, s->req_meta.version);
    if (n < 0) return -1;
    offset += (size_t)n;

    // 2. Copy original headers except Connection:
    const char * header_end = s->req_buf + s->req_meta.header_len - 4;
    const char *line = strstr(s->req_buf, "\r\n") + 2;

    while (line < header_end) {
        const char *next = strstr(line, "\r\n");
        size_t line_len = (size_t)(next - line);

        if (strncasecmp(line, "Connection:", 11) != 0) {
            if (offset + line_len + 2 >= sizeof(rewritten)) return -1;
            memcpy(rewritten + offset, line, line_len);
            offset += line_len;
            memcpy(rewritten + offset, "\r\n", 2);
            offset += 2;
        }
        line = next + 2;
    }

    // 3. Inject L7 Proxy Headers
    n = snprintf(rewritten + offset, sizeof(rewritten) - offset,
                 "X-Forwarded-For: %s\r\n"
                 "X-Forwarded-Proto: http\r\n"
                 "Connection: close\r\n\r\n",
                 s->client_ip);
    if (n < 0) return -1;
    offset += (size_t)n;

    // 4. Append any request body bytes that were read alongside the headers
    size_t body_bytes_in_buf = s->req_buf_len - s->req_meta.header_len;
    if (offset + body_bytes_in_buf >= sizeof(rewritten)) return -1;

    if (body_bytes_in_buf > 0) {
        memcpy(rewritten + offset, s->req_buf + s->req_meta.header_len, body_bytes_in_buf);
        offset += body_bytes_in_buf;
    }

    // Replace session request buffer with rewritten payload
    memcpy(s->req_buf, rewritten, offset);
    s->req_buf_len = offset;
    s->req_buf_sent = 0;
    return 0;
}
```

---

## 6. Milestone 3: L7 Routing & Load Balancing Algorithms

### Step 6.1: Matching the L7 Route (`src/balancer.c`)

Evaluate routes in order of specificity (longest `path_prefix` first, plus optional `Host` match):

```c
upstream_pool_t *match_l7_route(l7_route_t *routes, size_t route_count,
                                const char *host, const char *uri) {
    upstream_pool_t *best_pool = NULL;
    size_t best_prefix_len = 0;

    for (size_t i = 0; i < route_count; i++) {
        // Check Host match ("*" matches any host)
        bool host_ok = (strcmp(routes[i].host_match, "*") == 0) ||
                       (strcasecmp(routes[i].host_match, host) == 0);
        if (!host_ok) continue;

        // Check URI prefix match
        size_t plen = strlen(routes[i].path_prefix);
        if (strncmp(uri, routes[i].path_prefix, plen) == 0) {
            if (plen >= best_prefix_len) {
                best_prefix_len = plen;
                best_pool = routes[i].pool;
            }
        }
    }
    return best_pool;
}
```

### Step 6.2: Implementing the Load Balancing Schedulers

Implement Round Robin, Smooth Weighted Round Robin (NGINX-style), and Least Connections in `src/balancer.c`:

```c
// 1. Standard Round Robin
backend_t *select_round_robin(upstream_pool_t *pool) {
    size_t n = pool->backend_count;
    for (size_t i = 0; i < n; i++) {
        size_t idx = (pool->rr_index + i) % n;
        if (pool->backends[idx].is_healthy) {
            pool->rr_index = (idx + 1) % n;
            return &pool->backends[idx];
        }
    }
    return NULL; // All backends down (502/503)
}

// 2. Least Connections
backend_t *select_least_connections(upstream_pool_t *pool) {
    backend_t *best = NULL;
    for (size_t i = 0; i < pool->backend_count; i++) {
        backend_t *b = &pool->backends[i];
        if (!b->is_healthy) continue;
        if (best == NULL || b->active_connections < best->active_connections) {
            best = b;
        }
    }
    return best;
}

// 3. Smooth Weighted Round Robin (NGINX algorithm)
backend_t *select_weighted_rr(upstream_pool_t *pool) {
    backend_t *best = NULL;
    int total_weight = 0;

    for (size_t i = 0; i < pool->backend_count; i++) {
        backend_t *b = &pool->backends[i];
        if (!b->is_healthy) continue;

        b->current_weight += b->weight;
        total_weight += b->weight;

        if (best == NULL || b->current_weight > best->current_weight) {
            best = b;
        }
    }

    if (best != NULL) {
        best->current_weight -= total_weight;
    }
    return best;
}
```

---

## 7. Milestone 4: State Machine & Bidirectional Buffer Relay (`src/session.c`)

### Step 7.1: Handling Client Socket Events

Follow this state-transition logic inside `handle_client_event()`:

```c
void handle_client_event(int epoll_fd, proxy_session_t *s, uint32_t events) {
    if (events & (EPOLLERR | EPOLLHUP)) {
        close_session(epoll_fd, s);
        return;
    }

    // Phase A: Reading HTTP request from client
    if ((events & EPOLLIN) && s->state == SESSION_READ_CLIENT_HEADERS) {
        ssize_t n = read(s->client_ctx.fd,
                         s->req_buf + s->req_buf_len,
                         BUFFER_CAPACITY - s->req_buf_len - 1);
        if (n <= 0) {
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
            close_session(epoll_fd, s);
            return;
        }
        s->req_buf_len += (size_t)n;
        s->req_buf[s->req_buf_len] = '\0';

        int parse_rc = parse_http_headers(s->req_buf, s->req_buf_len, &s->req_meta);
        if (parse_rc < 0) {
            send_error_response(s->client_ctx.fd, 400, "Bad Request");
            close_session(epoll_fd, s);
            return;
        }
        if (parse_rc == 0) return; // Wait for more header bytes

        // Headers complete -> Match L7 Route & Pick Backend
        upstream_pool_t *pool = match_l7_route(global_routes, global_route_count,
                                               s->req_meta.host, s->req_meta.uri);
        backend_t *backend = pool ? select_backend(pool) : NULL;
        if (!backend) {
            send_error_response(s->client_ctx.fd, 503, "Service Unavailable");
            close_session(epoll_fd, s);
            return;
        }

        // Rewrite headers (inject X-Forwarded-For, Connection: close)
        if (rewrite_request_for_backend(s) < 0) {
            send_error_response(s->client_ctx.fd, 500, "Internal Server Error");
            close_session(epoll_fd, s);
            return;
        }

        // Connect asynchronously to backend
        int bfd = connect_to_backend_async(backend->ip, backend->port);
        if (bfd < 0) {
            record_backend_failure(backend);
            send_error_response(s->client_ctx.fd, 502, "Bad Gateway");
            close_session(epoll_fd, s);
            return;
        }

        s->assigned_backend = backend;
        backend->active_connections++;
        s->backend_ctx.fd = bfd;
        s->backend_ctx.role = FD_ROLE_BACKEND;
        s->backend_ctx.session = s;
        s->state = SESSION_CONNECTING_BACKEND;

        // Wait for backend socket to become writable (EPOLLOUT)
        register_epoll(epoll_fd, bfd, EPOLLIN | EPOLLOUT, &s->backend_ctx);
    }

    // Phase B: Flushing buffered response bytes back to slow client
    if ((events & EPOLLOUT) && s->state == SESSION_RELAYING_RESPONSE) {
        flush_response_to_client(epoll_fd, s);
    }
}
```

### Step 7.2: Handling Backend Socket Events

When the backend socket triggers `EPOLLOUT` or `EPOLLIN`, verify the async connection, stream the rewritten request buffer, and stream the backend response back to the client:

```c
void handle_backend_event(int epoll_fd, proxy_session_t *s, uint32_t events) {
    // 1. Check for connection failure
    if (events & (EPOLLERR | EPOLLHUP)) {
        if (s->state == SESSION_CONNECTING_BACKEND) {
            record_backend_failure(s->assigned_backend);
            send_error_response(s->client_ctx.fd, 502, "Bad Gateway");
        }
        close_session(epoll_fd, s);
        return;
    }

    // 2. Complete async connect & forward request buffer
    if (events & EPOLLOUT) {
        if (s->state == SESSION_CONNECTING_BACKEND) {
            int err = 0;
            socklen_t len = sizeof(err);
            if (getsockopt(s->backend_ctx.fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
                record_backend_failure(s->assigned_backend);
                send_error_response(s->client_ctx.fd, 502, "Bad Gateway");
                close_session(epoll_fd, s);
                return;
            }
            s->assigned_backend->consecutive_failures = 0;
            s->state = SESSION_FORWARDING_REQUEST;
        }

        if (s->state == SESSION_FORWARDING_REQUEST) {
            while (s->req_buf_sent < s->req_buf_len) {
                ssize_t w = write(s->backend_ctx.fd,
                                  s->req_buf + s->req_buf_sent,
                                  s->req_buf_len - s->req_buf_sent);
                if (w < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                    close_session(epoll_fd, s);
                    return;
                }
                s->req_buf_sent += (size_t)w;
            }
            // All request bytes sent; switch backend monitoring to read-only (EPOLLIN)
            s->state = SESSION_RELAYING_RESPONSE;
            modify_epoll(epoll_fd, s->backend_ctx.fd, EPOLLIN, &s->backend_ctx);
        }
    }

    // 3. Read HTTP response from backend and stream to client
    if (events & EPOLLIN) {
        ssize_t r = read(s->backend_ctx.fd,
                         s->resp_buf + s->resp_buf_len,
                         BUFFER_CAPACITY - s->resp_buf_len);
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            close_session(epoll_fd, s);
            return;
        }
        if (r == 0) {
            // Backend closed connection (EOF); flush remaining bytes to client
            flush_response_to_client(epoll_fd, s);
            if (s->resp_buf_sent == s->resp_buf_len) {
                close_session(epoll_fd, s);
            }
            return;
        }

        s->resp_buf_len += (size_t)r;
        flush_response_to_client(epoll_fd, s);
    }
}
```

### Step 7.3: Safe Session Teardown (`close_session`)

Always decrement `active_connections` and remove both file descriptors from `epoll` before `free(s)` to prevent use-after-free bugs when multiple events fire in the same `epoll_wait` batch.

```c
void close_session(int epoll_fd, proxy_session_t *s) {
    if (s->state == SESSION_CLOSING) return;
    s->state = SESSION_CLOSING;

    if (s->client_ctx.fd >= 0) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, s->client_ctx.fd, NULL);
        close(s->client_ctx.fd);
        s->client_ctx.fd = -1;
    }
    if (s->backend_ctx.fd >= 0) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, s->backend_ctx.fd, NULL);
        close(s->backend_ctx.fd);
        s->backend_ctx.fd = -1;
    }
    if (s->assigned_backend) {
        if (s->assigned_backend->active_connections > 0) {
            s->assigned_backend->active_connections--;
        }
        s->assigned_backend = NULL;
    }
    free(s);
}
```

---

## 8. Milestone 5: Passive & Active Health Checks (`src/health.c`)

### Step 8.1: Passive Health Check (Circuit Breaker)

Whenever a backend connection fails or times out, increment `consecutive_failures`. If it reaches `MAX_FAILURES` (e.g., `3`), mark the backend unhealthy for a cooldown period (e.g., `10` seconds).

```c
#define MAX_FAILURES 3
#define FAIL_TIMEOUT_SEC 10

void record_backend_failure(backend_t *b) {
    b->consecutive_failures++;
    if (b->consecutive_failures >= MAX_FAILURES) {
        b->is_healthy = false;
        b->marked_down_until = time(NULL) + FAIL_TIMEOUT_SEC;
        fprintf(stderr, "[WARN] Backend %s:%u marked UNHEALTHY for %ds\n",
                b->ip, b->port, FAIL_TIMEOUT_SEC);
    }
}
```

### Step 8.2: Active Health Check via `timerfd`

Use Linux's `timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK)` to trigger periodic health checks directly inside your `epoll` loop every 5 seconds without needing locking primitives or separate threads:

```c
#include <sys/timerfd.h>

int create_health_timer_fd(int interval_sec) {
    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (tfd < 0) return -1;

    struct itimerspec ts = {0};
    ts.it_value.tv_sec = interval_sec;
    ts.it_interval.tv_sec = interval_sec;
    timerfd_settime(tfd, 0, &ts, NULL);
    return tfd;
}

// Pseudocode when FD_ROLE_HEALTH_TIMER triggers EPOLLIN:
void run_active_health_checks(int epoll_fd, int timer_fd) {
    uint64_t expirations;
    read(timer_fd, &expirations, sizeof(expirations)); // Drain timerfd

    time_t now = time(NULL);
    for (each pool in global_pools) {
        for (each backend b in pool) {
            if (!b->is_healthy && now >= b->marked_down_until) {
                // Probe backend with quick non-blocking HTTP GET /health
                if (probe_backend_http_200(b->ip, b->port, "/health")) {
                    b->is_healthy = true;
                    b->consecutive_failures = 0;
                    printf("[INFO] Backend %s:%u recovered -> HEALTHY\n", b->ip, b->port);
                } else {
                    b->marked_down_until = now + FAIL_TIMEOUT_SEC;
                }
            }
        }
    }
}
```

---

## 9. Milestone 6: Build System (`Makefile`) & Verification

### Step 9.1: Create the `Makefile`

Enable strict compiler warnings and AddressSanitizer (`-fsanitize=address,undefined`) during development to immediately catch buffer overflows or memory leaks.

```makefile
CC       = gcc
CFLAGS   = -std=c11 -Wall -Wextra -Werror -O2 -g -Iinclude -D_POSIX_C_SOURCE=200809L
SANFLAGS = -fsanitize=address,undefined
SRCS     = src/main.c src/net.c src/http_parser.c src/balancer.c src/session.c src/health.c
OBJS     = $(SRCS:.c=.o)
TARGET   = l7lb

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

debug: CFLAGS += $(SANFLAGS)
debug: clean $(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all debug clean
```

### Step 9.2: Step-by-Step Verification Workflow

1. **Start Three Local Upstream Servers**:
   Open three terminals (or background jobs) running lightweight HTTP servers on ports `9001`, `9002`, and `9003`:
   ```bash
   python3 -m http.server 9001 &
   python3 -m http.server 9002 &
   python3 -m http.server 9003 &
   ```

2. **Compile and Run the Load Balancer**:
   ```bash
   make debug
   ./l7lb --port 8080
   ```

3. **Test L7 Path Routing & Header Injection**:
   Send requests with custom `Host` headers and URI paths using `curl`:
   ```bash
   curl -i -H "Host: api.local" http://127.0.0.1:8080/
   ```
   Verify in the backend terminal logs that requests rotate across ports `9001`, `9002`, and `9003`.

4. **Test Passive Health Check & Failover**:
   Kill the backend on port `9002` (`kill %2`), then run 10 sequential requests:
   ```bash
   for i in $(seq 1 10); do curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8080/; done
   ```
   Verify that after `MAX_FAILURES` connection failures, `9002` is marked `UNHEALTHY` and all subsequent requests succeed (`200`) via `9001` and `9003`.

5. **Stress Test Concurrency & Check for FD Leaks**:
   Run ApacheBench (`ab`) or `wrk` with 100 concurrent connections:
   ```bash
   ab -n 10000 -c 100 http://127.0.0.1:8080/
   ls -l /proc/$(pgrep l7lb)/fd | wc -l
   ```
   Confirm that after the benchmark completes, the open file descriptor count in `/proc/<pid>/fd` returns to its baseline (`4`: `stdin`, `stdout`, `stderr`, `epoll_fd`, `listen_fd`, `timer_fd`).
