/*
 * loadgen.c - a small, dependency-free epoll HTTP/1.1 load generator.
 *
 * Why this exists: on a 4 vCPU / 3.9 GB WSL box a co-located k6 becomes the
 * bottleneck long before the server does. This generator keeps the hot path
 * malloc-free, uses one epoll loop, and opens keep-alive connections from
 * several loopback source addresses (127.0.0.2 .. 127.0.0.9) so that the
 * local ephemeral-port range (~28k ports per IP) is not a hard ceiling.
 *
 * Design:
 *   - nonblocking connect()/send()/recv(), level-triggered epoll
 *   - per-connection 8 KiB input buffer; response bodies are consumed without
 *     being stored, so the header is the only thing kept
 *   - all request bytes are precomputed once at startup (four request shapes
 *     per token: GET /feed, GET /posts/:id, POST /posts/:id/like, POST /posts)
 *   - a binary min-heap schedules think-time / rate-paced next requests
 *   - log-scale (4 buckets per octave) latency histogram
 *
 * Build:
 *   gcc -O2 -g -pthread -o loadgen loadgen.c -Wall -Wextra
 */

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 3000
#define DEFAULT_TOKEN_FILE "/home/nioha/bench-ab/tools/tokens.txt"
#define FALLBACK_TOKEN_JSON "/home/nioha/twelve-dollar-server-challenge/seed/tokens.json"

#define INBUF_SIZE 8192u
#define REQ_CAP 512
#define MAX_EVENTS 1024
#define SRC_IP_COUNT 8
/* tcp_max_syn_backlog is only 256 on this box; staying well below it
 * avoids SYN-queue overflow/SYN-cookie retransmits during the ramp. */
#define CONNECT_INFLIGHT_MAX 128
#define CONNECT_TIMEOUT_NS (10ull * 1000000000ull)
#define REQUEST_TIMEOUT_NS (30ull * 1000000000ull)
#define TIMEOUT_SCAN_NS (100ull * 1000000ull)

#define HIST_BUCKETS 256u

enum mode {
    MODE_FEED = 0,
    MODE_POST,
    MODE_MIXED
};

enum req_type {
    REQ_FEED = 0,
    REQ_GET_POST,
    REQ_LIKE,
    REQ_CREATE
};

enum conn_state {
    CS_FREE = 0,
    CS_CONNECTING,
    CS_WRITING,
    CS_READING,     /* reading response headers */
    CS_READ_BODY,   /* draining a response body */
    CS_WAITING
};

struct token_reqs {
    char feed[REQ_CAP];
    int feed_len;
    char get[REQ_CAP];
    int get_len;
    char like[REQ_CAP];
    int like_len;
    char create[REQ_CAP];
    int create_len;
};

struct conn {
    int fd;
    uint8_t state;
    uint8_t close_after;
    uint8_t mode_phase;   /* mixed-mode iteration phase: 0 feed, 1 post, 2 likes/create */
    uint8_t did_like;
    uint8_t did_create;
    int heap_pos;         /* -1 when not in the timer heap */
    int token_idx;
    int src_idx;
    uint32_t epoll_events;
    const char *sbuf;     /* points into a precomputed token template */
    int slen;
    int soff;
    size_t inlen;
    size_t body_remaining;
    int status;
    uint64_t lat_start_ns;
    uint64_t wake_ns;
    uint64_t prng;
    char inbuf[INBUF_SIZE];
};

/* ------------------------------- globals -------------------------------- */

static volatile sig_atomic_t g_stop = 0;
static int g_epfd = -1;
static struct conn *g_conns = NULL;

static struct conn **g_heap = NULL;
static int g_heap_n = 0;

static struct token_reqs *g_templates = NULL;
static int g_token_count = 0;

static int g_mode = MODE_MIXED;
static int g_rate = 0;              /* requests/sec, 0 = closed loop */
static uint64_t g_rate_interval_ns = 0;
static uint64_t g_global_next_ns = 0;
static uint64_t g_think_ns = 0;

static struct sockaddr_in g_target;

static uint64_t g_hist[HIST_BUCKETS];
static unsigned long long g_requested = 0;
static unsigned long long g_completed = 0;
static unsigned long long g_connected = 0;
static unsigned long long g_app_errors = 0;
static unsigned long long g_connect_errors = 0;
static unsigned long long g_timeouts = 0;
static unsigned long long g_proto_errors = 0;
static int g_last_connect_errno = 0;

static void die(const char *msg) {
    fprintf(stderr, "loadgen: %s\n", msg);
    exit(1);
}

static uint64_t now_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void on_signal(int sig) {
    (void)sig;
    g_stop = 1;
}

static inline uint64_t prng_next(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

/* --------------------------- latency histogram -------------------------- */

static unsigned hist_bucket(uint64_t us) {
    if (us < 8u) {
        return (unsigned)us;
    }
    unsigned bit = 63u - (unsigned)__builtin_clzll(us);
    unsigned sub = (unsigned)((us >> (bit - 2u)) & 3u);
    unsigned idx = bit * 4u + sub;
    if (idx >= HIST_BUCKETS) {
        idx = HIST_BUCKETS - 1u;
    }
    return idx;
}

static double hist_bucket_us(unsigned idx) {
    if (idx < 8u) {
        return (double)idx;
    }
    unsigned bit = idx / 4u;
    unsigned sub = idx % 4u;
    uint64_t base = 1ull << bit;
    uint64_t step = 1ull << (bit - 2u);
    return (double)(base + (uint64_t)(sub + 1u) * step);
}

static double hist_percentile(double pct) {
    unsigned long long total = 0;
    unsigned i;

    for (i = 0; i < HIST_BUCKETS; i++) {
        total += g_hist[i];
    }
    if (total == 0) {
        return 0.0;
    }
    unsigned long long target = (unsigned long long)((pct / 100.0) * (double)total + 0.5);
    if (target == 0) {
        target = 1;
    }
    unsigned long long seen = 0;
    for (i = 0; i < HIST_BUCKETS; i++) {
        seen += g_hist[i];
        if (seen >= target) {
            return hist_bucket_us(i) / 1000.0;
        }
    }
    return hist_bucket_us(HIST_BUCKETS - 1u) / 1000.0;
}

/* ------------------------------ timer heap ------------------------------ */

static void heap_swap(int a, int b) {
    struct conn *ta = g_heap[a];
    struct conn *tb = g_heap[b];
    g_heap[a] = tb;
    g_heap[b] = ta;
    tb->heap_pos = a;
    ta->heap_pos = b;
}

static void heap_up(int pos) {
    while (pos > 0) {
        int parent = (pos - 1) / 2;
        if (g_heap[parent]->wake_ns <= g_heap[pos]->wake_ns) {
            break;
        }
        heap_swap(parent, pos);
        pos = parent;
    }
}

static void heap_down(int pos) {
    for (;;) {
        int left = pos * 2 + 1;
        int right = left + 1;
        int best = pos;
        if (left < g_heap_n && g_heap[left]->wake_ns < g_heap[best]->wake_ns) {
            best = left;
        }
        if (right < g_heap_n && g_heap[right]->wake_ns < g_heap[best]->wake_ns) {
            best = right;
        }
        if (best == pos) {
            break;
        }
        heap_swap(pos, best);
        pos = best;
    }
}

static void heap_push(struct conn *c) {
    if (c->heap_pos >= 0) {
        heap_up(c->heap_pos);
        heap_down(c->heap_pos);
        return;
    }
    c->heap_pos = g_heap_n;
    g_heap[g_heap_n++] = c;
    heap_up(c->heap_pos);
}

static void heap_remove(struct conn *c) {
    int pos = c->heap_pos;
    if (pos < 0) {
        return;
    }
    c->heap_pos = -1;
    g_heap_n--;
    if (pos == g_heap_n) {
        return;
    }
    g_heap[pos] = g_heap[g_heap_n];
    g_heap[pos]->heap_pos = pos;
    heap_up(pos);
    heap_down(pos);
}

/* ------------------------------ connection ------------------------------ */

static void set_interest(struct conn *c, uint32_t events) {
    if (c->epoll_events == events) {
        return;
    }
    struct epoll_event ev;
    ev.events = events;
    ev.data.ptr = c;
    if (epoll_ctl(g_epfd, EPOLL_CTL_MOD, c->fd, &ev) != 0) {
        /* The fd may already be gone; the read/write path will notice. */
    } else {
        c->epoll_events = events;
    }
}

static void close_conn(struct conn *c) {
    if (c->state == CS_FREE) {
        return;
    }
    heap_remove(c);
    if (c->fd >= 0) {
        epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->fd, NULL);
        close(c->fd);
        c->fd = -1;
    }
    c->state = CS_FREE;
    c->inlen = 0;
    c->body_remaining = 0;
    c->sbuf = NULL;
    c->soff = 0;
    c->slen = 0;
}

static void schedule_wake(struct conn *c, uint64_t when) {
    c->wake_ns = when;
    c->state = CS_WAITING;
    set_interest(c, EPOLLIN);
    heap_push(c);
}

static int choose_type(struct conn *c) {
    uint64_t r;
    switch (g_mode) {
    case MODE_FEED:
        return REQ_FEED;
    case MODE_POST:
        /* Write-heavy mix: 30% feed, 30% single post, 35% like, 5% create. */
        r = prng_next(&c->prng) % 100u;
        if (r < 30u) {
            return REQ_FEED;
        }
        if (r < 60u) {
            return REQ_GET_POST;
        }
        if (r < 95u) {
            return REQ_LIKE;
        }
        return REQ_CREATE;
    case MODE_MIXED:
    default:
        /* bench/load.js: feed -> post -> 15% like -> 2% create -> repeat. */
        if (c->mode_phase == 0) {
            c->mode_phase = 1;
            return REQ_FEED;
        }
        if (c->mode_phase == 1) {
            c->mode_phase = 2;
            c->did_like = 0;
            c->did_create = 0;
            return REQ_GET_POST;
        }
        if (c->mode_phase == 2 && !c->did_like && !c->did_create) {
            r = prng_next(&c->prng);
            c->did_like = (uint8_t)((r % 100u) < 15u);
            c->did_create = (uint8_t)(((r >> 8) % 100u) < 2u);
        }
        if (c->did_like) {
            c->did_like = 0;
            return REQ_LIKE;
        }
        if (c->did_create) {
            c->did_create = 0;
            return REQ_CREATE;
        }
        c->mode_phase = 0;
        return REQ_FEED;
    }
}

static int flush_send(struct conn *c);

static void send_next(struct conn *c) {
    const struct token_reqs *t = &g_templates[c->token_idx];
    switch (choose_type(c)) {
    case REQ_FEED:
        c->sbuf = t->feed;
        c->slen = t->feed_len;
        break;
    case REQ_GET_POST:
        c->sbuf = t->get;
        c->slen = t->get_len;
        break;
    case REQ_LIKE:
        c->sbuf = t->like;
        c->slen = t->like_len;
        break;
    case REQ_CREATE:
    default:
        c->sbuf = t->create;
        c->slen = t->create_len;
        break;
    }
    c->soff = 0;
    c->inlen = 0;
    c->body_remaining = 0;
    c->close_after = 0;
    c->status = 0;
    c->lat_start_ns = now_ns();
    c->state = CS_WRITING;
    g_requested++;
    set_interest(c, EPOLLIN | EPOLLOUT);
    (void)flush_send(c);
}

static void schedule_next(struct conn *c) {
    if (g_rate > 0) {
        uint64_t when = now_ns();
        if (when < g_global_next_ns) {
            when = g_global_next_ns;
        }
        g_global_next_ns = when + g_rate_interval_ns;
        if (when <= now_ns()) {
            send_next(c);
        } else {
            schedule_wake(c, when);
        }
    } else if (g_think_ns == 0) {
        send_next(c);
    } else {
        schedule_wake(c, now_ns() + g_think_ns);
    }
}

static void finish_request(struct conn *c) {
    uint64_t dt = now_ns() - c->lat_start_ns;
    uint64_t us = dt / 1000ull;

    if (c->status < 200 || c->status >= 300) {
        g_app_errors++;
    }
    g_completed++;
    g_hist[hist_bucket(us)]++;

    if (c->close_after) {
        close_conn(c);
        return;
    }

    schedule_next(c);
}

static int flush_send(struct conn *c) {
    while (c->soff < c->slen) {
        ssize_t n = send(c->fd, c->sbuf + c->soff, (size_t)(c->slen - c->soff), MSG_NOSIGNAL);
        if (n > 0) {
            c->soff += (int)n;
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            set_interest(c, EPOLLIN | EPOLLOUT);
            return 0;
        }
        close_conn(c);
        return -1;
    }
    c->state = CS_READING;
    set_interest(c, EPOLLIN);
    return 0;
}

/* Returns 1 when a full response was consumed, 0 when more bytes are needed. */
static int consume_one(struct conn *c) {
    if (c->state == CS_READING) {
        size_t hdr = 0;
        size_t i;
        int found = 0;
        for (i = 0; i + 3u < c->inlen; i++) {
            if (c->inbuf[i] == '\r' && c->inbuf[i + 1] == '\n' &&
                c->inbuf[i + 2] == '\r' && c->inbuf[i + 3] == '\n') {
                hdr = i;
                found = 1;
                break;
            }
        }
        if (!found) {
            if (c->inlen >= INBUF_SIZE) {
                g_proto_errors++;
                close_conn(c);
                return -1;
            }
            return 0;
        }

        /* Status line: "HTTP/1.1 200 ..." */
        c->status = 0;
        if (c->inlen < 12u || memcmp(c->inbuf, "HTTP/", 5) != 0) {
            g_proto_errors++;
            close_conn(c);
            return -1;
        } else {
            const char *sp = memchr(c->inbuf, ' ', hdr);
            if (sp == NULL) {
                g_proto_errors++;
                close_conn(c);
                return -1;
            }
            sp++;
            if (sp + 3 > c->inbuf + hdr) {
                g_proto_errors++;
                close_conn(c);
                return -1;
            }
            int code = 0;
            for (i = 0; i < 3u; i++) {
                if (sp[i] < '0' || sp[i] > '9') {
                    code = -1;
                    break;
                }
                code = code * 10 + (sp[i] - '0');
            }
            if (code < 0) {
                g_proto_errors++;
                close_conn(c);
                return -1;
            }
            c->status = code;
        }

        /* Headers: only Content-Length and Connection matter. */
        c->body_remaining = 0;
        c->close_after = 0;
        {
            const char *p = memchr(c->inbuf, '\n', hdr);
            if (p != NULL) {
                p++;
            }
            while (p != NULL && p < c->inbuf + hdr) {
                const char *eol = memchr(p, '\n', (size_t)(c->inbuf + hdr - p));
                size_t line_len = (eol != NULL) ? (size_t)(eol - p) : (size_t)(c->inbuf + hdr - p);
                if (line_len > 0 && p[line_len - 1] == '\r') {
                    line_len--;
                }
                if (line_len >= 15u && strncasecmp(p, "Content-Length:", 15) == 0) {
                    const char *v = p + 15;
                    while (v < p + line_len && (*v == ' ' || *v == '\t')) {
                        v++;
                    }
                    unsigned long long cl = 0;
                    while (v < p + line_len && *v >= '0' && *v <= '9') {
                        cl = cl * 10u + (unsigned long long)(*v - '0');
                        v++;
                    }
                    c->body_remaining = (size_t)cl;
                } else if (line_len >= 11u && strncasecmp(p, "Connection:", 11) == 0) {
                    const char *v = p + 11;
                    size_t rem = line_len - 11u;
                    if (rem > 0u && memmem(v, rem, "close", 5) != NULL) {
                        c->close_after = 1;
                    }
                } else if (line_len >= 18u && strncasecmp(p, "Transfer-Encoding:", 18) == 0) {
                    /* Not expected from this server; framing would be ambiguous. */
                    g_proto_errors++;
                    close_conn(c);
                    return -1;
                }
                p = (eol != NULL) ? eol + 1 : NULL;
            }
        }

        {
            size_t consumed = hdr + 4u;
            size_t leftover = c->inlen - consumed;
            if (leftover > 0) {
                memmove(c->inbuf, c->inbuf + consumed, leftover);
            }
            c->inlen = leftover;
        }

        if (c->body_remaining == 0) {
            finish_request(c);
            return 1;
        }
        c->state = CS_READ_BODY;
    }

    /* Body: consume and discard. */
    if (c->state == CS_READ_BODY) {
        size_t take = c->inlen < c->body_remaining ? c->inlen : c->body_remaining;
        c->body_remaining -= take;
        if (take < c->inlen) {
            memmove(c->inbuf, c->inbuf + take, c->inlen - take);
        }
        c->inlen -= take;
        if (c->body_remaining > 0) {
            return 0;
        }
        finish_request(c);
        return 1;
    }
    return 0;
}

static void handle_read(struct conn *c) {
    for (;;) {
        if (c->inlen >= INBUF_SIZE) {
            g_proto_errors++;
            close_conn(c);
            return;
        }
        ssize_t n = recv(c->fd, c->inbuf + c->inlen, INBUF_SIZE - c->inlen, 0);
        if (n > 0) {
            c->inlen += (size_t)n;
            if (c->state == CS_WAITING) {
                /* No request is outstanding; discard unexpected bytes. */
                c->inlen = 0;
                continue;
            }
            for (;;) {
                int r = consume_one(c);
                if (r < 0 || c->state == CS_FREE) {
                    return;
                }
                if (r == 0) {
                    break;
                }
                if (c->state == CS_WAITING || c->state == CS_WRITING) {
                    /* finish_request() may have scheduled or sent the next
                     * request; any bytes left belong to nothing we sent. */
                    c->inlen = 0;
                    break;
                }
            }
            continue;
        }
        if (n == 0) {
            close_conn(c);
            return;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        close_conn(c);
        return;
    }
}

static void handle_connect_done(struct conn *c) {
    int err = 0;
    socklen_t len = sizeof(err);
    if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
        err = errno;
    }
    if (err != 0) {
        g_connect_errors++;
        g_last_connect_errno = err;
        close_conn(c);
        return;
    }
    g_connected++;
    schedule_next(c);
}

static void handle_event(struct conn *c, uint32_t events) {
    if (c->state == CS_FREE) {
        return;
    }
    if (events & (EPOLLERR | EPOLLHUP)) {
        if (c->state == CS_CONNECTING) {
            int err = 0;
            socklen_t len = sizeof(err);
            if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
                err = errno;
            }
            if (err != 0) {
                g_last_connect_errno = err;
            }
            g_connect_errors++;
        }
        close_conn(c);
        return;
    }
    if (c->state == CS_CONNECTING) {
        if (events & (EPOLLOUT | EPOLLIN)) {
            handle_connect_done(c);
        }
        return;
    }
    if ((events & EPOLLIN) && c->state != CS_FREE) {
        handle_read(c);
    }
    if ((events & EPOLLOUT) && c->state == CS_WRITING) {
        flush_send(c);
    }
}

static void launch_conn(int idx) {
    struct conn *c = &g_conns[idx];
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        g_connect_errors++;
        g_last_connect_errno = errno;
        c->state = CS_FREE;
        return;
    }

    struct sockaddr_in src;
    memset(&src, 0, sizeof(src));
    src.sin_family = AF_INET;
    src.sin_port = 0;
    src.sin_addr.s_addr = htonl(0x7f000002u + (uint32_t)(idx % SRC_IP_COUNT));
    if (bind(fd, (struct sockaddr *)&src, sizeof(src)) != 0) {
        g_connect_errors++;
        g_last_connect_errno = errno;
        close(fd);
        c->fd = -1;
        c->state = CS_FREE;
        return;
    }

    c->fd = fd;
    c->state = CS_CONNECTING;
    c->heap_pos = -1;
    c->token_idx = idx % g_token_count;
    c->src_idx = idx % SRC_IP_COUNT;
    c->epoll_events = 0;
    c->inlen = 0;
    c->body_remaining = 0;
    c->close_after = 0;
    c->mode_phase = 0;
    c->did_like = 0;
    c->did_create = 0;
    c->soff = 0;
    c->slen = 0;
    c->status = 0;
    c->prng = 0x9e3779b97f4a7c15ull ^ (uint64_t)(idx + 1) * 0xbf58476d1ce4e5b9ull;
    c->lat_start_ns = now_ns();

    int rc = connect(fd, (struct sockaddr *)&g_target, sizeof(g_target));
    if (rc != 0 && errno != EINPROGRESS && errno != EINTR) {
        g_connect_errors++;
        g_last_connect_errno = errno;
        close_conn(c);
        return;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT;
    ev.data.ptr = c;
    if (epoll_ctl(g_epfd, EPOLL_CTL_ADD, fd, &ev) != 0) {
        g_connect_errors++;
        g_last_connect_errno = errno;
        close_conn(c);
        return;
    }
    c->epoll_events = EPOLLIN | EPOLLOUT;

    if (rc == 0) {
        handle_connect_done(c);
    }
}

/* ------------------------------- startup -------------------------------- */

static void usage(FILE *out) {
    fprintf(out,
        "usage: loadgen [options]\n"
        "  -H host       target host (default " DEFAULT_HOST ")\n"
        "  -p port       target port (default %d)\n"
        "  -c conns      number of keep-alive connections (default 100)\n"
        "  -d seconds    run duration (default 10)\n"
        "  -r rate       target requests/sec, 0 = closed loop (default 0)\n"
        "  -t think-ms   per-connection delay between requests in closed loop (default 0)\n"
        "  -T tokenfile  token file (default " DEFAULT_TOKEN_FILE ");\n"
        "                a seed tokens.json is accepted and scanned for \"token\":\"...\"\n"
        "  -m mode       feed | post | mixed (default mixed)\n"
        "                  feed  : GET /feed only\n"
        "                  post  : 30%% feed, 30%% GET /posts/:id, 35%% like, 5%% create\n"
        "                  mixed : bench/load.js loop: feed, post, 15%% like, 2%% create\n"
        "  -h            show this help\n",
        DEFAULT_PORT);
}

static char *read_whole_file(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (f == NULL) {
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long sz = ftell(f);
    if (sz < 0) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    char *buf = malloc((size_t)sz + 1u);
    if (buf == NULL) {
        fclose(f);
        return NULL;
    }
    size_t got = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[got] = '\0';
    *out_len = got;
    return buf;
}

static int load_tokens(const char *path, char ***out_tokens) {
    size_t len = 0;
    char *buf = read_whole_file(path, &len);
    if (buf == NULL) {
        return -1;
    }

    char **tokens = calloc(65536u, sizeof(char *));
    if (tokens == NULL) {
        free(buf);
        return -1;
    }
    int n = 0;

    if (strstr(buf, "\"token\"") != NULL) {
        const char *p = buf;
        while ((p = strstr(p, "\"token\"")) != NULL) {
            p += 7;
            while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') {
                p++;
            }
            if (*p != ':') {
                continue;
            }
            p++;
            while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') {
                p++;
            }
            if (*p != '"') {
                continue;
            }
            p++;
            const char *start = p;
            while (*p != '\0' && *p != '"') {
                p++;
            }
            size_t tl = (size_t)(p - start);
            if (tl > 0 && tl < 1024u) {
                char *tok = malloc(tl + 1u);
                if (tok == NULL) {
                    break;
                }
                memcpy(tok, start, tl);
                tok[tl] = '\0';
                tokens[n++] = tok;
            }
            if (*p == '"') {
                p++;
            }
        }
    } else {
        char *save = NULL;
        char *line = strtok_r(buf, "\n", &save);
        while (line != NULL && n < 65536) {
            char *s = line;
            while (*s == ' ' || *s == '\t' || *s == '\r') {
                s++;
            }
            size_t tl = strlen(s);
            while (tl > 0 && (s[tl - 1] == '\r' || s[tl - 1] == ' ' || s[tl - 1] == '\t')) {
                s[--tl] = '\0';
            }
            if (tl > 0 && s[0] != '#') {
                char *tok = malloc(tl + 1u);
                if (tok == NULL) {
                    break;
                }
                memcpy(tok, s, tl + 1u);
                tokens[n++] = tok;
            }
            line = strtok_r(NULL, "\n", &save);
        }
    }

    free(buf);
    if (n == 0) {
        free(tokens);
        return -1;
    }
    *out_tokens = tokens;
    return n;
}

static void build_templates(const char *host, int port, char **tokens, int token_count) {
    g_templates = calloc((size_t)token_count, sizeof(struct token_reqs));
    if (g_templates == NULL) {
        die("out of memory allocating request templates");
    }
    for (int i = 0; i < token_count; i++) {
        struct token_reqs *t = &g_templates[i];
        int post_id = 1 + (int)(((unsigned long long)i * 7919ull) % 500000ull);
        int n;

        n = snprintf(t->feed, REQ_CAP,
            "GET /feed HTTP/1.1\r\nHost: %s:%d\r\nAuthorization: Bearer %s\r\n\r\n",
            host, port, tokens[i]);
        if (n < 0 || n >= REQ_CAP) {
            die("request template overflow (feed)");
        }
        t->feed_len = n;

        n = snprintf(t->get, REQ_CAP,
            "GET /posts/%d HTTP/1.1\r\nHost: %s:%d\r\nAuthorization: Bearer %s\r\n\r\n",
            post_id, host, port, tokens[i]);
        if (n < 0 || n >= REQ_CAP) {
            die("request template overflow (get)");
        }
        t->get_len = n;

        n = snprintf(t->like, REQ_CAP,
            "POST /posts/%d/like HTTP/1.1\r\nHost: %s:%d\r\n"
            "Authorization: Bearer %s\r\nContent-Length: 0\r\n\r\n",
            post_id, host, port, tokens[i]);
        if (n < 0 || n >= REQ_CAP) {
            die("request template overflow (like)");
        }
        t->like_len = n;

        {
            static const char body[] = "{\"body\":\"loadgen synthetic post\"}";
            n = snprintf(t->create, REQ_CAP,
                "POST /posts HTTP/1.1\r\nHost: %s:%d\r\n"
                "Authorization: Bearer %s\r\nContent-Type: application/json\r\n"
                "Content-Length: %zu\r\n\r\n%s",
                host, port, tokens[i], sizeof(body) - 1u, body);
            if (n < 0 || n >= REQ_CAP) {
                die("request template overflow (create)");
            }
            t->create_len = n;
        }
    }
}

static void raise_nofile(long need) {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) != 0) {
        return;
    }
    if ((long)rl.rlim_cur >= need) {
        return;
    }
    rlim_t want = (rlim_t)need;
    if (rl.rlim_max != RLIM_INFINITY && want > rl.rlim_max) {
        want = rl.rlim_max;
    }
    rl.rlim_cur = want;
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
        fprintf(stderr, "loadgen: warning: could not raise RLIMIT_NOFILE to %ld: %s\n",
                need, strerror(errno));
        return;
    }
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && (long)rl.rlim_cur < need) {
        fprintf(stderr, "loadgen: warning: RLIMIT_NOFILE is %ld, need %ld; "
                        "connections may fail with EMFILE\n", (long)rl.rlim_cur, need);
    }
}

static int resolve_target(const char *host, int port) {
    char portbuf[16];
    snprintf(portbuf, sizeof(portbuf), "%d", port);
    struct addrinfo hints;
    struct addrinfo *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int rc = getaddrinfo(host, portbuf, &hints, &res);
    if (rc != 0 || res == NULL) {
        fprintf(stderr, "loadgen: cannot resolve %s:%d: %s\n", host, port, gai_strerror(rc));
        return -1;
    }
    memcpy(&g_target, res->ai_addr, sizeof(struct sockaddr_in));
    freeaddrinfo(res);
    return 0;
}

static int parse_int_arg(const char *s, int *out) {
    char *end = NULL;
    long v;
    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v < -2147483648L || v > 2147483647L) {
        return -1;
    }
    *out = (int)v;
    return 0;
}

int main(int argc, char **argv) {
    const char *host = DEFAULT_HOST;
    const char *token_path = DEFAULT_TOKEN_FILE;
    int port = DEFAULT_PORT;
    int conns = 100;
    int duration = 10;
    int rate = 0;
    int think_ms = 0;

    int opt;
    opterr = 0;
    while ((opt = getopt(argc, argv, "H:p:c:d:r:t:T:m:h")) != -1) {
        switch (opt) {
        case 'H':
            host = optarg;
            break;
        case 'p':
            if (parse_int_arg(optarg, &port) != 0) {
                fprintf(stderr, "loadgen: invalid port '%s'\n", optarg);
                usage(stderr);
                return 2;
            }
            break;
        case 'c':
            if (parse_int_arg(optarg, &conns) != 0) {
                fprintf(stderr, "loadgen: invalid connection count '%s'\n", optarg);
                usage(stderr);
                return 2;
            }
            break;
        case 'd':
            if (parse_int_arg(optarg, &duration) != 0) {
                fprintf(stderr, "loadgen: invalid duration '%s'\n", optarg);
                usage(stderr);
                return 2;
            }
            break;
        case 'r':
            if (parse_int_arg(optarg, &rate) != 0) {
                fprintf(stderr, "loadgen: invalid rate '%s'\n", optarg);
                usage(stderr);
                return 2;
            }
            break;
        case 't':
            if (parse_int_arg(optarg, &think_ms) != 0) {
                fprintf(stderr, "loadgen: invalid think time '%s'\n", optarg);
                usage(stderr);
                return 2;
            }
            break;
        case 'T':
            token_path = optarg;
            break;
        case 'm':
            if (strcmp(optarg, "feed") == 0) {
                g_mode = MODE_FEED;
            } else if (strcmp(optarg, "post") == 0) {
                g_mode = MODE_POST;
            } else if (strcmp(optarg, "mixed") == 0) {
                g_mode = MODE_MIXED;
            } else {
                fprintf(stderr, "loadgen: invalid mode '%s'\n", optarg);
                usage(stderr);
                return 2;
            }
            break;
        case 'h':
            usage(stdout);
            return 0;
        default:
            fprintf(stderr, "loadgen: invalid option\n");
            usage(stderr);
            return 2;
        }
    }

    if (optind != argc) {
        fprintf(stderr, "loadgen: unexpected argument '%s'\n", argv[optind]);
        usage(stderr);
        return 2;
    }
    if (conns < 1 || conns > 1000000) {
        fprintf(stderr, "loadgen: invalid connection count\n");
        usage(stderr);
        return 2;
    }
    if (port < 1 || port > 65535) {
        fprintf(stderr, "loadgen: invalid port\n");
        usage(stderr);
        return 2;
    }
    if (duration < 1) {
        fprintf(stderr, "loadgen: invalid duration\n");
        usage(stderr);
        return 2;
    }
    if (rate < 0) {
        fprintf(stderr, "loadgen: invalid rate\n");
        usage(stderr);
        return 2;
    }
    if (think_ms < 0) {
        fprintf(stderr, "loadgen: invalid think time\n");
        usage(stderr);
        return 2;
    }

    if (resolve_target(host, port) != 0) {
        return 2;
    }

    char **tokens = NULL;
    int token_count = load_tokens(token_path, &tokens);
    if (token_count < 0 && strcmp(token_path, DEFAULT_TOKEN_FILE) == 0) {
        fprintf(stderr, "loadgen: %s not readable, falling back to %s\n",
                DEFAULT_TOKEN_FILE, FALLBACK_TOKEN_JSON);
        token_count = load_tokens(FALLBACK_TOKEN_JSON, &tokens);
    }
    if (token_count <= 0) {
        fprintf(stderr, "loadgen: no tokens found in %s\n", token_path);
        return 2;
    }
    g_token_count = token_count;

    printf("loadgen: target %s:%d conns=%d duration=%ds rate=%d think_ms=%d mode=%s tokens=%d\n",
           host, port, conns, duration, rate, think_ms,
           g_mode == MODE_FEED ? "feed" : (g_mode == MODE_POST ? "post" : "mixed"),
           token_count);

    build_templates(host, port, tokens, token_count);
    for (int i = 0; i < token_count; i++) {
        free(tokens[i]);
    }
    free(tokens);

    if (rate > 0) {
        g_rate = rate;
        g_rate_interval_ns = 1000000000ull / (uint64_t)rate;
        if (g_rate_interval_ns == 0) {
            g_rate_interval_ns = 1;
        }
        g_global_next_ns = now_ns();
    }
    g_think_ns = (uint64_t)think_ms * 1000000ull;

    raise_nofile((long)conns + 128L);

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    g_epfd = epoll_create1(EPOLL_CLOEXEC);
    if (g_epfd < 0) {
        perror("loadgen: epoll_create1");
        return 1;
    }

    g_conns = calloc((size_t)conns, sizeof(struct conn));
    g_heap = calloc((size_t)conns, sizeof(struct conn *));
    if (g_conns == NULL || g_heap == NULL) {
        die("out of memory allocating connections");
    }
    g_heap_n = 0;

    uint64_t start_ns = now_ns();
    uint64_t deadline_ns = start_ns + (uint64_t)duration * 1000000000ull;
    uint64_t last_timeout_scan = start_ns;
    uint64_t last_progress = start_ns;
    int launched = 0;
    int inflight = 0;

    while (!g_stop) {
        uint64_t now = now_ns();
        if (now >= deadline_ns) {
            break;
        }

        /* Pace connection establishment so the server's accept queue is not
         * overrun; the kernel backlog here is only somaxconn (4096). */
        while (launched < conns && inflight < CONNECT_INFLIGHT_MAX) {
            launch_conn(launched);
            struct conn *c = &g_conns[launched];
            if (c->state == CS_CONNECTING) {
                inflight++;
            }
            launched++;
        }
        (void)inflight;

        /* fire due timers */
        while (g_heap_n > 0 && g_heap[0]->wake_ns <= now) {
            struct conn *c = g_heap[0];
            heap_remove(c);
            if (c->state == CS_WAITING) {
                send_next(c);
            }
        }

        if (now - last_timeout_scan >= TIMEOUT_SCAN_NS) {
            uint64_t scan_now = now_ns();
            for (int i = 0; i < launched; i++) {
                struct conn *c = &g_conns[i];
                if (c->state == CS_FREE) {
                    continue;
                }
                if (c->state == CS_CONNECTING) {
                    if (scan_now > c->lat_start_ns &&
                        scan_now - c->lat_start_ns > CONNECT_TIMEOUT_NS) {
                        g_connect_errors++;
                        g_last_connect_errno = ETIMEDOUT;
                        close_conn(c);
                        inflight--;
                    }
                } else if (scan_now > c->lat_start_ns &&
                           scan_now - c->lat_start_ns > REQUEST_TIMEOUT_NS) {
                    g_timeouts++;
                    close_conn(c);
                }
            }
            last_timeout_scan = scan_now;
        }

        int timeout_ms = 1000;
        if (launched < conns) {
            /* One millisecond per 128-connection ramp batch: enough to let the
             * kernel handshake completions arrive without busy-spinning. */
            timeout_ms = 1;
        } else if (g_heap_n > 0) {
            uint64_t next = g_heap[0]->wake_ns;
            if (next <= now) {
                timeout_ms = 0;
            } else {
                /* Round up: epoll's timeout is in milliseconds and truncating
                 * a sub-ms delta to 0 turns the loop into a busy poll. */
                uint64_t delta_ms = (next - now + 999999ull) / 1000000ull;
                timeout_ms = delta_ms > 200ull ? 200 : (int)delta_ms;
                if (timeout_ms < 1) {
                    timeout_ms = 1;
                }
            }
        } else {
            timeout_ms = 200;
        }

        struct epoll_event events[MAX_EVENTS];
        int n = epoll_wait(g_epfd, events, MAX_EVENTS, timeout_ms);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("loadgen: epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            struct conn *c = (struct conn *)events[i].data.ptr;
            int was_connecting = (c->state == CS_CONNECTING);
            handle_event(c, events[i].events);
            if (was_connecting && c->state != CS_CONNECTING && inflight > 0) {
                inflight--;
            }
        }

        now = now_ns();
        if (now - last_progress >= 5000000000ull) {
            double secs = (double)(now - start_ns) / 1e9;
            double rps = secs > 0 ? (double)g_completed / secs : 0.0;
            printf("loadgen: t=%.0fs launched=%d connected=%llu completed=%llu rps=%.0f "
                   "errors=%llu\n",
                   secs, launched, g_connected, g_completed, rps,
                   g_app_errors + g_connect_errors + g_timeouts + g_proto_errors);
            fflush(stdout);
            last_progress = now;
        }
    }

    uint64_t end_ns = now_ns();
    double elapsed = (double)(end_ns - start_ns) / 1e9;
    if (elapsed <= 0.0) {
        elapsed = 0.000001;
    }

    for (int i = 0; i < conns; i++) {
        if (g_conns[i].state != CS_FREE) {
            close_conn(&g_conns[i]);
        }
    }

    unsigned long long errors = g_app_errors + g_connect_errors + g_timeouts + g_proto_errors;

    printf("\n=== loadgen summary ===\n");
    printf("target          %s:%d\n", host, port);
    printf("mode            %s\n",
           g_mode == MODE_FEED ? "feed" : (g_mode == MODE_POST ? "post" : "mixed"));
    printf("connections     requested=%d launched=%d connected=%llu\n",
           conns, launched, g_connected);
    printf("duration_s      %.2f (requested %d)\n", elapsed, duration);
    printf("rate_limit      %d req/s (%s)\n", rate, rate > 0 ? "open loop" : "closed loop");
    printf("think_ms        %d\n", think_ms);
    printf("requests        completed=%llu issued=%llu\n", g_completed, g_requested);
    printf("rps             %.2f\n", (double)g_completed / elapsed);
    printf("errors          total=%llu app=%llu connect=%llu timeout=%llu protocol=%llu\n",
           errors, g_app_errors, g_connect_errors, g_timeouts, g_proto_errors);
    if (g_connect_errors > 0) {
        printf("connect_errno   %d (%s)\n", g_last_connect_errno, strerror(g_last_connect_errno));
    }
    printf("latency_ms      p50=%.3f p95=%.3f p99=%.3f (n=%llu)\n",
           hist_percentile(50.0), hist_percentile(95.0), hist_percentile(99.0), g_completed);

    close(g_epfd);
    free(g_heap);
    free(g_conns);
    free(g_templates);
    return 0;
}
