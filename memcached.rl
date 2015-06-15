#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>

#include "memcached.h"

#include <luajit-2.0/lua.h>

%%{
    machine memcached;
    write data;
}%%
#if defined(__APPLE__)
#  define MC_EXPORT __attribute__((visibility("default")))
#elif defined(_WIN32) || defined(__WIN32__)
#  define MC_EXPORT __declspec(dllexport)
#else
#  define MC_EXPORT
#endif

static int
mc_strtol(const char *start, const char *end, int64_t *num)
{
    *num = 0;
    char sign = 1;
    if (*start == '-') { sign *= -1; start++; }
    while (start < end) {
        uint8_t code = *start++;
        if (code < '0' || code > '9')
            return -1;
        *num = (*num) * 10 + (code - '0');
    }
    return 0;
}

static int
mc_strtoul(const char *start, const char *end, uint64_t *num)
{
    *num = 0;
    while (start < end) {
        uint8_t code = *start++;
        if (code < '0' || code > '9')
            return -1;
        *num = (*num) * 10 + (code - '0');
    }
    return 0;
}

MC_EXPORT int
memcache_parse(struct memcache_request *req, const char *p, const char *pe) {
    int cs = 0;
    const char *s = NULL;
    bool done = false;

    memset(req, 0, sizeof(struct memcache_request));
    %%{
        action set {
            req->op = MC_SET;
        }
        action add {
            req->op = MC_ADD;
        }
        action replace {
            req->op = MC_REPLACE;
        }
        action append {
            req->op = MC_APPEND;
        }
        action prepend {
            req->op = MC_PREPEND;
        }
        action cas {
            req->op = MC_CAS;
        }
        action get {
            req->op = MC_GET;
        }
        action gets {
            req->op = MC_GETS;
        }
        action delete {
            req->op = MC_DELETE;
        }
        action incr {
            req->op = MC_INCR;
        }
        action decr {
            req->op = MC_DECR;
            req->inc_val *= -1;
        }
        action flush_all {
            req->op = MC_FLUSH;
        }
        action stats {
            req->op = MC_STATS;
        }
        action quit {
            req->op = MC_QUIT;
        }

        action key_start {
            printf("key\n");
            s = p;
            for (; p < pe && *p != ' ' && *p != '\r' && *p != '\n'; p++);
            if (*p == ' ' || *p == '\r' || *p == '\n') {
                if (req->key == NULL)
                    req->key = s;
                req->key_len = (p - req->key);
                p -= 1;
                req->key_count += 1;
            } else {
                p = s;
            }
        }
        action read_data {
            printf("data\n");
            req->data = p;
            req->data_len = req->bytes;

            if (strncmp(req->data + req->data_len, "\r\n", 2) == 0) {
                p += req->bytes + 2;
            } else {
                printf("goto exit\n");
                goto exit;
            }
        }
        action done {
            printf("done\n");
            done = true;
        }
        printable = [^ \t\r\n];
        key = printable >key_start ;

        exptime = digit+
                >{ printf("exptime\n"); s = p; }
                %{ if (mc_strtoul(s, p, &req->exptime) == -1) assert(0); };
        flags = digit+
                >{ printf("flags\n"); s = p; }
                %{ if (mc_strtoul(s, p, &req->flags) == -1) assert(0); };
        bytes = digit+
                >{ printf("bytes\n"); s = p; }
                %{ if (mc_strtoul(s, p, &req->bytes) == -1) assert(0); };
        cas_value = digit+
                >{ printf("cas\n"); s = p; }
                %{ if (mc_strtoul(s, p, &req->cas) == -1) assert(0); };
        incr_value = digit+
                >{ printf("incr\n"); s = p; }
                %{ if (mc_strtoul(s, p, &req->inc_val) == -1) assert(0); };
        flush_delay = digit+
                >{ printf("flush_delay\n"); s = p; }
                %{ if (mc_strtoul(s, p, &req->exptime) == -1) assert(0); };

        eol = ("\r\n" | "\n") @{ p++; printf("eof\n"); };
        spc = " "+ %{printf("spc\n");};
        noreply = (spc "noreply"i %{ req->noreply = true; })?;

        store_body = spc key spc flags spc exptime spc bytes               noreply spc? eol;
        cas_body   = spc key spc flags spc exptime spc bytes spc cas_value noreply spc? eol;
        get_body   = (spc key)+                                                    spc? eol;
        del_body   = spc key (spc exptime)?                                noreply spc? eol;
        cr_body    = spc key spc incr_value                                noreply spc? eol;
        flush_body = (spc flush_delay)?                                    noreply spc? eol;

        set     = ("set"i store_body)     @read_data @done @set;
        add     = ("add"i store_body)     @read_data @done @add;
        replace = ("replace"i store_body) @read_data @done @replace;
        append  = ("append"i  store_body) @read_data @done @append;
        prepend = ("prepend"i store_body) @read_data @done @prepend;
        cas     = ("cas"i cas_body)       @read_data @done @cas;

        get     = ("get"i    get_body) @done @get;
        gets    = ("gets"i   get_body) @done @gets;
        delete  = ("delete"i del_body) @done @delete;
        incr    = ("incr"i   cr_body)  @done @incr;
        decr    = ("decr"i   cr_body)  @done @decr;

        stats = "stats"i eol @done @stats;
        flush_all = "flush_all"i flush_body @done @flush_all;
        quit = "quit"i eol @done @quit;

        main := set | cas | add | replace | append | prepend | get | gets |
                delete | incr | decr | stats | flush_all | quit;

        write init;
        write exec;
    }%%

    if (!done) {
exit:
        return -1;
    }
    return 0;
}

int main() {
    struct memcache_request req;
    const char s[] = "replace notexist 0 0 6\r\nbarva2\r\n";
    memcache_parse(&req, s, s + sizeof(s) - 1);
    return 0;
}

int luaopen_memctnt(lua_State *L) {
    return 0;
}
