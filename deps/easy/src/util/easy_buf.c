#include <stdarg.h>
#include <unistd.h>
#include "easy_buf.h"
#include "easy_string.h"
#include "easy_log.h"

easy_buf_t *easy_buf_create(easy_pool_t *pool, uint32_t size)
{
    easy_buf_t              *b;

    if ((b = (easy_buf_t *)easy_pool_calloc(pool, sizeof(easy_buf_t))) == NULL)
        return NULL;

    if (size == 0)
        size = pool->end - pool->last;

    if ((b->data = (char *)easy_pool_alloc(pool, size)) == NULL)
        return NULL;

    b->pos = b->data;
    b->last = b->pos;
    b->end = b->last + size;
    b->cleanup = NULL;
    b->args = pool;
    easy_list_init(&b->node);

    return b;
}

static uint64_t priv_pool_created = 0;
static uint64_t priv_pool_destroyed = 0;
static void easy_buf_free_private_pool(easy_buf_t * b, easy_pool_t * pool)
{
    easy_debug_log("easy free residual buffer: %p remain=%d\n", b, easy_buf_len(b));
    easy_pool_destroy(pool);
    priv_pool_destroyed++;
}

easy_buf_t* easy_buf_clone_with_private_pool(easy_buf_t* b)
{
    easy_buf_t* nb = NULL;
    int64_t data_len = easy_buf_len(b);
    easy_pool_t* pool = easy_pool_create(data_len + sizeof(*b) + sizeof(*pool));
    if (NULL != pool) {
        nb = easy_buf_create(pool, data_len);
    }
    if (NULL != nb) {
        memcpy(nb->last, b->pos, data_len);
        nb->last += data_len;
        nb->cleanup = (easy_buf_cleanup_pt*)easy_buf_free_private_pool;
        {
            if ((priv_pool_created & 0x1ff) == 0) {
                easy_info_log("easy created (%ld) private pools, and destoyed (%ld) pools.\n",
                              priv_pool_created, priv_pool_destroyed);
            }
            priv_pool_created++;
        }
    } else {
        if (NULL != pool) {
            easy_pool_destroy(pool);
        }
    }
    return nb;
}

easy_buf_t *easy_buf_pack(easy_pool_t *pool, const void *data, uint32_t size)
{
    easy_buf_t              *b;

    if ((b = (easy_buf_t *)easy_pool_calloc(pool, sizeof(easy_buf_t))) == NULL)
        return NULL;

    easy_buf_set_data(pool, b, data, size);

    return b;
}

void easy_buf_set_data(easy_pool_t *pool, easy_buf_t *b, const void *data, uint32_t size)
{
    b->data = (char *)data;
    b->pos = b->data;
    b->last = b->pos + size;
    b->end = b->last;
    b->cleanup = NULL;
    b->args = pool;
    b->flags = 0;
    easy_list_init(&b->node);
}

easy_file_buf_t *easy_file_buf_create(easy_pool_t *pool)
{
    easy_file_buf_t         *b;

    b = (easy_file_buf_t *)easy_pool_calloc(pool, sizeof(easy_file_buf_t));
    b->flags = EASY_BUF_FILE;
    b->cleanup = NULL;
    b->args = pool;
    easy_list_init(&b->node);

    return b;
}

void easy_file_buf_set_close(easy_file_buf_t *b)
{
    if ((b->flags & EASY_BUF_FILE))
        b->flags = EASY_BUF_CLOSE_FILE;
}

void easy_buf_set_cleanup(easy_buf_t *b, easy_buf_cleanup_pt *cleanup, void *args)
{
    b->cleanup = cleanup;
    b->args = args;
}

void easy_buf_destroy(easy_buf_t *b)
{
    easy_session_t *s;
    easy_buf_cleanup_pt *cleanup;

    /*
     * Session must be got before cleanup is called, because cleanup may free
     * the memory pool and then the memory space of b becomes illegal.
     */
    s = b->session;
    easy_list_del(&b->node);
    if ((b->flags & EASY_BUF_CLOSE_FILE) == EASY_BUF_CLOSE_FILE) {
        close(((easy_file_buf_t *)b)->fd);
    }

    if ((cleanup = b->cleanup)) {
        b->cleanup = NULL;
        (*cleanup)(b, b->args);
    }

    if (s != NULL) {
        if ((s->type == EASY_TYPE_SESSION) || (s->type == EASY_TYPE_KEEPALIVE_SESSION)) {
            s->buf_count--;
            s->sent_buf_count++;
            if (unlikely(s->enable_trace)) {
                easy_debug_log("destroy buffer, session=%p, count=%ld, on_write_success=%p",
                               s, s->buf_count, s->on_write_success);
            }
            if (s->buf_count == 0) {
                s->nextb = NULL;
                if (s->on_write_success) {
                    s->on_write_success(s);
                }
            }
        }
    }
}

int easy_buf_check_read_space(easy_pool_t *pool, easy_buf_t *b, uint32_t size)
{
    int                     dsize;
    char                    *ptr;

    if ((b->end - b->last) >= (int)size)
        return EASY_OK;

    dsize = (b->last - b->pos);
    size = easy_max(dsize * 3 / 2, size + dsize);
    size = easy_align(size, EASY_POOL_PAGE_SIZE);

    // alloc
    if ((ptr = (char *)easy_pool_alloc(pool, size)) == NULL)
        return EASY_ERROR;

    // copy old buf to new buf
    if (dsize > 0)
        memcpy(ptr, b->pos, dsize);

    b->data = ptr;
    b->pos = ptr;
    b->last = b->pos + dsize;
    b->end = b->pos + size;

    return EASY_OK;
}

easy_buf_t *easy_buf_check_write_space(easy_pool_t *pool, easy_list_t *bc, uint32_t size)
{
    easy_buf_t              *b = easy_list_get_last(bc, easy_buf_t, node);

    if (b != NULL && (b->end - b->last) >= (int)size)
        return b;

    size = easy_align(size, EASY_POOL_PAGE_SIZE);

    if ((b = easy_buf_create(pool, size)) == NULL)
        return NULL;

    easy_list_add_tail(&b->node, bc);

    return b;
}

void easy_buf_chain_clear(easy_list_t *l)
{
    easy_buf_t              *b, *b1;

    easy_list_for_each_entry_safe(b, b1, l, node) {
        easy_buf_destroy(b);
    }
    easy_list_init(l);
}

void easy_buf_chain_offer(easy_list_t *l, easy_buf_t *b)
{
    if (!l->next) easy_list_init(l);

    easy_list_add_tail(&b->node, l);
}

int easy_buf_string_copy(easy_pool_t *pool, easy_buf_string_t *d, const easy_buf_string_t *s)
{
    if (s->len > 0) {
        d->data = (char *)easy_pool_alloc(pool, s->len + 1);
        memcpy(d->data, s->data, s->len);
        d->data[s->len] = '\0';
        d->len = s->len;
    }

    return s->len;
}

int easy_buf_string_printf(easy_pool_t *pool, easy_buf_string_t *d, const char *fmt, ...)
{
    int                     len;
    char                    buffer[2048];

    va_list                 args;
    va_start(args, fmt);
    len = easy_vsnprintf(buffer, 2048, fmt, args);
    va_end(args);
    d->data = (char *)easy_pool_alloc(pool, len + 1);
    memcpy(d->data, buffer, len);
    d->data[len] = '\0';
    d->len = len;
    return len;
}

int easy_buf_list_len(easy_list_t *l)
{
    easy_buf_t              *b;
    int                     len = 0;

    easy_list_for_each_entry(b, l, node) {
        len += easy_buf_len(b);
    }

    return len;
}

