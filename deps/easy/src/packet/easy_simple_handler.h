#ifndef EASY_SIMPLE_HANDLER_H_
#define EASY_SIMPLE_HANDLER_H_

#include <easy_define.h>

EASY_CPP_START

#include <easy_io.h>

#define EASY_SIMPLE_PACKET_HEADER_SIZE (sizeof(int)*2)
typedef struct easy_simple_packet_t easy_simple_packet_t;

/**
 * len + data
 */
struct easy_simple_packet_t {
    uint32_t                len;
    uint32_t                chid;
    char                    *data;
    easy_list_t             list;
    char                    buffer[0];
};

/**
 * decode
 */
static inline void *easy_simple_decode(easy_message_t *m)
{
    easy_simple_packet_t    *packet;
    uint32_t                len, datalen;

    // length
    if ((len = m->input->last - m->input->pos) < EASY_SIMPLE_PACKET_HEADER_SIZE)
        return NULL;

    // data len
    datalen = *((uint32_t *)m->input->pos);

    if (datalen > 0x4000000) { // 64M
        easy_error_log("data_len is invalid: %d\n", datalen);
        m->status = EASY_ERROR;
        return NULL;
    }

    len -= EASY_SIMPLE_PACKET_HEADER_SIZE;

    if (len < datalen) {
        m->next_read_len = datalen - len;
        return NULL;
    }

    // alloc packet
    if ((packet = (easy_simple_packet_t *)easy_pool_calloc(m->pool,
                  sizeof(easy_simple_packet_t))) == NULL) {
        m->status = EASY_ERROR;
        return NULL;
    }

    packet->chid = *((uint32_t *)(m->input->pos + sizeof(int)));
    m->input->pos += EASY_SIMPLE_PACKET_HEADER_SIZE;
    packet->len = datalen;
    packet->data = (char *)m->input->pos;
    m->input->pos += datalen;

    return packet;
}

/**
 * encode
 */
static inline int easy_simple_encode(easy_request_t *r, void *data)
{
    easy_simple_packet_t     *packet;
    easy_buf_t              *b, *b1;

    packet = (easy_simple_packet_t *) data;

    if ((b = easy_buf_create(r->ms->pool, EASY_SIMPLE_PACKET_HEADER_SIZE)) == NULL)
        return EASY_ERROR;

    *((uint32_t *)b->last) = packet->len;
    *((uint32_t *)(b->last + sizeof(int))) = packet->chid;
    b->last += EASY_SIMPLE_PACKET_HEADER_SIZE;
    easy_request_addbuf(r, b);

    if (packet->data) {
        b1 = easy_buf_pack(r->ms->pool, packet->data, packet->len);
        easy_request_addbuf(r, b1);
    } else {
        easy_request_addbuf_list(r, &packet->list);
    }

    return EASY_OK;
}

static inline uint64_t easy_simple_packet_id(easy_connection_t *c, void *packet)
{
    return ((easy_simple_packet_t *) packet)->chid;
}

static inline easy_simple_packet_t *easy_simple_rnew(easy_request_t *r, int size)
{
    easy_simple_packet_t    *packet;

    size += sizeof(easy_simple_packet_t);
    packet = (easy_simple_packet_t *)easy_pool_alloc(r->ms->pool, size);
    memset(packet, 0, sizeof(easy_simple_packet_t));

    return packet;
}

static inline easy_simple_packet_t *easy_simple_new(easy_session_t **s, int size)
{
    easy_simple_packet_t    *packet = easy_session_packet_create(easy_simple_packet_t, *s, size);
    easy_list_init(&packet->list);
    return packet;
}

EASY_CPP_END

#endif
