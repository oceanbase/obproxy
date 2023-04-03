/**
 * Copyright (c) 2021 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_OPROXY_VC_C_H
#define _OB_OPROXY_VC_C_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief 
 *  libobclient calls this function to init obproxy and get driver_client handle 
 * @param config          obproxy config string
 * @param driver_client   point to driver_client handle
 * @return int            OB_SUCCESS means success and other means failure
 */
int ob_proxy_vc_c_init(const char *config, void **driver_client);

/**
 * @brief 
 *  libobclient calls this function to connect to obproxy
 * @param driver_client   driver_client handle
 * @param conn_timeout    connection timeout
 * @param sock_timeout    socket timeout
 * @return int 
 */
int ob_proxy_vc_c_connect(void *driver_client, int64_t conn_timeout, int64_t sock_timeout);

/**
 * @brief 
 *  libobclient calls this function to receive data from obproxy 
 * @param driver_client   driver_client handle
 * @param buffer          receive buffer
 * @param offset          buffer offset
 * @param mlen            max receive length
 * @param recv_len        point to receive length
 * @param flag            receive flag
 * @return int 
 */
int ob_proxy_vc_c_recv(void *driver_client, char *buffer, int64_t offset, int64_t mlen, int64_t *recv_len, int64_t flag);

/**
 * @brief 
 *  libobclient calls this function to send data to obproxy
 * @param driver_client   driver_client handle
 * @param buffer          send buffer
 * @param offset          buffer offset
 * @param mlen            send buffer len
 * @param send_len        point to send length
 * @param flag            send flag
 * @return int 
 */
int ob_proxy_vc_c_send(void *driver_client, const char *buffer, int64_t offset, int64_t mlen, int64_t *send_len, int64_t flag);

/**
 * @brief 
 *  libobclient calls this function to close driver client, but not stop obproxy service
 * @param driver_client   driver_client handle
 * @return int 
 */
int ob_proxy_vc_c_close(void *driver_client);

/**
 * @brief 
 *  libobclient calls this function to set timeout
 * @param driver_client   driver_client handle
 * @param type            type for timeout
 * @param timeout         ms
 * @return int 
 */
int ob_proxy_vc_set_timeout(void *driver_client, int type, int timeout);

/**
 * @brief 
 *  libobclient calls this function to get timeout 
 * @param driver_client   driver_client handle
 * @param type            type for timeout
 * @param timeout         ms
 * @return int 
 */
int ob_proxy_vc_get_timeout(void *driver_client_handle, int type, int *timeout);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /*_OB_OPROXY_VC_JNI_H */