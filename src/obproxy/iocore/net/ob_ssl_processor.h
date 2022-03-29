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

#ifndef OBPROXY_SSL_PROCESSOR_H_
#define OBPROXY_SSL_PROCESSOR_H_

#include <openssl/ssl.h>
#include <openssl/err.h>

#include "lib/lock/ob_drw_lock.h"
#include "lib/hash/ob_hashmap.h"
#include "utils/ob_proxy_lib.h"

#define OB_SSL_SUCC_RET 1

namespace oceanbase
{
namespace obproxy
{
namespace net
{

class ObSSLProcessor
{
public:
  ObSSLProcessor() : ssl_ctx_map_(), ssl_ctx_lock_(obsys::WRITE_PRIORITY) {}
  ~ObSSLProcessor() {}
  int init();
  SSL* create_new_ssl(const common::ObString &cluster_name,
                      const common::ObString &tenant_name);
  void release_ssl(SSL* ssl, const bool can_shutdown_ssl);
  void release_ssl_ctx(common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> &delete_info);
  void openssl_lock(int n);
  void openssl_unlock(int n);
  int update_key(const common::ObString &cluster_name,
                 const common::ObString &tenant_name,
                 const common::ObString &source_type,
                 const common::ObString &ca,
                 const common::ObString &public_key,
                 const common::ObString &private_key);

private:
  int update_key_from_file(SSL_CTX *ssl_ctx,
                           const common::ObString &ca,
                           const common::ObString &public_key,
                           const common::ObString &private_key);

  int update_key_from_string(SSL_CTX *ssl_ctx,
                             const common::ObString &ca,
                             const common::ObString &public_key,
                             const common::ObString &private_key);
private:
  static void *malloc_for_ssl(size_t num);
  static void *realloc_for_ssl(void *p, size_t num);
  static void free_for_ssl(void *str);

private:
  typedef common::hash::ObHashMap<common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH>, SSL_CTX*> SSLCtxHashMap;
  SSLCtxHashMap ssl_ctx_map_;
  common::DRWLock ssl_ctx_lock_;
};

extern ObSSLProcessor g_ssl_processor;

} // end net
} // end obproxy
} // end oceanbase

#endif