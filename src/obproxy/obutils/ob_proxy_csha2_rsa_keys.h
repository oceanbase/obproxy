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

#ifndef OB_PROXY_CSHA2_RSA_KEYS_H_
#define OB_PROXY_CSHA2_RSA_KEYS_H_

#include <openssl/rsa.h>

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
// Generate RSA keys for caching_sha2_password client leg at startup (fixed paths .conf/rsa_*.pem).
// Call once after proxy config is loaded. Does not affect startup outcome: on failure logs and leaves
// ob_proxy_csha2_rsa_get_private_key() == NULL so client-leg RSA is unavailable until next restart.
void init_csha2_rsa_keys();

RSA *ob_proxy_csha2_rsa_get_private_key();
const char *ob_proxy_csha2_rsa_get_public_key_path();
const char *ob_proxy_csha2_rsa_get_private_key_path();

} // namespace obutils
} // namespace obproxy
} // namespace oceanbase

#endif /* OB_PROXY_CSHA2_RSA_KEYS_H_ */
