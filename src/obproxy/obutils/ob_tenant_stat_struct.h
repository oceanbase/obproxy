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

#ifndef OBPROXY_TENANT_STAT_STRUCT_H
#define OBPROXY_TENANT_STAT_STRUCT_H

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/hash/ob_link_hashmap.h"
#include "utils/ob_proxy_lib.h"
#include "utils/ob_proxy_monitor_utils.h"
#include "opsql/parser/ob_proxy_parse_result.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

enum ObTenantStatEnum {
  STAT_TOTAL_COUNT,
  STAT_LOW_COUNT,
  STAT_MIDDLE_COUNT,
  STAT_HIGH_COUNT,
  STAT_TOTAL_TIME,
  STAT_PREPARE_TIME,
  STAT_SERVER_TIME,
  MAX_SESSION_STAT_CONT
};

struct ObTenantStatValue
{
  ObTenantStatValue()
  {
    reset();
  }
  void reset() { MEMSET(values_, 0, sizeof(values_)); }
  ~ObTenantStatValue() {}
  int64_t get_value(const int32_t id) const;
  int64_t atomic_get_and_reset_value(const ObTenantStatEnum key);
  inline void atomic_add(const ObTenantStatEnum key, const int64_t value)
  {
    (void)ATOMIC_AAF(values_ + key, value);
  }
  bool is_active() const;
  TO_STRING_KV("value", common::ObArrayWrap<int64_t>(values_, MAX_SESSION_STAT_CONT));

  int64_t values_[MAX_SESSION_STAT_CONT];
};

class ObTenantStatKey
{
public:
  ObTenantStatKey() : logic_tenant_hash_(0), logic_database_hash_(0),
                      cluster_hash_(0), tenant_hash_(0), database_hash_(0),
                      stmt_type_(OBPROXY_T_MAX), error_code_hash_(0) {}
  ObTenantStatKey(const common::ObString &logic_tenant_name,
                  const common::ObString &logic_database_name,
                  const common::ObString &cluster_name,
                  const common::ObString &tenant_name,
                  const common::ObString &database_name,
                  ObProxyBasicStmtType stmt_type,
                  const common::ObString &error_code)
    : logic_tenant_hash_(logic_tenant_name.hash()),
      logic_database_hash_(logic_database_name.hash()),
      cluster_hash_(cluster_name.hash()), tenant_hash_(tenant_name.hash()),
      database_hash_(database_name.hash()), stmt_type_(stmt_type),
      error_code_hash_(error_code.hash()) {}
  void reset()
  {
    logic_tenant_hash_ = 0;
    logic_database_hash_ = 0;
    cluster_hash_ = 0;
    tenant_hash_ = 0;
    database_hash_ = 0;
    stmt_type_ = OBPROXY_T_MAX;
    error_code_hash_ = 0;
  }
  bool operator==(const ObTenantStatKey &other) const
  {
    return (other.logic_tenant_hash_ == logic_tenant_hash_
            && other.logic_database_hash_ == logic_database_hash_
            && other.cluster_hash_ == cluster_hash_
            && other.tenant_hash_ == tenant_hash_
            && other.database_hash_ == database_hash_
            && other.stmt_type_ == stmt_type_
            && other.error_code_hash_ == error_code_hash_);
  }

  int compare(const ObTenantStatKey &other) {
    int ret = 0;
    if (logic_tenant_hash_ < other.logic_tenant_hash_) {
      ret = -1;
    } else if (logic_tenant_hash_ > other.logic_tenant_hash_) {
      ret = 1;
    } else if (logic_database_hash_ < other.logic_database_hash_) {
      ret = -1;
    } else if (logic_database_hash_ > other.logic_database_hash_) {
      ret = 1;
    } else if (cluster_hash_ < other.cluster_hash_) {
      ret = -1;
    } else if (cluster_hash_ > other.cluster_hash_) {
      ret = 1;
    } else if (tenant_hash_ < other.tenant_hash_) {
      ret = -1;
    } else if (tenant_hash_ > other.tenant_hash_) {
      ret = 1;
    } else if (database_hash_  < other.database_hash_ ) {
      ret = -1;
    } else if (database_hash_  > other.database_hash_ ) {
      ret = 1;
    } else if (stmt_type_ < other.stmt_type_) {
      ret = -1;
    } else if (stmt_type_ > other.stmt_type_) {
      ret = 1;
    } else if (error_code_hash_ < other.error_code_hash_) {
      ret = -1;
    } else if (error_code_hash_ > other.error_code_hash_) {
      ret = 1;
    } else {
      ret = 0;
    }
    return ret;
  }

  static uint64_t get_hash(const uint64_t logic_tenant_hash,
                           const uint64_t logic_database_hash,
                           const uint64_t cluster_hash,
                           const uint64_t tenant_hash,
                           const uint64_t datatbase_hash,
                           const ObProxyBasicStmtType stmt_type,
                           const uint64_t error_code_hash)
  {
    uint64_t ret_hash = common::murmurhash(&logic_database_hash, sizeof(logic_database_hash), logic_tenant_hash);
    ret_hash = common::murmurhash(&cluster_hash, sizeof(cluster_hash), ret_hash);
    ret_hash = common::murmurhash(&tenant_hash, sizeof(tenant_hash), ret_hash);
    ret_hash = common::murmurhash(&datatbase_hash, sizeof(datatbase_hash), ret_hash);
    ret_hash = common::murmurhash(&stmt_type, sizeof(stmt_type), ret_hash);
    ret_hash = common::murmurhash(&error_code_hash, sizeof(error_code_hash), ret_hash);
    return ret_hash;
  }

  uint64_t hash() const
  {
    return ObTenantStatKey::get_hash(logic_tenant_hash_, logic_database_hash_,
                                     cluster_hash_, tenant_hash_, database_hash_,
                                     stmt_type_, error_code_hash_);
  }

  TO_STRING_KV(K_(logic_tenant_hash), K_(logic_database_hash),
               K_(cluster_hash), K_(tenant_hash), K_(database_hash),
               K_(stmt_type), K_(error_code_hash));

public:
  uint64_t logic_tenant_hash_;
  uint64_t logic_database_hash_;
  uint64_t cluster_hash_;
  uint64_t tenant_hash_;
  uint64_t database_hash_;
  ObProxyBasicStmtType stmt_type_;
  uint64_t error_code_hash_;
};

typedef common::LinkHashNode<ObTenantStatKey> ObTenantStatHashNode;

class ObTenantStatItem : public ObTenantStatHashNode
{
public:
  ObTenantStatItem()
    : idle_period_count_(0), logic_tenant_name_(), logic_database_name_(),
      cluster_name_(), tenant_name_(), database_name_(),
      database_type_(common::DB_MAX), stmt_type_(OBPROXY_T_MAX),
      error_code_()
  {
    MEMSET(logic_tenant_name_str_, 0, sizeof(logic_tenant_name_str_));
    MEMSET(logic_database_name_str_, 0, sizeof(logic_database_name_str_));
    MEMSET(cluster_name_str_, 0, sizeof(cluster_name_str_));
    MEMSET(tenant_name_str_, 0, sizeof(tenant_name_str_));
    MEMSET(database_name_str_, 0, sizeof(database_name_str_));
    MEMSET(error_code_str_, 0, sizeof(error_code_str_));
  }
  virtual ~ObTenantStatItem() {}
  int64_t get_value(const int32_t id) const { return stat_value_.get_value(id); };
  bool is_active() const { return stat_value_.is_active(); };

  common::DRWLock &get_lock() { return lock_; }
  common::ObString get_logic_tenant_name() const { return logic_tenant_name_; }
  common::ObString get_logic_database_name() const { return logic_database_name_; }
  common::ObString get_cluster_name() const { return cluster_name_; }
  common::ObString get_tenant_name() const { return tenant_name_; }
  common::ObString get_database_name() const { return database_name_; }
  common::DBServerType get_database_type() const {return database_type_;}
  void set_key(const common::ObString &logic_tenant_name,
               const common::ObString &logic_database_name,
               const common::ObString &cluster_name,
               const common::ObString &tenant_name,
               const common::ObString &database_name,
               const common::DBServerType database_type,
               const ObProxyBasicStmtType stmt_type,
               const common::ObString &error_code);

  inline void atomic_add(const ObTenantStatEnum key, const int64_t value = 1)
  {
    stat_value_.atomic_add(key, value);
  }
  int64_t inc_and_fetch_idle_period_count() { return ATOMIC_AAF(&idle_period_count_, 1); }
  void reset_idle_period_count() { idle_period_count_ = 0; }
  int64_t to_monitor_stat_string(char* buf, const int64_t buf_len);

  TO_STRING_KV(K_(logic_tenant_name), K_(logic_database_name),
               K_(cluster_name), K_(tenant_name), K_(database_name), K_(database_type),
               K_(stmt_type), K_(error_code), K_(idle_period_count), K_(stat_value));

private:
  common::DRWLock lock_;

  ObTenantStatValue stat_value_;
  int64_t idle_period_count_;

  common::ObString logic_tenant_name_;
  common::ObString logic_database_name_;
  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString database_name_;
  common::DBServerType database_type_;
  ObProxyBasicStmtType stmt_type_;
  common::ObString error_code_;
  char logic_tenant_name_str_[common::OB_MAX_TENANT_NAME_LENGTH];//64B
  char logic_database_name_str_[common::OB_MAX_DATABASE_NAME_LENGTH];//128B
  char cluster_name_str_[OB_PROXY_MAX_CLUSTER_NAME_LENGTH];//48B
  char tenant_name_str_[common::OB_MAX_TENANT_NAME_LENGTH];//64B
  char database_name_str_[common::OB_MAX_DATABASE_NAME_LENGTH];//128B
  char error_code_str_[common::OB_MAX_ERROR_CODE_LEN];

  DISALLOW_COPY_AND_ASSIGN(ObTenantStatItem);
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_TENANT_STAT_STRUCT_H
