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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_PRIV_INFO_H_
#define OCEANBASE_SHARE_SCHEMA_OB_PRIV_INFO_H_

#include <stdint.h>
#include "lib/lock/tbrwlock.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

template<class T, class V>
struct ObGetTablePrivKeyV2
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};
template<>
struct ObGetTablePrivKeyV2<ObTablePrivSortKey, ObTablePriv *>
{
  ObTablePrivSortKey operator()(const ObTablePriv *table_priv) const
  {
    ObTablePrivSortKey key;
    return NULL != table_priv ?
      table_priv->get_sort_key() :
      key;
  }
};
class ObPrivInfo
{
public:
  typedef common::ObSortedVector<ObDBPriv *>::iterator DBPrivIter;
  typedef common::ObSortedVector<ObTablePriv *>::iterator TablePrivIter;
  typedef common::ObSortedVector<ObDBPriv *>::const_iterator ConstDBPrivIter;
  typedef common::ObSortedVector<ObTablePriv *>::const_iterator ConstTablePrivIter;
public:
  ObPrivInfo();
  void reset();
  int assign(const ObPrivInfo &priv_info);

  // db privilege
  int add_db_priv(ObDBPriv *db_priv,
                  const bool is_replace);
  int del_db_priv(const ObOriginalDBKey &db_key);
  ObDBPriv* get_db_priv(const ObOriginalDBKey &db_key,
                        DBPrivIter &target_db_priv_iter) const;
  const ObDBPriv* get_db_priv(const uint64_t tenant_id,
                              const uint64_t user_id,
                              const common::ObString &db,
                              bool db_is_pattern = false) const;
  ObPrivSet get_db_priv_set(const uint64_t tenant_id,
                            const uint64_t user_id,
                            const common::ObString &db) const;
  // table privilege
  int add_table_priv(ObTablePriv *table_priv,
                     const bool is_replace);
  int del_table_priv(const ObTablePrivSortKey &table_key);
  int build_table_priv_hashmap();
  bool table_grant_in_db(const uint64_t tenant_id,
                         const uint64_t user_id,
                         const common::ObString &db) const;
  const ObTablePriv* get_table_priv(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    const common::ObString &db,
                                    const common::ObString &table) const;
  const ObTablePriv* get_table_priv(const ObTablePrivSortKey &table_priv_key) const;
  const ObTablePriv* get_table_priv_from_vector(const ObTablePrivSortKey &table_priv_key) const;
  ObPrivSet get_table_priv_set(const uint64_t tenant_id,
                               const uint64_t user_id,
                               const common::ObString &db,
                               const common::ObString &table) const;
  int check_db_priv(const ObSessionPrivInfo &session_priv,
                    const common::ObString &db,
                    const ObPrivSet need_priv);
  int check_db_priv(const ObSessionPrivInfo &session_priv,
                    const common::ObString &db,
                    const ObPrivSet need_priv,
                    ObPrivSet &user_db_priv_set);
  int check_single_table_priv(const ObSessionPrivInfo &session_priv,
                              const ObNeedPriv &table_need_priv);
  int check_user_priv(const ObSessionPrivInfo &session_priv,
                      const ObPrivSet priv_set);
private:
  static const char *get_first_priv_name(ObPrivSet priv_set);
  static const char *get_priv_name(int64_t priv_shift);
  ConstDBPrivIter db_priv_begin() const { return db_privs_.begin(); }
  ConstDBPrivIter db_priv_end() const { return db_privs_.end(); }
  ConstTablePrivIter table_priv_begin() const { return table_privs_.begin(); }
  ConstTablePrivIter table_priv_end() const { return table_privs_.end(); }

public:
  common::SpinRWLock rwlock_;
  int64_t ref_count_;
  common::ObSortedVector<ObDBPriv *, common::ModulePageAllocator> db_privs_;
  common::ObSortedVector<ObTablePriv *, common::ModulePageAllocator> table_privs_;
  common::hash::ObPointerHashMap<ObTablePrivSortKey, ObTablePriv *, ObGetTablePrivKeyV2> table_priv_map_;
  static const char *priv_names_[];
};

}
}
}
#endif
