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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_PRIV_MANAGER_H_
#define OCEANBASE_SHARE_SCHEMA_OB_PRIV_MANAGER_H_

#include <stdint.h>
#include "lib/tbsys.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/allocator/ob_memfrag_recycle_allocator.h"
#include "common/ob_hint.h"
#include "common/ob_object.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

template<class T, class V>
struct ObGetTablePrivKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetTablePrivKey<ObTablePrivSortKey, ObTablePriv *>
{
  ObTablePrivSortKey operator()(const ObTablePriv *table_priv) const
  {
    ObTablePrivSortKey key;
    return NULL != table_priv ?
      table_priv->get_sort_key() :
      key;
  }
};

template<class T, class V>
struct ObGetTenantName
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetTenantName<common::ObString, ObTenantSchema *>
{
  common::ObString operator()(const ObTenantSchema *tenant_info) const
  {
    common::ObString str;
    return (NULL != tenant_info ?
      tenant_info->get_tenant_name_str() :
      str);
  }
};

class ObPrivManager
{
public:
  static const int64_t  PRIV_MEM_EXPIRE_TIME = 3600 * 1000 * 1000L; //one hour
  static const char *priv_names_[];
  typedef common::ObSortedVector<ObTenantSchema *>::iterator TenantInfoIter;
  typedef common::ObSortedVector<ObUserInfo *>::iterator UserInfoIter;
  typedef common::ObSortedVector<ObDBPriv *>::iterator DBPrivIter;
  typedef common::ObSortedVector<ObTablePriv *>::iterator TablePrivIter;
  typedef common::ObSortedVector<ObTenantSchema *>::const_iterator ConstTenantInfoIter;
  typedef common::ObSortedVector<ObUserInfo *>::const_iterator ConstUserInfoIter;
  typedef common::ObSortedVector<ObDBPriv *>::const_iterator ConstDBPrivIter;
  typedef common::ObSortedVector<ObTablePriv *>::const_iterator ConstTablePrivIter;

  ObPrivManager();
  explicit ObPrivManager(common::ObMemfragRecycleAllocator &global_allocator);
  int assign(const ObPrivManager &priv_manager);
  ~ObPrivManager();
  int init(const bool is_init_sys_tenant = true);
  int init_sys_tenant();
  void reset();

  void print_priv_infos() const;
public:
  //---------memory related functions-----
  common::ObMemfragRecycleAllocator &get_allocator() {return allocator_; }
  int copy_priv_infos(const ObPrivManager &priv_manager);
  int deep_copy(const ObPrivManager &priv_manager);

  ConstTenantInfoIter tenant_info_begin() const { return tenant_infos_.begin(); }
  ConstTenantInfoIter tenant_info_end() const { return tenant_infos_.end(); }
  ConstUserInfoIter user_info_begin() const { return user_infos_.begin(); }
  ConstUserInfoIter user_info_end() const { return user_infos_.end(); }
  ConstDBPrivIter db_priv_begin() const { return db_privs_.begin(); }
  ConstDBPrivIter db_priv_end() const { return db_privs_.end(); }
  ConstTablePrivIter table_priv_begin() const { return table_privs_.begin(); }
  ConstTablePrivIter table_priv_end() const { return table_privs_.end(); }

  ///@brief Get tenant id with tenant name
  ///@return OB_INVALID_ID:tenant not exist. others:get valid tenant_id
  int get_tenant_id(const common::ObString &tenant_name, uint64_t &tenant_id_res) const;

  uint64_t get_user_id(const uint64_t tenant_id, const common::ObString &user_name) const;
  ///@brief Used for checking privileges.
  ///A user's privileges are caculated as:
  ///Global privileges(privileges in ObUserInfo)
  ///OR database privileges
  ///OR table privileges.
  ///Once user have the privileges at some level, the access is allowed.
  ///@param [in] session_priv the privileges info stored in current session.
  ///@param [in] stmt_need_privs the privileges wanted by the SQL statement
  ///@return OB_SUCCESS: OK. others: access deined.
  int check_priv(const ObSessionPrivInfo &session_priv,
                 const ObStmtNeedPrivs &stmt_need_privs) const;

  int check_priv_or(const ObSessionPrivInfo &session_priv,
                    const ObStmtNeedPrivs &stmt_need_privs) const;

  ///@brief Used for checking whether a table is allowed to be shown.
  ///If a user has privilege for access in
  ///global privileges(privileges in ObUserInfo)
  ///OR database privileges
  ///OR table privileges, the table is shown. Otherwise not shown.
  ///@param [in] session_priv the privileges info stored in current session.
  ///@param [in] db table's database
  ///@param [in] table table to be checked
  ///@param [out] allow_show
  ///@return OB_SUCCESS: check success.
  int check_table_show(const ObSessionPrivInfo &session_priv,
                       const common::ObString &db,
                       const common::ObString &table,
                       bool &allow_show) const;

  int check_db_show(const ObSessionPrivInfo &session_priv,
                    const common::ObString &db,
                    bool &allow_show) const;
  ///@brief Used for check whether the user can access the database
  ///@param [out] db_priv_set the privileges that the user granted on the database.
  int check_db_access(const ObSessionPrivInfo &session_priv,
                      const common::ObString &db,
                      ObPrivSet &db_priv_set,
                      bool print_warn = true) const;

  ///@brief Used for user login checking.
  ///@param [in] ObUserLoginInfo the infos needed when login
  ///@param [out] user_id the ID of the user
  ///@param [out] user_priv_set the global privileges of the user, should be stored in session.
  ///@param [out] db_priv_set the  database privileges of the user, should be stored in session.
  ///@return OB_SUCCESS: login be accessed. others: login be deined
  int check_user_access(const ObUserLoginInfo &login_info,
                        ObSessionPrivInfo &session_priv) const;

  ///@brief Used for checking whether user exist.
  ///@param [out] exist Whether user exist
  ///@return OB_SUCCESS: Without invalid arguments. others: Invalid arguments.
  int check_user_exist(const uint64_t tenant_id,
                       const common::ObString &user_name,
                       bool &exist) const;
  int check_user_exist(const uint64_t tenant_id,
                       const common::ObString &user_name,
                       uint64_t &user_id,
                       bool &exist) const;
  int check_user_exist(const uint64_t tenant_id,
                       const uint64_t user_id,
                       bool &exist) const;

  //Add privileges for incremental updating.
  int add_tenant_info(ObTenantSchema *tenant_schema, const bool is_replace);
  int add_user_info(ObUserInfo *user_info, const bool is_replace);
  int add_db_priv(ObDBPriv *db_priv, const bool is_replace);
  int add_table_priv(ObTablePriv *table_priv, const bool is_replace);
  int add_new_tenant_info(const ObTenantSchema &tenant_info);
  int add_new_user_info(const ObUserInfo &user_info);
  int add_new_db_priv(const ObDBPriv &db_priv);
  int add_new_table_priv(const ObTablePriv &table_priv);
  int add_new_tenant_info_array(const common::ObIArray<ObTenantSchema> &tenant_array);
  int add_new_user_info_array(const common::ObIArray<ObUserInfo> &priv_array);
  int add_new_db_priv_array(const common::ObIArray<ObDBPriv> &priv_array);
  int add_new_table_priv_array(const common::ObIArray<ObTablePriv> &priv_array);
  //Delete privileges for incremental updating.
  int del_tenants(const common::hash::ObHashSet<uint64_t> &tenant_id_set);
  int del_users(const common::hash::ObHashSet<ObTenantUserId> &user_id_set);
  int del_db_privs(const common::hash::ObHashSet<ObOriginalDBKey> &db_key_set);
  int del_table_privs(const common::hash::ObHashSet<ObTablePrivSortKey> &table_key_set);
  int del_tenant(const uint64_t tenant_id);
  int del_user(const ObTenantUserId tenant_user_id);
  int del_db_priv(const ObOriginalDBKey &db_key);
  int del_table_priv(const ObTablePrivSortKey &table_sort_key);

  //Get privileges info for incremental updating and rewriting priv_manager.
  const ObTenantSchema* get_tenant_info(const uint64_t &tenant_id) const;
  const ObTenantSchema* get_tenant_info(const common::ObString &tenant_name) const;
  const ObUserInfo* get_user_info(const ObTenantUserId &tenant_user_id) const;
  const ObDBPriv* get_db_priv(const ObOriginalDBKey &db_priv_key) const;
  const ObTablePriv* get_table_priv(const ObTablePrivSortKey &table_priv_key) const;

  ///@brief Get user infos with tenant_id
  int get_user_info_with_tenant_id(const uint64_t tenant_id,
                                   common::ObIArray<const ObUserInfo *> &user_infos) const;

  ///@brief Get database privileges of the user for delete user operation.
  int get_db_priv_with_user_id(const uint64_t tenant_id,
                               const uint64_t user_id,
                               common::ObIArray<ObDBPriv> &db_privs) const;
  int get_db_priv_with_user_id(const uint64_t tenant_id,
                               const uint64_t user_id,
                               common::ObIArray<const ObDBPriv*> &db_privs) const;
  ///@brief Get database privileges of a tenant
  int get_db_priv_with_tenant_id(const uint64_t tenant_id,
                                 common::ObIArray<const ObDBPriv*> &db_privs) const;
  ///@brief Get table privileges of the user for delete user operation.
  int get_table_priv_with_user_id(const uint64_t tenant_id,
                                  const uint64_t user_id,
                                  common::ObIArray<ObTablePriv> &table_privs) const;
  int get_table_priv_with_user_id(const uint64_t tenant_id,
                                  const uint64_t user_id,
                                  common::ObIArray<const ObTablePriv*> &table_privs) const;
  ///@brief Get table privileges of given tenant
  int get_table_priv_with_tenant_id(const uint64_t tenant_id,
                                    common::ObIArray<const ObTablePriv*> &table_privs) const;
  ///@brief Get user infos of given tenant
  int get_user_infos_with_tenant_id(const uint64_t tenant_id,
                                    common::ObIArray<const ObUserInfo *> &user_infos) const;

  static const char *get_first_priv_name(ObPrivSet priv_set);

  static const char *get_priv_name(int64_t priv_shift);

  ObPrivSet get_db_priv_set(const uint64_t tenant_id,
                            const uint64_t user_id,
                            const common::ObString &db) const;

  ObPrivSet get_user_priv_set(const uint64_t tenant_id, const common::ObString &user_name) const;

private:
  ObPrivManager &operator = (const ObPrivManager &priv_manager);
  template<class T>
  void free(T *&p);
  int build_tenant_name_hashmap();
  int build_table_priv_hashmap();

  ///@brief Used for check user privilege at user_level
  ///@param [in] session_priv the privileges info stored in current session.
  ///@param [in] priv_set privileges needed.
  int check_user_priv(
      const ObSessionPrivInfo &session_priv,
      const ObPrivSet priv_set) const;
  ///@brief Used for check database privileges when using SQL statement manipulate
  ///database, like 'create database', 'drop database'.
  ///@param [in] session_priv the privileges info stored in current session.
  ///@param [in] db the database name be manipulated.
  ///@param [in] need_priv the privileges needed.
  int check_db_priv(const ObSessionPrivInfo &session_priv,
                    const common::ObString &db,
                    const ObPrivSet need_priv) const;
  ///@brief Used for check database privileges.
  ///@param [in] session_priv the privileges info stored in current session.
  ///@param [in] db the database name accessed.
  ///@param [in] need_priv the privileges needed.
  ///@param [out] user_db_priv store the privileges at both user and db level.
  int check_db_priv(const ObSessionPrivInfo &session_priv,
                    const common::ObString &db,
                    const ObPrivSet need_priv,
                    ObPrivSet &user_db_priv) const;
  ///@brief check table privileges.
  int check_table_priv(const ObSessionPrivInfo &session_priv,
                       const common::ObIArray<ObNeedPriv> &table_need_privs) const;
  ///@brief check table privileges.
  int check_single_table_priv(const ObSessionPrivInfo &session_priv,
                                  const ObNeedPriv &table_need_priv) const;
  ///@brief check whether there is table granted to the user on the database
  bool table_grant_in_db(const uint64_t tenant_id,
                         const uint64_t user_id,
                         const common::ObString &db) const;

  const ObUserInfo* get_user_info(const uint64_t tenant_id,
                                  const common::ObString &user_name) const;

  const ObDBPriv* get_db_priv(const uint64_t tenant_id,
                              const uint64_t user_id,
                              const common::ObString &db,
                              bool db_is_pattern = false) const;
  ObDBPriv* get_db_priv(const ObOriginalDBKey &db_priv_key,
                        DBPrivIter &target_db_priv_iter) const;
  const ObTablePriv* get_table_priv_from_vector(const ObTablePrivSortKey &table_priv_key) const;
  const ObTablePriv* get_table_priv(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    const common::ObString &db,
                                    const common::ObString &table) const;

  common::ObSortedVector<ObTenantSchema *, common::ModulePageAllocator> tenant_infos_;
  common::hash::ObPointerHashMap<common::ObString, ObTenantSchema *,
      ObGetTenantName, common::OB_MALLOC_NORMAL_BLOCK_SIZE> tenant_name_map_;
  common::ObSortedVector<ObUserInfo *, common::ModulePageAllocator> user_infos_;
  common::ObSortedVector<ObDBPriv *, common::ModulePageAllocator> db_privs_;
  common::ObSortedVector<ObTablePriv *, common::ModulePageAllocator> table_privs_;
  common::hash::ObPointerHashMap<ObTablePrivSortKey, ObTablePriv *, ObGetTablePrivKey>
    table_priv_map_;

  bool use_global_allocator_;
  common::ObMemfragRecycleAllocator local_allocator_;
  common::ObMemfragRecycleAllocator &allocator_;
};

template<class T>
void ObPrivManager::free(T *&p)
{
  if (NULL != p) {
    p->~T();
    allocator_.free(p);
    BACKTRACE(INFO, true, "free schema[%p]", p);
    p = NULL;
  }
}

}
}
}
#endif
