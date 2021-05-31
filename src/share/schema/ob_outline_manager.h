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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_OUTLINE_MANAGER_H
#define OCEANBASE_SHARE_SCHEMA_OB_OUTLINE_MANAGER_H

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/allocator/ob_memfrag_recycle_allocator.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

template<class T, class V>
struct ObGetOutlineKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetOutlineKey<uint64_t, ObOutlineInfo *>
{
  uint64_t operator()(const ObOutlineInfo *outline_info) const
  {
    return NULL != outline_info ? outline_info->get_outline_id() : common::OB_INVALID_ID;
  }
};

template<>
struct ObGetOutlineKey<ObOutlineNameHashWrapper, ObOutlineInfo *>
{
  ObOutlineNameHashWrapper operator()(const ObOutlineInfo *outline_info) const
  {
    ObOutlineNameHashWrapper name_wrap;
    if (!OB_ISNULL(outline_info)) {
      name_wrap.set_tenant_id(outline_info->get_tenant_id());
      name_wrap.set_database_id(outline_info->get_database_id());
      name_wrap.set_name(outline_info->get_name_str());
    }
    return name_wrap;
  }
};

template<>
struct ObGetOutlineKey<ObOutlineSignatureHashWrapper, ObOutlineInfo *>
{
  ObOutlineSignatureHashWrapper operator()(const ObOutlineInfo *outline_info) const
  {
    ObOutlineSignatureHashWrapper sql_wrap;
    if (!OB_ISNULL(outline_info)) {
      sql_wrap.set_tenant_id(outline_info->get_tenant_id());
      sql_wrap.set_database_id(outline_info->get_database_id());
      sql_wrap.set_signature(outline_info->get_signature_str());
    }
    return sql_wrap;
  }
};

class ObOutlineManager
{
public:

  static const int64_t OUTLINE_MEM_EXPIRE_TIME = 3600 * 1000 * 1000L; //one hour
  typedef common::ObSortedVector<ObOutlineInfo *>::iterator OutlineIterator;
  typedef common::ObSortedVector<ObOutlineInfo *>::const_iterator ConstOutlineIterator;

  ObOutlineManager();
  explicit ObOutlineManager(common::ObMemfragRecycleAllocator &global_allocator);
  virtual ~ObOutlineManager();
  int init();
  void reset();
  int assign(const ObOutlineManager &outline_manager);
  void print_outline_infos() const;
public:
  common::ObMemfragRecycleAllocator &get_allocator() {return allocator_;}
  int copy_outline_infos(const ObOutlineManager &priv_manager);
  virtual int deep_copy(const ObOutlineManager &priv_manager);

  ConstOutlineIterator outline_begin() const { return outline_infos_.begin(); }
  ConstOutlineIterator outline_end() const { return outline_infos_.end(); }

//  int check_outline_exist(const uint64_t outline_id, bool &exist) const;
//  int check_outline_exist(const uint64_t tenant_id,
//                          const common::ObString &database_name,
//                          const common::ObString &outline_name,
//                          bool &exist) const;

  int check_outline_exist_with_name(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const common::ObString &outline_name,
                                    uint64_t &outline_id,
                                    bool &exist) const;
  int check_outline_exist_with_sql(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const common::ObString &paramlized_sql,
                                    bool &exist) const;

  const ObOutlineInfo *get_outline_info(const uint64_t outline_id) const;
  //  const ObOutlineInfo *get_outline_info(const uint64_t tenant_id,
//                                        const uint64_t database_id,
//                                        const common::ObString &outline_name) const;
  //  const ObOutlineInfo *get_outline_info(const uint64_t tenant_id,
//                                            const common::ObString &database_name,
//                                            const common::ObString &outline_name) const;
  int get_outline_info_with_name(const uint64_t tenant_id,
                                 const uint64_t database_id,
                                 const common::ObString &name,
                                 ObOutlineInfo *&outline_info) const;
  int get_outline_info_with_signature(const uint64_t tenant_id,
                                      const uint64_t database_id,
                                      const common::ObString &signature,
                                      ObOutlineInfo *&outline_info) const;
  //  const ObOutlineInfo *get_outline_info(const uint64_t outline_id) const;
  int add_outline_info(ObOutlineInfo *outline_info, const bool is_replace);
  int del_outlines(const common::hash::ObHashSet<uint64_t> &outline_id_set);
  virtual int add_new_outline_info_array(const common::ObArray<ObOutlineInfo> &outline_info_array);

  int get_outline_infos_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<const ObOutlineInfo *> &outline_infos) const;
  int get_outline_infos_in_database(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  common::ObIArray<const ObOutlineInfo *> &outline_infos) const;
  int del_outline(const ObTenantOutlineId outline_to_delete);
private:
  int add_new_outline_info(const ObOutlineInfo &outline_info);

private:
  template<class T>
      void free(T *&p);

  static bool compare_outline_info(const ObOutlineInfo *lhs,
                                   const ObOutlineInfo *rhs);
  static bool equal_outline_info(const ObOutlineInfo *lhs,
                                 const ObOutlineInfo *rhs);

  static bool equal_tenant_outline_id(const ObOutlineInfo *lhs,
                                      const ObTenantOutlineId &tenant_outline_id);
  static bool compare_tenant_outline_id(const ObOutlineInfo *lhs,
                                      const ObTenantOutlineId &outline_id);

  int build_outline_hashmap();
private:
  common::ObSortedVector<ObOutlineInfo *, common::ModulePageAllocator> outline_infos_;
  common::hash::ObPointerHashMap<uint64_t, ObOutlineInfo *, ObGetOutlineKey> outline_id_map_;
  common::hash::ObPointerHashMap<ObOutlineNameHashWrapper, ObOutlineInfo *, ObGetOutlineKey>
      outline_name_map_;
  common::hash::ObPointerHashMap<ObOutlineSignatureHashWrapper, ObOutlineInfo *, ObGetOutlineKey>
      outline_sql_map_;

  bool use_global_allocator_;
  common::ObMemfragRecycleAllocator local_allocator_;
  common::ObMemfragRecycleAllocator &allocator_;
};

template<class T>
void ObOutlineManager::free(T *&p)
{
  if (NULL != p) {
    p->~T();
    allocator_.free(p);
    BACKTRACE(INFO, true, "free outline schema[%p]", p);
    p = NULL;
  }
}
}//end of schema
}//end of share
}//end of oceanbase
#endif
