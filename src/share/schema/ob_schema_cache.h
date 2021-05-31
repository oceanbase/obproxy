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

#ifndef OB_OCEANBASE_SCHEMA_SCHEMA_CACHE_H_
#define OB_OCEANBASE_SCHEMA_SCHEMA_CACHE_H_

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "common/cache/ob_kv_storecache.h"
#include "share/schema/ob_schema_struct.h"


namespace oceanbase
{
namespace common
{
class ObKVCacheHandle;
class ObISQLClient;
class ObIAllocator;
}
namespace share
{
namespace schema
{


class ObSchemaCacheKey : public common::ObIKVCacheKey
{
public:
  ObSchemaCacheKey();
  ObSchemaCacheKey(const ObSchemaType schema_type,
                   const uint64_t schema_id,
                   const uint64_t schema_version);
  virtual ~ObSchemaCacheKey() {}
  virtual uint64_t get_tenant_id() const;
  virtual bool operator ==(const ObIKVCacheKey &other) const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char *buf,
                        int64_t buf_len,
                        ObIKVCacheKey *&key) const;
  TO_STRING_KV(K_(schema_type),
               K_(schema_id),
               K_(schema_version));

  ObSchemaType schema_type_;
  uint64_t schema_id_;
  uint64_t schema_version_;
};

class ObSchemaCacheValue : public common::ObIKVCacheValue
{
public:
  ObSchemaCacheValue();
  ObSchemaCacheValue(ObSchemaType schema_type,
                     ObSchema *schema);
  virtual ~ObSchemaCacheValue() {}
  virtual int64_t size() const;
  virtual int deep_copy(char *buf,
                        int64_t buf_len,
                        ObIKVCacheValue *&value) const;
  TO_STRING_KV(K_(schema_type),
               K(reinterpret_cast<char *>(schema_)));

  ObSchemaType schema_type_;
  ObSchema *schema_;
};

class ObSchemaFetcher;
class ObSchemaCache
{
  static const int64_t OB_SCHEMA_CACHE_SYS_CACHE_MAP_BUCKET_NUM = 512;
public:
  ObSchemaCache();
  virtual ~ObSchemaCache() {}

  int init(ObSchemaFetcher *schema_fetcher);
  // get schema
  int get_schema(ObSchemaType schema_type,
                 uint64_t schema_id,
                 int64_t schema_version,
                 common::ObKVCacheHandle &handle,
                 const ObSchema *&schema);

  // TODO, should be private
  int put_schema(ObSchemaType schema_type,
                 uint64_t schema_id,
                 int64_t schema_version,
                 const ObSchema &schema);

private:
  bool is_valid_key(ObSchemaType schema_type,
                    uint64_t schema_id,
                    int64_t schema_version);

  bool need_use_sys_cache(ObSchemaCacheKey &cache_key);

  int get_schema_from_cache(ObSchemaType schema_type,
                            uint64_t schema_id,
                            int64_t schema_version,
                            common::ObKVCacheHandle &handle,
                            const ObSchema *&schema);
private:
  typedef common::hash::ObHashMap<ObSchemaCacheKey,
                                  const ObSchemaCacheValue*,
                                  common::hash::ReadWriteDefendMode> NoSwapCache;
  typedef common::ObKVCache<ObSchemaCacheKey, ObSchemaCacheValue> KVCache;

  NoSwapCache sys_cache_;
  KVCache cache_;
  ObSchemaFetcher *schema_fetcher_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaCache);
};

class ObSchemaService;
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObSchemaFetcher
{
public:
  ObSchemaFetcher();
  virtual ~ObSchemaFetcher() {}
  bool check_inner_stat();
  int init(ObSchemaService *schema_service,
           ObMultiVersionSchemaService *multi_schema_service,
           common::ObISQLClient *sql_client);
  int fetch_schema(ObSchemaType schema_type,
                   uint64_t schema_id,
                   int64_t schema_version,
                   common::ObIAllocator &allocator,
                   ObSchema *&schema);
private:
  int fetch_tenant_schema(uint64_t tenant_id,
                         int64_t schema_version,
                         common::ObIAllocator &allocator,
                         ObTenantSchema *&tenant_schema);
  int fetch_outline_info(uint64_t outline_id,
      int64_t schema_version,
      common::ObIAllocator &allocator,
      ObOutlineInfo *&outline_info);

  int fetch_user_info(uint64_t user_id,
                      int64_t schema_version,
                      common::ObIAllocator &allocator,
                      ObUserInfo *&user_info);
  int fetch_database_schema(uint64_t database_id,
                            int64_t schema_version,
                            common::ObIAllocator &allocator,
                            ObDatabaseSchema *&database_schema);
  int fetch_tablegroup_schema(uint64_t tablegroup_id,
                              int64_t schema_version,
                              common::ObIAllocator &allocator,
                              ObTablegroupSchema *&tablegroup_schema);
  int fetch_table_schema(uint64_t table_id,
                         int64_t schema_version,
                         common::ObIAllocator &allocator,
                         ObTableSchema *&table_schema);
private:
  ObSchemaService *schema_service_;
  ObMultiVersionSchemaService *multi_schema_service_;
  common::ObISQLClient *sql_client_;
};

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_SCHEMA_CACHE_H_
