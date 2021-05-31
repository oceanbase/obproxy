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

#ifndef OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_H_
#define OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_H_

#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_role.h"
#include "common/ob_partition_key.h"

namespace oceanbase
{
namespace share
{
struct ObReplicaLocation
{
  OB_UNIS_VERSION(1);
public:
  common::ObAddr server_;
  common::ObRole role_;
  int64_t sql_port_;
  int64_t reserved_;

  ObReplicaLocation();
  void reset();
  inline bool is_valid() const;
  inline bool operator==(const ObReplicaLocation &other) const;
  inline bool operator!=(const ObReplicaLocation &other) const;
  TO_STRING_KV(K_(server), K_(role), K_(sql_port), K_(reserved));
};

inline bool ObReplicaLocation::is_valid() const
{
  return server_.is_valid();
}

bool ObReplicaLocation::operator==(const ObReplicaLocation &other) const
{
  return server_ == other.server_ && role_ == other.role_
      && sql_port_ == other.sql_port_;
}

bool ObReplicaLocation::operator!=(const ObReplicaLocation &other) const
{
  return !(*this == other);
}

class ObPartitionLocation
{
  OB_UNIS_VERSION(1);
  friend class ObPartitionLocationCache;
public:
  typedef common::ObSEArray<ObReplicaLocation, common::OB_MAX_MEMBER_NUMBER,
      common::ObNullAllocator, common::ObArrayDefaultCallBack<ObReplicaLocation>,
      common::DefaultItemEncode<ObReplicaLocation> > ObReplicaLocationArray;

  ObPartitionLocation();
  virtual ~ObPartitionLocation();

  void reset();
  bool is_valid() const;
  bool operator==(const ObPartitionLocation &other) const;
  int add(const ObReplicaLocation &replica_location);
  int del(const common::ObAddr &server);
  // return OB_LOCATION_LEADER_NOT_EXIST for leader not exist.
  int get_leader(ObReplicaLocation &replica_location) const;
  // return OB_LOCATION_LEADER_NOT_EXIST for leader not exist.
  int check_leader_exist() const;

  int64_t size() const { return replica_locations_.count(); }

  inline uint64_t get_table_id() const { return table_id_; }
  inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }

  inline int64_t get_partition_id() const { return partition_id_; }
  inline void set_partition_id(const int64_t partition_id) { partition_id_ = partition_id; }

  inline int64_t get_partition_cnt() const { return partition_cnt_; }
  inline void set_partition_cnt(const int64_t partition_cnt) { partition_cnt_ = partition_cnt; }

  inline int64_t get_renew_time() const { return renew_time_; }
  inline void set_renew_time(const int64_t renew_time) { renew_time_ = renew_time; }

  inline const common::ObIArray<ObReplicaLocation> &get_replica_locations() const { return replica_locations_; }

  inline int get_partition_key(common::ObPartitionKey &key) const
  {
    return key.init(table_id_,
                    static_cast<int32_t> (partition_id_),
                    static_cast<int32_t> (partition_cnt_));
  }
  inline void mark_fail() { is_mark_fail_ = true; }
  inline void unmark_fail() { is_mark_fail_ = false; }
  inline bool is_mark_fail() const { return is_mark_fail_; }
  int change_leader(const common::ObAddr &new_leader);

  TO_STRING_KV(KT_(table_id), K_(partition_id), K_(partition_cnt),
      K_(replica_locations), K_(renew_time), K_(is_mark_fail));

private:
  // return OB_ENTRY_NOT_EXIST for not found.
  int find(const common::ObAddr &server, int64_t &idx) const;

private:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t partition_cnt_;
  ObReplicaLocationArray replica_locations_;
  int64_t renew_time_;
  bool is_mark_fail_;
};

}//end namespace share
}//end namespace oceanbase

#endif
