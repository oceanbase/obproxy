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

#ifndef OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_INFO_H_
#define OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_INFO_H_

#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "lib/list/ob_dlist.h"
#include "common/ob_role.h"
#include "common/ob_row_checksum.h"
#include "common/ob_zone.h"
#include "common/ob_partition_key.h"

namespace oceanbase
{
namespace share
{
class ObIReplicaFilter;

enum ObReplicaType
{
  // normal replica, will static data and log sync with partition leader
  OB_NORMAL_REPLICA,
  // temporary replica, no static data bug log sync with partition leader
  OB_TEMP_REPLICA,
  // temporary offline replica, with static data bug log not sync with partition leader
  OB_TEMP_OFFLINE_REPLICA,
  // flag replica, insert into partition table before create associated storage,
  // to indicate that we will create replica on the server later.
  OB_FLAG_REPLICA,

  OB_REPLICA_TYPE_MAX,
};
const char *ob_replica_type_str(const ObReplicaType type);

struct ObPartitionReplica
{
  OB_UNIS_VERSION(1);
public:
  struct Member {
    OB_UNIS_VERSION(1);
  public:
    Member() : timestamp_(0) {}
    Member(const common::ObAddr &server, const int64_t timestamp)
        : server_(server), timestamp_(timestamp) {}
    TO_STRING_KV(K_(server), K_(timestamp));

    operator const common::ObAddr &() const { return server_; }
    bool operator ==(const Member &o) const
    { return server_ == o.server_ && timestamp_ == o.timestamp_; }
    bool operator !=(const Member &o) const { return !(*this == o); }

    common::ObAddr server_;
    int64_t timestamp_;
  };


public:
  static const int64_t DEFAULT_REPLICA_COUNT = 7;
  typedef common::ObSEArray<Member, DEFAULT_REPLICA_COUNT> MemberList;

  ObPartitionReplica();
  void reset();
  inline bool is_valid() const;
  inline bool is_flag_replica() const;
  common::ObPartitionKey partition_key() const;

  static int member_list2text(const MemberList &member_list,
      char *text, const int64_t length);
  static int text2member_list(const char *text, MemberList &member_list);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  int assign(const ObPartitionReplica &other);

  uint64_t table_id_;
  int64_t partition_id_;
  int64_t partition_cnt_;

  common::ObZone zone_;
  common::ObAddr server_;
  int64_t sql_port_;
  uint64_t unit_id_;

  common::ObRole role_;
  MemberList member_list_;;
  int64_t row_count_;
  int64_t data_size_;
  int64_t data_version_;
  int64_t data_checksum_;
  common::ObRowChecksumValue row_checksum_;
  int64_t modify_time_us_;
  int64_t create_time_us_;

  // is original leader before daily merge
  bool is_original_leader_;
  // is previous partition leader
  bool is_previous_leader_;

  bool in_member_list_;
  ObReplicaType replica_type_;
  bool rebuild_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionReplica);
};

class ObPartitionInfo
{
  OB_UNIS_VERSION(1);

public:
  typedef common::ObIArray<ObPartitionReplica> ReplicaArray;

  ObPartitionInfo();
  virtual ~ObPartitionInfo();

  uint64_t get_tenant_id() const { return common::extract_tenant_id(table_id_); }

  uint64_t get_table_id() const { return table_id_; }
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }

  int64_t get_partition_id() const { return partition_id_; }
  void set_partition_id(const int64_t partition_id) { partition_id_ = partition_id; }

  common::ObIAllocator *get_allocator() const { return allocator_; }
  void set_allocator(common::ObIAllocator *allocator) { allocator_ = allocator; }

  void reuse() { replicas_.reuse(); }
  inline bool is_valid() const;

  const ReplicaArray &get_replicas() const { return replicas_; }
  ReplicaArray &get_replicas() { return replicas_; }
  int64_t replica_count() const { return replicas_.count(); }

  // return OB_ENTRY_NOT_EXIST for not found (set %replica to NULL too).
  int find(const common::ObAddr &server, const ObPartitionReplica *&replica) const;
  int find_leader(const ObPartitionReplica *&replica) const;

  // insert or replace replica
  virtual int add_replica(const ObPartitionReplica &replica);
  virtual int remove(const common::ObAddr &server);

  virtual int set_unit_id(const common::ObAddr &server, const uint64_t unit_id);

  virtual int filter(const ObIReplicaFilter &filter);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  int update_replica_type();
  void reset_row_checksum();
  int assign(const ObPartitionInfo &other);
private:
  // found:
  //   return OB_SUCCESS, set %idx to array index of %replicas_
  //
  // not found:
  //   return OB_ENTRY_NOT_EXIST, set %idx to OB_INVALID_INDEX
  int find_idx(const common::ObAddr &server, int64_t &idx) const;
  int find_idx(const ObPartitionReplica &replica, int64_t &idx) const;

  int verify_checksum(const ObPartitionReplica &replica) const;

private:
  uint64_t table_id_;
  int64_t partition_id_;
  common::ObIAllocator *allocator_;
  common::ObSEArray<ObPartitionReplica, ObPartitionReplica::DEFAULT_REPLICA_COUNT,
      common::ModulePageAllocator,
      common::ObArrayDefaultCallBack<ObPartitionReplica>,
      common::DefaultItemEncode<ObPartitionReplica> > replicas_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionInfo);
};

inline bool ObPartitionReplica::is_valid() const
{
  return (common::OB_INVALID_ID != common::extract_tenant_id(table_id_)
      && common::OB_INVALID_ID != table_id_
      && partition_id_ >= 0
      && partition_cnt_ > 0
      && !zone_.is_empty()
      && server_.is_valid());
}

inline bool ObPartitionReplica::is_flag_replica() const
{
  return -1 == data_version_ && !rebuild_;
}

inline bool ObPartitionInfo::is_valid() const
{
  return common::OB_INVALID_ID != table_id_ && partition_id_ >= 0;
}

} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_INFO_H_
