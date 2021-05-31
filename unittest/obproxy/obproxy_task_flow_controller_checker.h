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

#define DEFAULT_TABLE_COUNT 10000
#define DEFAULT_QUERY_PER_SECOND 1000
#define DEFAULT_MERGING_LAST_TIME 1 * 1000 * 1000
#define DEFAULT_MERGING_INTERVAL  5 * 1000 * 1000
#define ONE_SECOND 1 * 1000 * 1000

#include "proxy/route/ob_table_entry.h"
namespace oceanbase
{
namespace obproxy
{
namespace test
{
bool is_exit = false;

// Server
struct MockServer
{
  MockServer() :
                 merging_time_(DEFAULT_MERGING_LAST_TIME),
                 merging_interval_(DEFAULT_MERGING_INTERVAL),
                 version_(0)
  {}

  unsigned int merging_time_;
  unsigned int merging_interval_;
  int64_t version_;
};
MockServer g_mock_server;

struct MockTableLocation;
struct MockWorker
{
  MockWorker(int64_t qps, MockTableLocation &table_location) :
      query_per_second_(qps),
      table_location_(table_location)
  {}

  int64_t query_per_second_;
  MockTableLocation &table_location_;
};

// Location
enum MockTableEntryState
{
  AVAIL,
  DIRTY
};

struct MockTableEntry
{
MockTableEntry() : version_(0) {}
MockTableEntryState state_;
int64_t version_;
};

class MockTableLocation
{
public:
  MockTableLocation(int64_t table_count = DEFAULT_TABLE_COUNT);
  ~MockTableLocation();
  int64_t get_table_count() const { return table_count_; }
  void handle_request(const int64_t table_idx);
  void handle_responce(const int64_t table_idx);
private:
  int64_t table_count_;
  MockTableEntry *entrys_;
};
MockTableLocation g_table_location;

// Stat
struct PartitionLocationStat
{
  PartitionLocationStat() : cur_request_count_(0),
                            cur_pl_miss_count_(0),
                            cur_pl_update_delivery_count_(0),
                            cur_dirty_count_(0),
                            total_dirty_count_(0)
  {}

  int64_t cur_request_count_;
  int64_t cur_pl_miss_count_;
  int64_t cur_pl_update_delivery_count_;
  int64_t cur_dirty_count_;

  int64_t total_dirty_count_;
};
PartitionLocationStat g_pl_stat;

} // namespace of test
} // namespace of obproxy
} // namespace of oceanbasse
