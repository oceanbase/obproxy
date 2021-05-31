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

#ifndef OBPROXY_TABLE_PROCESSOR_H
#define OBPROXY_TABLE_PROCESSOR_H

#include "proxy/route/ob_table_entry_cont.h"

#define TABLE_ENTRY_LOOKUP_CACHE_EVENT (ROUTE_EVENT_EVENTS_START + 1)
#define TABLE_ENTRY_LOOKUP_REMOTE_EVENT (ROUTE_EVENT_EVENTS_START + 2)
#define TABLE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT (ROUTE_EVENT_EVENTS_START + 3)
#define TABLE_ENTRY_NOTIFY_CALLER_EVENT (ROUTE_EVENT_EVENTS_START + 4)

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
}
namespace proxy
{
class ObTableCache;
class ObTableProcessor
{
public:
  ObTableProcessor() : is_inited_(false), table_cache_(NULL) {}
  ~ObTableProcessor() {}

  int init(ObTableCache *table_cache);
  int get_table_entry(ObTableRouteParam &table_param, event::ObAction *&action);

  static int get_table_entry_from_global_cache(ObTableRouteParam &table_param,
                                               ObTableCache &table_cache,
                                               ObTableEntryCont *te_cont, event::ObAction *&action,
                                               ObTableEntry *&entry, ObTableEntryLookupOp &op);
  DECLARE_TO_STRING;

private:
  static int add_table_entry_with_rslist(ObTableRouteParam &table_param, ObTableEntry *&entry,
                                         const bool is_old_entry_from_rslist);
  static int get_table_entry_from_rslist(ObTableRouteParam &table_param, ObTableEntry *&entry,
                                         ObTableEntryLookupOp &op,
                                         const bool is_old_entry_from_rslist);
  static int get_table_entry_from_thread_cache(ObTableRouteParam &table_param,
                                               ObTableCache &table_cache,
                                               ObTableEntry *&entry);


  static int handle_lookup_global_cache_done(ObTableRouteParam &table_param,
                                             ObTableCache &table_cache,
                                             ObTableEntry *entry, event::ObAction *&action,
                                             const ObTableEntryLookupOp &op);

  static int add_building_state_table_entry(const ObTableRouteParam &table_param,
                                            ObTableCache &table_cache, ObTableEntry *&entry);

private:
  bool is_inited_;
  ObTableCache *table_cache_;

  DISALLOW_COPY_AND_ASSIGN(ObTableProcessor);
};

extern ObTableProcessor &get_global_table_processor();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_TABLE_PROCESSOR_H
