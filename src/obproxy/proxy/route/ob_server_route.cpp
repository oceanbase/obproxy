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

#include "proxy/route/ob_server_route.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int64_t ObServerRoute::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("consistency_level", get_consistency_level_str(consistency_level_),
       "cur_chosen_route_type", get_route_type_string(cur_chosen_route_type_),
       K_(cur_chosen_server),
       K_(cur_chosen_pl),
       K_(valid_count),
       K_(leader_item),
       K_(ldc_route),
       K_(is_table_entry_from_remote),
       K_(is_part_entry_from_remote),
       KP_(table_entry),
       KP_(part_entry),
       KP_(dummy_entry));
  J_OBJ_END();

  return pos;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
