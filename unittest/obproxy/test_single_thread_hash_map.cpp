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
#define USING_LOG_PREFIX PROXY
#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/hash/ob_dynamic_build_in_hashmap.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/hash/ob_hashmap.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_hrtime.h"

using namespace oceanbase::common;
using namespace hash;
namespace oceanbase
{
namespace obproxy
{
class ObItem
{
public:
  int64_t id_;
  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(id));
    J_OBJ_END();
    return pos;
  }
  LINK(ObItem, item_link_);
};

struct ObItemHashing
{
  typedef const int64_t &Key;
  typedef ObItem Value;
  typedef ObDLList(ObItem, item_link_) ListHead;

  static uint64_t hash(Key key) { return static_cast<uint64_t>(key); }
  static Key key(Value const *value) { return value->id_; }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

static const int64_t BUCKET_NUM = 16;

typedef common::hash::ObBuildInHashMap<ObItemHashing, BUCKET_NUM> ObItemMap_V1;
typedef common::hash::ObDynamicBuildInHashMap<ObItemHashing, BUCKET_NUM> ObItemMap_V2;
typedef common::hash::ObHashMap<int64_t, ObItem> ObItemMap_V3;
typedef common::hash::ObHashMap<int64_t, ObItem, NoPthreadDefendMode> ObItemMap_V4;

void *test(int64_t item_count) {
  int ret = OB_SUCCESS;

  ObItemMap_V1 map_v1;
  ObItemMap_V2 map_v2;
  ObItemMap_V3 map_v3;
  ObItemMap_V4 map_v4;

  ObSEArray<ObItem*, 10> store_array;
  if (OB_FAIL(map_v3.create(BUCKET_NUM, ObModIds::OB_HASH_NODE, ObModIds::OB_HASH_NODE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc", K(ret));
  } else if (OB_FAIL(map_v4.create(BUCKET_NUM, ObModIds::OB_HASH_NODE, ObModIds::OB_HASH_NODE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc", K(ret));
  } else if (OB_FAIL(store_array.prepare_allocate(item_count))) {
    LOG_WDIAG("fail to alloc", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < item_count; ++i) {
      ObItem* item = NULL;
      if (OB_ISNULL(item = op_alloc(ObItem))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc", K(ret));
      } else if (FALSE_IT(item->id_ = i)) {
        // impossible
      } else {
        store_array.at(i) = item;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObHRTime start = 0, end = 0;
    ObHRTime insert_time = 0;
    ObHRTime select_time = 0;
    ObHRTime random_select_time = 0;
    ObHRTime erase_time = 0;

    {
      // map_v1
      /******insert**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v1.unique_set(store_array.at(i));
        // LOG_INFO("map info", K(map_v1));
      }
      end = get_hrtime_internal();
      insert_time = hrtime_to_usec(end - start);

      /******select**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v1.get(i);
      }
      end = get_hrtime_internal();
      select_time = hrtime_to_usec(end - start);

      /******random select**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        int64_t random_num = 0;
        ObRandomNumUtils::get_random_num(0, item_count, random_num);
        map_v1.get(random_num);
      }
      end = get_hrtime_internal();
      random_select_time = hrtime_to_usec(end - start);

      /******remove**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v1.remove(store_array.at(i));
      }
      end = get_hrtime_internal();
      erase_time = hrtime_to_usec(end - start);
      LOG_INFO("map_v1 stat", K(insert_time), K(select_time),
               K(random_select_time), K(erase_time));
    }

    {
      // map_v2
      /******insert**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v2.unique_set(store_array.at(i));
      }
      end = get_hrtime_internal();
      insert_time = hrtime_to_usec(end - start);

      /******select**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v2.get(i);
      }
      end = get_hrtime_internal();
      select_time = hrtime_to_usec(end - start);

      /******random select**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        int64_t random_num = 0;
        ObRandomNumUtils::get_random_num(0, item_count, random_num);
        map_v2.get(random_num);
      }
      end = get_hrtime_internal();
      random_select_time = hrtime_to_usec(end - start);

      /******remove**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v2.remove(store_array.at(i));
      }
      end = get_hrtime_internal();
      erase_time = hrtime_to_usec(end - start);
      LOG_INFO("map_v2 stat", K(insert_time), K(select_time),
               K(random_select_time), K(erase_time));
    }

    {
      // map_v3
      /******insert**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v3.set_refactored(i, *store_array.at(i));
      }
      end = get_hrtime_internal();
      insert_time = hrtime_to_usec(end - start);

      /******select**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v3.get(i);
      }
      end = get_hrtime_internal();
      select_time = hrtime_to_usec(end - start);

      /******random select**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        int64_t random_num = 0;
        ObRandomNumUtils::get_random_num(0, item_count, random_num);
        map_v3.get(random_num);
      }
      end = get_hrtime_internal();
      random_select_time = hrtime_to_usec(end - start);

      /******remove**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v3.erase_refactored(store_array.at(i)->id_);
      }
      end = get_hrtime_internal();
      erase_time = hrtime_to_usec(end - start);
      LOG_INFO("map_v3 stat", K(insert_time), K(select_time),
               K(random_select_time), K(erase_time));
    }

    {
      // map_v4
      /******insert**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v4.set_refactored(i, *store_array.at(i));
      }
      end = get_hrtime_internal();
      insert_time = hrtime_to_usec(end - start);

      /******select**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v4.get(i);
      }
      end = get_hrtime_internal();
      select_time = hrtime_to_usec(end - start);

      /******random select**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        int64_t random_num = 0;
        ObRandomNumUtils::get_random_num(0, item_count, random_num);
        map_v4.get(random_num);
      }
      end = get_hrtime_internal();
      random_select_time = hrtime_to_usec(end - start);

      /******remove**** */
      start = get_hrtime_internal();
      for (int64_t i = 0; i < item_count; ++i) {
        map_v4.erase_refactored(store_array.at(i)->id_);
      }
      end = get_hrtime_internal();
      erase_time = hrtime_to_usec(end - start);
      LOG_INFO("map_v4 stat", K(insert_time), K(select_time),
               K(random_select_time), K(erase_time));
    }
  }

  return NULL;
}

} // end of obproxy
} // end of oceanbase

int main(int argc, char **argv) {
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  int64_t item_count = 100000;
  // unittest stack info is error
  // init_proc_map_info();
  // LOG_INFO("stack", K(lbt()));
  if (argc > 1) {
    item_count = atoll(argv[1]);
  }

  oceanbase::obproxy::test(item_count);

  return 0;
}