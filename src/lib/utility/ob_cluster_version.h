/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_CLUSTER_VERSION
#define OCEANBASE_SHARE_OB_CLUSTER_VERSION
#include <stdint.h>

namespace oceanbase
{
namespace common
{
#define OB_VSN_MAJOR_SHIFT 32
#define OB_VSN_MINOR_SHIFT 16
#define OB_VSN_MAJOR_PATCH_SHIFT 8
#define OB_VSN_MINOR_PATCH_SHIFT 0
#define OB_VSN_MAJOR_MASK 0xffffffff
#define OB_VSN_MINOR_MASK 0xffff
#define OB_VSN_MAJOR_PATCH_MASK 0xff
#define OB_VSN_MINOR_PATCH_MASK 0xff
#define OB_VSN_MAJOR(version) (static_cast<const uint32_t>((version >> OB_VSN_MAJOR_SHIFT) & OB_VSN_MAJOR_MASK))
#define OB_VSN_MINOR(version) (static_cast<const uint16_t>((version >> OB_VSN_MINOR_SHIFT) & OB_VSN_MINOR_MASK))
#define OB_VSN_MAJOR_PATCH(version) (static_cast<const uint8_t>((version >> OB_VSN_MAJOR_PATCH_SHIFT) & OB_VSN_MAJOR_PATCH_MASK))
#define OB_VSN_MINOR_PATCH(version) (static_cast<const uint8_t>(version & OB_VSN_MINOR_PATCH_MASK))

#define CALC_VERSION(major, minor, major_patch, minor_patch) \
        (((major) << OB_VSN_MAJOR_SHIFT) + \
         ((minor) << OB_VSN_MINOR_SHIFT) + \
         ((major_patch) << OB_VSN_MAJOR_PATCH_SHIFT) + \
         ((minor_patch)))
constexpr static inline uint64_t
cal_version(const uint64_t major, const uint64_t minor, const uint64_t major_patch, const uint64_t minor_patch)
{
  return CALC_VERSION(major, minor, major_patch, minor_patch);
}

#define DEF_MAJOR_VERSION 1
#define DEF_MINOR_VERSION 4
#define DEF_MAJOR_PATCH_VERSION 40
#define DEF_MINOR_PATCH_VERSION 0

#define CLUSTER_VERSION_2_1_0_0 (oceanbase::common::cal_version(2, 1, 0, 0))
#define CLUSTER_VERSION_4_0_0_0 (oceanbase::common::cal_version(4, 0, 0, 0))
#define CLUSTER_VERSION_4_3_5_2 (oceanbase::common::cal_version(4, 3, 5, 2))

// 仅用于判断一级分区为空结果集时，是不是按照错误进行返回的报错
#define IS_CLUSTER_VERSION_LESS_THAN_V2_1_0_0(version) (version < CLUSTER_VERSION_2_1_0_0)

#define IS_CLUSTER_VERSION_LESS_THAN_V4(version) (version < CLUSTER_VERSION_4_0_0_0)
// 仅用于判断获取路由的sql, observer 4.3.5.2新增字段rpc_port 以及schema_version
#define IS_CLUSTER_VERSION_BEFORE_4_3_5_2(version) (version < CLUSTER_VERSION_4_3_5_2)

} // end of namespace common
} // end of namespace oceanbase

#endif /* OCEABASE_SHARE_OB_CLUSTER_VERSION*/
