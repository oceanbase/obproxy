/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_ZONE_TYPE_H_
#define OCEANBASE_COMMON_OB_ZONE_TYPE_H_

namespace oceanbase
{
namespace common
{
enum ObZoneType
{
  ZONE_TYPE_READWRITE = 0,
  ZONE_TYPE_READONLY = 1,
  ZONE_TYPE_ENCRYPTION = 2,
  ZONE_TYPE_INVALID = 3,
};

const char *zone_type_to_str(ObZoneType zone_type);
ObZoneType str_to_zone_type(const char *zone_type_str);

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_ZONE_TYPE_H_
