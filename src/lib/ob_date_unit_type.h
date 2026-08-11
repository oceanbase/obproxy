/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_COMMON_DATE_UNIT_TYPE_H_
#define _OCEANBASE_COMMON_DATE_UNIT_TYPE_H_

#ifdef __cplusplus
extern "C" {
#endif
enum ObDateUnitType
{
  /* the type of date unit */
  DATE_UNIT_MICROSECOND = 0,
  DATE_UNIT_SECOND,
  DATE_UNIT_MINUTE,
  DATE_UNIT_HOUR,
  DATE_UNIT_DAY,
  DATE_UNIT_WEEK,
  DATE_UNIT_MONTH,
  DATE_UNIT_QUARTER,
  DATE_UNIT_YEAR,
  DATE_UNIT_SECOND_MICROSECOND,
  DATE_UNIT_MINUTE_MICROSECOND,
  DATE_UNIT_MINUTE_SECOND,
  DATE_UNIT_HOUR_MICROSECOND,
  DATE_UNIT_HOUR_SECOND,
  DATE_UNIT_HOUR_MINUTE,
  DATE_UNIT_DAY_MICROSECOND,
  DATE_UNIT_DAY_SECOND,
  DATE_UNIT_DAY_MINUTE,
  DATE_UNIT_DAY_HOUR,
  DATE_UNIT_YEAR_MONTH,
  DATE_UNIT_MAX
};

const char* ob_date_unit_type_str(enum ObDateUnitType type);
const char* ob_date_unit_type_num_str(enum ObDateUnitType type);

#ifdef __cplusplus
}
#endif
#endif //_OCEANBASE_COMMON_DATE_UNIT_TYPE_H_
