/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ROWKEY_HELPER_H_
#define OB_ROWKEY_HELPER_H_
#include "lib/string/ob_string.h"
#include "lib/utility/serialization.h"
#include "common/ob_object.h"
#include "common/ob_rowkey_info.h"

namespace oceanbase
{
namespace common
{
class ObRowkeyHelper
{
public:
  static int prepare_obj_array(const ObRowkeyInfo &info, ObObj *array, const int64_t size);
  /**
   * convert rowkey from ObObj array to ObString
   * @param array[in]         obobj array
   * @param size[in]          array size
   * @param key[out]          rowkey
   *
   * @return int              return OB_SUCCESS if convert success, otherwise return OB_ERROR
   */
  static int get_row_key(const ObObj *array, const int64_t size, ObString &key);

  /**
   * convert rowkey from ObString to ObObj array
   * @param key[in]           rowkey to convert
   * @param array[out]        array to store ObObj
   * @param size[in/out]         ObObj array size
   *
   * @return int              return OB_SUCCESS if convert success, otherwist return OB_ERROR
   */
  static int get_obj_array(const ObString &key, ObObj *array, const int64_t size);

  static int binary_rowkey_to_obj_array(const ObRowkeyInfo &info,
                                        const ObString &binary_key, ObObj *array, const int64_t size);
  static int obj_array_to_binary_rowkey(const ObRowkeyInfo &info,
                                        ObString &binary_key, const ObObj *array, const int64_t size);

private:
  static int serialize_obj(const ObObj *obj, char *buf, const int64_t data_len, int64_t &pos);
  static int deserialize_obj(ObObj *obj, const char *buf, const int64_t data_len, int64_t &pos);
};
}
}
#endif
