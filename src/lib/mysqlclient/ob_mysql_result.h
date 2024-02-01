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

#ifndef OCEANBASE_OB_MYSQL_RESULT_H_
#define OCEANBASE_OB_MYSQL_RESULT_H_

#include "lib/string/ob_string.h"
#include "common/ob_obj_cast.h"
#include "lib/number/ob_number_v2.h"
#include "lib/hash/ob_hashmap.h"

#define COLUMN_MAP_BUCKET_NUM 107

#define EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(result, column_name, field) \
  if (OB_SUCC(ret)) \
  { \
    bool bool_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_bool(column_name, bool_value)))  \
    { \
      field = bool_value; \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      field = false; \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", K(column_name), K(ret)); \
    }\
  }

#define EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, obj) \
  if (OB_SUCC(ret)) \
  { \
    bool bool_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_bool(#column_name, bool_value)))  \
    { \
      (obj).set_##column_name(bool_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      (obj).set_##column_name(false); \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }


#define EXTRACT_BOOL_FIELD_MYSQL(result, column_name, field) \
  if (OB_SUCC(ret)) \
  { \
    bool bool_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_bool(column_name, bool_value)))  \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", K(column_name), K(ret)); \
    } \
    else \
    { \
      field = bool_value; \
    }\
  }

#define EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_int(column_name, int_value))) \
    { \
      field = static_cast<type>(int_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      field = static_cast<type>(0); \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", K(column_name), K(ret)); \
    } \
  }

#define EXTRACT_INT_FIELD_MYSQL(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_int(column_name, int_value)))  \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } \
    else \
    { \
      field = static_cast<type>(int_value); \
    }\
  }


#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_int(#column_name, int_value)))  \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(static_cast<type>(int_value)); \
    }\
  }

#define EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    uint64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_uint(#column_name, int_value)))  \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(static_cast<type>(int_value)); \
    }\
  }

#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_int(#column_name, int_value)))  \
    { \
      (obj).set_##column_name(static_cast<type>(int_value)); \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      (obj).set_##column_name(static_cast<type>(0)); \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }


#define EXTRACT_DOUBLE_FIELD_MYSQL_SKIP_RET(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    double double_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_double(column_name, double_value))) \
    { \
      field = static_cast<type>(double_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      field = static_cast<type>(0); \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", K(column_name), K(ret)); \
    } \
  }

#define EXTRACT_DOUBLE_FIELD_MYSQL(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    double double_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_double(column_name, double_value)))  \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } \
    else \
    { \
      field = static_cast<type>(double_value); \
    }\
  }


#define EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    double double_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_double(#column_name, double_value)))  \
    { \
      SQL_LOG(WDIAG, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(static_cast<type>(double_value)); \
    }\
  }


#define EXTRACT_VARCHAR_FIELD_MYSQL(result, column_name, field) \
  if (OB_SUCC(ret)) \
  { \
    if (OB_SUCCESS != (ret = (result).get_varchar(column_name, field))) \
    { \
      SQL_LOG(WDIAG, "fail to get varchar. ", K(ret)); \
    } \
  }

#define EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, column_name, field) \
  if (OB_SUCC(ret)) \
  { \
    if (OB_SUCCESS != (ret = (result).get_varchar(column_name, field))) \
    { \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
      { \
        ret = OB_SUCCESS; \
      } \
      else \
      { \
        SQL_LOG(WDIAG, "get varchar failed", K(ret)); \
      } \
    } \
  }

#define EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(result, column_name, field, max_length, real_length) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(column_name, str_value))) \
    { \
      if(str_value.length() >= max_length) \
      { \
        ret = OB_SIZE_OVERFLOW; \
        SQL_LOG(WDIAG, "field max length is not enough:", \
                  "max length", max_length, "str length", str_value.length()); \
      } \
      else \
      { \
        MEMCPY(field, str_value.ptr(), str_value.length()); \
        real_length = str_value.length(); \
        field[str_value.length()] = '\0'; \
      } \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      real_length = 0; \
      field[0] = '\0'; \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to extract strbuf field mysql. ", K(ret)); \
    } \
  }


#define EXTRACT_STRBUF_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, class_obj, max_length) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) \
    { \
      if(str_value.length() >= max_length) \
      { \
        ret = OB_SIZE_OVERFLOW; \
        SQL_LOG(WDIAG, "field max length is not enough:", \
                  "max length", max_length, "str length", str_value.length()); \
      } \
      else \
      { \
        ret = (class_obj).set_##column_name(str_value); \
      } \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to extract strbuf field mysql. ", K(ret)); \
    } \
  }


#define EXTRACT_STRBUF_FIELD_TO_CLASS_MYSQL(result, column_name, class_obj, max_length) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) \
    { \
      if(str_value.length() > max_length) \
      { \
        ret = OB_SIZE_OVERFLOW; \
        SQL_LOG(WDIAG, "field max length is not enough:", \
                  "max length", max_length, "str length", str_value.length()); \
      } \
      else \
      { \
        ret = (class_obj).set_##column_name(str_value); \
      } \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to extract strbuf field mysql. ", K(ret)); \
    } \
  }


#define EXTRACT_STRBUF_FIELD_MYSQL(result, column_name, field, max_length, real_length) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(column_name, str_value))) \
    { \
      if(str_value.length() >= max_length) \
      { \
        ret = OB_SIZE_OVERFLOW; \
        SQL_LOG(WDIAG, "field max length is not enough:", \
                  "max length", max_length, "str length", str_value.length()); \
      } \
      else \
      { \
        MEMCPY(field, str_value.ptr(), str_value.length()); \
        real_length = str_value.length(); \
        field[str_value.length()] = '\0'; \
      } \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to extract strbuf field mysql. ", K(ret)); \
    } \
  }


#define EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, column_name, data_type, class_obj) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    ObObj res_obj; \
    ret = (result).get_varchar(#column_name, str_value); \
    \
    if (OB_ERR_NULL_VALUE == ret) \
    { /* without default value */ \
      res_obj.set_null(); \
      ret = (class_obj).set_##column_name(res_obj); \
    } \
    else if (OB_SUCC(ret)) \
    { \
      if (IS_DEFAULT_NOW_STR(data_type, str_value)) \
      { \
        res_obj.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG); \
        ret = (class_obj).set_##column_name(res_obj); \
      } \
      else if (ObVarcharType == data_type || ObCharType == data_type) \
      { \
        res_obj.set_string(data_type, str_value); \
        res_obj.meta_.set_collation_type(column.get_collation_type());  \
        /* will override the collaction level set in set_varchar */ \
        res_obj.meta_.set_collation_level(column.get_meta_type().get_collation_level()); \
        ret = (class_obj).set_##column_name(res_obj); \
      } \
      else \
      { \
        ObObj def_obj; \
        char buffer[OB_CAST_TO_VARCHAR_MAX_LENGTH]; \
        int64_t used_buf_len = 0; \
        def_obj.set_varchar(str_value); \
        const ObObj *res_cell = NULL; \
        ObObjCaster<FromBuffer> caster(buffer, OB_CAST_TO_VARCHAR_MAX_LENGTH, used_buf_len); \
        if (OB_SUCCESS != (ret = caster.obj_cast(def_obj, data_type, NULL, res_obj, res_cell))) \
        { \
          SQL_LOG(WDIAG, "cast obj failed, ", "src type", def_obj.get_type(), "dest type", data_type); \
        } \
        else \
        { \
          ret = (class_obj).set_##column_name(*res_cell); \
        } \
      } \
    } \
    else \
    { \
      SQL_LOG(WDIAG, "fail to default value field mysql. ", K(ret)); \
    } \
  }

#define EXTRACT_CREATE_TIME_FIELD_MYSQL(result, column_name, field, type) \
  EXTRACT_INT_FIELD_MYSQL(result, column_name, field, type)

#define EXTRACT_MODIFY_TIME_FIELD_MYSQL(result, column_name, field, type) \
  EXTRACT_INT_FIELD_MYSQL(result, column_name, field, type)

#define GET_COL_IGNORE_NULL(func, ...)                  \
  ({                                                    \
    {                                                   \
      if (OB_SUCC(ret)) {                          \
        if (OB_SUCCESS != (ret = func(__VA_ARGS__))) {  \
          if (OB_ERR_NULL_VALUE == ret) {               \
            ret = OB_SUCCESS;                           \
          } else {                                      \
            LOG_WDIAG("get column failed", K(ret));      \
          }                                             \
        }                                               \
      }                                                 \
    }                                                   \
    ret;                                                \
  })


namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObMySQLResult
{
public:
  //see this for template virtual function
  //http://cxh.me/2014/07/01/nvi-usage-of-virtual-template/
  DEFINE_ALLOCATOR_WRAPPER
  ObMySQLResult();
  virtual ~ObMySQLResult();
  //virtual int64_t get_row_count(void) const = 0;
  //virtual int64_t get_column_count(void) const = 0;
  /*
   * move result cursor to next row
   */
  virtual int close() = 0;
  virtual int next() = 0;

  //debug function
  virtual int print_info() const;
  /*
   * read int/str/TODO from result set
   * col_idx: indicate which column to read, [0, max_read_col)
   */
  virtual int get_int(const int64_t col_idx, int64_t &int_val) const = 0;
  virtual int get_uint(const int64_t col_idx, uint64_t &int_val) const = 0;
  virtual int get_datetime(const int64_t col_idx, int64_t &datetime) const = 0;
  virtual int get_bool(const int64_t col_idx, bool &bool_val) const = 0;
  virtual int get_varchar(const int64_t col_idx, common::ObString &varchar_val) const = 0;
  virtual int get_float(const int64_t col_idx, float &float_val) const = 0;
  virtual int get_double(const int64_t col_idx, double &double_val) const = 0;

  template <class Allocator>
  int get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                 Allocator &allocator) const;
  /*
  * read int/str/TODO from result set
  * col_name: indicate which column to read
  * @return  OB_INVALID_PARAM if col_name does not exsit
  */
  virtual int get_int(const char *col_name, int64_t &int_val) const = 0;
  virtual int get_uint(const char *col_name, uint64_t &int_val) const = 0;
  virtual int get_datetime(const char *col_name, int64_t &datetime) const = 0;
  virtual int get_bool(const char *col_name, bool &bool_val) const = 0;
  virtual int get_varchar(const char *col_name, common::ObString &varchar_val) const = 0;
  virtual int get_float(const char *col_name, float &float_val) const = 0;
  virtual int get_double(const char *col_name, double &double_val) const = 0;
  template <class Allocator>
  int get_number(const char *col_name, common::number::ObNumber &nmb_val, Allocator &allocator) const;

protected:
  static const int64_t FAKE_TABLE_ID = 1;
  int varchar2datetime(const ObString &varchar, int64_t &datetime) const;
private:
  virtual int get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                          IAllocator &allocator) const = 0;
  virtual int get_number(const char *col_name, common::number::ObNumber &nmb_val,
                          IAllocator &allocator) const = 0;
};

template <class Allocator>
int ObMySQLResult::get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                              Allocator &allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return get_number_(col_idx, nmb_val, ta);
}

template <class Allocator>
int ObMySQLResult::get_number(const char *col_name, common::number::ObNumber &nmb_val,
                              Allocator &allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return get_number_(col_name, nmb_val, ta);
}
}
}
}
#endif //OCEANBASE_OB_MYSQL_RESULT_H_
