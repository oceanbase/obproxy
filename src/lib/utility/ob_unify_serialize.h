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
 *
 * *************************************************************
 *
 * This file defines unify serialize and deserialize macros with which
 * you can create a serializable structure easily.
 *
 * This is a new way for decoding/encoding an object that try to solve
 * compatibility problem gracefully, both for backward and forward
 * compatibility.
 *
 * There are three compatible levels, Full, Half and None. Full
 * compatibility means the mechanism guarantees protocol compatibility
 * so that you can use it without extra thinking to achieve the
 * goal. Half compatibility means the object's compatibility is your
 * responsibility, but this mechanism will solve others. No
 * compatibility interfaces mean that not only this object's
 * compatibility should been considered but also this object's
 * boundary must been dealt with well, otherwise the compatibility of
 * object contains this object will be broken.
 *
 * Full compatible interfaces:
 *
 *     OB_SERIALIZE_MEMBER
 *     OB_SERIALIZE_MEMBER_TEMP
 *
 * Half compatible interfaces:
 *
 *     OB_DEF_SERIALIZE
 *     OB_DEF_DESERIALIZE
 *     OB_DEF_SERIALIZE_SIZE
 *
 * No compatibility interfaces:
 *
 *     OB_SERIALIZE_MEMBER_INHERIT
 *     OB_SERIALIZE_MEMBER_SIMPLE
 */

#ifndef _OCEABASE_LIB_UTILITY_OB_UNIFY_SERIALIZE_H_
#define _OCEABASE_LIB_UTILITY_OB_UNIFY_SERIALIZE_H_

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/serialization.h"
#include "lib/oblog/ob_log.h"

#define SERIAL_PARAMS char *buf, const int64_t buf_len, int64_t &pos
#define DESERIAL_PARAMS const char *buf, const int64_t data_len, int64_t &pos

#define UNF_UNUSED_SER ({(void)buf; (void)buf_len; (void)pos;})
#define UNF_UNUSED_DES ({(void)buf; (void)data_len; (void)pos;})

#ifndef RPC_WARN
#define RPC_WARN(...) OB_LOG(WDIAG, __VA_ARGS__)
#endif

///
// define essential macros used for encode/decode single object
//----------------------------------------------------------------------
#define NS_ ::oceanbase::common::serialization
#define OK_ ::oceanbase::common::OB_SUCCESS

#define OB_UNIS_ENCODE(obj)                                             \
  if (OB_SUCC(ret)) {                                                   \
    if (OB_FAIL(NS_::encode(buf, buf_len, pos, obj))) {                 \
      RPC_WARN("encode object fail",                                    \
               "name", MSTR(obj), K(buf_len), K(pos), K(ret));          \
    }                                                                   \
  }

#define OB_UNIS_DECODEx(obj)                                            \
  if (OB_SUCC(ret)) {                                                   \
    if (OB_FAIL(NS_::decode(buf, data_len, pos, obj))) {                \
      RPC_WARN("decode object fail",                                    \
               "name", MSTR(obj), K(data_len), K(pos), K(ret));         \
    }                                                                   \
  }

#define OB_UNIS_DECODE(obj)                                             \
  if (OB_SUCC(ret) && pos < data_len) {                                 \
    if (OB_FAIL(NS_::decode(buf, data_len, pos, obj))) {                \
      RPC_WARN("decode object fail",                                    \
               "name", MSTR(obj), K(data_len), K(pos), K(ret));         \
    }                                                                   \
  }

#define OB_UNIS_ADD_LEN(obj)                                            \
  len += NS_::encoded_length(obj)
//-----------------------------------------------------------------------

/// utility macros to deal with C native array
#define OB_UNIS_ENCODE_ARRAY(objs, objs_count)                          \
  OB_UNIS_ENCODE((objs_count));                                         \
  for (int64_t i = 0; OB_SUCC(ret) && i < (objs_count); ++i) {          \
    OB_UNIS_ENCODE(objs[i]);                                            \
  }

#define OB_UNIS_ADD_LEN_ARRAY(objs, objs_count)                         \
  OB_UNIS_ADD_LEN((objs_count));                                        \
  for (int64_t i = 0; i < (objs_count); ++i) {                          \
    OB_UNIS_ADD_LEN(objs[i]);                                           \
  }

#define OB_UNIS_DECODE_ARRAY(objs, objs_count)                          \
  for (int64_t i = 0; OB_SUCC(ret) && i < (objs_count); ++i) {          \
    OB_UNIS_DECODE(objs[i]);                                            \
  }

///
// define macros deal with parent class
//-----------------------------------------------------------------------
#define UNF_CONCAT_(a, b) a##b
#define UNF_CONCAT(a, b) UNF_CONCAT_(a, b)
#define UNF_IGNORE(...)
#define UNF_uSELF(...) __VA_ARGS__
#define UNF_SAFE_DO(M)                                                  \
  do {                                                                  \
    if (OB_SUCC(ret)) {                                                 \
      if (OB_FAIL((M))) {                                               \
        RPC_WARN("fail to execute: " #M, K(ret));                       \
      }                                                                 \
    }                                                                   \
  } while (0)

#define UNF_MYCLS_ UNF_uSELF(
#define MYCLS_(D, B) uSELF( D
#define UNF_MYCLS(x) UNF_CONCAT(UNF_, MYCLS_ x) )

#define UNF_SBASE_ UNF_IGNORE(
#define SBASE_(D, B)                                                    \
  uSELF( UNF_SAFE_DO(B::serialize(buf, buf_len, pos))
#define BASE_SER(x) UNF_CONCAT(UNF_, SBASE_ x) )

#define UNF_DBASE_ UNF_IGNORE(
#define DBASE_(D, B)                                                    \
  uSELF( UNF_SAFE_DO(B::deserialize(buf, data_len, pos))
#define BASE_DESER(x) UNF_CONCAT(UNF_, DBASE_ x) )

#define UNF_LBASE_ UNF_IGNORE(
#define LBASE_(D, B) uSELF( len += B::get_serialize_size()
#define BASE_ADD_LEN(x) UNF_CONCAT(UNF_, LBASE_ x) )

///
// define serialize/desrialize wrapper which helps hide "version" and
// "length"
//-----------------------------------------------------------------------
#define CHECK_VERSION_LENGTH(CLS, VER, LEN)                             \
  if (OB_SUCC(ret)) {                                                   \
    if (VER != UNIS_VERSION) {                                          \
      ret = ::oceanbase::common::OB_NOT_SUPPORTED;                      \
      RPC_WARN("object version mismatch", "cls", #CLS, K(ret), K(VER)); \
    } else if (LEN < 0) {                                               \
      ret = ::oceanbase::common::OB_ERR_UNEXPECTED;                     \
      RPC_WARN("can't decode object with negative length", K(LEN));     \
    } else if (data_len < LEN + pos) {                                  \
      ret = ::oceanbase::common::OB_DESERIALIZE_ERROR;                  \
      RPC_WARN("buf length not enough", K(LEN), K(pos), K(data_len));   \
    }                                                                   \
  }

#define CALL_SERIALIZE_()                                               \
  if (OB_SUCC(ret)) {                                                   \
    if (OB_FAIL(serialize_(buf, buf_len, pos))) {                       \
      RPC_WARN("serialize fail", K(ret));                               \
    }                                                                   \
  }

#define CALL_DESERIALIZE_(SLEN)                                         \
  if (OB_SUCC(ret)) {                                                   \
    int64_t pos_orig = pos;                                             \
    pos = 0;                                                            \
    if (OB_FAIL(deserialize_(buf + pos_orig, SLEN, pos))) {             \
      RPC_WARN("deserialize_ fail",                                     \
               "slen", SLEN, K(pos), K(ret));                           \
    }                                                                   \
    pos = pos_orig + SLEN;                                              \
  }

#define SERIALIZE_HEADER(version, len)                                  \
  if (OB_SUCC(ret)) {                                                   \
    OB_UNIS_ENCODE(version);                                            \
    OB_UNIS_ENCODE(len);                                                \
  }

#define OB_UNIS_SERIALIZE(CLS)                                          \
  int CLS::serialize(SERIAL_PARAMS) const                               \
  {                                                                     \
    int ret = OK_;                                                      \
    int64_t len = get_serialize_size_();                                \
    SERIALIZE_HEADER(UNIS_VERSION, len);                                \
    CALL_SERIALIZE_();                                                  \
    return ret;                                                         \
  }

#define DESERIALIZE_HEADER(CLS, version, len)                           \
  if (OB_SUCC(ret)) {                                                   \
    OB_UNIS_DECODEx(version);                                           \
    OB_UNIS_DECODEx(len);                                               \
    CHECK_VERSION_LENGTH(CLS, version, len);                            \
  }

#define OB_UNIS_DESERIALIZE(CLS)                                        \
  int CLS::deserialize(DESERIAL_PARAMS)                                 \
  {                                                                     \
    int ret = OK_;                                                      \
    int64_t version = 0;                                                \
    int64_t len = 0;                                                    \
    DESERIALIZE_HEADER(CLS, version, len);                              \
    CALL_DESERIALIZE_(len);                                             \
    return ret;                                                         \
  }

#define SERIALIZE_SIZE_HEADER(version, len)                             \
    OB_UNIS_ADD_LEN(len);                                               \
    OB_UNIS_ADD_LEN(UNIS_VERSION);                                      \

#define OB_UNIS_SERIALIZE_SIZE(CLS)                                     \
  int64_t CLS::get_serialize_size(void) const                           \
  {                                                                     \
    int64_t len = get_serialize_size_();                                \
    SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);                           \
    return len;                                                         \
  }

//-----------------------------------------------------------------------

///
// macro to declare unis structure, here would be non-implement
// functions in pure class but it's ok coz they wouldn't be invoked
// and the derived class should overwrite these functions.
// -----------------------------------------------------------------------
#define OB_DECLARE_UNIS(VIR,PURE)                                       \
  VIR int serialize(SERIAL_PARAMS) const PURE;                          \
  int serialize_(SERIAL_PARAMS) const;                                  \
  VIR int deserialize(DESERIAL_PARAMS) PURE;                            \
  int deserialize_(DESERIAL_PARAMS);                                    \
  VIR int64_t get_serialize_size() const PURE;                          \
  int64_t get_serialize_size_() const

//-----------------------------------------------------------------------

///
// public entries, define interfaces of manual serialization
//-----------------------------------------------------------------------
#define OB_UNIS_VERSION(VER)                                            \
  public: OB_DECLARE_UNIS(,);                                           \
  private:                                                              \
    const static int64_t UNIS_VERSION = VER

#define OB_UNIS_VERSION_V(VER)                                          \
  public: OB_DECLARE_UNIS(virtual,);                                    \
  private:                                                              \
    const static int64_t UNIS_VERSION = VER

#define OB_UNIS_VERSION_PV()                                            \
  public: OB_DECLARE_UNIS(virtual,=0); private:

#define OB_DEF_SERIALIZE(CLS, TEMP...)                                  \
  TEMP OB_UNIS_SERIALIZE(CLS)                                           \
  TEMP int CLS::serialize_(SERIAL_PARAMS) const

#define OB_DEF_DESERIALIZE(CLS, TEMP...)                                \
  TEMP OB_UNIS_DESERIALIZE(CLS)                                         \
  TEMP int CLS::deserialize_(DESERIAL_PARAMS)

#define OB_DEF_SERIALIZE_SIZE(CLS, TEMP...)                             \
  TEMP OB_UNIS_SERIALIZE_SIZE(CLS)                                      \
  TEMP int64_t CLS::get_serialize_size_(void) const
//-----------------------------------------------------------------------

///
// public entries, define interfaces of list encode/decode members
//-----------------------------------------------------------------------
#define OB_SERIALIZE_MEMBER_TEMP(TEMP, CLS, ...)                        \
  OB_DEF_SERIALIZE(UNF_MYCLS(CLS), TEMP)                                \
  {                                                                     \
    int ret = OK_;                                                      \
    UNF_UNUSED_SER;                                                     \
    BASE_SER(CLS);                                                      \
    LST_DO_CODE(OB_UNIS_ENCODE, ##__VA_ARGS__);                         \
    return ret;                                                         \
  }                                                                     \
  OB_DEF_DESERIALIZE(UNF_MYCLS(CLS), TEMP)                              \
  {                                                                     \
    int ret = OK_;                                                      \
    UNF_UNUSED_DES;                                                     \
    BASE_DESER(CLS);                                                    \
    LST_DO_CODE(OB_UNIS_DECODE, ##__VA_ARGS__);                         \
    return ret;                                                         \
  }                                                                     \
  OB_DEF_SERIALIZE_SIZE(UNF_MYCLS(CLS), TEMP)                           \
  {                                                                     \
    int64_t len = 0;                                                    \
    BASE_ADD_LEN(CLS);                                                  \
    LST_DO_CODE(OB_UNIS_ADD_LEN, ##__VA_ARGS__);                        \
    return len;                                                         \
  }

#define OB_SERIALIZE_MEMBER(CLS, ...)                                   \
  OB_SERIALIZE_MEMBER_TEMP(, CLS, ##__VA_ARGS__)
//-----------------------------------------------------------------------

#define OB_DEF_SERIALIZE_SIMPLE(CLS)                                    \
  int CLS::serialize(SERIAL_PARAMS) const

#define OB_DEF_DESERIALIZE_SIMPLE(CLS)                                  \
  int CLS::deserialize(DESERIAL_PARAMS)

#define OB_DEF_SERIALIZE_SIZE_SIMPLE(CLS)                               \
  int64_t CLS::get_serialize_size(void) const

#define OB_SERIALIZE_MEMBER_SIMPLE(CLS, ...)                            \
  OB_DEF_SERIALIZE_SIMPLE(CLS)                                          \
  {                                                                     \
    int ret = OK_;                                                      \
    LST_DO_CODE(OB_UNIS_ENCODE, ##__VA_ARGS__);                         \
    return ret;                                                         \
  }                                                                     \
  OB_DEF_DESERIALIZE_SIMPLE(CLS)                                        \
  {                                                                     \
    int ret = OK_;                                                      \
    LST_DO_CODE(OB_UNIS_DECODE, ##__VA_ARGS__);                         \
    return ret;                                                         \
  }                                                                     \
  OB_DEF_SERIALIZE_SIZE_SIMPLE(CLS)                                     \
  {                                                                     \
    int64_t len = 0;                                                    \
    LST_DO_CODE(OB_UNIS_ADD_LEN, ##__VA_ARGS__);                        \
    return len;                                                         \
  }

#define OB_SERIALIZE_MEMBER_INHERIT(CLS, PARENT, ...)                   \
  OB_DEF_SERIALIZE_SIMPLE(CLS)                                          \
  {                                                                     \
    int ret = PARENT::serialize(buf, buf_len, pos);                     \
    if (OB_SUCC(ret)) {                                                 \
      LST_DO_CODE(OB_UNIS_ENCODE, ##__VA_ARGS__);                       \
    }                                                                   \
    return ret;                                                         \
  }                                                                     \
  OB_DEF_DESERIALIZE_SIMPLE(CLS)                                        \
  {                                                                     \
    int ret = PARENT::deserialize(buf, data_len, pos);                  \
    if (OB_SUCC(ret)) {                                                 \
      LST_DO_CODE(OB_UNIS_DECODE, ##__VA_ARGS__);                       \
    }                                                                   \
    return ret;                                                         \
  }                                                                     \
  OB_DEF_SERIALIZE_SIZE_SIMPLE(CLS)                                     \
  {                                                                     \
    int64_t len = PARENT::get_serialize_size();                         \
    LST_DO_CODE(OB_UNIS_ADD_LEN, ##__VA_ARGS__);                        \
    return len;                                                         \
  }

#endif /* _OCEABASE_LIB_UTILITY_OB_UNIFY_SERIALIZE_H_ */
