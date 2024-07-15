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
#include "ob_table.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/ob_row.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/oblog/ob_log.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "common/ob_table_object.h"
#include "lib/hash/ob_hashmap.h"
#include <algorithm> 
#include "ob_rpc_struct.h"

using namespace oceanbase::obproxy::obkv;
using namespace oceanbase::common;

int ObTableSingleOpEntity::construct_column_names(const ObTableBitMap &names_bit_map,
                                                  const ObIArray<ObString> &dictionary,
                                                  ObIArray<ObString> &column_names)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> names_pos;
  if (OB_FAIL(names_bit_map.get_true_bit_positions(names_pos))) {
    LOG_WDIAG("failed to get col names pos", K(ret), K(names_bit_map));
  } else {
    if (OB_FAIL(column_names.reserve(names_pos.count()))) {
      LOG_WDIAG("failed to reserve column_names", K(ret), K(names_pos));
    }
    int64_t all_keys_count = dictionary.count();
    ObString column_name;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < names_pos.count(); ++idx) {
      if (OB_UNLIKELY(names_pos[idx] >= all_keys_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("names_pos idx greater than all_keys_count", K(ret), K(all_keys_count), K(names_pos));
      } else if (dictionary.at(names_pos[idx], column_name)) {
        LOG_WDIAG("failed to get real_names", K(ret), K(idx), K(dictionary), K(names_pos));
      } else if (OB_FAIL(column_names.push_back(column_name))) {
        LOG_WDIAG("failed to push real_names", K(ret), K(idx));
      }
    }
  }

  return ret;
}

OB_DEF_SERIALIZE(ObITableEntity)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    const int64_t rowkey_size = get_rowkey_size();
    OB_UNIS_ENCODE(rowkey_size);
    ObObj obj;
    for (int64_t i = 0; i < rowkey_size && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(this->get_rowkey_value(i, obj))) {
        LOG_WDIAG("failed to get value", K(ret), K(i));
      }
      OB_UNIS_ENCODE(obj);
    }
  }
  if (OB_SUCC(ret)) {
    if (OBKV_ENTITY_MODE == EntityMode::LAZY_MODE) {
      const ObRpcFieldBuf &properties_buf = get_properties_buf();
      if (properties_buf.buf_len_ <= 0 || OB_ISNULL(properties_buf.buf_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected properties buf", K(properties_buf), K(ret));
      }
      OB_UNIS_ENCODE(properties_buf);
    } else {
      ObSEArray<std::pair<ObString, ObObj>, 8> properties;
      if (OB_FAIL(this->get_properties(properties))) {  // @todo optimize, use iterator
        LOG_WDIAG("failed to get properties", K(ret));
      } else {
        const int64_t properties_count = properties.count();
        OB_UNIS_ENCODE(properties_count);
        for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
          const std::pair<ObString, ObObj> &kv_pair = properties.at(i);
          OB_UNIS_ENCODE(kv_pair.first);
          OB_UNIS_ENCODE(kv_pair.second);
        }
      }
    }
  }
  return ret;
}

ODP_DEF_DESERIALIZE(ObITableEntity)
{
  int ret = OB_SUCCESS;
  reset();
  ObString key;
  ObObj value;
  // shallow copy
  if (OB_SUCC(ret)) {
    ROWKEY_VALUE rpc_req_rowkey_val;
    int64_t rowkey_size = -1;
    OB_UNIS_DECODE(rowkey_size);
    if (OB_SUCC(ret) && OB_FAIL(rpc_req_rowkey_val.prepare_allocate(rowkey_size))) {
      LOG_WDIAG("fail to preallocate memory for rpc rowkey val", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_size; ++i) {
      OB_UNIS_DECODE(rpc_req_rowkey_val.at(i));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_rowkey_value(rpc_req_rowkey_val.at(i)))) {
          LOG_WDIAG("failed to add rowkey value", K(ret), "value", rpc_req_rowkey_val.at(i));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(rpc_request->add_sub_req_rowkey_val(rpc_req_rowkey_val))) {
      LOG_WDIAG("fail to add rowkey value", K(ret));
    } 
  }
  if (OB_SUCC(ret)) {
    int64_t properties_count = -1;
    int64_t origin_pos = pos;
    const char *origin_buf = buf + pos;
    int64_t buffer_length = 0;

    // only record properties buffer, dont' save properties info
    OB_UNIS_DECODE(properties_count);
    for (int64_t i = 0; i < properties_count && OB_SUCC(ret); ++i) {
      OB_UNIS_DECODE(key);
      OB_UNIS_DECODE(value);
    }

    buffer_length = pos - origin_pos;
    if (OB_FAIL(ret)) {
      LOG_WDIAG("fail to deserialize properties of table entity", K(ret), K(buf), K(pos), K(data_len));
    } else {
      ObRpcFieldBuf buf(const_cast<char*>(origin_buf), buffer_length);
      set_properties_buf(buf);
    }
    /*} else {
      OB_UNIS_DECODE(properties_count);
      for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
        OB_UNIS_DECODE(key);
        OB_UNIS_DECODE(value);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(this->set_property(key, value))) {
            LOG_WDIAG("failed to set property", K(ret), K(key), K(value));
          }
        }
      }
    }*/
  }

  return ret;
}
/*
OB_DEF_DESERIALIZE(ObITableEntity)
{
  int ret = OB_SUCCESS;
  reset();
  ObString key;
  ObObj value;
  // if (NULL == alloc_) {
  //  shallow copy
  if (OB_SUCC(ret)) {
    int64_t rowkey_size = -1;
    OB_UNIS_DECODE(rowkey_size);
    for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_size; ++i) {
      OB_UNIS_DECODE(value);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(this->add_rowkey_value(value))) {
          LOG_WDIAG("failed to add rowkey value", K(ret), K(value));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t properties_count = -1;
    if (OBKV_ENTITY_MODE == EntityMode::LAZY_MODE) {
      int64_t origin_pos = pos;
      int64_t buffer_length = 0;
      const char *buffer = buf + pos;
      // const void *data_start = buf + pos; //flag data start

      // reset_properties_buffer();
      OB_UNIS_DECODE(properties_count);
      for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
        OB_UNIS_DECODE(key);
        OB_UNIS_DECODE(value);
      }

      buffer_length = pos - origin_pos;

      if (OB_FAIL(set_properties_buffer_length(buffer_length))) {
        LOG_WDIAG("fail to call set_properties_buffer_length", K(buffer_length), K(ret));
      } else if (OB_FAIL(set_properties_buffer(buffer))) {
        LOG_WDIAG("fail to call set_properties_buffer", K(buffer_length), K(ret));
      }
    } else {
      OB_UNIS_DECODE(properties_count);
      for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
        OB_UNIS_DECODE(key);
        OB_UNIS_DECODE(value);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(this->set_property(key, value))) {
            LOG_WDIAG("failed to set property", K(ret), K(key), K(value));
          }
        }
      }
    }
  }
  //}
  [>else {
    // deep copy
    ObObj value_clone;
    if (OB_SUCC(ret)) {
      int64_t rowkey_size = -1;
      OB_UNIS_DECODE(rowkey_size);
      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_size; ++i) {
        OB_UNIS_DECODE(value);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_write_obj(*alloc_, value, value_clone))) {
            LOG_WDIAG("failed to copy value", K(ret));
          } else if (OB_FAIL(this->add_rowkey_value(value_clone))) {
            LOG_WDIAG("failed to add rowkey value", K(ret), K(value_clone));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString key_clone;
      int64_t properties_count = -1;
      if (OBKV_ENTITY_MODE == EntityMode::LAZY_MODE) {
        int64_t origin_pos = pos;
        int64_t buffer_length = 0;
        const char *buffer = buf + pos;
        // const void *data_start = buf + pos; //flag data start

        // reset_properties_buffer();
        OB_UNIS_DECODE(properties_count);
        for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
          OB_UNIS_DECODE(key);
          OB_UNIS_DECODE(value);
        }

        buffer_length = pos - origin_pos;

        if (OB_FAIL(set_properties_buffer_length(buffer_length))) {
          LOG_WDIAG("fail to call set_properties_buffer_length", K(buffer_length), K(ret));
        } else if (OB_FAIL(set_properties_buffer(buffer))) {
          LOG_WDIAG("fail to call set_properties_buffer", K(buffer_length), K(ret));
        }
      } else {
        OB_UNIS_DECODE(properties_count);
        for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
          OB_UNIS_DECODE(key);
          OB_UNIS_DECODE(value);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ob_write_string(*alloc_, key, key_clone))) {
              LOG_WDIAG("failed to clone string", K(ret));
            } else if (OB_FAIL(ob_write_obj(*alloc_, value, value_clone))) {
              LOG_WDIAG("failed to copy value", K(ret));
            } else if (OB_FAIL(this->set_property(key_clone, value_clone))) {
              LOG_WDIAG("failed to set property", K(ret), K(key), K(value_clone));
            }
          }
        }
      }
    }
  }<]
  return ret;
}
*/
OB_DEF_SERIALIZE_SIZE(ObITableEntity)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  ObString key;
  ObObj value;
  const int64_t rowkey_size = get_rowkey_size();
  OB_UNIS_ADD_LEN(rowkey_size);
  for (int64_t i = 0; i < rowkey_size && OB_SUCCESS == ret; ++i) {
    if (OB_FAIL(this->get_rowkey_value(i, value))) {
      LOG_WDIAG("failed to get value", K(ret), K(i));
    }
    OB_UNIS_ADD_LEN(value);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_ADD_LEN(get_properties_buf());
  }
  return len;
}
/*
int ObITableEntity::deep_copy(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(deep_copy_rowkey(allocator, other))) {
  } else if (OB_FAIL(deep_copy_properties(allocator, other))) {
  }
  return ret;
}

int ObITableEntity::deep_copy_rowkey(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  ObObj value;
  ObObj cell_clone;
  const int64_t rowkey_size = other.get_rowkey_size();
  for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_size; ++i)
  {
    if (OB_FAIL(other.get_rowkey_value(i, value))) {
      LOG_WDIAG("failed to get rowkey value", K(ret), K(i), K(value));
    } else if (OB_FAIL(ob_write_obj(allocator, value, cell_clone))) {
      LOG_WDIAG("failed to copy cell", K(ret));
    } else if (OB_FAIL(this->add_rowkey_value(cell_clone))) {
      LOG_WDIAG("failed to add rowkey value", K(ret), K(value));
    }
  } // end for
  return ret;
}

int ObITableEntity::deep_copy_properties(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  if (OBKV_ENTITY_MODE == EntityMode::LAZY_MODE) {
    if (other.get_properties_buffer_length() > 0) {
      const char *buffer = NULL;
      reset_properties_buffer();
      set_properties_buffer_length(other.get_properties_buffer_length());
      other.get_properties_buffer(buffer);
      set_properties_buffer(buffer);
    }
  } else if (OB_FAIL(deep_copy_properties_normal_mode(allocator, other))) {
    LOG_WDIAG("fail to deep copy properties in normal mode", K(ret));
  }
  return ret;
}

int ObITableEntity::deep_copy_properties_normal_mode(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  ObObj value;
  ObObj cell_clone;
  ObString name_clone;
  ObSEArray<std::pair<ObString, ObObj>, 8> properties;
  if (OB_FAIL(other.get_properties(properties))) {  // @todo optimize, use iterator
    LOG_WDIAG("failed to get properties", K(ret));
  } else {
    const int64_t properties_count = properties.count();
    for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
      const std::pair<ObString, ObObj> &kv_pair = properties.at(i);
      if (OB_FAIL(ob_write_string(allocator, kv_pair.first, name_clone))) {
        LOG_WDIAG("failed to clone string", K(ret));
      } else if (OB_FAIL(ob_write_obj(allocator, kv_pair.second, cell_clone))) {
        LOG_WDIAG("failed to copy cell", K(ret));
      } else if (OB_FAIL(this->set_property(name_clone, cell_clone))) {
        LOG_WDIAG("failed to set property", K(ret));
      }
    }  // end for
  }
  return ret;
}
*/
int ObITableEntity::add_retrieve_property(const ObString &prop_name)
{
  ObObj null_obj;
  return set_property(prop_name, null_obj);
}

////////////////////////////////////////////////////////////////
ObTableEntity::ObTableEntity() : properties_buf_() {}

ObTableEntity::~ObTableEntity()
{
  reset_properties_buf();
  reset();
}

void ObTableEntity::reset()
{
  rowkey_.reset();
  properties_names_.reset();
  properties_values_.reset();
  reset_properties_buf();
}

void ObTableEntity::reset_properties_buf()
{
  properties_buf_.reset();
}

void ObTableEntity::set_properties_buf(const ObRpcFieldBuf &buf)
{
  properties_buf_ = buf;
}

void ObTableEntity::set_dictionary(ObIArray<ObString> *all_rowkey_names, ObIArray<ObString> *all_properties_names)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(all_rowkey_names);
  UNUSED(all_properties_names);
  LOG_WDIAG("not surpport set_dictionary", K(ret));
  return;
}

int ObTableEntity::construct_names_bitmap(const ObITableEntity &req_entity)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(req_entity);
  LOG_WDIAG("not surpport to construct_names_bitmap", K(ret));
  return OB_NOT_IMPLEMENT;
}

const ObTableBitMap *ObTableEntity::get_rowkey_names_bp() const
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WDIAG("not surpport to get_rowkey_names_bp", K(ret));
  return nullptr;
}

ObTableBitMap *ObTableEntity::get_rowkey_names_bp()
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WDIAG("not surpport to get_rowkey_names_bp", K(ret));
  return nullptr;
}

const ObTableBitMap *ObTableEntity::get_properties_names_bp() const
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WDIAG("not surpport to get_properties_names_bp", K(ret));
  return nullptr;
}

ObTableBitMap *ObTableEntity::get_properties_names_bp()
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WDIAG("not surpport to get_properties_names_bp", K(ret));
  return nullptr;
}

const ObIArray<ObString> *ObTableEntity::get_all_rowkey_names() const
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WDIAG("not surpport", K(ret));
  return nullptr;
}

const ObIArray<ObString> *ObTableEntity::get_all_properties_names() const
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WDIAG("not surpport", K(ret));
  return nullptr;
}

void ObTableEntity::set_is_same_properties_names(bool is_same_properties_names)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(is_same_properties_names);
  LOG_WDIAG("not surpport", K(ret));
}

int ObTableEntity::set_rowkey_value(int64_t idx, const ObObj &value)
{
  int ret = OB_SUCCESS;
  if (idx < 0) {
    ret = OB_INDEX_OUT_OF_RANGE;
  } else if (idx < rowkey_.count()) {
    rowkey_.at(idx) = value;
  } else {
    int64_t N = rowkey_.count();
    ObObj null_obj;
    for (int64_t i = N; OB_SUCC(ret) && i < idx; ++i) {
      if (OB_FAIL(rowkey_.push_back(null_obj))) {
        LOG_WDIAG("failed to pad null obj", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowkey_.push_back(value))) {
        LOG_WDIAG("failed to add value obj", K(ret), K(value));
      }
    }
  }
  return ret;
}

int ObTableEntity::add_rowkey_value(const ObObj &value)
{
  return rowkey_.push_back(value);
}

int ObTableEntity::get_rowkey_value(int64_t idx, ObObj &value) const
{
  return rowkey_.at(idx, value);
}

int ObTableEntity::set_rowkey(const ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  rowkey_.reset();
  const int64_t N = rowkey.get_obj_cnt();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    if (OB_FAIL(rowkey_.push_back(rowkey.ptr()[i]))) {
      LOG_WDIAG("failed to push back rowkey", K(ret), K(i));
    }
  } // end for
  return ret;
}

int ObTableEntity::set_rowkey(const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  const ObTableEntity *other_entity = dynamic_cast<const ObTableEntity*>(&other);
  if (NULL == other_entity) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid type of other entity");
  } else {
    rowkey_.reset();
    int64_t N = other_entity->rowkey_.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(rowkey_.push_back(other_entity->rowkey_.at(i)))) {
        LOG_WDIAG("failed to push back rowkey");
      }
    } // end for
  }
  return ret;
}

int64_t ObTableEntity::hash_rowkey() const
{
  int64_t hash_value = 0;
  const int64_t N = rowkey_.count();
  for (int64_t i = 0; i < N; ++i)
  {
    hash_value = rowkey_.at(i).hash(hash_value);
  } // end for
  return hash_value;
}

bool ObTableEntity::has_exist_in_properties(const ObString &name, int64_t *idx /* =nullptr */) const
{
  bool exist = false;
  int64_t num = properties_names_.count();
  for (int64_t i = 0; i < num && !exist; i++) {
    if (0 == name.case_compare(properties_names_.at(i))) {
      exist = true;
      if (idx != NULL) {
        *idx = i;
      }
    }
  }
  return exist;
}

int ObTableEntity::get_property(const ObString &prop_name, ObObj &prop_value) const
{
  int ret = OB_SUCCESS;
  if (prop_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("property name should not be empty string", K(ret), K(prop_name));
  } else {
    int64_t idx = -1;
    if (has_exist_in_properties(prop_name, &idx)) {
      prop_value = properties_values_.at(idx);
    } else {
      ret = OB_SEARCH_NOT_FOUND;
    }
  }
  return ret;
}

int ObTableEntity::set_property(const ObString &prop_name, const ObObj &prop_value)
{
  int ret = OB_SUCCESS;
  if (prop_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("property name should not be empty string", K(ret), K(prop_name));
  } else {
    int64_t idx = -1;
    if (has_exist_in_properties(prop_name, &idx)) {
      properties_values_.at(idx) = prop_value;
    } else {
      if (OB_FAIL(properties_names_.push_back(prop_name))) {
        LOG_WDIAG("failed to add prop name", K(ret), K(prop_name));
      } else if (OB_FAIL(properties_values_.push_back(prop_value))) {
        LOG_WDIAG("failed to add prop value", K(ret), K(prop_value));
      }
    }
  }
  return ret;
}

int ObTableEntity::push_value(const ObObj &prop_value)
{
  return properties_values_.push_back(prop_value);
}

int ObTableEntity::get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < properties_names_.count(); i++) {
    if (OB_FAIL(properties.push_back(std::make_pair(
                                     properties_names_.at(i),
                                     properties_values_.at(i))))) {
      LOG_WDIAG("failed to add name-value pair", K(ret), K(i));
    }
  }
  return ret;
}

int ObTableEntity::get_properties_names(ObIArray<ObString> &properties_names) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(properties_names.assign(properties_names_))) {
    LOG_WDIAG("fail to assign properties name array", K(ret));
  }
  return ret;
}

int ObTableEntity::get_properties_values(ObIArray<ObObj> &properties_values) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(properties_values.assign(properties_values_))) {
    LOG_WDIAG("failed to assign properties values array", K(ret));
  }
  return ret;
}

const ObObj &ObTableEntity::get_properties_value(int64_t idx) const
{
  return properties_values_.at(idx);
}

int64_t ObTableEntity::get_properties_count() const
{
  return properties_names_.count();
}

ObRowkey ObTableEntity::get_rowkey()
{
  ObRowkey rowkey;
  int64_t obj_cnt = rowkey_.count();
  if (obj_cnt > 0) {
    rowkey.assign(&rowkey_.at(0), obj_cnt);
  }
  return rowkey;
}

void ObTableEntity::get_rowkey(ObRowkey &rowkey)
{
  int64_t obj_cnt = rowkey_.count();
  if (obj_cnt > 0) {
    rowkey.assign(&rowkey_.at(0), obj_cnt);
  }
}

DEF_TO_STRING(ObTableEntity)
{
  int64_t pos = 0;
  J_OBJ_START();

  J_NAME("rowkey_names");
  J_COLON();
  BUF_PRINTO(rowkey_names_);

  J_NAME("rowkey");
  J_COLON();
  BUF_PRINTO(rowkey_);

  ObSEArray<std::pair<ObString, ObObj>, 8> properties;
  int ret = OB_SUCCESS;
  if (OB_FAIL(this->get_properties(properties))) {
    LOG_WDIAG("failed to get properties", K(ret));
  } else {
    J_COMMA();
    J_NAME("properties");
    J_COLON();
    J_OBJ_START();
    int64_t N = properties.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      const std::pair<ObString, ObObj> &prop = properties.at(i);
      if (0 != i) {
        J_COMMA();
      }
      BUF_PRINTF("%.*s", prop.first.length(), prop.first.ptr());
      J_COLON();
      BUF_PRINTO(prop.second);
    }
    J_OBJ_END();

    J_COMMA();
    J_NAME("properties_buf");
    J_COLON();
    J_OBJ_START();
    if (OB_NOT_NULL(properties_buf_.buf_) && properties_buf_.buf_len_ > 0) {
      BUF_PRINTF("%.*s", static_cast<int32_t>(std::min(properties_buf_.buf_len_, 256L)), properties_buf_.buf_);
    }
    J_OBJ_END();
  }
  J_OBJ_END();
  return pos;
}

////////////////////////////////////////////////////////////////
/*int ObTableOperation::deep_copy(common::ObIAllocator &allocator, ObITableEntity &entity, const ObTableOperation &other)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *src_entity = NULL;
  ObITableEntity *dest_entity = &entity;
  if (OB_FAIL(other.get_entity(src_entity))) {
    LOG_WDIAG("failed to get entity", K(ret));
  } else if (OB_FAIL(dest_entity->deep_copy(allocator, *src_entity))) {
    LOG_WDIAG("failed to copy entity", K(ret));
  } else {
    operation_type_ = other.operation_type_;
    entity_ = dest_entity;
  }
  return ret;
}
int ObTableOperation::deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableOperation &other)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *src_entity = NULL;
  ObITableEntity *dest_entity = NULL;
  if (OB_FAIL(other.get_entity(src_entity))) {
    LOG_WDIAG("failed to get entity", K(ret));
  } else if (NULL == (dest_entity = entity_factory.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("no memory", K(ret));
  } else if (OB_FAIL(dest_entity->deep_copy(allocator, *src_entity))) {
    LOG_WDIAG("failed to copy entity", K(ret));
  } else {
    operation_type_ = other.operation_type_;
    entity_ = dest_entity;
  }
  return ret;
}

int ObTableOperation::get_entity(const ObITableEntity *&entity) const
{
  int ret = OB_SUCCESS;
  if (NULL == entity_) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    entity = this->entity_;
  }
  return ret;
}

int ObTableOperation::get_entity(ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  if (NULL == entity_) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    entity = const_cast<ObITableEntity*>(this->entity_);
  }
  return ret;
}

uint64_t ObTableOperation::get_checksum()
{
  uint64_t checksum = 0;
  ObITableEntity *entity = NULL;
  if (OB_SUCCESS != get_entity(entity) || OB_ISNULL(entity)) {
    // ignore
  } else {
    const int64_t rowkey_size = entity->get_rowkey_size();
    const int64_t property_count = entity->get_properties_count();
    checksum = ob_crc64(checksum, &rowkey_size, sizeof(rowkey_size));
    checksum = ob_crc64(checksum, &property_count, sizeof(property_count));
  }
  checksum = ob_crc64(checksum, &operation_type_, sizeof(operation_type_));
  return checksum;
}*/

//OB_SERIALIZE_MEMBER(ObTableOperation, operation_type_, const_cast<ObITableEntity&>(*entity_));

OB_UNIS_DEF_SERIALIZE(ObTableOperation, operation_type_, entity_);
OB_UNIS_DEF_SERIALIZE_SIZE(ObTableOperation, operation_type_, entity_);
ODP_DEF_DESERIALIZE(ObTableOperation)
{
  int ret= OB_SUCCESS;
  OB_UNIS_DECODE(operation_type_);
  if (OB_FAIL(entity_.deserialize(buf, data_len, pos, rpc_request))) {
    LOG_WDIAG("fail to deserialize ObTableEntity", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObTableRequestOptions::ObTableRequestOptions()
    :consistency_level_(ObTableConsistencyLevel::STRONG),
     server_timeout_us_(10*1000*1000),
     max_execution_time_us_(10*1000*1000),
     retry_policy_(NULL),
     returning_affected_rows_(false),
     returning_rowkey_(false),
     returning_affected_entity_(false),
     binlog_row_image_type_(ObBinlogRowImageType::FULL)
{}

////////////////////////////////////////////////////////////////
/*int ObTableBatchOperation::deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableBatchOperation &other)
{
  int ret = OB_SUCCESS;
  ObTableOperation tmp_operation;

  for (int i = 0; OB_SUCC(ret) && i < other.count(); ++i) {
    const ObTableOperation &other_operation = other.at(i);
    if (OB_FAIL(tmp_operation.deep_copy(allocator, entity_factory, other_operation))) {
      LOG_WDIAG("fail to call deep_copy for tableOperation", K(ret));
    } else if (OB_FAIL(push_back(tmp_operation))) {
      LOG_WDIAG("fail to call push_back for tableOperation", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    is_readonly_ = other.is_readonly_;
    is_same_type_ = other.is_same_type_;
    is_same_properties_names_ = other.is_same_properties_names_;
    entity_factory_ = &entity_factory;
  }

  return ret;
}

uint64_t ObTableBatchOperation::get_checksum()
{
  uint64_t checksum = 0;
  const int64_t op_count = table_operations_.count();
  if (op_count > 0) {
    if (is_same_type()) {
      const uint64_t first_op_checksum = table_operations_.at(0).get_checksum();
      checksum = ob_crc64(checksum, &first_op_checksum, sizeof(first_op_checksum));
    } else {
      for (int64_t i = 0; i < op_count; ++i) {
        const uint64_t cur_op_checksum = table_operations_.at(i).get_checksum();
        checksum = ob_crc64(checksum, &cur_op_checksum, sizeof(cur_op_checksum));
      }
    }
  }
  checksum = ob_crc64(checksum, &is_readonly_, sizeof(is_readonly_));
  checksum = ob_crc64(checksum, &is_same_type_, sizeof(is_same_type_));
  checksum = ob_crc64(checksum, &is_same_properties_names_, sizeof(is_same_properties_names_));

  return checksum;
}
*/
/*int ObTableBatchOperation::get_sub_table_operation(ObTableBatchOperation &sub_batch_operation, const common::ObIArray<int64_t> &sub_index)
{
  int ret = OB_SUCCESS;

  if (0 == sub_index.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("sub_index size is zero", K(ret));
  } else {
    int64_t index = 0;
    sub_batch_operation.reset();

    sub_batch_operation.set_readonly(is_readonly_);
    sub_batch_operation.set_same_properties_names(is_same_properties_names_);
    sub_batch_operation.set_same_type(is_same_type_);

    LOG_DEBUG("succ to get sub operation before", K(table_operations_));

    for(int64_t i = 0; i < sub_index.count(); ++i) {
      index = sub_index.at(i);
      if (index > table_operations_.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("sub_index is greater than operation size", K(index), "operation size", count());
      } else if (OB_FAIL(sub_batch_operation.push_back(table_operations_.at(index)))) {
        LOG_WDIAG("fail to call push back", K(ret), K(index));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("succ to get sub operation", K(sub_batch_operation), K(sub_index));
    }
  }

  return ret;
}
*/
int ObTableBatchOperation::set_table_ops(const common::ObIArray<ObRpcFieldBuf> &table_op_buf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_operations_.prepare_allocate(table_op_buf.count()))) {
    PROXY_RPC_SM_LOG(WDIAG, "fail to reserve mem for single op buf", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < table_op_buf.count(); i++) {
      table_operations_.at(i) = (OB_IGNORE_TABLE_OPERATION(table_op_buf.at(i).buf_, table_op_buf.at(i).buf_len_));
    }
  }
  return ret;
}

void ObTableBatchOperation::reset()
{
  table_operations_.reset();
  is_readonly_ = true;
  is_same_type_ = true;
  is_same_properties_names_ = true;
}

OB_DEF_SERIALIZE(ObTableBatchOperation,)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              table_operations_,
              is_readonly_,
              is_same_type_,
              is_same_properties_names_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableBatchOperation,)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_operations_,
              is_readonly_,
              is_same_type_,
              is_same_properties_names_);
  return len;
}

// for deserialize, need alloc entity, so can not use LIST_DO_CODE
ODP_DEF_DESERIALIZE(ObTableBatchOperation,)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  reset();
  int64_t batch_size = 0;
  OB_UNIS_DECODE(batch_size);
  if (OB_FAIL(rpc_request->init_rowkey_info(batch_size))) {
    LOG_WDIAG("fail to init rpc request", K(ret));
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < batch_size; ++i) {
    OB_IGNORE_TABLE_OPERATION table_operation;
    char *origin_buf = const_cast<char*>(buf + pos);
    int64_t origin_pos = pos;
    if (OB_FAIL(table_operation.deserialize(buf, data_len, pos, rpc_request))) {
      LOG_WDIAG("fail to deserialize table operation", K(ret));
    } else if (OB_FAIL(table_operations_.push_back(table_operation))) {
      LOG_WDIAG("failed to push back", K(ret));
    } else if (OB_FAIL(rpc_request->add_sub_req_buf(ObRpcFieldBuf(origin_buf, pos - origin_pos)))) {
      LOG_WDIAG("fail to add sub req buf", K(ret));
    }
  } // end for
  LST_DO_CODE(OB_UNIS_DECODE,
              is_readonly_,
              is_same_type_,
              is_same_properties_names_);
  return ret;
}

////////////////////////////////////////////////////////////////
OB_SERIALIZE_MEMBER(ObTableResult, errno_, sqlstate_, msg_);

int ObTableResult::assign(const ObTableResult &other)
{
  errno_ = other.errno_;
  strncpy(sqlstate_, other.sqlstate_, sizeof(sqlstate_));
  strncpy(msg_, other.msg_, sizeof(msg_));
  return OB_SUCCESS;
}

////////////////////////////////////////////////////////////////
ObTableOperationResult::ObTableOperationResult()
    :operation_type_(ObTableOperationType::GET),
     affected_rows_(0)
{}
/*
int ObTableOperationResult::get_entity(const ObITableEntity *&entity) const
{
  int ret = OB_SUCCESS;
  if (NULL == entity_) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    entity = entity_;
  }
  return ret;
}

int ObTableOperationResult::get_entity(ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  if (NULL == entity_) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    entity = entity_;
  }
  return ret;
}*/

DEF_TO_STRING(ObTableOperationResult)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(errno),
       K_(operation_type),
       K_(affected_rows));
  J_COMMA();
  J_KV("entity", entity_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObTableOperationResult, ObTableResult),
                    operation_type_, entity_, affected_rows_);

/*int ObTableOperationResult::deep_copy(common::ObIAllocator &allocator, ObITableEntity &result_entity, const ObTableOperationResult &other)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *src_entity = NULL;
  ObITableEntity *dest_entity = &result_entity;
  if (OB_FAIL(other.get_entity(src_entity))) {
    LOG_WDIAG("failed to get entity", K(ret));
  } else if (OB_FAIL(dest_entity->deep_copy(allocator, *src_entity))) {
    LOG_WDIAG("failed to copy entity", K(ret));
  } else if (OB_FAIL(ObTableResult::assign(other))) {
    LOG_WDIAG("failed to copy result", K(ret));
  } else {
    operation_type_ = other.operation_type_;
    entity_ = dest_entity;
    affected_rows_ = other.affected_rows_;
  }
  return ret;
}

int ObTableOperationResult::deep_copy(common::ObIAllocator &allocator,
                                      ObITableEntityFactory &entity_factory, const ObTableOperationResult &other)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *src_entity = NULL;
  ObITableEntity *dest_entity = NULL;
  if (OB_FAIL(other.get_entity(src_entity))) {
    LOG_WDIAG("failed to get entity", K(ret));
  } else if (NULL == (dest_entity = entity_factory.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("no memory", K(ret));
  } else if (OB_FAIL(dest_entity->deep_copy(allocator, *src_entity))) {
    LOG_WDIAG("failed to copy entity", K(ret));
  } else if (OB_FAIL(ObTableResult::assign(other))) {
    LOG_WDIAG("failed to copy result", K(ret));
  } else {
    operation_type_ = other.operation_type_;
    entity_ = dest_entity;
    affected_rows_ = other.affected_rows_;
  }
  return ret;
}
*/
////////////////////////////////////////////////////////////////
/*int ObTableBatchOperationResult::deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableBatchOperationResult &other)
{
  int ret = OB_SUCCESS;
  ObTableOperationResult tmp_operation_result;

  for (int i = 0; OB_SUCC(ret) && i < other.table_operations_result_.count(); ++i) {
    const ObTableOperationResult &other_operation_result = other.at(i);
    if (OB_FAIL(tmp_operation_result.deep_copy(allocator, entity_factory, other_operation_result))) {
      LOG_WDIAG("fail to call deep_copy for tableOperationResult", K(ret));
    } else if (OB_FAIL(push_back(tmp_operation_result))) {
      LOG_WDIAG("fail to call push_back for tableOperationResult", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    entity_factory_ = &entity_factory;
  }

  return ret;
}*/

void ObTableBatchOperationResult::reset()
{
  // just reset array
  table_operations_result_.reset();
}

int ObTableBatchOperationResult::push_back(const ObTableOperationResult &res)
{
  return table_operations_result_.push_back(res);
}

OB_DEF_SERIALIZE(ObTableBatchOperationResult,)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(table_operations_result_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableBatchOperationResult,)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(table_operations_result_);
  return len;
}

OB_DEF_DESERIALIZE(ObTableBatchOperationResult,)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  int64_t batch_size = 0;
  OB_UNIS_DECODE(batch_size);
  ObTableOperationResult table_operation_result;
  reset();
  for (int64_t i = 0; OB_SUCCESS == ret && i < batch_size; ++i) {
    if (OB_FAIL(table_operation_result.deserialize(buf, data_len, pos))) {
      LOG_WDIAG("fail to decode array item", K(ret), K(i), K(batch_size), K(data_len), K(pos),
                K(table_operation_result));
    } else if (OB_FAIL(table_operations_result_.push_back(table_operation_result))) {
      LOG_WDIAG("fail to add item to array", K(ret), K(i), K(batch_size));
    }
  } // end for
  return ret;
}
////////////////////////////////////////////////////////////////
int ObHTableFilter::deep_copy(common::ObIAllocator &allocator, const ObHTableFilter &other)
{
  int ret = OB_SUCCESS;
  ObString dest_string;

  for (int i = 0; OB_SUCC(ret) && i < other.select_column_qualifier_.count(); ++i) {
    if (OB_FAIL(ob_write_string(allocator, other.select_column_qualifier_.at(i), dest_string))) {
      LOG_WDIAG("fail to call ob_write_string", K(ret));
    } else if (OB_FAIL(select_column_qualifier_.push_back(dest_string))) {
      LOG_WDIAG("fail to call push_back", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, other.filter_string_, filter_string_))) {
      LOG_WDIAG("fail to call ob_write_string", K(ret));
    } else {
      is_valid_ = other.is_valid_;
      min_stamp_ = other.min_stamp_;  // default -1
      max_stamp_ = other.max_stamp_;  // default -1
      max_versions_ = other.max_versions_;  // default 1
      limit_per_row_per_cf_ = other.limit_per_row_per_cf_;  // default -1 means unlimited
      offset_per_row_per_cf_ = other.offset_per_row_per_cf_; // default 0
    }
  }

  return ret;
}

/*int ObTableQuery::deep_copy(common::ObIAllocator &allocator, const ObTableQuery &other)
{
  int ret = OB_SUCCESS;
  ObString dest_string;
  ObNewRange dest_range;

  for (int i = 0; OB_SUCC(ret) && i < other.key_ranges_.count(); ++i) {
    if (OB_FAIL(deep_copy_range(allocator, other.key_ranges_.at(i), dest_range))) {
      LOG_WDIAG("fail to call deep_copy_range", K(ret));
    } else if (OB_FAIL(key_ranges_.push_back(dest_range))) {
      LOG_WDIAG("fail to call push_back", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < other.select_columns_.count(); ++i) {
    if (OB_FAIL(ob_write_string(allocator, other.select_columns_.at(i), dest_string))) {
      LOG_WDIAG("fail to call ob_write_string", K(ret));
    } else if (OB_FAIL(select_columns_.push_back(dest_string))) {
      LOG_WDIAG("fail to call push_back", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < other.rowkey_columns_.count(); ++i) {
    if (OB_FAIL(ob_write_string(allocator, other.rowkey_columns_.at(i), dest_string))) {
      LOG_WDIAG("fail to call ob_write_string", K(ret));
    } else if (OB_FAIL(rowkey_columns_.push_back(dest_string))) {
      LOG_WDIAG("fail to call push_back", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, other.filter_string_, filter_string_))) {
      LOG_WDIAG("fail to call ob_write_string", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, other.index_name_, index_name_))) {
      LOG_WDIAG("fail to call ob_write_string", K(ret));
    } else if (OB_FAIL(htable_filter_.deep_copy(allocator, other.htable_filter_))) {
      LOG_WDIAG("fail to call deep_copy for htable_filter", K(ret));
    } else {
      limit_ = other.limit_;
      offset_ = other.offset_;
      scan_order_ = other.scan_order_;
      batch_size_ = other.batch_size_;
      max_result_size_ = other.max_result_size_;
    }
  }

  return ret;
}
*/
void ObTableQuery::reset()
{
  //deserialize_allocator_ = NULL;
  key_ranges_.reset();
  select_columns_.reset();
  filter_string_.reset();
  limit_ = -1;  // no limit
  offset_ = 0;
  scan_order_ = ObQueryFlag::Forward;
  index_name_.reset();
  batch_size_ = -1;
  max_result_size_ = -1;
  htable_filter_.reset();
  rowkey_columns_.reset();
  cluster_version_ = 0;
}

bool ObTableQuery::is_valid() const
{
  return (limit_ == -1 || limit_ > 0)
      && (offset_ >= 0)
      && key_ranges_.count() > 0;
      // && select_columns_.count() > 0;  // for empty columns, return result with all columns
}

int ObTableQuery::add_scan_range(common::ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key_ranges_.push_back(scan_range))) {
    LOG_WDIAG("failed to add rowkey range", K(ret), K(scan_range));
  }
  return ret;
}

int ObTableQuery::set_scan_order(common::ObQueryFlag::ScanOrder scan_order)
{
  int ret = OB_SUCCESS;
  if (scan_order != ObQueryFlag::Forward
      && scan_order != ObQueryFlag::Reverse) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid scan order", K(ret), K(scan_order));
  } else {
    scan_order_ = scan_order;
  }
  return ret;
}

int ObTableQuery::add_select_column(const ObString &column)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(select_columns_.push_back(column))) {
    LOG_WDIAG("failed to add select column", K(ret), K(column));
  }
  return ret;
}

int ObTableQuery::add_rowkey_column(const ObString &rowkey_element)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowkey_columns_.push_back(rowkey_element))) {
    LOG_WDIAG("failed to add select column", K(ret), K(rowkey_element));
  }
  return ret;
}

int ObTableQuery::set_scan_index(const ObString &index_name)
{
  index_name_ = index_name;
  return OB_SUCCESS;
}

int ObTableQuery::set_filter(const ObString &filter)
{
  LOG_WDIAG("general filter not supported", K(filter));
  return OB_NOT_SUPPORTED;
}

int ObTableQuery::set_limit(int32_t limit)
{
  int ret = OB_SUCCESS;
  if (limit < -1 || 0 == limit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("limit cannot be negative or zero", K(ret), K(limit));
  } else {
    limit_ = limit;
  }
  return ret;
}

int ObTableQuery::set_offset(int32_t offset)
{
  int ret = OB_SUCCESS;
  if (offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("offset cannot be negative", K(ret), K(offset));
  } else {
    offset_ = offset;
  }
  return ret;
}

int ObTableQuery::set_batch(int32_t batch_size)
{
  int ret = OB_SUCCESS;
  if (batch_size == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("batch_size cannot be zero", K(ret), K(batch_size));
  } else {
    batch_size_ = batch_size;
  }
  return ret;
}

int ObTableQuery::set_max_result_size(int64_t max_result_size)
{
  int ret = OB_SUCCESS;
  if (max_result_size == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("max_result_size cannot be zero", K(ret), K(max_result_size));
  } else {
    max_result_size_ = max_result_size;
  }
  return ret;
}

/*uint64_t ObTableQuery::get_checksum() const
{
  uint64_t checksum = 0;
  const int64_t range_count = get_range_count();
  checksum = ob_crc64(checksum, &range_count, sizeof(range_count));
  for (int64_t i = 0; i < select_columns_.count(); ++i) {
    const ObString &cur_column = select_columns_.at(i);
    checksum = ob_crc64(checksum, cur_column.ptr(), cur_column.length());
  }
  checksum = ob_crc64(checksum, filter_string_.ptr(), filter_string_.length());
  checksum = ob_crc64(checksum, &limit_, sizeof(limit_));
  checksum = ob_crc64(checksum, &offset_, sizeof(offset_));
  checksum = ob_crc64(checksum, &scan_order_, sizeof(scan_order_));
  checksum = ob_crc64(checksum, index_name_.ptr(), index_name_.length());
  checksum = ob_crc64(checksum, &batch_size_, sizeof(batch_size_));
  checksum = ob_crc64(checksum, &max_result_size_, sizeof(max_result_size_));
  if (htable_filter_.is_valid()) {
    const uint64_t htable_filter_checksum = htable_filter_.get_checksum();
    checksum = ob_crc64(checksum, &htable_filter_checksum, sizeof(htable_filter_checksum));
  }
  for (int64_t i = 0; i < rowkey_columns_.count(); ++i) {
    const ObString &cur_column = rowkey_columns_.at(i);
    checksum = ob_crc64(checksum, cur_column.ptr(), cur_column.length());
  }
  return checksum;
}*/

OB_DEF_SERIALIZE(ObTableQuery,)
{
  int ret = OB_SUCCESS;
  int64_t count = key_ranges_.count();
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, count))) {
    LOG_WDIAG("fail to encode key ranges count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      if (OB_FAIL(key_ranges_.at(i).serialize(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize range", K(ret));
      }
    } else {
      if (OB_FAIL(key_ranges_.at(i).serialize_v4(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize range", K(ret));
      }
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              select_columns_,
              filter_string_,
              limit_,
              offset_,
              scan_order_,
              index_name_,
              batch_size_,
              max_result_size_,
              htable_filter_,
              rowkey_columns_)
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableQuery,)
{
  int64_t len = 0;
  int64_t count = key_ranges_.count();
  OB_UNIS_ADD_LEN(count);
  for (int64_t i = 0; i < count; i ++) {
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      len += key_ranges_.at(i).get_serialize_size();
    } else {
      len += key_ranges_.at(i).get_serialize_size_v4();
    }
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              select_columns_,
              filter_string_,
              limit_,
              offset_,
              scan_order_,
              index_name_,
              batch_size_,
              max_result_size_,
              htable_filter_,
              rowkey_columns_);
  return len;
}

ODP_DEF_DESERIALIZE(ObTableQuery)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    key_ranges_.reset();
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WDIAG("fail to decode key ranges count", K(ret));
    }
    RANGE_VALUE range_value;
    if (OB_FAIL(range_value.prepare_allocate(count))) {
      LOG_WDIAG("fail to prepare_allocate for range valu ", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      ObNewRange copy_range;
      ObNewRange &key_range = range_value.at(i);
      copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
      if (IS_CLUSTER_VERSION_LESS_THAN_V4(rpc_request->get_cluster_version())) {
        if (OB_FAIL(copy_range.deserialize(buf, data_len, pos))) {
          LOG_WDIAG("fail to deserialize range", K(ret));
        }
      } else {
        if (OB_FAIL(copy_range.deserialize_v4(buf, data_len, pos))) {
          LOG_WDIAG("fail to deserialize range", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(common::deep_copy_range(rpc_request->allocator_, copy_range, key_range))) {
          LOG_WDIAG("fail to deep copy range", K(ret));
        } else if (OB_FAIL(key_ranges_.push_back(key_range))) {
          LOG_WDIAG("fail to add key range to array", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(rpc_request->add_sub_req_range(range_value))) {
      LOG_WDIAG("fail to add_sub_req_range", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, select_columns_, filter_string_, limit_, offset_, scan_order_, index_name_, batch_size_,
                max_result_size_, htable_filter_, rowkey_columns_);
    // When parsing, if there is still data, additionally process the primary key column name
  }

  if (OB_SUCC(ret)) {
    rpc_request->set_index_name(index_name_);
    rpc_request->set_batch_size(batch_size_);
  }
  if (OB_SUCC(ret) && OB_FAIL(rpc_request->add_sub_req_columns(rowkey_columns_))) {
    LOG_WDIAG("fail to add rowkey columns to rpc_request", K(ret));
  }
  return ret;
}
/*
void ObTableSingleOpQuery::reset()
{
  scan_range_cols_bp_.clear();
  ObTableQuery::reset();
}
*/
OB_DEF_SERIALIZE(ObTableSingleOpQuery)
{
  int ret = OB_SUCCESS;
  // ensure all_rowkey_names_ is not null!
  if (!has_dictionary()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("has not valid all_names", K(ret));
  } else if (!scan_range_cols_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("scan_range_cols_bp is not init by rowkey_names", K(ret), K_(scan_range_cols_bp),
      K_(rowkey_columns), KPC(all_rowkey_names_));
  } else {
    OB_UNIS_ENCODE(index_name_);
    if (OB_SUCC(ret)) {
      // construct scan_range_cols_bp_
      // msg_format: index_name | scan_cols_count | scan_range_cols_bp | key_ranges | filter_string
      // properties_count | properties_values encode row_names_bp
      if (OB_FAIL(scan_range_cols_bp_.serialize(buf, buf_len, pos))) {
        LOG_WDIAG("failed to encode scan_range_cols_bp_", K(ret),  K(pos));
      } else {
        int64_t key_range_size = key_ranges_.count();
        OB_UNIS_ENCODE(key_range_size);
        for (int64_t i = 0; OB_SUCC(ret) && i < key_range_size; i++) {
          if (OB_FAIL(ObTableSerialUtil::serialize(buf, buf_len, pos, key_ranges_[i]))) {
            LOG_WDIAG("fail to deserialize range", K(ret), K(i));
          }
        }
        OB_UNIS_ENCODE(filter_string_);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableSingleOpQuery)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (!has_dictionary() && get_scan_range_columns_count() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("has not valid all_names", K(ret));
  } else if (!scan_range_cols_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("scan_range_cols_bp_ is not init by rowkey_names", K(ret), K_(scan_range_cols_bp),
      K_(rowkey_columns), KPC(all_rowkey_names_));
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN, index_name_);

    if (OB_SUCC(ret)) {
      // scan_range_cols_bp_ size
      len += scan_range_cols_bp_.get_serialize_size();
      int64_t key_range_size = key_ranges_.count();
      OB_UNIS_ADD_LEN(key_range_size);
      for (int64_t i = 0; OB_SUCC(ret) && i < key_range_size; i++) {
        len += ObTableSerialUtil::get_serialize_size(key_ranges_[i]);
      }
      OB_UNIS_ADD_LEN(filter_string_);
    }
  }

  return len;
}

ODP_DEF_DESERIALIZE(ObTableSingleOpQuery)
{
  int ret = OB_SUCCESS;
  if (!has_dictionary()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObTableSingleOpQuery has not valid all_names", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, index_name_);
    // decode scan_range_cols_bp_
    if (OB_SUCC(ret) && pos < data_len) {
      if (OB_FAIL(scan_range_cols_bp_.deserialize(buf, data_len, pos))) {
        LOG_WDIAG("failed to decode scan_range_cols_bp", K(ret), K_(scan_range_cols_bp), K(pos));
      } else if (OB_FAIL(ObTableSingleOpEntity::construct_column_names(
              scan_range_cols_bp_, *all_rowkey_names_, rowkey_columns_))) {
        LOG_WDIAG("failed to construct scan_range_columns", K(ret), K_(scan_range_cols_bp),
          KPC(all_rowkey_names_), K_(rowkey_columns));
      }
    }

    if (OB_SUCC(ret) && pos < data_len) {
      int64_t count = 0;
      if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
        LOG_WDIAG("fail to decode key ranges count", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
          ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
          ObNewRange copy_range;
          ObNewRange key_range;
          copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
          copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
          if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, &rpc_request->allocator_, copy_range))) {
            LOG_WDIAG("fail to deserialize range", K(ret));
          } else if (OB_FAIL(common::deep_copy_range(rpc_request->allocator_, copy_range, key_range))) {
            LOG_WDIAG("fail to deep copy range", K(ret));
          } else if (OB_FAIL(key_ranges_.push_back(key_range))) {
            LOG_WDIAG("fail to add key range to array", K(ret));
          }
        }
    }
    if (OB_SUCC(ret) && pos < data_len) {
      LST_DO_CODE(OB_UNIS_DECODE, filter_string_);
    }
  }

  return ret;
}
/*
int ObTableSingleOpEntity::deep_copy_properties(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_properties_normal_mode(allocator, other))) {
    LOG_WDIAG("fail to deep copy properties int normal mode", K(ret));
  }
  return ret;
}
*/
////////////////////////////////////////////////////////////////
ObTableEntityIterator::~ObTableEntityIterator()
{}
////////////////////////////////////////////////////////////////
const char* const ObHTableConstants::ROWKEY_CNAME = "K";
const char* const ObHTableConstants::CQ_CNAME = "Q";
const char* const ObHTableConstants::VERSION_CNAME = "T";
const char* const ObHTableConstants::VALUE_CNAME = "V";

const ObString ObHTableConstants::ROWKEY_CNAME_STR = ObString::make_string(ROWKEY_CNAME);
const ObString ObHTableConstants::CQ_CNAME_STR = ObString::make_string(CQ_CNAME);
const ObString ObHTableConstants::VERSION_CNAME_STR = ObString::make_string(VERSION_CNAME);
const ObString ObHTableConstants::VALUE_CNAME_STR = ObString::make_string(VALUE_CNAME);

ObHTableFilter::ObHTableFilter()
    :is_valid_(false),
     select_column_qualifier_(),
     min_stamp_(ObHTableConstants::INITIAL_MIN_STAMP),
     max_stamp_(ObHTableConstants::INITIAL_MAX_STAMP),
     max_versions_(1),
     limit_per_row_per_cf_(-1),
     offset_per_row_per_cf_(0),
     filter_string_()
{}

void ObHTableFilter::reset()
{
  is_valid_ = false;
  select_column_qualifier_.reset();
  min_stamp_ = ObHTableConstants::INITIAL_MIN_STAMP;
  max_stamp_ = ObHTableConstants::INITIAL_MAX_STAMP;
  max_versions_ = 1;
  limit_per_row_per_cf_ = -1;
  offset_per_row_per_cf_ = 0;
  filter_string_.reset();

}

int ObHTableFilter::add_column(const ObString &qualifier)
{
  int ret = OB_SUCCESS;
  const int64_t N = select_column_qualifier_.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    if (0 == select_column_qualifier_.at(i).case_compare(qualifier)) {
      ret = OB_ERR_COLUMN_DUPLICATE;
      LOG_WDIAG("column already exists", K(ret), K(qualifier));
      break;
    }
  } // end for
  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_column_qualifier_.push_back(qualifier))) {
      LOG_WDIAG("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObHTableFilter::set_time_range(int64_t min_stamp, int64_t max_stamp)
{
  int ret = OB_SUCCESS;
  if (min_stamp >= max_stamp || min_stamp_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid time range", K(ret), K(min_stamp), K(max_stamp));
  } else {
    min_stamp_ = min_stamp;
    max_stamp_ = max_stamp;
  }
  return ret;
}

int ObHTableFilter::set_max_versions(int32_t versions)
{
  int ret = OB_SUCCESS;
  if (versions <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid max versions", K(ret), K(versions));
  } else {
    max_versions_ = versions;
  }
  return ret;
}

int ObHTableFilter::set_max_results_per_column_family(int32_t limit)
{
  int ret = OB_SUCCESS;
  if (limit < -1 || 0 == limit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("limit cannot be negative or zero", K(ret), K(limit));
  } else {
    limit_per_row_per_cf_ = limit;
  }
  return ret;
}

int ObHTableFilter::set_row_offset_per_column_family(int32_t offset)
{
  int ret = OB_SUCCESS;
  if (offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("offset cannot be negative", K(ret), K(offset));
  } else {
    offset_per_row_per_cf_ = offset;
  }
  return ret;
}

int ObHTableFilter::set_filter(const ObString &filter)
{
  filter_string_ = filter;
  return OB_SUCCESS;
}

uint64_t ObHTableFilter::get_checksum() const
{
  uint64_t checksum = 0;
  for (int64_t i = 0; i < select_column_qualifier_.count(); ++i) {
    const ObString &cur_qualifier = select_column_qualifier_.at(i);
    checksum = ob_crc64(checksum, cur_qualifier.ptr(), cur_qualifier.length());
  }
  checksum = ob_crc64(checksum, &min_stamp_, sizeof(min_stamp_));
  checksum = ob_crc64(checksum, &max_stamp_, sizeof(max_stamp_));
  checksum = ob_crc64(checksum, &max_versions_, sizeof(max_versions_));
  checksum = ob_crc64(checksum, &limit_per_row_per_cf_, sizeof(limit_per_row_per_cf_));
  checksum = ob_crc64(checksum, &offset_per_row_per_cf_, sizeof(offset_per_row_per_cf_));
  checksum = ob_crc64(checksum, filter_string_.ptr(), filter_string_.length());
  return checksum;
}

OB_DEF_SERIALIZE(ObHTableFilter,)
{
  int ret = OB_SUCCESS;
  if (is_valid_) {
    LST_DO_CODE(OB_UNIS_ENCODE,
                is_valid_,
                select_column_qualifier_,
                min_stamp_,
                max_stamp_,
                max_versions_,
                limit_per_row_per_cf_,
                offset_per_row_per_cf_,
                filter_string_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObHTableFilter,)
{
  int64_t len = 0;
  if (is_valid_) {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                is_valid_,
                select_column_qualifier_,
                min_stamp_,
                max_stamp_,
                max_versions_,
                limit_per_row_per_cf_,
                offset_per_row_per_cf_,
                filter_string_);
  }
  return len;
}

OB_DEF_DESERIALIZE(ObHTableFilter,)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              is_valid_,
              select_column_qualifier_,
              min_stamp_,
              max_stamp_,
              max_versions_,
              limit_per_row_per_cf_,
              offset_per_row_per_cf_,
              filter_string_);
  return ret;
}

////////////////////////////////////////////////////////////////
ObTableQueryResult::ObTableQueryResult()
    :row_count_(0),
     proxy_agg_data_buf_(),
     allocator_(ObModIds::TABLE_PROC),
     fixed_result_size_(0),
     curr_idx_(0),
     next_row_offset_(0),
     next_row_count_(0)
{
}

void ObTableQueryResult::reset_except_property()
{
  row_count_ = 0;
  buf_.reset();
  allocator_.reset();
  fixed_result_size_ = 0;
  curr_idx_ = 0;
  proxy_agg_data_buf_.reset();
}

void ObTableQueryResult::reset()
{
  properties_names_.reset();
  reset_except_property();
}

void ObTableQueryResult::rewind()
{
  curr_idx_ = 0;
  buf_.get_position() = 0;
}

/*int ObTableQueryResult::deep_copy(common::ObIAllocator &allocator, const ObTableQueryResult &other)
{
  int ret = OB_SUCCESS;
  ObString dest_string;

  for (int i = 0; OB_SUCC(ret) && i < other.properties_names_.count(); ++i) {
    if (OB_FAIL(ob_write_string(allocator, other.properties_names_.at(i), dest_string))) {
      LOG_WDIAG("fail to call ob_write_string", K(ret));
    } else if (OB_FAIL(properties_names_.push_back(dest_string))) {
      LOG_WDIAG("fail to call push_back", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    char *buff = NULL;
    int64_t databuff_len = other.buf_.get_capacity();
    if (databuff_len == 0) {
      buf_.set_data(NULL, 0);
    } else if (NULL == (buff = static_cast<char*>(allocator_.alloc(databuff_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no memory", K(ret), K(databuff_len));
    } else {
      // deep copy
      MEMCPY(buff, other.buf_.get_data(), databuff_len);
      buf_.set_data(buff, databuff_len);
      buf_.get_position() = other.buf_.get_position();
      buf_.get_limit() = other.buf_.get_limit();
    }

    if (OB_SUCC(ret)) {
      fixed_result_size_ = other.fixed_result_size_;
      row_count_ = other.row_count_;
      curr_idx_ = other.curr_idx_;
      next_row_count_ = other.next_row_count_;
      next_row_offset_ = other.next_row_offset_;

      if (OB_FAIL(curr_entity_.deep_copy(allocator, other.curr_entity_))) {
        LOG_WDIAG("fail to call deep_copy for ObTableEntity", K(ret));
      }
    }
  }

  return ret;
}
*/
/*int ObTableQueryResult::get_next_entity(const ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  if (0 >= properties_names_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid properties_names", K(ret));
  } else if (curr_idx_ >= row_count_) {
    ret = OB_ITER_END;
  } else {
    curr_entity_.reset();
    ObObj value;
    const int64_t N = properties_names_.count();
    for (int i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(value.deserialize(buf_.get_data(), buf_.get_capacity(), buf_.get_position()))) {
        LOG_WDIAG("failed to deserialize obj", K(ret), K_(buf));
      } else if (OB_FAIL(curr_entity_.set_property(properties_names_.at(i), value))) {
        LOG_WDIAG("failed to set entity property", K(ret), K(i), K(value));
      }
    } // end for
    if (OB_SUCC(ret)) {
      entity = &curr_entity_;
      ++curr_idx_;
    }
  }
  return ret;
}
*/
/*
int ObTableQueryResult::get_empty_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  // allocator_ alloc memmory for  ObObj
  common::ObNewRow *new_row = NULL;
  ObObj *obj = NULL;
  char *buf = NULL;
  if (OB_ISNULL(buf = (char *)allocator_.alloc(sizeof(common::ObNewRow)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("invalid to alloc memory for ObNewRow");
  } else {
    int64_t count = properties_names_.count();
    new_row = new (buf) ObNewRow();
    if (OB_FAIL(get_empty_obobj(obj, count))) {
      new_row = NULL;
      LOG_WDIAG("get_empty_row failed for init ObObj", K(ret));
    } else {
      new_row->assign(obj, count);
    }
  }

  if (OB_NOT_NULL(new_row)) {
    row = new_row;
  }

  return ret;
}
*/
int ObTableQueryResult::get_empty_obobj(ObObj *&obobj, int64_t count)
{
  int ret = OB_SUCCESS;
  // allocator_ alloc memmory for  ObObj
  ObObj *new_obobj = NULL;
  char *buf = NULL;
  if (OB_ISNULL(buf = (char *)allocator_.alloc(sizeof(ObObj) * count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("invalid to alloc memory for ObNewRow");
  } else {
    new_obobj = new (buf) ObObj();
    buf += sizeof(ObObj);

    for (int i = 0; i < count - 1; i++) {
      obobj = new (buf) ObObj();
      buf += sizeof(ObObj);
    }
  }

  if (OB_NOT_NULL(new_obobj)) {
    obobj = new_obobj;
  }

  return ret;
}
/*
int ObTableQueryResult::get_first_row(common::ObNewRow &row) const
{
  int ret = OB_SUCCESS;
  if (row.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid row object", K(ret));
  } else if (0 >= row_count_) {
    ret = OB_ITER_END;
  } else {
    const char *databuf = buf_.get_data();
    const int64_t datalen = buf_.get_position();
    int64_t pos = 0;
    const int64_t N = row.count_;
    for (int i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(row.cells_[i].deserialize(databuf, datalen, pos))) {
        LOG_WDIAG("failed to deserialize obj", K(ret), K(datalen), K(pos));
      }
    } // end for
  }
  return ret;
}

int ObTableQueryResult::get_next_row(common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (row.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid row object", K(ret));
  } else if (0 >= row_count_) {
    ret = OB_ITER_END;
  } else {
    const char *databuf = buf_.get_data() + next_row_offset_;
    const int64_t datalen = buf_.get_position() - next_row_offset_;
    int64_t pos = 0;
    const int64_t N = row.count_;
    for (int i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(row.cells_[i].deserialize(databuf, datalen, pos))) {
        LOG_WDIAG("failed to deserialize obj", K(ret), K(datalen), K(pos));
      }
    } // end for

    if (OB_SUCCESS == ret) { //has fetched an record
      next_row_offset_ += pos;
      next_row_count_++;
      LOG_DEBUG("get_next_row has fetched row", "count", next_row_count_, "offset", next_row_offset_,
                "sum len", datalen, "sum count", row_count_);
    }
  }
  return ret;
}
*/
/*
int ObTableQueryResult::add_property_name(const ObString &name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(properties_names_.push_back(name))) {
    LOG_WDIAG("failed to add name", K(ret), K(name));
  }
  return ret;
}
*/
int ObTableQueryResult::alloc_buf_if_need(const int64_t need_size)
{
  int ret = OB_SUCCESS;
  if (need_size <= 0 || need_size > MAX_BUF_BLOCK_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(need_size), LITERAL_K(MAX_BUF_BLOCK_SIZE));
  } else if (NULL == buf_.get_data()) { // first alloc
    int64_t actual_size = 0;
    if (need_size <= DEFAULT_BUF_BLOCK_SIZE) {
      actual_size = DEFAULT_BUF_BLOCK_SIZE;
    } else {
      actual_size = need_size;
    }
    char *tmp_buf = static_cast<char*>(allocator_.alloc(actual_size));
    if (NULL == tmp_buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no memory", K(ret), K(actual_size));
    } else {
      buf_.set_data(tmp_buf, actual_size);
    }
  } else if (buf_.get_remain() < need_size) {
    if (need_size + buf_.get_position() > MAX_BUF_BLOCK_SIZE) { // check max buf size when expand buf
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WDIAG("will exceed max buf need_size", K(ret), K(need_size), K(buf_.get_position()), LITERAL_K(MAX_BUF_BLOCK_SIZE));
    } else {
      int64_t actual_size = MAX(need_size + buf_.get_position(), 2 * buf_.get_capacity());
      actual_size = MIN(actual_size, MAX_BUF_BLOCK_SIZE);
      char *tmp_buf = static_cast<char*>(allocator_.alloc(actual_size));
      if (NULL == tmp_buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("no memory", K(ret), K(actual_size));
      } else {
        const int64_t old_buf_size = buf_.get_position();
        MEMCPY(tmp_buf, buf_.get_data(), old_buf_size);
        buf_.set_data(tmp_buf, actual_size);
        buf_.get_position() = old_buf_size;
      }
    }

  }
  return ret;
}

/*int ObTableQueryResult::add_row(const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  ret = alloc_buf_if_need(row.get_serialize_size());
  const int64_t N = row.get_count();
  if (OB_SUCC(ret)) {
    if (0 != properties_names_.count()
        && N != properties_names_.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("cell count not match with property count", K(ret), K(N),
               "properties_count", properties_names_.count());
    }
  }
  for (int i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    if (OB_ISNULL(row.get_cell(i)) || OB_FAIL(row.get_cell(i)->serialize(buf_.get_data(), buf_.get_capacity(), buf_.get_position()))) {
      LOG_WDIAG("failed to serialize obj", K(ret), K_(buf));
    }
  } // end for
  if (OB_SUCC(ret)) {
    ++row_count_;
  }
  return ret;
}
*/
int ObTableQueryResult::add_all_property_shallow_copy(const ObTableQueryResult &other)
{
  int ret = OB_SUCCESS;
  if (0 != properties_names_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid properties which has been initialized", K(ret));
  } else if (OB_FAIL(append(properties_names_, other.properties_names_))) {
    LOG_WDIAG("failed to append property", K(ret));
  }
  return ret;
}

/*int ObTableQueryResult::add_all_property_deep_copy(const ObTableQueryResult &other)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (0 != (count = properties_names_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid properties which has been initialized", K(ret), K(count));
  } else {  //if (OB_FAIL(append(properties_names_, other.properties_names_))) {
    count = other.get_property_count();
    for (int64_t i = 0; i < count; i++) {
      ObString name;
      ob_write_string(allocator_, other.properties_names_.at(i), name);
      properties_names_.push_back(name);
    }
    LOG_DEBUG("to append property by deep copy method", K(ret), K(count));
  }
  return ret;
}

int ObTableQueryResult::add_all_row_deep_copy(const ObTableQueryResult &other)
{
  int ret = OB_SUCCESS;
  // const common::ObDataBuffer &buf = other.get_buf();
  if (other.get_buf().get_capacity() > 0 && OB_FAIL(alloc_buf_if_need(other.get_buf().get_capacity()))) {
    LOG_WDIAG("failed to alloc memory", K(ret));
  } else if (buf_.get_remain() < other.get_buf().get_capacity()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    LOG_DEBUG("ObProxyRpcQueryOp::handle_response_result before", K_(buf), K(other.get_buf()));
    MEMCPY(buf_.get_cur_pos(), other.get_buf().get_data(), other.get_buf().get_capacity());
    buf_.get_position() += other.get_buf().get_capacity();
    row_count_ += other.row_count_;
    LOG_DEBUG("ObProxyRpcQueryOp::handle_response_result after", K_(buf), K(other.get_buf()));
  }
  return ret;
}
*/
int ObTableQueryResult::add_all_row_shallow_copy(const ObTableQueryResult &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(proxy_agg_data_buf_.push_back(other.get_buf()))) {
    LOG_WDIAG("fail to add resp buffer", K(ret));
  } else {
    row_count_ += other.row_count_;
  }
  return ret;
}

int64_t ObTableQueryResult::get_result_size()
{
  if (0 >= fixed_result_size_) {
    fixed_result_size_ = properties_names_.get_serialize_size();
    fixed_result_size_ += obrpc::ObRpcPacketHeader::HEADER_SIZE
                          + 8/*appr. row_count*/
                          + 8/*appr. buf_position*/;
  }
  return fixed_result_size_ + buf_.get_position();
}

/*bool ObTableQueryResult::reach_batch_size_or_result_size(const int32_t batch_count, const int64_t max_result_size)
{
  bool reach_size = false;
  if (batch_count > 0 && this->get_row_count() >= batch_count) {
    LOG_DEBUG("[yzfdebug] reach batch limit", K(batch_count));
    reach_size = true;
  } else if (max_result_size > 0 && this->get_result_size() >= max_result_size) {
    LOG_DEBUG("[yzfdebug] reach size limit", K(max_result_size));
    reach_size = true;
  }
  return reach_size;
}
*/
OB_DEF_SERIALIZE(ObTableQueryResult)
{
  int ret = OB_SUCCESS;
  int resp_buf_len = buf_.get_position();
  for(int i = 0; i < proxy_agg_data_buf_.count() && OB_SUCC(ret); i++) {
    resp_buf_len += proxy_agg_data_buf_.at(i).get_position();
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              properties_names_,
              row_count_,
              resp_buf_len);
  if (OB_SUCC(ret)) {
    if (buf_len - pos  + 1 < resp_buf_len) {
      LOG_WDIAG("failed to serialize ObTableQueryResult", K(ret), K(buf_len), K(pos), "datalen", resp_buf_len);
    } else if (buf_.get_position() > 0 &&  OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, buf_.get_data(), buf_.get_position()))) {
      LOG_WDIAG("fail to encode buf of query resp", K(ret));
    } else {
      for (int i = 0; i < proxy_agg_data_buf_.count() && OB_SUCC(ret); i++) {
        if (proxy_agg_data_buf_.at(i).get_position() > 0 &&
            OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, proxy_agg_data_buf_.at(i).get_data(),
                                                  proxy_agg_data_buf_.at(i).get_position()))) {
          LOG_WDIAG("fail to encode query result buffer", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableQueryResult)
{
  int64_t len = 0;
  int resp_buf_len = buf_.get_position(); 
  for (int i = 0; i < proxy_agg_data_buf_.count(); i++) {
    resp_buf_len += proxy_agg_data_buf_.at(i).get_position();
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, properties_names_, row_count_, resp_buf_len);
  len += resp_buf_len;
  return len;
}

OB_DEF_DESERIALIZE(ObTableQueryResult)
{
  int ret = OB_SUCCESS;
  allocator_.reset(); // deep copy all
  properties_names_.reset();
  curr_idx_ = 0;
  int64_t databuff_len = 0;
  int64_t properties_count = 0;
  OB_UNIS_DECODE(properties_count);
  if (OB_SUCC(ret)) {
    ObString property_name;
    ObString name_clone;
    for (int64_t i = 0; OB_SUCCESS == ret && i < properties_count; ++i) {
      OB_UNIS_DECODE(property_name);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ob_write_string(allocator_, property_name, name_clone))) {
          LOG_WDIAG("failed to deep copy string", K(ret), K(property_name));
        } else if (OB_FAIL(properties_names_.push_back(name_clone))) {
          LOG_WDIAG("failed to push back", K(ret));
        }
      }
    } // end for
  }
  LST_DO_CODE(OB_UNIS_DECODE, row_count_, databuff_len);

  // shallow copy
  char *origin_buf = const_cast<char *>(buf + pos);
  if (OB_FAIL(ret)) {
  } else if (databuff_len > data_len - pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("invalid data", K(ret), K(databuff_len), K(pos), K(data_len));
  } else if (databuff_len == 0) {
    buf_.set_data(NULL, 0);
  } else {
    // deep copy
    int64_t &buf_position = buf_.get_position();
    buf_.set_data(origin_buf, databuff_len);
    buf_position = databuff_len;
    pos += databuff_len;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
/*uint64_t ObTableQueryAndMutate::get_checksum()
{
  uint64_t checksum = 0;
  const uint64_t query_checksum = query_.get_checksum();
  const uint64_t mutation_checksum = mutations_.get_checksum();
  checksum = ob_crc64(checksum, &query_checksum, sizeof(query_checksum));
  checksum = ob_crc64(checksum, &mutation_checksum, sizeof(mutation_checksum));
  checksum = ob_crc64(checksum, &return_affected_entity_, sizeof(return_affected_entity_));
  return checksum;
}

int ObTableQueryAndMutate::deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableQueryAndMutate &other)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(query_.deep_copy(allocator, other.query_))) {
    LOG_WDIAG("fail to call deep_copy for ObTableQuery", K(ret));
  } else if (OB_FAIL(mutations_.deep_copy(allocator, entity_factory, other.mutations_))) {
    LOG_WDIAG("fail to call deep_copy for ObTableBatchOperation", K(ret));
  } else {
    return_affected_entity_ = other.return_affected_entity_;
  }

  return ret;
}
*/

OB_UNIS_DEF_SERIALIZE(ObTableQueryAndMutate, query_, mutations_, return_affected_entity_);
OB_UNIS_DEF_SERIALIZE_SIZE(ObTableQueryAndMutate, query_, mutations_, return_affected_entity_);


ODP_DEF_DESERIALIZE(ObTableQueryAndMutate)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rpc_request->init_rowkey_info(1))) {
    LOG_WDIAG("fail to init rpc request", K(ret));
  } else if (OB_FAIL(query_.deserialize(buf, data_len, pos, rpc_request))) {
    LOG_WDIAG("fail to deserialize ob table query", K(ret));
  } else {
    OB_UNIS_DECODE(mutations_);
    OB_UNIS_DECODE(return_affected_entity_);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTableQueryAndMutateResult,
                    affected_rows_,
                    affected_entity_);

/*int ObTableQueryAndMutateResult::deep_copy(common::ObIAllocator &allocator, const ObTableQueryAndMutateResult &other)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(affected_entity_.deep_copy(allocator, other.affected_entity_))) {
    LOG_WDIAG("fail to call deep_copy for ObTableQueryResult", K(ret));
  } else {
    affected_rows_ = other.affected_rows_;
  }

  return ret;
}*/

/****** move response info (start) ******/
OB_SERIALIZE_MEMBER(ObTableMoveReplicaInfo,
                    table_id_,
                    schema_version_,
                    part_id_,
                    server_,
                    role_,
                    replica_type_,
                    part_renew_time_,
                    reserved_);

OB_SERIALIZE_MEMBER(ObTableMoveResult,
                    replica_info_,
                    reserved_);

OB_SERIALIZE_MEMBER((ObTableQuerySyncResult, ObTableQueryResult), is_end_, query_session_id_);

int64_t ObTableMoveResult::get_serialize_size_v4(void) const
{
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);
  return len;
}

int64_t ObTableMoveResult::get_serialize_size_v4_(void) const
{
  int64_t len = 0;
  BASE_ADD_LEN(CLS);
  len += replica_info_.get_serialize_size_v4();

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              reserved_);
  return len;
}

int ObTableMoveResult::deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER(CLS, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_v4_(buf + pos_orig, len, pos))) {
      LOG_WDIAG("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableMoveResult::deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(CLS);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(replica_info_.deserialize_v4(buf, data_len, pos))) {
      LOG_WDIAG("deserialize_ fail",
               "slen", data_len, K(pos), K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              reserved_);
  return ret;
}

int64_t ObTableMoveReplicaInfo::get_serialize_size_v4(void) const
{
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);
  return len;
}

int64_t ObTableMoveReplicaInfo::get_serialize_size_v4_(void) const
{
  int64_t len = 0;
  BASE_ADD_LEN(CLS);

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_id_,
              schema_version_);
  len += 8;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              server_,
              role_,
              replica_type_,
              replica_type_,
              reserved_); 
  return len;
}

int ObTableMoveReplicaInfo::deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER(CLS, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_v4_(buf + pos_orig, len, pos))) {
      LOG_WDIAG("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableMoveReplicaInfo::deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(CLS);

  LST_DO_CODE(OB_UNIS_DECODE,
            table_id_,
            schema_version_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&part_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              server_,
              role_,
              replica_type_,
              part_renew_time_,
              reserved_);
  return ret;
}
/****** move response info (end) ******/

/****** direct response info (start) ******/
OB_SERIALIZE_MEMBER(ObTableDirectLoadResultHeader,
                    addr_,
                    operation_type_);

OB_SERIALIZE_MEMBER(ObTableDirectLoadResult,
                    header_,
                    res_content_); /* not need to serialize res_content*/
/****** direct response info (start) ******/

OB_UNIS_DEF_SERIALIZE(ObTableTabletOp,
                      tablet_id_,
                      option_flag_,
                      single_ops_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableTabletOp,
                           tablet_id_,
                           option_flag_,
                           single_ops_);


ODP_DEF_DESERIALIZE_HEADER(ObTableTabletOp)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER(CLS, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos, rpc_request))) {
      RPC_WARN("deserialize_ fail", "slen", len, K(pos), K(ret));
    }
    rpc_request->partition_id_position_ += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

ODP_DEF_DESERIALIZE_PAYLOAD(ObTableTabletOp)
{
  int ret = OB_SUCCESS;
  // record partition id pos and len
  rpc_request->partition_id_position_ = pos;
  OB_UNIS_DECODE(tablet_id_);
  rpc_request->partition_id_len_ = pos - rpc_request->partition_id_position_;

  LST_DO_CODE(OB_UNIS_DECODE, option_flag_);
  int64_t single_op_size = 0;
  OB_UNIS_DECODE(single_op_size);
  
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rpc_request->init_rowkey_info(single_op_size))) {
      LOG_WDIAG("fail to init rpc request", K(ret));
    } if (OB_FAIL(single_ops_.prepare_allocate(single_op_size))) {
      LOG_WDIAG("fail to prepare allocatate single ops", K(ret), K(single_op_size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < single_op_size; ++i) {
      char *origin_buf = const_cast<char*>(buf + pos);
      int64_t origin_pos = pos;
      OB_IGNORE_TABLE_SINGLE_OP &single_op = single_ops_.at(i);
      if (OB_FAIL(single_op.deserialize(buf, data_len, pos, rpc_request))) {
        LOG_WDIAG("fail to deseriazlie single op", K(ret));
      } else if (OB_FAIL(rpc_request->add_sub_req_buf(ObRpcFieldBuf(origin_buf, pos - origin_pos)))) {
        LOG_WDIAG("fail to add sub req buf", K(ret));
      }
    }  // end for
  }

  return ret;
}

int ObTableTabletOp::set_single_ops(const common::ObIArray<ObRpcFieldBuf> &single_op_buf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(single_ops_.prepare_allocate(single_op_buf.count()))) {
    PROXY_RPC_SM_LOG(WDIAG, "fail to reserve mem for single op buf", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < single_op_buf.count(); i++) {
      single_ops_.at(i) = (OB_IGNORE_TABLE_SINGLE_OP(single_op_buf.at(i).buf_, single_op_buf.at(i).buf_len_));
    }
  }
  return ret;
}

/*int ObTableTabletOp::get_single_ops(ObIArray<ObTableSingleOp> &single_ops, const ObIArray<int64_t> &indexes) const 
{
  int ret = OB_SUCCESS;
  for(int i = 0; i < indexes.count(); i++) {
    int index = indexes.at(i);
    if (index >= single_ops_.count() || index < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid index for single ops", K(ret), K(index), "single ops count", single_ops_.count());
    } else if (OB_FAIL(single_ops.push_back(single_ops_.at(index)))){
      LOG_WDIAG("fail to push back single ops", K(ret));
    }
  }
  return ret;
}
*/
void ObTableLSOp::reset()
{
  tablet_ops_.reset();
  ls_id_ = common::ObLSID::INVALID_LS_ID;
  option_flag_ = 0;
}

/*int64_t ObTableLSOp::get_single_op_count() const
{
  int64_t count = 0;
  int64_t tablet_op_count = tablet_ops_.count();
  for (int i = 0; i < tablet_op_count; i++) {
    count += tablet_ops_.at(i).count();
  }
  return count;
}*/

OB_UNIS_DEF_SERIALIZE(ObTableLSOp,
                      ls_id_,
                      table_name_,
                      table_id_,
                      rowkey_names_,
                      properties_names_,
                      option_flag_,
                      tablet_ops_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableLSOp,
                           ls_id_,
                           table_name_,
                           table_id_,
                           rowkey_names_,
                           properties_names_,
                           option_flag_,
                           tablet_ops_);

ODP_DEF_DESERIALIZE_HEADER(ObTableLSOp)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER(CLS, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos, rpc_request))) {
      RPC_WARN("deserialize_ fail", "slen", len, K(pos), K(ret));
    }
    rpc_request->table_id_position_ += pos_orig;
    rpc_request->partition_id_position_ += pos_orig;
    rpc_request->ls_id_postition_ += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

ODP_DEF_DESERIALIZE_PAYLOAD(ObTableLSOp)
{
  int ret = OB_SUCCESS;
  reset();
  // record ls id pos and len
  rpc_request->ls_id_postition_ = pos;
  OB_UNIS_DECODE(ls_id_);
  rpc_request->ls_id_len_ = pos - rpc_request->ls_id_postition_;

  OB_UNIS_DECODE(table_name_);

  // record table_id pos and len
  rpc_request->table_id_position_ = pos;
  OB_UNIS_DECODE(table_id_);
  rpc_request->table_id_len_ = pos - rpc_request->table_id_position_;

  LST_DO_CODE(OB_UNIS_DECODE,
              rowkey_names_,
              properties_names_,
              option_flag_);

  int64_t tablet_op_size = 0;
  OB_UNIS_DECODE(tablet_op_size);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_ops_.prepare_allocate(tablet_op_size))) {
      LOG_WDIAG("fail to prepare allocate tablet ops", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op_size; ++i) {
      ObTableTabletOp &tablet_op = tablet_ops_.at(i); 
      //tablet_op.set_entity_factory(entity_factory_);
      //tablet_op.set_deserialize_allocator(deserialize_alloc_);
      rpc_request->set_all_rowkey_names(&rowkey_names_);
      //tablet_op.set_dictionary(&rowkey_names_, &properties_names_);
      //tablet_op.set_is_ls_same_prop_name(is_same_properties_names_);
      if (OB_FAIL(tablet_op.deserialize(buf, data_len, pos, rpc_request))) {
        LOG_WDIAG("fail to deserialize tablet op", K(ret));
      }
    }  // end for
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableSingleOp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, op_type_, flag_);
  if (OB_SUCC(ret) && this->need_query()) {
    if (OB_ISNULL(op_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("query op is NULL", K(ret));
    } else {
      OB_UNIS_ENCODE(*op_query_);
    }
  }
  OB_UNIS_ENCODE(entities_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableSingleOp)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ADD_LEN, op_type_, flag_);
  if (OB_SUCC(ret) && this->need_query()) {
    if (OB_ISNULL(op_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("query op is NULL", K(ret));
    } else {
      OB_UNIS_ADD_LEN(*op_query_);
    }
  }
  OB_UNIS_ADD_LEN(entities_);
  return len;
}

ODP_DEF_DESERIALIZE(ObTableSingleOp)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  LST_DO_CODE(OB_UNIS_DECODE, op_type_, flag_);

  if (OB_SUCC(ret) && this->need_query()) {
    // decode op_query
    if (OB_ISNULL(op_query_ = OB_NEWx(OB_UNIS_IGNORE_TABLE_SINGLE_OP_QUERY, (&rpc_request->allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc memory", K(ret), K(sizeof(ObTableSingleOpQuery)));
    } else {
      if (OB_FAIL(op_query_->deserialize(buf, data_len, pos))) {
        LOG_WDIAG("fail to deserialize table query", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // decode entities
    int64_t entities_size = 0;
    OB_UNIS_DECODE(entities_size);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(entities_.prepare_allocate(entities_size))) {
        LOG_WDIAG("failed to prepare allocate single op entities", K(ret), K(entities_size));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < entities_size; ++i) {
        OB_IGNORE_TABLE_SINGLE_OP_ENTITY &op_entity = entities_.at(i);
        if (OB_FAIL(op_entity.deserialize(buf, data_len, pos, rpc_request))) {
          LOG_WDIAG("fail to deserialize table entity", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObTableSingleOp::reset()
{
  op_type_ = ObTableOperationType::INVALID;
  flag_ = 0;
  op_query_ = NULL;
  entities_.reset();
}

/*uint64_t ObTableSingleOp::get_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, &op_type_, sizeof(op_type_));
  checksum = ob_crc64(checksum, &flag_, sizeof(flag_));
  if (OB_NOT_NULL(op_query_)) {
    const uint64_t query_checksum = op_query_->get_checksum();
    checksum = ob_crc64(checksum, &query_checksum, sizeof(query_checksum));
  }

  for (int64_t i = 0; i < entities_.count(); i++) {
    const int64_t rowkey_size = entities_.at(i).get_rowkey_size();
    const int64_t property_count = entities_.at(i).get_properties_count();
    checksum = ob_crc64(checksum, &rowkey_size, sizeof(rowkey_size));
    checksum = ob_crc64(checksum, &property_count, sizeof(property_count));
  }

  return checksum;
}
*/
void ObTableSingleOpEntity::reset()
{
    rowkey_names_bp_.clear();
    properties_names_bp_.clear();
    ObTableEntity::reset();
}

/*int ObTableSingleOpEntity::deep_copy(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(deep_copy_rowkey(allocator, other))) {
    LOG_WDIAG("failed to deep copy rowkey", K(ret), K(other));
  } else if (OB_FAIL(deep_copy_properties(allocator, other))) {
    LOG_WDIAG("failed to deep copy properties", K(ret), K(other));
  } else {
    //this->all_rowkey_names_ = other.get_all_rowkey_names();
    //this->all_properties_names_ = other.get_all_properties_names();

    const ObTableBitMap *other_rowkey_bp = other.get_rowkey_names_bp();
    if (OB_ISNULL(other_rowkey_bp)) {
      LOG_WDIAG("failed to get_rowkey_names_bp", K(ret), K(other));
    } else if (OB_FAIL(rowkey_names_bp_.init_bitmap_size(other_rowkey_bp->get_valid_bits_num()))) {
      LOG_WDIAG("failed to init_bitmap_size", K(ret), KPC(other_rowkey_bp));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < other_rowkey_bp->get_block_count(); ++i) {
        ObTableBitMap::size_type block_val;
        if (OB_FAIL(other_rowkey_bp->get_block_value(i, block_val))) {
          LOG_WDIAG("failed to get_block_value", K(ret), K(i), KPC(other_rowkey_bp));
        } else if (OB_FAIL(rowkey_names_bp_.push_block_data(block_val))) {
          LOG_WDIAG("failed to push_block_data", K(ret), K(i), K(block_val), K(rowkey_names_bp_));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTableBitMap *other_prop_name_bp = other.get_properties_names_bp();
        if (OB_ISNULL(other_prop_name_bp)) {
          LOG_WDIAG("failed to get_properties_names_bp", K(ret), K(other));
        } else if (OB_FAIL(properties_names_bp_.init_bitmap_size(other_prop_name_bp->get_valid_bits_num()))) {
          LOG_WDIAG("failed to init_bitmap_size", K(ret), KPC(other_prop_name_bp));
        } else {
          for (int64_t i = 0; i < other_prop_name_bp->get_block_count(); ++i) {
            ObTableBitMap::size_type block_val;
            if (other_prop_name_bp->get_block_value(i, block_val)) {
              LOG_WDIAG("failed to get_block_value", K(ret), K(i), KPC(other_prop_name_bp));
            } else if (properties_names_bp_.push_block_data(block_val)) {
              LOG_WDIAG("failed to push_block_data", K(ret), K(i), K(block_val), K(properties_names_bp_));
            }
          }
        }
      }
    }
  }

  return ret;
}
*/

int ObTableSingleOpEntity::construct_names_bitmap(const ObITableEntity &req_entity)
{
  int ret = OB_SUCCESS;
  if (this->get_rowkey_size() != 0) {
    const ObTableBitMap *other_rowkey_bp = req_entity.get_rowkey_names_bp();
    if (OB_ISNULL(other_rowkey_bp)) {
      LOG_WDIAG("failed to get_rowkey_names_bp", K(ret), K(req_entity));
    } else {
      rowkey_names_bp_ = *other_rowkey_bp;
    }
  } else if (OB_FAIL(rowkey_names_bp_.init_bitmap_size(0))) {
    LOG_WDIAG("failed to init bitmap size", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (this->get_properties_count() != 0) {
      const ObTableBitMap *other_prop_name_bp =
          req_entity.get_properties_names_bp();
      if (OB_ISNULL(other_prop_name_bp)) {
        LOG_WDIAG("failed to get_properties_names_bp", K(ret), K(req_entity));
      } else {
        properties_names_bp_ = *other_prop_name_bp;
      }
    } else if (OB_FAIL(properties_names_bp_.init_bitmap_size(0))) {
      LOG_WDIAG("failed to init bitmap size", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableSingleOpEntity)
{
  int ret = OB_SUCCESS;
  if (!rowkey_names_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rowkey_names_bp is not init by rowkey_names", K(ret), K(rowkey_names_bp_), K(rowkey_names_));
  } else if (!properties_names_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("properties_names_bp is not init by prop_names", K(ret), K(properties_names_bp_),
      K(properties_names_), KPC(all_properties_names_));
  } else {
    // construct row_names_bp, properties_names_bp
    // msg_format: row_bp_count | row_names_bp | all_rowkey_names_count | rowkeys | all_properties_names_count |
    // properties_names_bp | properties_count | properties_values encode row_names_bp
    if (OB_FAIL(rowkey_names_bp_.serialize(buf, buf_len, pos))) {
      LOG_WDIAG("failed to encode rowkey_names_bp_", K(ret), K(rowkey_names_bp_), K(pos));
    } else {
      // encode rowkeys
      const int64_t rowkey_size = this->get_rowkey_size();
      OB_UNIS_ENCODE(rowkey_size);

      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_.count(); ++i) {
        const ObObj &obj = rowkey_.at(i);
        if (OB_FAIL(ObTableSerialUtil::serialize(buf, buf_len, pos, obj))) {
          LOG_WDIAG("fail to serialize table object", K(ret), K(buf), K(buf_len), K(pos));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(properties_names_bp_.serialize(buf, buf_len, pos))) {
        // encode properties_names_bp
        LOG_WDIAG("failed to encode properties_names_bp", K(ret), K(properties_names_bp_), K(pos));
      } else {
        // encode properties_values
        const int64_t properties_count = this->get_properties_count();
        OB_UNIS_ENCODE(properties_count);

        for (int64_t i = 0; OB_SUCC(ret) && i < properties_count; ++i) {
          const ObObj &obj = properties_values_.at(i);
          if (OB_FAIL(ObTableSerialUtil::serialize(buf, buf_len, pos, obj))) {
            LOG_WDIAG("fail to serialize table object", K(ret), K(buf), K(buf_len), K(pos));
          }
        }
      }
    }
  }

  return ret;
}

ODP_DEF_DESERIALIZE(ObTableSingleOpEntity)
{
  // msg_format: row_names_bp | rowkey_count | rowkeys | properties_names_bp | properties_count | properties_values
  int ret = OB_SUCCESS;
  reset();

  if (OB_ISNULL(rpc_request->get_all_rowkey_names())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObTableSingleOpEntity has not valid all_names", K(ret));
  } else {
    // decode rowkey_names_bp_, rowkeys
    if (OB_FAIL(rowkey_names_bp_.deserialize(buf, data_len, pos))) {
      LOG_WDIAG("failed to decode rowkey_names_bp", K(ret), K(pos));
    } else {
      int64_t rowkey_size = -1;
      OB_UNIS_DECODE(rowkey_size);
      // deserialize and record rowkey and properties info in lazy mode for single entity
      // we need row key and properties info to build bitmap of sub req
      if (OB_SUCC(ret)) {
        ROWKEY_VALUE rowkey_value;
        if (OB_FAIL(rowkey_value.prepare_allocate(rowkey_size))) {
          LOG_WDIAG("fail to pre allocate mem for rowkey value", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_size; ++i) {
          if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, rowkey_value.at(i)))) {
            LOG_WDIAG("fail to deserialize table object", K(ret), K(buf), K(data_len), K(pos));
          }
        }
        // don't need record rowkey value to single entity, ObTableSingleOp serialization will copy buf direcly
        // and ObTableSingleOp may contains multi entities, we only record the first entity, keep buf count equal to rowkey count
        if (OB_SUCC(ret)
            && rpc_request->get_sub_req_buf_arr()->count() == rpc_request->get_sub_req_rowkey_val_arr()->count()
            && OB_FAIL(rpc_request->add_sub_req_rowkey_val(rowkey_value))) {
          LOG_WDIAG("fail to add sub req rowkey val", K(ret));
        }
      }
    }

    // decode properties_names_bp
    if (OB_SUCC(ret)) {
      if (OB_FAIL(properties_names_bp_.deserialize(buf, data_len, pos))) {
        LOG_WDIAG("failed to decode properties_names_bp", K(ret), K(pos));
      } else {
        // decode properties_values
        int64_t properties_count = -1;
        OB_UNIS_DECODE(properties_count);
        if (OB_FAIL(ret)) {
        } else {
          ObObj value;
          // shallow copy
          for (int64_t i = 0; OB_SUCC(ret) && i < properties_count; ++i) {
            if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, value))) {
              LOG_WDIAG("fail to deserialize table object", K(ret), K(buf), K(data_len), K(pos));
            /*} else if (OB_FAIL(this->push_value(value))) {
              LOG_WDIAG("failed to push property values", K(ret), K(value));*/
            }
          }
        }
      }
    }

    // bitmap key to real val
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTableSingleOpEntity::construct_column_names(rowkey_names_bp_, *rpc_request->get_all_rowkey_names(), rowkey_names_))) {
        LOG_WDIAG("failed to construct rowkey names from bitmap", K(ret), K(rowkey_names_bp_),
          KPC(all_rowkey_names_), K(rowkey_names_));
      } else {
        if (OB_FAIL(rpc_request->add_sub_req_columns(rowkey_names_))) {
          LOG_WDIAG("fail to add sub req columns", K(ret));
        }
        // don't need record properties names
        /*if (is_same_properties_names_) {
          if (OB_FAIL(properties_names_.assign(*all_properties_names_))) {
            LOG_WDIAG("failed to assign properties_names", K(ret));
          }
        } else if (OB_FAIL(ObTableSingleOpEntity::construct_column_names(
                        properties_names_bp_, *all_properties_names_, properties_names_))) {
          LOG_WDIAG("failed to construct prop_names names from bitmap", K(ret), 
            K(properties_names_bp_), KPC(all_properties_names_), K(properties_names_));
        }*/
      }
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableSingleOpEntity)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (!rowkey_names_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rowkey_names_bp is not init by rowkey_names", K(ret), K(rowkey_names_bp_),
      K(rowkey_names_));
  } else if (!properties_names_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("properties_names_bp is not init by prop_names", K(ret), 
      K(properties_names_bp_), K(properties_names_), KPC(all_properties_names_));
  } else {
    // row_key_names_bp size
    len += rowkey_names_bp_.get_serialize_size();
    // row_keys size
    ObString key;
    ObObj value;
    const int64_t rowkey_size = get_rowkey_size();
    OB_UNIS_ADD_LEN(rowkey_size);
    for (int64_t i = 0; i < rowkey_size && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(this->get_rowkey_value(i, value))) {
        LOG_WDIAG("failed to get value", K(ret), K(i));
      }
      len += ObTableSerialUtil::get_serialize_size(value);
    }
    // properties_names_bp size
    len += properties_names_bp_.get_serialize_size();
    // prop_vals size
    const int64_t properties_count = this->get_properties_count();
    OB_UNIS_ADD_LEN(properties_count);

    for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
      const ObObj &obj = this->get_properties_value(i);
      len += ObTableSerialUtil::get_serialize_size(obj);
    }
  }

  return len;
}

  ////////////////////////////////////////////////

int ObTableBitMap::init_bitmap_size(int64_t valid_bits_num)
{
  int ret = OB_SUCCESS;
  int64_t block_nums = get_need_blocks_num(valid_bits_num);
  if (block_nums < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("block_nums less than zero", K(ret), K(block_nums));
  } else if (block_count_ >= 0) {
    if (block_nums != block_count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("bitmap had init before with diffrent val", K(ret), K(block_count_), K(block_nums));
    }
  } else if (OB_FAIL(datas_.reserve(block_nums))) {
    LOG_WDIAG("datas_ reserve failed", K(ret), K(block_nums));
  } else {
    block_count_ = block_nums;
    valid_bits_num_ = valid_bits_num;
  }

  return ret;
}

int ObTableBitMap::reset()
{
  int ret = OB_SUCCESS;

  if (block_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("bitmap is not init", K(ret), K(block_count_));
  } else {
    datas_.reset();
  }
  return ret;
}

int ObTableBitMap::push_block_data(size_type data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datas_.count() > block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG(
        "failed to push block data, block size less than datas size", K(block_count_), K(datas_.count()), K(data));
  } else if (OB_FAIL(datas_.push_back(data))) {
    LOG_WDIAG("failed to push block data", K(ret), K(datas_), K(data));
  }
  return ret;
}

int ObTableBitMap::get_true_bit_positions(ObIArray<int64_t> & true_pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datas_.count() != block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("datas_.count() is not equal to block_count_", K(ret), K(block_count_), K(datas_.count()));
  }
  for (int64_t block_index = 0; OB_SUCC(ret) && block_index < block_count_; ++block_index) {
    size_type byte;
    if (OB_FAIL(datas_.at(block_index, byte))) {
      LOG_WDIAG("failed to get datas", K(ret), K(datas_), K(block_index));
    } else if (byte != 0){
      int64_t new_begin_pos = block_index << BLOCK_MOD_BITS;
      for (int64_t bit_index = 0; OB_SUCC(ret) && bit_index < BITS_PER_BLOCK; ++bit_index) {
        if (byte & (1 << bit_index)) {
          if (OB_FAIL(true_pos.push_back(new_begin_pos + bit_index))) {
            LOG_WDIAG("failed to add true pos", K(ret), K(new_begin_pos + bit_index));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableBitMap::set(int64_t bit_pos)
{
  // datas_[bit_pos / 8] |= (1 << (bit_pos % 8));
  int ret = OB_SUCCESS;
  // bit_pos / 8
  int64_t bit_to_block_index = bit_pos >> BLOCK_MOD_BITS;
  if (bit_to_block_index >= block_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("bit_pos is overflow", K(ret), K(bit_pos), K(block_count_));
  } else if (OB_UNLIKELY(block_count_ != datas_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("block_count_ no equal to datas_.count()", K(ret), K(block_count_), K(datas_.count()));
  } else {
    size_type byte;
    if (OB_FAIL(datas_.at(bit_to_block_index, byte))) {
      LOG_WDIAG("failed get block val", K(ret), K(datas_), K(bit_to_block_index));
    } else {
      //  byte | (1 << (bit_pos % 8))
      datas_.at(bit_to_block_index) = (byte | (1 << (bit_pos & (BITS_PER_BLOCK - 1))));
    }
  }

  return ret;
}

int64_t ObTableBitMap::get_serialize_size() const
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (block_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("bitmap is not init", K(ret), K(block_count_));
  } else { 
    int64_t bit_count = get_valid_bits_num();
    OB_UNIS_ADD_LEN(bit_count);
    if (block_count_ == 0) {
      len += 0;
    } else {
      len += (block_count_ * sizeof(size_type));
    }
  }
  return len;
}

int ObTableBitMap::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (block_count_ != datas_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("block_count is not datas_.count() ", K(ret), K_(block_count), K_(datas));
  }
  const int64_t bit_count = get_valid_bits_num();
  OB_UNIS_ENCODE(bit_count);
  for (int64_t i = 0; OB_SUCCESS == ret && i < block_count_; ++i) {
    size_type block_val;
    if (OB_FAIL(this->get_block_value(i, block_val))) {
      LOG_WDIAG("failed to get block value", K(ret), K(i));
    } else {
      OB_UNIS_ENCODE(block_val);
    }
  }
  return ret;
}

int ObTableBitMap::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t bit_count;
  OB_UNIS_DECODE(bit_count);
  if (OB_FAIL(ret)) {
    // do noting
  } else if (OB_FAIL(init_bitmap_size(bit_count))) {
    LOG_WDIAG("fail to init bitmap", K(bit_count), K(ret));
  } else if (block_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("bitmap is not init", K(ret), K(block_count_));
  } else {
    size_type block_data;
    for (int64_t i = 0; OB_SUCCESS == ret && i < block_count_; ++i) {
      OB_UNIS_DECODE(block_data);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(this->push_block_data(block_data))) {
          LOG_WDIAG("failed to add table bitmap block data", K(ret), K(block_data));
        }
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObTableBitMap)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("valid_bits_num");
  J_COLON();
  BUF_PRINTO(valid_bits_num_);

  J_NAME("block_size");
  J_COLON();
  BUF_PRINTO(block_count_);

  J_NAME("block_data");
  J_COLON();
  BUF_PRINTO(datas_);

  J_OBJ_END();
  return pos;
}


OB_DEF_SERIALIZE(ObTableLSOpResult) {
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(properties_names_);
  OB_UNIS_ENCODE(tablet_op_result_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLSOpResult) {
  int64_t len = 0;
  OB_UNIS_ADD_LEN(properties_names_);
  OB_UNIS_ADD_LEN(tablet_op_result_);
  return len;
}

OB_DEF_DESERIALIZE(ObTableLSOpResult, )
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  LST_DO_CODE(OB_UNIS_DECODE, properties_names_);

  int64_t tablet_op_size = 0;
  OB_UNIS_DECODE(tablet_op_size);

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op_size; ++i) {
    ObTableTabletOpResult tablet_op_result;
    tablet_op_result.set_all_properties_names(&properties_names_);
    tablet_op_result.set_all_rowkey_names(&rowkey_names_);
    //tablet_op_result.set_allocator(alloc_);
    //tablet_op_result.set_entity_factory(entity_factory_);
    OB_UNIS_DECODE(tablet_op_result);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tablet_op_result_.push_back(tablet_op_result))) {
        LOG_WDIAG("failed to push back", K(ret));
      }
    }
  }  // end for
  return ret;
}

OB_DEF_SERIALIZE(ObTableTabletOpResult)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(single_op_result_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableTabletOpResult)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(single_op_result_);
  return len;
}

OB_DEF_DESERIALIZE(ObTableTabletOpResult,)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  int64_t single_op_size = 0;
  OB_UNIS_DECODE(single_op_size);
  for (int64_t i = 0; OB_SUCC(ret) && i < single_op_size; ++i) {
    ObTableSingleOpResult single_op_result;
    OB_UNIS_DECODE(single_op_result);
    if (OB_SUCC(ret) && OB_FAIL(single_op_result_.push_back(single_op_result))) {
      LOG_WDIAG("fail to add item to array", K(ret), K(i), K(single_op_size));
    }
  } // end for
  return ret;
}

OB_SERIALIZE_MEMBER(ObTableApiCredential,
                    cluster_id_,
                    tenant_id_,
                    user_id_,
                    database_id_,
                    expire_ts_,
                    hash_val_);

ObTableApiCredential::ObTableApiCredential()
  :cluster_id_(0),
   tenant_id_(0),
   user_id_(0),
   database_id_(0),
   expire_ts_(0),
   hash_val_(0)
{}

ObTableApiCredential::~ObTableApiCredential()
{}

OB_DEF_DESERIALIZE(ObTableSingleOpResult,)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, table_result_, operation_type_, single_entity_, affected_rows_);
  return ret;
}

OB_UNIS_DEF_SERIALIZE(ObTableSingleOpResult, table_result_, operation_type_, single_entity_, affected_rows_);
OB_UNIS_DEF_SERIALIZE_SIZE(ObTableSingleOpResult, table_result_, operation_type_, single_entity_, affected_rows_);
