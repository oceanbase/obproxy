/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX  LIB

#include "ob_number_format_models.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{
const int64_t MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS= 256;

const ObNFMKeyWord ObNFMElem::NFM_KEYWORDS[MAX_TYPE_NUMBER] =
{
  {",", NFM_COMMA, GROUPING_GROUP, 1},
  {".", NFM_PERIOD, GROUPING_GROUP, 1},
  {"$", NFM_DOLLAR, DOLLAR_GROUP, 1},
  {"0", NFM_ZERO, NUMBER_GROUP, 1},
  {"9", NFM_NINE, NUMBER_GROUP, 1},
  {"B", NFM_B, BLANK_GROUP, 0},
  {"C", NFM_C, CURRENCY_GROUP, 7},
  {"D", NFM_D, ISO_GROUPING_GROUP, 1},
  {"EEEE", NFM_EEEE, EEEE_GROUP, 5},
  {"G", NFM_G, ISO_GROUPING_GROUP, 1},
  {"L", NFM_L, CURRENCY_GROUP, 10},
  {"MI", NFM_MI, SIGN_GROUP, 1},
  {"PR", NFM_PR, SIGN_GROUP, 2},
  {"RN", NFM_RN, ROMAN_GROUP, 15},
  {"S", NFM_S, SIGN_GROUP, 1},
  {"TME", NFM_TME, TM_GROUP, 64},
  {"TM9", NFM_TM9, TM_GROUP, 64},
  {"TM", NFM_TM, TM_GROUP, 64},
  {"U", NFM_U, CURRENCY_GROUP, 10},
  {"V", NFM_V, MULTI_GROUP, 0},
  {"X", NFM_X, HEX_GROUP, 1},
  {"FM", NFM_FM, FILLMODE_GROUP, 0}
};

ObString ObNFMElem::get_elem_type_name() const
{
  ObString result;
  if (OB_ISNULL(keyword_) || is_valid_type(keyword_->elem_type_) || offset_ < 0) {
    result = ObString("INVALID ELEMENT TYPE");
  } else {
    result = ObNFMElem::NFM_KEYWORDS[keyword_->elem_type_].to_obstring();
  }
  return result;
}

int ObNFMDescPrepare::check_conflict_group(const ObNFMElem *elem_item,
                                           const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(elem_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid elem item", K(ret));
  } else {
    const ObNFMElem::ElementGroup elem_group = elem_item->keyword_->elem_group_;
    switch(elem_group) {
    case ObNFMElem::NUMBER_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::GROUPING_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_iso_grouping_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::ISO_GROUPING_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_grouping_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::DOLLAR_GROUP:
      if (ObNFMElem::has_type(NFM_DOLLAR_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_currency_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::CURRENCY_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_DOLLAR_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_currency_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::EEEE_GROUP:
      if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::ROMAN_GROUP:
      if (ObNFMElem::has_type(~NFM_FILLMODE_FLAG, fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::MULTI_GROUP:
      if (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::HEX_GROUP:
      if (ObNFMElem::has_type((~NFM_HEX_FLAG) & (~NFM_ZERO_FLAG) & (~NFM_NINE_FLAG)
                              & (~NFM_FILLMODE_FLAG) & (~NFM_COMMA_FLAG)
                              & (~NFM_G_FLAG), fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::SIGN_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_sign_group(fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::BLANK_GROUP:
      if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)
          || ObNFMElem::has_tm_group(fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::TM_GROUP:
      if (ObNFMElem::has_type(~NFM_FILLMODE_FLAG, fmt_desc.elem_flag_)) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("incompatible with other formats", K(ret), K(elem_group));
      }
      break;
    case ObNFMElem::FILLMODE_GROUP:
      // do nothing
      break;
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown group type", K(ret), K(elem_group));
    }
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_comma_is_valid(const ObNFMElem *elem_item,
                                                const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear in the integral part of a number
  // can't appear before the number
  // can't appear at the same time as element 'EEEE'
  // can't appear after the element 'V'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (-1 == fmt_desc.digital_start_
            || elem_item->offset_ >= fmt_desc.decimal_pos_
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem comma is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_period_is_valid(const ObNFMElem *elem_item,
                                                 const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can appear only once
  // can't appear at the same time as element 'V'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (fmt_desc.decimal_pos_ != INT32_MAX
            || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem period is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_dollar_is_valid(const ObNFMElem *elem_item,
                                                 const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can appear only once
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_DOLLAR_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem dollar is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_zero_is_valid(const ObNFMElem *elem_item,
                                               const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear after element 'EEEE' or element 'X'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem zero is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_nine_is_valid(const ObNFMElem *elem_item,
                                               const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear after element 'EEEE' or element 'X'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem nine is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_b_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can appear only once
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_BLANK_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem blank is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_c_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can appear only once
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_currency_group(fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem currency is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_d_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear at the same time as element 'V'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (fmt_desc.decimal_pos_ != INT32_MAX
            || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_HEX_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem d is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_eeee_is_valid(const ObNFMElem *elem_item,
                                               const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear at the same time as thousands separator
  // when element 'V' appears in front of all numbers, element 'EEEE' can't appear behind
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)
            || (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
            && fmt_desc.multi_ == fmt_desc.pre_num_count_)
            || ObNFMElem::has_type(NFM_COMMA_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem eeee is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_g_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear in the integral part of a number
  // can't appear before the number
  // can't appear after the element 'V'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (-1 == fmt_desc.digital_start_
            || elem_item->offset_ >= fmt_desc.decimal_pos_
            || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem g is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_l_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_C_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem l is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_mi_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear at the end of the fmt string
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_MI_FLAG, fmt_desc.elem_flag_)
            || !elem_item->is_last_elem_) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem mi is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_pr_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear at the end of the fmt string
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_PR_FLAG, fmt_desc.elem_flag_)
            || !elem_item->is_last_elem_) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem pr is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_rn_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem rn is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_s_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear at the front or the end of the fmt string
  // but 'FM' + 'S' is an exception
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc.elem_flag_)
            || (0 !=elem_item->offset_
            && (!elem_item->is_last_elem_
            && elem_item->prefix_type_ != ObNFMElem::NFM_FM))) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem s is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_tm_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_TM_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem tm is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_u_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_U_FLAG, fmt_desc.elem_flag_)
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem u is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_v_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can't appear at the same time as element '.' or element 'D'
  // can't appear after the element 'EEEE'
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)
            || fmt_desc.decimal_pos_ != INT32_MAX
            || ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem multi is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_x_is_valid(const ObNFMElem *elem_item,
                                            const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::NFM_NINE == elem_item->prefix_type_) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem multi is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::check_elem_fm_is_valid(const ObNFMElem *elem_item,
                                             const OBNFMDesc &fmt_desc)
{
  int ret = OB_SUCCESS;
  // can only appear at the front of the fmt string
  if (OB_FAIL(check_conflict_group(elem_item, fmt_desc))) {
    LOG_WDIAG("check conflict group failed", K(ret));
  } else if (ObNFMElem::has_type(NFM_FILLMODE_FLAG, fmt_desc.elem_flag_)
            || elem_item->offset_ != 0) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("check elem hex is invalid", K(ret));
  }
  return ret;
}

int ObNFMDescPrepare::fmt_desc_prepare(const common::ObSEArray<ObNFMElem*, 64> &fmt_elem_list,
                                       OBNFMDesc &fmt_desc, bool need_check/*true*/)
{
  int ret = OB_SUCCESS;

  for (int32_t i = 0; OB_SUCC(ret) && i < fmt_elem_list.count(); ++i) {
    const ObNFMElem *elem_item = fmt_elem_list.at(i);
    if (OB_ISNULL(elem_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid elem item", K(ret));
    } else {
      switch (elem_item->keyword_->elem_type_) {
      case ObNFMElem::NFM_COMMA:
        if (need_check && OB_FAIL(check_elem_comma_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem comma is valid", K(ret));
        } else {
          fmt_desc.last_separator_ = elem_item->offset_;
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_COMMA_FLAG;
        }
        break;
      case ObNFMElem::NFM_PERIOD:
        if (need_check && OB_FAIL(check_elem_period_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem period is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_PERIOD_FLAG;
          fmt_desc.decimal_pos_ = elem_item->offset_;
        }
        break;
      case ObNFMElem::NFM_DOLLAR:
        if (need_check && OB_FAIL(check_elem_dollar_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem dollar is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_DOLLAR_FLAG;
        }
        break;
      case ObNFMElem::NFM_ZERO:
        if (need_check && OB_FAIL(check_elem_zero_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem zero is valid", K(ret));
        } else {
          if (-1 == fmt_desc.digital_start_) {
            fmt_desc.digital_start_ = elem_item->offset_;
            fmt_desc.zero_start_ = elem_item->offset_;
          } else if (-1 == fmt_desc.zero_start_) {
            fmt_desc.zero_start_ = elem_item->offset_;
          }
          fmt_desc.zero_end_ = elem_item->offset_;
          fmt_desc.digital_end_ = elem_item->offset_;
          // whether the decimal point appears
          if (INT32_MAX == fmt_desc.decimal_pos_) {
            ++fmt_desc.pre_num_count_;
          } else {
            ++fmt_desc.post_num_count_;
          }
          // if element 'V' appears, count the number of digits after element 'V'
          if (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)) {
            ++fmt_desc.multi_;
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_ZERO_FLAG;
        }
        break;
      case ObNFMElem::NFM_NINE:
        if (need_check && OB_FAIL(check_elem_nine_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem nine is valid", K(ret));
        } else {
          if (-1 == fmt_desc.digital_start_) {
            fmt_desc.digital_start_ = elem_item->offset_;
          }
          fmt_desc.digital_end_ = elem_item->offset_;
          // whether the decimal point appears
          if (INT32_MAX == fmt_desc.decimal_pos_) {
            ++fmt_desc.pre_num_count_;
          } else {
            ++fmt_desc.post_num_count_;
          }
          // if element 'V' appears, count the number of digits after element 'V'
          if (ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_)) {
            ++fmt_desc.multi_;
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_NINE_FLAG;
        }
        break;
      case ObNFMElem::NFM_B:
        if (need_check && OB_FAIL(check_elem_b_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem blank is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_BLANK_FLAG;
        }
        break;
      case ObNFMElem::NFM_C:
        if (need_check && OB_FAIL(check_elem_c_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem c is valid", K(ret));
        } else {
          // if the iso currency ('C', 'L', 'U') does not appear in the first or last position of
          // the fmt string, it will also be treated as a decimal point
          // but element ('FM', 'S') + element ('C', 'L', 'U') is exception
          // element ('C', 'L', 'U') + element ('MI', 'PR', 'S') is exception
          if (0 == elem_item->offset_
            || (elem_item->prefix_type_ == ObNFMElem::NFM_FM
            || elem_item->prefix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::FIRST_POS;
          } else if (elem_item->is_last_elem_
                    || (elem_item->suffix_type_ == ObNFMElem::NFM_MI
                    || elem_item->suffix_type_ == ObNFMElem::NFM_PR
                    || elem_item->suffix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::LAST_POS;
          } else {
            // if decimal point has appeared, fmt is invalid
            if (need_check && (fmt_desc.decimal_pos_ != INT32_MAX
                || ObNFMElem::has_grouping_group(fmt_desc.elem_flag_)
                || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_))) {
              ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
              LOG_WDIAG("check elem currency is invalid", K(ret));
            } else {
              fmt_desc.decimal_pos_ = elem_item->offset_;
              fmt_desc.currency_appear_pos_ = OBNFMDesc::MIDDLE_POS;
            }
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_C_FLAG;
        }
        break;
      case ObNFMElem::NFM_D:
        if (need_check && OB_FAIL(check_elem_d_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem d is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_D_FLAG;
          fmt_desc.decimal_pos_ = elem_item->offset_;
        }
        break;
      case ObNFMElem::NFM_EEEE:
        if (need_check && OB_FAIL(check_elem_eeee_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem eeee is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_EEEE_FLAG;
        }
        break;
      case ObNFMElem::NFM_G:
        if (need_check && OB_FAIL(check_elem_g_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem g is valid", K(ret));
        } else {
          fmt_desc.last_separator_ = elem_item->offset_;
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_G_FLAG;
        }
        break;
      case ObNFMElem::NFM_L:
        if (need_check && OB_FAIL(check_elem_l_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem l is valid", K(ret));
        } else {
          // if the iso currency ('C', 'L', 'U') does not appear in the first or last position of
          // the fmt string, it will also be treated as a decimal point
          // but element ('FM', 'S') + element ('C', 'L', 'U') is exception
          // element ('C', 'L', 'U') + element ('MI', 'PR', 'S') is exception
          if (0 == elem_item->offset_
            || (elem_item->prefix_type_ == ObNFMElem::NFM_FM
            || elem_item->prefix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::FIRST_POS;
          } else if (elem_item->is_last_elem_
                    || (elem_item->suffix_type_ == ObNFMElem::NFM_MI
                    || elem_item->suffix_type_ == ObNFMElem::NFM_PR
                    || elem_item->suffix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::LAST_POS;
          } else {
            // if decimal point has appeared, fmt is invalid
            if (need_check && (fmt_desc.decimal_pos_ != INT32_MAX
                || ObNFMElem::has_grouping_group(fmt_desc.elem_flag_)
                || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_))) {
              ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
              LOG_WDIAG("check elem currency is invalid", K(ret));
            } else {
              fmt_desc.decimal_pos_ = elem_item->offset_;
              fmt_desc.currency_appear_pos_ = OBNFMDesc::MIDDLE_POS;
            }
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_L_FLAG;
        }
        break;
      case ObNFMElem::NFM_MI:
        if (need_check && OB_FAIL(check_elem_mi_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem mi is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_MI_FLAG;
        }
        break;
      case ObNFMElem::NFM_PR:
        if (need_check && OB_FAIL(check_elem_pr_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem pr is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_PR_FLAG;
        }
        break;
      case ObNFMElem::NFM_RN:
        if (need_check && OB_FAIL(check_elem_rn_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem rn is valid", K(ret));
        } else {
          if (elem_item->case_mode_ == ObNFMElem::UPPER_CASE) {
            fmt_desc.upper_case_flag_ |= NFM_RN_FLAG;
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_RN_FLAG;
        }
        break;
      case ObNFMElem::NFM_S:
        if (need_check && OB_FAIL(check_elem_s_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem s is valid", K(ret));
        } else {
          if (elem_item->is_last_elem_) {
            fmt_desc.sign_appear_pos_ = OBNFMDesc::LAST_POS;
          } else {
            fmt_desc.sign_appear_pos_ = OBNFMDesc::FIRST_POS;
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_S_FLAG;
        }
        break;
      case ObNFMElem::NFM_TME:
        if (need_check && OB_FAIL(check_elem_tm_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem tme is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_TME_FLAG;
        }
        break;
      case ObNFMElem::NFM_TM:
      case ObNFMElem::NFM_TM9:
        if (need_check && OB_FAIL(check_elem_tm_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem tm9 is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_TM_FLAG;
        }
        break;
      case ObNFMElem::NFM_U:
        if (need_check && OB_FAIL(check_elem_u_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem u is valid", K(ret));
        } else {
          // if the iso currency ('C', 'L', 'U') does not appear in the first or last position of
          // the fmt string, it will also be treated as a decimal point
          // but element ('FM', 'S') + element ('C', 'L', 'U') is exception
          // element ('C', 'L', 'U') + element ('MI', 'PR', 'S') is exception
          if (0 == elem_item->offset_
            || (elem_item->prefix_type_ == ObNFMElem::NFM_FM
            || elem_item->prefix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::FIRST_POS;
          } else if (elem_item->is_last_elem_
                    || (elem_item->suffix_type_ == ObNFMElem::NFM_MI
                    || elem_item->suffix_type_ == ObNFMElem::NFM_PR
                    || elem_item->suffix_type_ == ObNFMElem::NFM_S)) {
            fmt_desc.currency_appear_pos_ = OBNFMDesc::LAST_POS;
          } else {
            // if decimal point has appeared, fmt is invalid
            if (need_check && (fmt_desc.decimal_pos_ != INT32_MAX
                || ObNFMElem::has_grouping_group(fmt_desc.elem_flag_)
                || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc.elem_flag_))) {
              ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
              LOG_WDIAG("check elem currency is invalid", K(ret));
            } else {
              fmt_desc.decimal_pos_ = elem_item->offset_;
              fmt_desc.currency_appear_pos_ = OBNFMDesc::MIDDLE_POS;
            }
          }
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_U_FLAG;
        }
        break;
      case ObNFMElem::NFM_V:
        if (need_check && OB_FAIL(check_elem_v_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem v is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_MULTI_FLAG;
        }
        break;
      case ObNFMElem::NFM_X:
        if (0 == fmt_desc.elem_x_count_
            && elem_item->case_mode_ == ObNFMElem::UPPER_CASE) {
          fmt_desc.upper_case_flag_ |= NFM_HEX_FLAG;
        }
        if (need_check && OB_FAIL(check_elem_x_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem hex is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_HEX_FLAG;
          fmt_desc.elem_x_count_++;
        }
        break;
      case ObNFMElem::NFM_FM:
        if (need_check && OB_FAIL(check_elem_fm_is_valid(elem_item, fmt_desc))) {
          LOG_WDIAG("check elem fm is valid", K(ret));
        } else {
          fmt_desc.output_len_ += elem_item->keyword_->output_len_;
          fmt_desc.elem_flag_ |= NFM_FILLMODE_FLAG;
        }
        break;
      default :
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknown elem type", K(ret), K(elem_item->keyword_->elem_type_));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc.elem_flag_)) {
      fmt_desc.output_len_ = fmt_desc.output_len_ - fmt_desc.pre_num_count_ + 1;
    }
    if (!ObNFMElem::has_sign_group(fmt_desc.elem_flag_)
      && !ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc.elem_flag_)) {
      // if no sign or element 'RN' appears, add a sign character
      fmt_desc.output_len_ += 1;
    }
  }
  return ret;
}

const char *const ObNFMBase::LOWER_RM1[9] =
{"i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix"};
const char *const ObNFMBase::LOWER_RM10[9] =
{"x", "xx", "xxx", "xl", "l", "lx", "lxx", "lxxx", "xc"};
const char *const ObNFMBase::LOWER_RM100[9] =
{"c", "cc", "ccc", "cd", "d", "dc", "dcc", "dccc", "cm"};
const char *const ObNFMBase::UPPER_RM1[9] =
{"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX"};
const char *const ObNFMBase::UPPER_RM10[9] =
{"X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC"};
const char *const ObNFMBase::UPPER_RM100[9] =
{"C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM"};

int ObNFMBase::search_keyword(const char *cur_ch, const int32_t remain_len,
                              const ObNFMKeyWord *&match_key,
                              ObNFMElem::ElemCaseMode &case_mode) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_ch) || remain_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid fmt str", K(ret), K(fmt_str_), K(remain_len));
  } else {
    // if the first character of an element is lowercase, it is interpreted as lowercase
    // otherwise, it is interpreted as uppercase
    if ((*cur_ch >= 'a') && (*cur_ch <= 'z')) {
      case_mode = ObNFMElem::LOWER_CASE;
    } else if ((*cur_ch >= 'A') && (*cur_ch <= 'Z')) {
      case_mode = ObNFMElem::UPPER_CASE;
    } else {
      case_mode = ObNFMElem::IGNORE_CASE;
    }
    int32_t index = 0;
    for (index = 0; index < ObNFMElem::MAX_TYPE_NUMBER; ++index) {
      const ObNFMKeyWord &keyword = ObNFMElem::NFM_KEYWORDS[index];
      if (remain_len >= keyword.len_
          && (0 == strncasecmp(cur_ch, keyword.ptr_, keyword.len_))) {
        match_key = &keyword;
        break;
      }
    }
    if (ObNFMElem::MAX_TYPE_NUMBER == index) {
      ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
      LOG_WDIAG("invalid fmt character ", K(ret), K(fmt_str_), K(remain_len));
    }
  }
  return ret;
}

int ObNFMBase::parse_fmt(const char* fmt_str, const int32_t fmt_len, bool need_check/*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fmt_str) || fmt_len <= 0) {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WDIAG("invalid fmt str", K(ret), K(fmt_len));
  } else {
    fmt_str_.assign_ptr(fmt_str, fmt_len);
    int32_t remain_len = fmt_len;
    int32_t index = 0;
    const char* cur_ch = fmt_str;
    const char* fmt_end = fmt_str + fmt_len;
    ObNFMElem *last_elem = NULL;
    ObNFMElem::ElementType prefix_type = ObNFMElem::INVALID_TYPE;
    for (index = 0; OB_SUCC(ret) && remain_len > 0 && cur_ch < fmt_end; ++index) {
      char *elem_buf = NULL;
      const ObNFMKeyWord *match_keyword = NULL;
      ObNFMElem::ElemCaseMode case_mode = ObNFMElem::IGNORE_CASE;
      if (OB_FAIL(search_keyword(cur_ch, remain_len, match_keyword, case_mode))) {
        LOG_WDIAG("fail to search match keyword", K(ret), K(fmt_str_), K(remain_len));
      } else if (OB_ISNULL(elem_buf = static_cast<char*>(allocator_.alloc(sizeof(ObNFMElem))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc memory");
      } else {
        ObNFMElem *cur_elem = new(elem_buf) ObNFMElem;
        cur_elem->keyword_ = match_keyword;
        cur_elem->offset_ = index;
        cur_elem->case_mode_ = case_mode;
        cur_elem->prefix_type_ = prefix_type;
        if (OB_NOT_NULL(last_elem)) {
          last_elem->suffix_type_ = cur_elem->keyword_->elem_type_;
        }
        prefix_type = cur_elem->keyword_->elem_type_;
        last_elem = cur_elem;
        cur_ch += cur_elem->keyword_->len_;
        remain_len -= cur_elem->keyword_->len_;
        if (remain_len <= 0) {
          cur_elem->is_last_elem_ = true;
        }
        if (OB_FAIL(fmt_elem_list_.push_back(cur_elem))) {
          LOG_WDIAG("fail to push back elem to fmt_elem_list", K(ret));
        }
      }
    }
    // fill format desc
    if (OB_SUCC(ret) && OB_FAIL(ObNFMDescPrepare::fmt_desc_prepare(fmt_elem_list_,
                                                                   fmt_desc_,
                                                                   need_check))) {
      LOG_WDIAG("fail to prepare format desc", K(ret));
    }
  }
  return ret;
}

int ObNFMBase::check_hex_str_valid(const char *str,
                                   const int32_t str_len,
                                   const int32_t offset) const
{
  int ret = OB_SUCCESS;
  int32_t hex_str_len = 0;
  int32_t fmt_len = fmt_desc_.elem_x_count_ + fmt_desc_.pre_num_count_;
  for (int32_t i = offset; OB_SUCC(ret) && i < str_len; i++) {
    if ((str[i] >= '0' && str[i] <= '9')
        || (str[i] >= 'a' && str[i] <= 'f')
        || (str[i] >= 'A' && str[i] <= 'F')) {
      ++hex_str_len;
    } else {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WDIAG("invalid hex character", K(ret));
    }
  }
  if (OB_SUCC(ret) && hex_str_len > fmt_len) {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WDIAG("overflowed digit format", K(ret), K(hex_str_len), K(fmt_len));
  }
  return ret;
}

int ObNFMBase::get_integer_part_len(const common::ObString &num_str,
                                    int32_t &integer_part_len) const
{
  int ret = OB_SUCCESS;
  const char *ptr = num_str.ptr();
  integer_part_len = 0;
  bool digit_appear = false;
  for (int32_t i = 0; i < num_str.length(); ++i) {
    if (ptr[i] >= '0' && ptr[i] <= '9') {
      integer_part_len++;
      digit_appear = true;
    } else if ('.' == ptr[i] || 'E' == ptr[i]) {
      // if it's scientific notation
      // eg: to_number('1E+00', '9BEEEE') --> integer_part_len = 1
      break;
    } else if (digit_appear && ptr[i] != ',') {
      // to_number('USD123.123', 'c999.999')
      // to_number('23USD00', '99C99') --> integer_part_len = 2
      break;
    }
  }
  return ret;
}

bool ObNFMBase::is_digit(const char c) const
{
  return c >= '0' && c <= '9';
}

bool ObNFMBase::is_zero(const common::ObString &num_str) const
{
  const char *ptr = num_str.ptr();
  for (int32_t i = 0; i < num_str.length(); i++) {
    if (ptr[i] != '-' && ptr[i] != '.' && ptr[i] != '0') {
      return false;
    }
  }
  return true;
}

int ObNFMBase::hex_to_num(const char c, int32_t &val) const
{
  int ret = OB_SUCCESS;
  if (c >= '0' && c <= '9') {
    val = c - '0';
  } else if (c >= 'a' && c <= 'f') {
    val = c - 'a' + 10;
  } else if (c >= 'A' && c <= 'F') {
    val = c - 'A' + 10;
  } else {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WDIAG("invalid hex character", K(ret));
  }
  return ret;
}


void ObNFMBase::set_nls_currency(const common::ObString &nls_currency, const common::ObString &nls_iso_currency, const common::ObString &nls_dual_currency)
{
  if (ObNFMElem::has_type(NFM_C_FLAG, fmt_desc_.elem_flag_)) {
    fmt_desc_.nls_currency_ = nls_iso_currency;
  } else if (ObNFMElem::has_type(NFM_L_FLAG, fmt_desc_.elem_flag_)) {
    fmt_desc_.nls_currency_ = nls_currency;
  } else if (ObNFMElem::has_type(NFM_U_FLAG, fmt_desc_.elem_flag_)) {
    fmt_desc_.nls_currency_ = nls_dual_currency;
  }
}

void ObNFMBase::set_iso_grouping(const common::ObString &nls_numeric_characters)
{
  fmt_desc_.iso_grouping_ = nls_numeric_characters;
}

int ObNFMBase::remove_leading_zero(char *buf, int64_t &offset)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid buf");
  } else {
    bool is_negative = false;
    int32_t pos = 0;
    if (offset > 0 && '-' == buf[0]) {
      is_negative = true;
      pos++;
    }
    int32_t leading_zero = 0;
    for (; pos < offset; ++pos) {
      if ('0' == buf[pos]) {
        ++leading_zero;
      } else {
        break;
      }
    }
    if (leading_zero > 0) {
      if (is_negative) {
        MEMMOVE(buf + 1, buf + leading_zero + 1, offset - leading_zero);
      } else {
        MEMMOVE(buf, buf + leading_zero, offset - leading_zero);
      }
      offset -= leading_zero;
    }
  }
  return ret;
}

int ObNFMToNumber::process_output_fmt(const ObString &in_str,
                                      const int32_t integer_part_len,
                                      common::ObString &num_str)
{
  int ret = OB_SUCCESS;
  const char *str = in_str.ptr();
  int32_t str_len = in_str.length();
  int32_t str_pos = 0;
  int64_t offset = 0;
  bool is_negative = false;
  char *buf = NULL;
  const int64_t buf_len = MAX_TO_CHAR_BUFFER_SIZE_IN_FORMAT_MODELS;
  // skip leading spaces
  while (str_pos < str_len && ' ' == str[str_pos]) {
    ++str_pos;
  }
  // reserve the positive and negative sign position
  ++offset;
  // eg: to_number(' ', 'B') --> 0
  if (str_pos == str_len
      && (!ObNFMElem::has_type(NFM_BLANK_FLAG, fmt_desc_.elem_flag_)
      || str_len != fmt_elem_list_.count())) {
    ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
    LOG_WDIAG("invalid number", K(ret), K(in_str));
  } else if (OB_ISNULL(buf = static_cast<char *>(
                allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory", K(ret));
  } else if (ObNFMElem::has_sign_group(fmt_desc_.elem_flag_)) {
    // processing leading positive or negative sign element
    if (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc_.elem_flag_)
        && OBNFMDesc::FIRST_POS == fmt_desc_.sign_appear_pos_) {
      if (str_pos >= str_len) {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str));
      } else if ('-' == str[str_pos]) {
        is_negative = true;
        ++str_pos;
      } else if ('+' == str[str_pos]) {
        ++str_pos;
      } else {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str));
      }
    } else if (ObNFMElem::has_type(NFM_PR_FLAG, fmt_desc_.elem_flag_)) {
      if (str_pos < str_len) {
        if ('<' == str[str_pos]) {
          is_negative = true;
          ++str_pos;
        }
      }
    }
  } else {
    if (str_pos < str_len) {
      if ('-' == str[str_pos]) {
        is_negative = true;
        ++str_pos;
      }
    }
  }
  // processing element '$'
  if (OB_SUCC(ret) && ObNFMElem::has_type(NFM_DOLLAR_FLAG, fmt_desc_.elem_flag_)) {
    if (str_pos >= str_len || str[str_pos++] != '$'
       || (0 == fmt_desc_.pre_num_count_ && 0 == fmt_desc_.post_num_count_)) {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str));
    }
  }
  // processing the last positive or negative sign element
  if (OB_SUCC(ret) && ObNFMElem::has_sign_group(fmt_desc_.elem_flag_)) {
    if (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc_.elem_flag_)
        && OBNFMDesc::LAST_POS == fmt_desc_.sign_appear_pos_) {
      if ('-' == str[str_len - 1]) {
        is_negative = true;
        --str_len;
      } else if ('+' == str[str_len - 1]) {
        --str_len;
      } else {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str));
      }
    } else if (ObNFMElem::has_type(NFM_PR_FLAG, fmt_desc_.elem_flag_)) {
      if (is_negative) {
        if (str[str_len - 1] != '>') {
          ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
          LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str));
        }
        --str_len;
      }
    } else if (ObNFMElem::has_type(NFM_MI_FLAG, fmt_desc_.elem_flag_)) {
      if (is_negative) {
        if ('-' == str[str_len - 1]) {
          is_negative = true;
          --str_len;
        } else {
          ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
          LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str));
        }
      } else {
        if (' ' == str[str_len - 1]) {
          --str_len;
        } else if ('-' == str[str_len - 1]) {
          is_negative = true;
          --str_len;
        } else if (!ObNFMElem::has_type(NFM_FILLMODE_FLAG, fmt_desc_.elem_flag_)) {
          ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
          LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // need to skip leading and trailing redundant numbers in the fmt string
    // eg: to_number('123.123', '999999999.999999999')
    // pre_need_skip_num = 6;
    const int32_t pre_need_skip_num = fmt_desc_.pre_num_count_ - integer_part_len;
    int32_t traversed_num_count = 0;
    for (int32_t i = 0; OB_SUCC(ret) && i < fmt_elem_list_.count(); ++i) {
      const ObNFMElem *elem_item = fmt_elem_list_.at(i);
      if (OB_ISNULL(elem_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid elem item", K(ret));
      } else {
        ObNFMElem::ElementType elem_type = elem_item->keyword_->elem_type_;
        // skip thousands separator
        // eg: to_number('123,12,12,', '99999999999999') --> 1231212
        // eg: to_number('1,,,.235', '9GGD999') --> 1.235
        if (elem_type != ObNFMElem::NFM_COMMA && elem_type != ObNFMElem::NFM_G) {
          if (traversed_num_count > 0
              && traversed_num_count <= fmt_desc_.pre_num_count_
              && fmt_desc_.last_separator_ < i) {
            while (str_pos < str_len && ((',' == str[str_pos])
                  || (ObNFMElem::has_iso_grouping_group(fmt_desc_.elem_flag_)
                  && str[str_pos] == fmt_desc_.iso_grouping_.ptr()[1]))) {
              ++str_pos;
            }
          }
        }
        switch (elem_type) {
        case ObNFMElem::NFM_COMMA:
          if (traversed_num_count <= pre_need_skip_num
              && ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
            LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
            K(str_pos), K(i));
          } else if (traversed_num_count <= pre_need_skip_num
              && (fmt_desc_.zero_start_ < 0 || fmt_desc_.zero_start_ > i)) {
            // skip, do nothing
          } else if (str_pos >= str_len || str[str_pos] != ',') {
            ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
            LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),K(str_pos), K(i));
          } else {
            ++str_pos;
          }
          break;
        case ObNFMElem::NFM_PERIOD:
          // ignore decimal point
          // eg: to_number(1265, '0000.000')
          if (str_pos >= str_len) {
            // do nothing
          } else {
            if (str[str_pos] != '.') {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos]);
              ++str_pos;
            }
          }
          break;
        case ObNFMElem::NFM_ZERO:
          // if there is fractional part and the number of decimal parts exceeds the precision
          // the following numbers need to be skipped
          // eg: to_number('123.123', '999999999.999999999')
          if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            if (fmt_desc_.pre_num_count_ - traversed_num_count > 1
              || (fmt_desc_.decimal_pos_ != INT32_MAX
              && fmt_desc_.decimal_pos_ < i
              && str_pos >= str_len)) {
              // do nothing
            } else if (str_pos >= str_len || !is_digit(str[str_pos])) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos++]);
            }
          } else {
            if (fmt_desc_.decimal_pos_ != INT32_MAX
                && fmt_desc_.decimal_pos_ < i
                && str_pos >= str_len) {
              // do nothing
            } else if (str_pos >= str_len || !is_digit(str[str_pos])) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              if (traversed_num_count < pre_need_skip_num) {
                str_pos++;
              } else {
                ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos++]);
              }
            }
          }
          ++traversed_num_count;
          break;
        case ObNFMElem::NFM_NINE:
          if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            if (fmt_desc_.pre_num_count_ - traversed_num_count > 1
              || (fmt_desc_.decimal_pos_ != INT32_MAX
              && fmt_desc_.decimal_pos_ < i
              && str_pos >= str_len)) {
              // do nothing
            } else {
              if (str_pos >= str_len || !is_digit(str[str_pos])) {
                // do nothing
              } else {
                ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos++]);
              }
            }
          } else {
            // if there is fractional part and the number of decimal parts exceeds the precision
            // the following numbers need to be skipped
            // eg: to_number('123.123', '999999999.999999999')
            if (fmt_desc_.decimal_pos_ != INT32_MAX
                && fmt_desc_.decimal_pos_ < i
                && str_pos >= str_len) {
              // do nothing
            } else if (traversed_num_count < pre_need_skip_num) {
              // if element '0' has appeared, the following numbers need to fill '0'
              // eg: to_char(123.123, '9099999.999') --> 000123.123
              // eg: to_number('000123.123', '9099999.999') --> 123.123
              if ((fmt_desc_.zero_start_ >= 0 && fmt_desc_.zero_start_ < i)) {
                if (str_pos >= str_len || !is_digit(str[str_pos])) {
                  ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
                  LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
                  K(str_pos), K(i));
                } else {
                  ++str_pos;
                }
              }
            } else {
              if (str_pos >= str_len || !is_digit(str[str_pos])) {
                ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
                LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
                K(str_pos), K(i));
              } else {
                ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos++]);
              }
            }
          }
          ++traversed_num_count;
          break;
        case ObNFMElem::NFM_D:
          // ignore decimal point
          // eg: to_number(1265, '0000.000')
          if (str_pos >= str_len) {
            // do nothing
          } else {
            if (str[str_pos] != fmt_desc_.iso_grouping_.ptr()[0]) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              ret = databuff_printf(buf, buf_len, offset, "%c", str[str_pos]);
              ++str_pos;
            }
          }
          break;
        case ObNFMElem::NFM_G:
          if (traversed_num_count <= pre_need_skip_num
              && ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
            ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
            LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
            K(str_pos), K(i));
          } else if (traversed_num_count <= pre_need_skip_num
              && (fmt_desc_.zero_start_ < 0 || fmt_desc_.zero_start_ > i)) {
            // skip, do nothing
          } else {
            if (str_pos >= str_len || str[str_pos] != fmt_desc_.iso_grouping_.ptr()[1]) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              ++str_pos;
            }
          }
          break;
        case ObNFMElem::NFM_C:
        case ObNFMElem::NFM_L:
        case ObNFMElem::NFM_U:
          if (OBNFMDesc::MIDDLE_POS == fmt_desc_.currency_appear_pos_) {
            if (str_pos >= str_len) {
              // do nothing
            } else {
              if ((str[str_pos] != '.'
                  && (str_len - str_pos < fmt_desc_.nls_currency_.length()
                  || (0 != strncasecmp(str + str_pos, fmt_desc_.nls_currency_.ptr(),
                  fmt_desc_.nls_currency_.length()))))
                  || (ObNFMElem::has_type(NFM_S_FLAG, fmt_desc_.elem_flag_)
                  && OBNFMDesc::LAST_POS == fmt_desc_.sign_appear_pos_)) {
                // eg: to_number('23$00+', '99L99S') --> error 1722
                ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
                LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
                K(str_pos), K(i));
              } else {
                if (str[str_pos] == '.') {
                  ++str_pos;
                } else {
                  str_pos += fmt_desc_.nls_currency_.length();
                }
                // if the element('C', 'L', 'U') appears in the middle, it will be regarded as the
                // decimal point, so use decimal point to replace the iso currency
                ret = databuff_printf(buf, buf_len, offset, "%c", '.');
              }
            }
          } else {
            if (str_pos >= str_len || str_len - str_pos < fmt_desc_.nls_currency_.length()
              || (0 != strncasecmp(str + str_pos, fmt_desc_.nls_currency_.ptr(),
              fmt_desc_.nls_currency_.length()))) {
              ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
              LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
              K(str_pos), K(i));
            } else {
              str_pos += fmt_desc_.nls_currency_.length();
            }
          }
          break;
        case ObNFMElem::NFM_EEEE:
          if (str_pos < str_len && str[str_pos] != 'e' && str[str_pos] != 'E') {
            ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
            LOG_WDIAG("fmt does not match", K(ret), K(in_str), K_(fmt_str),
            K(str_pos), K(i));
          }
          // eg: to_number('1.2E+03 ', '9.9EEEEMI')
          while (str_pos < str_len && str[str_pos] != ' ') {
            buf[offset++] = str[str_pos++];
          }
          break;
        default:
          break;
        }
      }
    }
  }
  // eg: to_number('123.', '999') --> 123
  // eg: to_number('123,', '999') --> 123
  for (; OB_SUCC(ret) && str_pos < str_len; ++str_pos) {
    if (str[str_pos] != ',' && str[str_pos] != '.') {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WDIAG("fmt does not match", K(ret), K(str_pos), K(str_len), K(in_str), K_(fmt_str));
    }
  }
  if (OB_SUCC(ret)) {
    if (is_negative) {
      buf[0] = '-';
    } else {
      buf[0] = '+';
    }
    // if string is empty, set num_str to '+0' or '-0' as a valid number.
    if (offset == 1) {
      buf[offset++] = '0';
    }
    if (offset > INT_MAX32 || offset < 0) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      num_str.assign(buf, static_cast<int32_t>(offset));
    }
  }
  return ret;
}

int ObNFMToNumber::process_fmt_conv(const common::ObString &nls_currency,
                                    const common::ObString &nls_iso_currency,
                                    const common::ObString &nls_dual_currency,
                                    const common::ObString &nls_numeric_characters,
                                    const common::ObString &in_str,
                                    const common::ObString &in_fmt_str,
                                    common::ObIAllocator &alloc,
                                    common::number::ObNumber &res_num)
{
  int ret = OB_SUCCESS;
  int32_t integer_part_len = 0;
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  ObString num_str;
  if (OB_FAIL(parse_fmt(in_fmt_str.ptr(), in_fmt_str.length()))) {
    LOG_WDIAG("fail to parse format model", K(ret), K(in_fmt_str));
  } else if (ObNFMElem::has_type(NFM_RN_FLAG, fmt_desc_.elem_flag_)
        || ObNFMElem::has_type(NFM_TM_FLAG, fmt_desc_.elem_flag_)
        || ObNFMElem::has_type(NFM_TME_FLAG, fmt_desc_.elem_flag_)
        || ObNFMElem::has_type(NFM_MULTI_FLAG, fmt_desc_.elem_flag_)) {
      ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
      LOG_WDIAG("not support elem type", K(ret));
  } else if (OB_FAIL(get_integer_part_len(in_str, integer_part_len))) {
    LOG_WDIAG("fail to get integer part len", K(ret));
  } else {
    // here are two cases, return invalid number
    // eg: to_number('1.23E+03', '9.9EEEE') invalid number
    // eg: to_number('123.12', '99.99') invalid number
    if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
      if (fmt_desc_.pre_num_count_ <= 0) {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WDIAG("fmt does not match", K(ret), K(in_str), K(in_fmt_str));
      }
    } else {
      if (integer_part_len > fmt_desc_.pre_num_count_) {
        ret = OB_ERR_CAST_VARCHAR_TO_NUMBER;
        LOG_WDIAG("overflowed digit format", K(ret), K(integer_part_len),
                K(fmt_desc_.pre_num_count_), K(fmt_desc_.post_num_count_));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      if (ObNFMElem::has_currency_group(fmt_desc_.elem_flag_)) {
        set_nls_currency(nls_currency, nls_iso_currency, nls_dual_currency);
      } 
      if (ObNFMElem::has_iso_grouping_group(fmt_desc_.elem_flag_)) {
        set_iso_grouping(nls_numeric_characters);
      } 
      
      if (OB_FAIL(process_output_fmt(in_str, integer_part_len, num_str))) {
        LOG_WDIAG("fail to process output fmt", K(ret));
      } 
      if (ObNFMElem::has_type(NFM_EEEE_FLAG, fmt_desc_.elem_flag_)) {
        if (OB_FAIL(res_num.from_sci_opt(num_str.ptr(), num_str.length(), alloc,
                                        &res_precision, &res_scale))) {
          LOG_WDIAG("fail to calc function to_number with", K(ret), K(num_str));
        }
      } else {
        if (OB_FAIL(res_num.from(num_str.ptr(), num_str.length(), alloc,
                                &res_precision, &res_scale))) {
          LOG_WDIAG("fail to calc function to_number with", K(ret), K(num_str));
        }
      }
    }
  }
  return ret;
}

int ObNFMToNumber::convert_char_to_num(const common::ObString &nls_currency,
                                       const common::ObString &nls_iso_currency,
                                       const common::ObString &nls_dual_currency,
                                       const common::ObString &nls_numeric_characters,
                                       const common::ObString &in_str,
                                       const common::ObString &in_fmt_str,
                                       common::ObIAllocator &alloc,
                                       common::number::ObNumber &res_num)
{
  int ret = OB_SUCCESS;
  if (in_fmt_str.length() >= MAX_FMT_STR_LEN) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WDIAG("invalid format string", K(ret));
  } else if (OB_FAIL(process_fmt_conv(nls_currency, nls_iso_currency, nls_dual_currency, nls_numeric_characters, in_str, in_fmt_str,
                                      alloc, res_num))) {
    LOG_WDIAG("fail to parse format model", K(ret), K(in_fmt_str));
  }
  return ret;
}
}
}
