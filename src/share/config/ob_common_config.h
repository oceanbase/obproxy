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

#ifndef OCEANBASE_SHARE_CONFIG_OB_COMMON_CONFIG_H_
#define OCEANBASE_SHARE_CONFIG_OB_COMMON_CONFIG_H_

#include "lib/net/ob_addr.h"
#include "share/config/ob_config.h"

namespace oceanbase
{
namespace common
{
//SECTION
#define CFG_SECTION_OBSERVER        common::ObCfgSectionLabel("observer")
#define CFG_SECTION_ROOTSERVICE     common::ObCfgSectionLabel("rootservice")
#define CFG_SECTION_LOAD_BALANCE    common::ObCfgSectionLabel("load_balance")
#define CFG_SECTION_DAILY_MERGE     common::ObCfgSectionLabel("daily_merge")
#define CFG_SECTION_LOCATION_CACHE  common::ObCfgSectionLabel("location_cache")
#define CFG_SECTION_SSTABLE         common::ObCfgSectionLabel("sstable")
#define CFG_SECTION_CLOG            common::ObCfgSectionLabel("clog")
#define CFG_SECTION_CACHE           common::ObCfgSectionLabel("cache")
#define CFG_SECTION_TRANS           common::ObCfgSectionLabel("trans")
#define CFG_SECTION_TENANT          common::ObCfgSectionLabel("tenant")
#define CFG_SECTION_RPC             common::ObCfgSectionLabel("rpc")
#define CFG_SECTION_OBPROXY         common::ObCfgSectionLabel("obproxy")

//VISIBLE_LEVEL
#define CFG_VISIBLE_LEVEL_USER \
  common::ObCfgVisibleLevelLabel(common::OB_CONFIG_VISIBLE_LEVEL_USER)
#define CFG_VISIBLE_LEVEL_SYS  \
  common::ObCfgVisibleLevelLabel(common::OB_CONFIG_VISIBLE_LEVEL_SYS)
#define CFG_VISIBLE_LEVEL_DBA  \
  common::ObCfgVisibleLevelLabel(common::OB_CONFIG_VISIBLE_LEVEL_DBA)
#define CFG_VISIBLE_LEVEL_MEMORY  \
  common::ObCfgVisibleLevelLabel(common::OB_CONFIG_VISIBLE_LEVEL_MEMORY)

//NEED_REBOOT
#define CFG_NEED_REBOOT             common::ObCfgNeedRebootLabel(common::OB_CONFIG_NEED_REBOOT)
#define CFG_NO_NEED_REBOOT          common::ObCfgNeedRebootLabel(common::OB_CONFIG_NOT_NEED_REBOOT)

class ObInitConfigContainer
{
public:
  const ObConfigContainer &get_container();

protected:
  ObInitConfigContainer();
  virtual ~ObInitConfigContainer() {}
  static ObConfigContainer *&local_container();
  ObConfigContainer container_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObInitConfigContainer);
};

// derive from ObInitConfigContainer to make sure config container inited before config item.
class ObCommonConfig : public ObInitConfigContainer
{
public:
  ObCommonConfig();
  virtual ~ObCommonConfig();

  virtual int check_all() const = 0;
  virtual void print() const = 0;
  virtual void print_need_reboot_config() const {/*do nothing*/};
  virtual ObServerRole get_server_type() const = 0;
  virtual int add_extra_config(const char *config_str,
                               const int64_t version = 0,
                               const bool check_name = false);
  virtual bool is_debug_sync_enabled() const { return false; }

  NEED_SERIALIZE_AND_DESERIALIZE;

protected:
  static const int16_t OB_COMMON_CONFIG_MAGIC = static_cast<int16_t>(0XBCDE);
  static const int64_t MIN_LENGTH = 20;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCommonConfig);
};

} //end of namespace common
} //end of namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_COMMON_CONFIG_H_
