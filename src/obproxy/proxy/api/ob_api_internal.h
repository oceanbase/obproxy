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

#ifndef OBPROXY_API_INTERNAL_H
#define OBPROXY_API_INTERNAL_H

#include "iocore/net/ob_net.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "proxy/api/ob_iocore_api.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// max number of user arguments for Transactions and Sessions
#define MYSQL_SSN_TXN_MAX_USER_ARG         16

enum ObAPIHookScope
{
  API_HOOK_SCOPE_NONE,
  API_HOOK_SCOPE_GLOBAL,
  API_HOOK_SCOPE_LOCAL
};

// A single API hook that can be invoked.
class ObAPIHook
{
public:
  ObContInternal *cont_;
  int invoke(int event, void *edata);
  ObAPIHook *next() const;
  LINK(ObAPIHook, link_);
};

// A collection of API hooks.
class ObAPIHooks
{
public:
  int prepend(ObContInternal *cont);
  int append(ObContInternal *cont);
  ObAPIHook *get() const;
  void destroy();
  bool is_empty() const;
  void invoke(int event, void *data);

private:
  Que(ObAPIHook, link_) hooks_;
};

inline bool ObAPIHooks::is_empty() const
{
  return NULL == hooks_.head_;
}

inline void ObAPIHooks::invoke(int event, void *data)
{
  for (ObAPIHook* hook = hooks_.head_; NULL != hook; hook = hook->next()) {
    hook->invoke(event, data);
  }
}

/**
 * Container for API hooks for a specific feature.
 *
 * This is an array of hook lists, each identified by a numeric identifier (id).
 * Each array element is a list of all  hooks for that ID. Adding a hook means
 * adding to the list in the corresponding array element. There is no provision
 * for removing a hook.
 *
 * @note The minimum value for a hook ID is zero. Therefore the template parameter
 * N_ID should be one more than the  maximum hook ID so the valid ids are 0..(N-1)
 * in the standard C array style.
 */
template <typename ID, ID N>
class ObFeatureAPIHooks
{
public:
  ObFeatureAPIHooks(); // Constructor (empty container).
  ~ObFeatureAPIHooks(); // Destructor.

  // Remove all hooks.
  void destroy();

  // Remove hooks indicated by id
  void destroy(ID id);

  // Add the hook cont to the front of the hooks for id.
  int prepend(ID id, ObContInternal *cont);

  // Add the hook cont to the end of the hooks for id.
  int append(ID id, ObContInternal *cont);

  // Get the list of hooks for id.
  ObAPIHook *get(ID id) const;

  // @return true if id is a valid id, false otherwise.
  static bool is_valid(ID id);

  // Invoke the callbacks for the hook id.
  void invoke(ID id, int event, void *data);

  // Fast check for any hooks in this container.
  //
  // @return true if any list has at least one hook, false if
  // all lists have no hooks.
  bool has_hooks() const;

  // Check for existence of hooks of a specific id.
  // @return true if any hooks of type id are present.
  bool has_hooks_for(ID id) const;

private:
  bool hooks_p_; // Flag for (not) empty container.
  
  // The array of hooks lists.
  ObAPIHooks hooks_[N];
};

inline int ObAPIHook::invoke(int event, void *edata)
{
  if ((EVENT_IMMEDIATE == event) || (EVENT_INTERVAL == event)) {
    if (ATOMIC_FAA((int *)&cont_->event_count_, 1) < 0) {
      PROXY_API_LOG(ERROR, "not reached");
    }
  }

  return cont_->handle_event(event, edata);
}

inline ObAPIHook *ObAPIHook::next() const
{
  return link_.next_;
}

inline int ObAPIHooks::prepend(ObContInternal *cont)
{
  int ret = common::OB_SUCCESS;
  ObAPIHook *api_hook = NULL;
  if (OB_ISNULL(api_hook = op_reclaim_alloc(ObAPIHook))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_API_LOG(ERROR, "failed to allocate memory for ObAPIHook", K(ret));
  } else {
    api_hook->cont_ = cont;
    hooks_.push(api_hook);
  }
  return ret;
}

inline int ObAPIHooks::append(ObContInternal *cont)
{
  int ret = common::OB_SUCCESS;
  ObAPIHook *api_hook = NULL;
  if (OB_ISNULL(api_hook = op_reclaim_alloc(ObAPIHook))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_API_LOG(ERROR, "failed to allocate memory for ObAPIHook", K(ret));
  } else {
    api_hook->cont_ = cont;
    hooks_.enqueue(api_hook);
  }
  return ret;
}

inline ObAPIHook *ObAPIHooks::get() const
{
  return hooks_.head_;
}

inline void ObAPIHooks::destroy()
{
  ObAPIHook *hook = NULL;
  while (NULL != (hook = hooks_.pop())) {
    op_reclaim_free(hook);
  }
}

template <typename ID, ID N>
ObFeatureAPIHooks<ID, N>::ObFeatureAPIHooks() : hooks_p_(false)
{
}

template <typename ID, ID N>
ObFeatureAPIHooks<ID, N>::~ObFeatureAPIHooks()
{
  destroy();
}

template <typename ID, ID N>
inline void ObFeatureAPIHooks<ID, N>::destroy()
{
  for (int i = 0; i < N; ++i) {
    hooks_[i].destroy();
  }

  hooks_p_ = false;
}

template <typename ID, ID N>
inline void ObFeatureAPIHooks<ID, N>::destroy(ID id)
{
  if (is_valid(id)) {
    hooks_[id].destroy();

    bool has_hooks_or_not = false;
    for (int i = 0; i < N; ++i) {
      if (!hooks_[i].is_empty()) {
        has_hooks_or_not = true;
        break;
      }
    }
    hooks_p_ = has_hooks_or_not;
  }
}

template <typename ID, ID N>
inline int ObFeatureAPIHooks<ID, N>::prepend(ID id, ObContInternal *cont)
{
  hooks_p_ = true;
  return hooks_[id].prepend(cont);
}

template <typename ID, ID N>
inline int ObFeatureAPIHooks<ID, N>::append(ID id, ObContInternal *cont)
{
  hooks_p_ = true;
  return hooks_[id].append(cont);
}

template <typename ID, ID N>
inline ObAPIHook *ObFeatureAPIHooks<ID, N>::get(ID id) const
{
  return hooks_[id].get();
}

template <typename ID, ID N>
inline void ObFeatureAPIHooks<ID, N>::invoke(ID id, int event, void *data)
{
  hooks_[id].invoke(event, data);
}

template <typename ID, ID N>
inline bool ObFeatureAPIHooks<ID, N>::has_hooks() const
{
  return hooks_p_;
}

template <typename ID, ID N>
inline bool ObFeatureAPIHooks<ID, N>::is_valid(ID id)
{
  return 0 <= id && id < N;
}

class ObMysqlAPIHooks : public ObFeatureAPIHooks<ObMysqlHookID, OB_MYSQL_LAST_HOOK>
{
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_API_INTERNAL_H
