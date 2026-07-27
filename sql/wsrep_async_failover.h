/* Copyright (c) 2026, Percona and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef WSREP_ASYNC_FAILOVER_H
#define WSREP_ASYNC_FAILOVER_H

/**
  @file sql/wsrep_async_failover.h

  PXC-5201: Native Automatic Asynchronous Replication Failover for
  Multi-Datacenter PXC Clusters.

  This module implements the "Cluster-Aware Asynchronous Replication Failover
  Coordinator". It binds the lifecycle of a classic MySQL asynchronous
  replication channel to Galera cluster membership so that:

    * Scenario A (receiver/DR side): if the node currently acting as the
      asynchronous replica leaves the cluster, a surviving node is
      deterministically elected and resumes the channel automatically.

    * Scenario B (source/primary side): the candidate source list consumed by
      MySQL's Asynchronous Connection Failover (ACF) is kept aligned with the
      healthy members of the primary cluster, so a dead source is transparently
      replaced.

  The coordinator runs as a single background thread per node. It reacts to
  Galera view changes (delivered through Wsrep_server_service::log_view()) and
  to a periodic timer. All potentially blocking work (SQL, START/STOP REPLICA,
  super_read_only) happens on the background thread, never inside the
  total-order view callback.
*/

#include <atomic>
#include <string>

#include "my_thread.h"  // my_thread_handle
#include "mysql/components/services/bits/psi_thread_bits.h"
#include "mysql/psi/mysql_cond.h"   // mysql_cond_t
#include "mysql/psi/mysql_mutex.h"  // mysql_mutex_t

namespace wsrep {
class view;
}

/* Values for wsrep_async_failover_mode (keep in sync with sys_vars.cc). */
enum enum_wsrep_acf_mode {
  WSREP_ACF_MODE_OFF = 0,
  WSREP_ACF_MODE_RECEIVER = 1,
  WSREP_ACF_MODE_SOURCE = 2,
  WSREP_ACF_MODE_BOTH = 3
};

/* Values for wsrep_async_failover_gtid_check (keep in sync with sys_vars.cc). */
enum enum_wsrep_acf_gtid_check {
  WSREP_ACF_GTID_OFF = 0,
  WSREP_ACF_GTID_WARN = 1,
  WSREP_ACF_GTID_ENFORCE = 2
};

/**
  Cluster-Aware Asynchronous Replication Failover Coordinator.

  Singleton. Thread safe. The only methods that may be called from the Galera
  view-change context are on_view() and snapshot(); everything else runs on the
  main server threads (init/deinit) or the variable update path.
*/
class Wsrep_async_failover {
 public:
  /** Snapshot of coordinator state, used for Performance Schema / status. */
  struct Status {
    bool enabled{false};
    int mode{WSREP_ACF_MODE_OFF};
    char channel[64]{0};
    bool is_active_replica{false};
    long elected_index{-1};
    long cluster_size{0};
    char gtid_verdict[16]{"SKIPPED"};
    bool super_read_only_managed{false};
    char last_action[512]{0};
    unsigned long long last_action_time{0};
  };

  static Wsrep_async_failover &instance();

  /** Initialize coordinator (called once during server startup). */
  void init();

  /** Stop and join the coordinator thread (called during server shutdown). */
  void deinit();

  /**
    React to a Galera view change. Runs in total order; only snapshots the view
    and signals the worker. MUST be cheap and non-blocking.
  */
  void on_view(const wsrep::view &view);

  /** Start or stop the worker thread to match @@wsrep_async_failover. */
  void refresh_enabled();

  /** Wake the worker so a configuration change takes effect promptly. */
  void wakeup();

  /** Return a consistent copy of the current coordinator state. */
  Status snapshot();

  /* Worker thread entry point (must be public for the C trampoline). */
  void run();

 private:
  Wsrep_async_failover() = default;
  ~Wsrep_async_failover() = default;
  Wsrep_async_failover(const Wsrep_async_failover &) = delete;
  Wsrep_async_failover &operator=(const Wsrep_async_failover &) = delete;

  bool start_thread();
  void stop_thread();

  /* Per-iteration logic. Called on the worker THD. */
  void process_once(class THD *thd);
  void process_receiver(THD *thd, bool elected);

  /*
    Scenario B: reconcile the ACF candidate source list for @channel with the
    healthy members of the managed 'GaleraCluster' source group(s). Runs on the
    receiver's elected node (that is where the ACF tables live).
  */
  void refresh_source_list(THD *thd, const std::string &channel);

  /* Election: returns true if this node should be the active replica. */
  bool compute_election(long *elected_index, long *cluster_size) const;

  /* GTID consistency gate. Returns true if it is safe to (re)start the
     replica according to wsrep_async_failover_gtid_check. Fills verdict. */
  bool gtid_gate(THD *thd, char verdict[16]);

  /* super_read_only management on the DR cluster. */
  void manage_super_read_only(THD *thd, bool want_on);

  void set_last_action(const char *fmt, ...)
      MY_ATTRIBUTE((format(printf, 2, 3)));

  /* State protected by m_mutex. */
  mysql_mutex_t m_mutex;
  mysql_cond_t m_cond;
  my_thread_handle m_thread{};
  bool m_inited{false};
  bool m_thread_running{false};
  bool m_abort{false};
  bool m_view_pending{false};

  /*
    Membership snapshot captured by on_view() from the totally-ordered Galera
    view. compute_election() prefers these (they carry each member's incoming
    address, so arbitrator/garbd members with an empty address can be skipped)
    and falls back to the wsrep status globals until the first view arrives.
    All guarded by m_mutex.
  */
  bool m_view_seen{false};
  bool m_view_primary{false};
  long m_view_own_index{-1};
  long m_view_elected_index{-1};

  /* True while the coordinator itself is holding super_read_only. */
  bool m_holds_super_read_only{false};

  Status m_status;
};

#endif /* WSREP_ASYNC_FAILOVER_H */
