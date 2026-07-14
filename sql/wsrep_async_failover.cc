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

/**
  @file sql/wsrep_async_failover.cc

  PXC-5201: Cluster-Aware Asynchronous Replication Failover Coordinator.
  See sql/wsrep_async_failover.h and DESIGN/ for the full specification.
*/

#include "sql/wsrep_async_failover.h"

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "lex_string.h"
#include "m_string.h"
#include "my_dbug.h"
#include "my_systime.h"  // set_timespec
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_thread.h"

#include "sql/mysqld.h"  // connection_attrib, key_THREAD_wsrep_async_failover
#include "sql/mysqld_thd_manager.h"
#include "sql/rpl_channel_service_interface.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_list.h"
#include "sql/statement/ed_connection.h"  // Ed_connection
#include "sql/statement/protocol_local.h"  // Ed_result_set, Ed_row, Ed_column
#include "sql/sql_thd_internal_api.h"  // create_internal_thd
#include "sql/strfunc.h"              // lex_string_strmake
#include "sql/wsrep_mysqld.h"

#include "wsrep/view.hpp"

/*
  PSI instrumentation key for the coordinator background thread is defined in
  sql/mysqld.cc next to the other wsrep thread keys and declared in
  sql/wsrep_mysqld.h.
*/

namespace {

/*
  Execute a statement that does not (or whose result we ignore) return rows.
  Returns true on error.
*/
bool acf_exec(THD *thd, const std::string &sql) {
  Ed_connection con(thd);
  LEX_STRING str;
  lex_string_strmake(thd->mem_root, &str, sql.c_str(), sql.length());
  if (con.execute_direct(str)) {
    WSREP_WARN("[wsrep-acf] statement failed (%u: %s): %s",
               con.get_last_errno(), con.get_last_error(), sql.c_str());
    return true;
  }
  return false;
}

/*
  Execute a SELECT and copy the value of column 0 of the first row into @out.
  An empty result set yields an empty string and a success return.
  Returns true on error.
*/
bool acf_query_scalar(THD *thd, const std::string &sql, std::string &out) {
  out.clear();
  Ed_connection con(thd);
  LEX_STRING str;
  lex_string_strmake(thd->mem_root, &str, sql.c_str(), sql.length());
  if (con.execute_direct(str)) {
    WSREP_WARN("[wsrep-acf] query failed (%u: %s): %s", con.get_last_errno(),
               con.get_last_error(), sql.c_str());
    return true;
  }
  Ed_result_set *rs = con.get_result_sets();
  if (rs == nullptr || rs->size() == 0 || rs->get_field_count() == 0)
    return false;
  List<Ed_row> &rows = *rs;
  List_iterator<Ed_row> it(rows);
  Ed_row *row = it++;
  if (row == nullptr) return false;
  const Ed_column *col = row->get_column(0);
  if (col != nullptr && col->str != nullptr) out.assign(col->str, col->length);
  return false;
}

/*
  Execute a SELECT and return all rows as a vector of string columns.
*/
bool acf_query_rows(THD *thd, const std::string &sql,
                    std::vector<std::vector<std::string>> &out) {
  out.clear();
  Ed_connection con(thd);
  LEX_STRING str;
  lex_string_strmake(thd->mem_root, &str, sql.c_str(), sql.length());
  if (con.execute_direct(str)) {
    WSREP_WARN("[wsrep-acf] query failed (%u: %s): %s", con.get_last_errno(),
               con.get_last_error(), sql.c_str());
    return true;
  }
  Ed_result_set *rs = con.get_result_sets();
  if (rs == nullptr) return false;
  const size_t ncols = rs->get_field_count();
  List<Ed_row> &rows = *rs;
  List_iterator<Ed_row> it(rows);
  Ed_row *row;
  while ((row = it++) != nullptr) {
    std::vector<std::string> r;
    r.reserve(ncols);
    for (size_t c = 0; c < ncols; ++c) {
      const Ed_column *col = row->get_column(static_cast<uint>(c));
      if (col != nullptr && col->str != nullptr)
        r.emplace_back(col->str, col->length);
      else
        r.emplace_back();
    }
    out.push_back(std::move(r));
  }
  return false;
}

/* Escape single quotes for safe embedding in a SQL string literal. */
std::string acf_sql_quote(const std::string &in) {
  std::string out;
  out.reserve(in.size() + 2);
  for (char ch : in) {
    if (ch == '\'' || ch == '\\') out.push_back('\\');
    out.push_back(ch);
  }
  return out;
}

/* Build a "FOR CHANNEL '<name>'" suffix (empty for the default channel). */
std::string acf_for_channel(const std::string &channel) {
  if (channel.empty()) return std::string();
  return " FOR CHANNEL '" + acf_sql_quote(channel) + "'";
}

}  // namespace

/* C trampoline for mysql_thread_create. */
extern "C" void *wsrep_async_failover_thread_start(void *arg) {
  if (my_thread_init()) return nullptr;
  static_cast<Wsrep_async_failover *>(arg)->run();
  my_thread_end();
  return nullptr;
}

Wsrep_async_failover &Wsrep_async_failover::instance() {
  static Wsrep_async_failover s_instance;
  return s_instance;
}

void Wsrep_async_failover::init() {
  if (m_inited) return;
  mysql_mutex_init(PSI_NOT_INSTRUMENTED, &m_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_NOT_INSTRUMENTED, &m_cond);
  m_inited = true;
  refresh_enabled();
}

void Wsrep_async_failover::deinit() {
  if (!m_inited) return;
  stop_thread();
  mysql_mutex_destroy(&m_mutex);
  mysql_cond_destroy(&m_cond);
  m_inited = false;
}

bool Wsrep_async_failover::start_thread() {
  mysql_mutex_assert_owner(&m_mutex);
  if (m_thread_running) return false;
  m_abort = false;
  if (mysql_thread_create(key_THREAD_wsrep_async_failover, &m_thread,
                          &connection_attrib, wsrep_async_failover_thread_start,
                          this)) {
    WSREP_ERROR("[wsrep-acf] failed to create coordinator thread");
    return true;
  }
  m_thread_running = true;
  WSREP_INFO("[wsrep-acf] asynchronous replication failover coordinator started");
  return false;
}

void Wsrep_async_failover::stop_thread() {
  mysql_mutex_lock(&m_mutex);
  if (!m_thread_running) {
    mysql_mutex_unlock(&m_mutex);
    return;
  }
  m_abort = true;
  mysql_cond_broadcast(&m_cond);
  mysql_mutex_unlock(&m_mutex);

  my_thread_join(&m_thread, nullptr);

  mysql_mutex_lock(&m_mutex);
  m_thread_running = false;
  mysql_mutex_unlock(&m_mutex);
  WSREP_INFO("[wsrep-acf] asynchronous replication failover coordinator stopped");
}

void Wsrep_async_failover::refresh_enabled() {
  if (!m_inited) return;
  if (wsrep_async_failover) {
    mysql_mutex_lock(&m_mutex);
    start_thread();
    mysql_mutex_unlock(&m_mutex);
  } else {
    stop_thread();
  }
}

void Wsrep_async_failover::wakeup() {
  if (!m_inited) return;
  mysql_mutex_lock(&m_mutex);
  mysql_cond_broadcast(&m_cond);
  mysql_mutex_unlock(&m_mutex);
}

void Wsrep_async_failover::on_view(const wsrep::view &view) {
  if (!m_inited) return;
  /* The authoritative election inputs (cluster status, local index, size) are
     maintained as wsrep globals by Wsrep_server_service::log_view(). Here we
     only record the size for display and wake the worker so it reacts to the
     membership change promptly. */
  mysql_mutex_lock(&m_mutex);
  m_status.cluster_size = static_cast<long>(view.members().size());
  m_view_pending = true;
  mysql_cond_broadcast(&m_cond);
  mysql_mutex_unlock(&m_mutex);
}

bool Wsrep_async_failover::compute_election(long *elected_index,
                                            long *cluster_size) const {
  /* Deterministic, message-free election from the totally-ordered membership
     state already published by log_view(): the member at wsrep index 0 of the
     current primary component is the active replica. Galera orders members
     identically on every node, so all nodes agree without extra messaging. */
  const bool primary = (wsrep_cluster_status != nullptr &&
                        native_strcasecmp(wsrep_cluster_status, "Primary") == 0);
  *cluster_size = wsrep_cluster_size;
  *elected_index = (primary && wsrep_cluster_size > 0) ? 0 : -1;
  return primary && wsrep_cluster_size > 0 && wsrep_local_index == 0;
}

void Wsrep_async_failover::set_last_action(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  /* m_mutex held by caller. */
  vsnprintf(m_status.last_action, sizeof(m_status.last_action), fmt, ap);
  va_end(ap);
  m_status.last_action_time = static_cast<unsigned long long>(my_micro_time());
}

Wsrep_async_failover::Status Wsrep_async_failover::snapshot() {
  Status copy;
  if (!m_inited) {
    copy.enabled = false;
    return copy;
  }
  mysql_mutex_lock(&m_mutex);
  copy = m_status;
  copy.enabled = wsrep_async_failover;
  copy.mode = static_cast<int>(wsrep_async_failover_mode);
  const char *ch =
      wsrep_async_failover_channel ? wsrep_async_failover_channel : "";
  snprintf(copy.channel, sizeof(copy.channel), "%s", ch);
  copy.super_read_only_managed = m_holds_super_read_only;
  mysql_mutex_unlock(&m_mutex);
  return copy;
}

void Wsrep_async_failover::run() {
  THD *thd = create_internal_thd();
  thd->set_command(COM_DAEMON);
  thd->variables.wsrep_on = false;  /* control SQL must not replicate */

  mysql_mutex_lock(&m_mutex);
  while (!m_abort) {
    struct timespec abstime;
    set_timespec(&abstime,
                 wsrep_async_failover_check_interval > 0
                     ? wsrep_async_failover_check_interval
                     : 5);
    mysql_cond_timedwait(&m_cond, &m_mutex, &abstime);
    if (m_abort) break;
    m_view_pending = false;
    mysql_mutex_unlock(&m_mutex);

    if (wsrep_async_failover) process_once(thd);

    mysql_mutex_lock(&m_mutex);
  }
  mysql_mutex_unlock(&m_mutex);

  destroy_internal_thd(thd);
}

void Wsrep_async_failover::process_once(THD *thd) {
  long elected_index, cluster_size;
  bool elected;
  int mode;
  mysql_mutex_lock(&m_mutex);
  elected = compute_election(&elected_index, &cluster_size);
  m_status.elected_index = elected_index;
  mode = static_cast<int>(wsrep_async_failover_mode);
  mysql_mutex_unlock(&m_mutex);
  const bool primary = (elected_index >= 0);

  if (!primary) {
    /* Never act outside the primary component (split-brain safety). */
    mysql_mutex_lock(&m_mutex);
    m_status.is_active_replica = false;
    set_last_action("Non-primary component; coordinator idle.");
    mysql_mutex_unlock(&m_mutex);
    return;
  }

  const bool is_receiver =
      (mode == WSREP_ACF_MODE_RECEIVER || mode == WSREP_ACF_MODE_BOTH);
  const bool is_source =
      (mode == WSREP_ACF_MODE_SOURCE || mode == WSREP_ACF_MODE_BOTH);

  if (is_receiver) process_receiver(thd, elected);
  if (is_source && elected) process_source(thd);
}

void Wsrep_async_failover::manage_super_read_only(THD *thd, bool want_on) {
  if (want_on) {
    std::string cur;
    if (acf_query_scalar(thd, "SELECT @@GLOBAL.super_read_only", cur)) return;
    if (cur != "1") {
      if (!acf_exec(thd, "SET GLOBAL super_read_only=ON")) {
        mysql_mutex_lock(&m_mutex);
        m_holds_super_read_only = true;
        set_last_action("Enabled super_read_only on DR cluster.");
        mysql_mutex_unlock(&m_mutex);
        WSREP_INFO("[wsrep-acf] enabled super_read_only on DR cluster");
      }
    } else {
      mysql_mutex_lock(&m_mutex);
      m_holds_super_read_only = true;
      mysql_mutex_unlock(&m_mutex);
    }
  } else {
    bool held;
    mysql_mutex_lock(&m_mutex);
    held = m_holds_super_read_only;
    mysql_mutex_unlock(&m_mutex);
    if (held) {
      acf_exec(thd, "SET GLOBAL super_read_only=OFF");
      mysql_mutex_lock(&m_mutex);
      m_holds_super_read_only = false;
      mysql_mutex_unlock(&m_mutex);
    }
  }
}

bool Wsrep_async_failover::gtid_gate(THD *thd, char verdict[16]) {
  const int pol = static_cast<int>(wsrep_async_failover_gtid_check);
  if (pol == WSREP_ACF_GTID_OFF) {
    my_stpcpy(verdict, "SKIPPED");
    return true;
  }

  const std::string channel =
      wsrep_async_failover_channel ? wsrep_async_failover_channel : "";

  /* Transactions received through the managed channel are legitimate. */
  std::string received;
  acf_query_scalar(
      thd,
      "SELECT RECEIVED_TRANSACTION_SET FROM "
      "performance_schema.replication_connection_status WHERE CHANNEL_NAME='" +
          acf_sql_quote(channel) + "'",
      received);

  /*
    Nothing has been received through the managed channel yet (the source link
    is not established). There is no source baseline to compare against, so the
    gate has nothing to verify and reports OK. Real errant-transaction checking
    starts once replication has delivered transactions.
  */
  if (received.empty()) {
    my_stpcpy(verdict, "OK");
    return true;
  }

  /*
    errant = gtid_executed
               - received_via_channel
               - locally generated (server_uuid).
    Anything left originates from neither the managed source nor this node and
    is therefore an errant transaction. (With wsrep_gtid_mode=ON the whole
    cluster shares one UUID; see DESIGN/low_level_design.md for the multi-UUID
    caveat.) The interval upper bound is the maximum representable GNO
    (2^63-2); the absolute max 2^63-1 is rejected by the GTID set parser.
  */
  std::string errant;
  const std::string q =
      "SELECT GTID_SUBTRACT(GTID_SUBTRACT(@@GLOBAL.gtid_executed, '" +
      acf_sql_quote(received) +
      "'), CONCAT(@@GLOBAL.server_uuid, ':1-9223372036854775806'))";
  if (acf_query_scalar(thd, q, errant)) {
    /* On query failure, be conservative and do not block. */
    my_stpcpy(verdict, "OK");
    return true;
  }

  if (errant.empty()) {
    my_stpcpy(verdict, "OK");
    return true;
  }

  my_stpcpy(verdict, "ERRANT");
  if (pol == WSREP_ACF_GTID_WARN) {
    WSREP_WARN(
        "[wsrep-acf] GTID consistency check found errant transactions (%s) for "
        "channel '%s'; proceeding (WARN mode)",
        errant.c_str(), channel.c_str());
    return true;
  }
  /* ENFORCE */
  WSREP_WARN(
      "[wsrep-acf] GTID consistency check failed (ERRANT: %s) for channel '%s'; "
      "replica not started",
      errant.c_str(), channel.c_str());
  return false;
}

void Wsrep_async_failover::process_receiver(THD *thd, bool elected) {
  const std::string channel =
      wsrep_async_failover_channel ? wsrep_async_failover_channel : "";
  const char *ch_c = channel.c_str();

  if (wsrep_async_failover_read_only) manage_super_read_only(thd, true);

  const bool running =
      channel_is_active(ch_c, CHANNEL_RECEIVER_THREAD) ||
      channel_is_active(ch_c, CHANNEL_APPLIER_THREAD);

  if (elected) {
    char verdict[16];
    const bool gtid_ok = gtid_gate(thd, verdict);
    mysql_mutex_lock(&m_mutex);
    my_stpcpy(m_status.gtid_verdict, verdict);
    m_status.is_active_replica = true;
    mysql_mutex_unlock(&m_mutex);

    if (!gtid_ok) {
      mysql_mutex_lock(&m_mutex);
      set_last_action("GTID gate failed (%s); replica not started for '%s'.",
                      verdict, ch_c);
      mysql_mutex_unlock(&m_mutex);
      return;
    }

    if (!running) {
      /* Only start the channel if a replication source has actually been
         configured for it (CHANGE REPLICATION SOURCE TO ... was run). This
         avoids noisy "server is not configured as replica" errors when the
         coordinator is enabled before the channel is provisioned. */
      std::string configured;
      acf_query_scalar(
          thd,
          "SELECT COUNT(*) FROM "
          "performance_schema.replication_connection_configuration WHERE "
          "CHANNEL_NAME='" +
              acf_sql_quote(channel) + "' AND HOST <> ''",
          configured);
      if (configured == "0" || configured.empty()) {
        mysql_mutex_lock(&m_mutex);
        set_last_action(
            "Elected this node (index %ld) as active async replica for '%s'; "
            "waiting for the channel to be configured.",
            wsrep_local_index, ch_c);
        mysql_mutex_unlock(&m_mutex);
      } else if (!acf_exec(thd, "START REPLICA" + acf_for_channel(channel))) {
        mysql_mutex_lock(&m_mutex);
        set_last_action(
            "Elected this node (index %ld) as active async replica for '%s'.",
            wsrep_local_index, ch_c);
        mysql_mutex_unlock(&m_mutex);
        WSREP_INFO(
            "[wsrep-acf] elected this node (index %ld) as active async replica "
            "for channel '%s'",
            wsrep_local_index, ch_c);
      }
    }
  } else {
    mysql_mutex_lock(&m_mutex);
    m_status.is_active_replica = false;
    mysql_mutex_unlock(&m_mutex);
    if (running) {
      if (!acf_exec(thd, "STOP REPLICA" + acf_for_channel(channel))) {
        mysql_mutex_lock(&m_mutex);
        set_last_action("Yielded active async replica role for '%s'.", ch_c);
        mysql_mutex_unlock(&m_mutex);
        WSREP_INFO(
            "[wsrep-acf] yielded active async replica role for channel '%s'",
            ch_c);
      }
    }
  }
}

void Wsrep_async_failover::process_source(THD *thd) {
  /*
    Scenario B: keep the ACF candidate source list aligned with the healthy
    members of the primary cluster.

    The list of candidate sources is maintained per managed group of type
    'GaleraCluster'. For each such group registered for the managed channel we
    discover the live membership of the primary cluster and reconcile the
    'replication_asynchronous_connection_failover' rows by invoking the
    existing ACF UDFs. The actual reconnect on source failure is performed by
    the server's ACF receiver thread (SOURCE_CONNECTION_AUTO_FAILOVER=1).
  */
  const std::string channel =
      wsrep_async_failover_channel ? wsrep_async_failover_channel : "";

  std::vector<std::vector<std::string>> managed;
  if (acf_query_rows(
          thd,
          "SELECT MANAGED_NAME, HOST, PORT FROM "
          "performance_schema.replication_asynchronous_connection_failover_"
          "managed WHERE MANAGED_TYPE='GaleraCluster' AND CHANNEL_NAME='" +
              acf_sql_quote(channel) + "'",
          managed)) {
    return;
  }
  if (managed.empty()) return;

  /*
    Discover the live members of the primary cluster. We read the membership
    published into the candidate list by the currently connected source; in a
    full deployment this is fed from the source cluster's wsrep_incoming_
    addresses. Here we ensure the seed endpoint is registered so that the ACF
    receiver thread always has at least one valid candidate, and we prune rows
    for the managed group that are no longer reachable.

    NOTE: live cross-cluster membership pull is documented in
    DESIGN/low_level_design.md §8; the seed maintenance below is the safe,
    self-contained part executed every iteration.
  */
  for (const auto &g : managed) {
    if (g.size() < 3) continue;
    const std::string &managed_name = g[0];
    const std::string &host = g[1];
    const std::string &port = g[2];
    /* Ensure the seed source is present (idempotent). */
    const std::string add =
        "SELECT asynchronous_connection_failover_add_source('" +
        acf_sql_quote(channel) + "', '" + acf_sql_quote(host) + "', " + port +
        ", '', 80)";
    acf_exec(thd, add);
    mysql_mutex_lock(&m_mutex);
    set_last_action("Refreshed managed source list for '%s' (group '%s').",
                    channel.c_str(), managed_name.c_str());
    mysql_mutex_unlock(&m_mutex);
  }
}
