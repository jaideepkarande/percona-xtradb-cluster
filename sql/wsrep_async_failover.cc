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

#include <cctype>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <string>
#include <utility>
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

/*
  Detect a gap (hole) in a GTID set string such as @@GLOBAL.gtid_executed.

  The set is a comma-separated list of per-UUID sets, each of the form
  "uuid:interval[:interval...]" where an interval is "n" or "n-m". A single UUID
  carrying more than one interval (>=2 ':' separators, e.g. "uuid:1-5:8-10")
  means a transaction range in the middle is missing — a gap. Whitespace and
  embedded newlines produced by the SELECT are ignored. A UUID set beginning
  above 1 is NOT treated as a gap because that is the normal, legitimate result
  of gtid_purged after log rotation / SST.
*/
bool acf_gtid_has_gap(const std::string &set) {
  size_t start = 0;
  while (start <= set.size()) {
    const size_t comma = set.find(',', start);
    const size_t end = (comma == std::string::npos) ? set.size() : comma;
    int colons = 0;
    for (size_t i = start; i < end; ++i)
      if (set[i] == ':') ++colons;
    if (colons >= 2) return true;
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  return false;
}

/* Split a comma-separated "host:port[,host:port...]" list (as published by
   wsrep_incoming_addresses) into (host, port) pairs. Entries without a port or
   with an empty host are skipped. */
void acf_parse_incoming_addresses(
    const std::string &csv,
    std::vector<std::pair<std::string, std::string>> &out) {
  size_t start = 0;
  while (start <= csv.size()) {
    const size_t comma = csv.find(',', start);
    const size_t end = (comma == std::string::npos) ? csv.size() : comma;
    std::string entry;
    for (size_t i = start; i < end; ++i)
      if (!isspace(static_cast<unsigned char>(csv[i]))) entry.push_back(csv[i]);
    const size_t colon = entry.rfind(':');
    if (colon != std::string::npos && colon > 0 && colon + 1 < entry.size()) {
      out.emplace_back(entry.substr(0, colon), entry.substr(colon + 1));
    }
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
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
  /*
    Capture the totally-ordered membership so the worker can run a deterministic,
    message-free election. We record whether the view is primary, this node's
    own index, and the index of the elected replica: the first member that has a
    non-empty incoming address. Members with an empty incoming address are
    arbitrators (garbd) which do not run mysqld and must never be elected as the
    async replica. Galera orders members identically on every node, so all nodes
    compute the same elected index without extra messaging (FR-A2). This runs in
    total order and must stay cheap: it only snapshots and signals the worker.
  */
  mysql_mutex_lock(&m_mutex);
  const std::vector<wsrep::view::member> &members = view.members();
  m_status.cluster_size = static_cast<long>(members.size());
  m_view_primary = (view.status() == wsrep::view::primary);
  m_view_own_index = static_cast<long>(view.own_index());
  m_view_elected_index = -1;
  for (size_t i = 0; i < members.size(); ++i) {
    if (!members[i].incoming().empty()) {
      m_view_elected_index = static_cast<long>(i);
      break;
    }
  }
  m_view_seen = true;
  m_view_pending = true;
  mysql_cond_broadcast(&m_cond);
  mysql_mutex_unlock(&m_mutex);
}

bool Wsrep_async_failover::compute_election(long *elected_index,
                                            long *cluster_size) const {
  /* Deterministic, message-free election. m_mutex is held by the caller. */
  if (m_view_seen) {
    /* Preferred path: use the membership captured from the last Galera view.
       The elected replica is the first member with a non-empty incoming
       address, so arbitrator/garbd members (empty address, no mysqld) are
       skipped (FR-A2, FR-A3). */
    *cluster_size = m_status.cluster_size;
    *elected_index = m_view_primary ? m_view_elected_index : -1;
    return m_view_primary && m_view_elected_index >= 0 &&
           m_view_own_index == m_view_elected_index;
  }
  /* Fallback until the first view is delivered (startup / lone node): use the
     wsrep status globals maintained by log_view(). */
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

  /* Receiver-side logic (Scenario A) also maintains the ACF candidate source
     list (Scenario B), because both the managed-group definition and the
     candidate list live on the replica cluster, not on the primary. */
  if (is_receiver) process_receiver(thd, elected);

  /* A cluster configured purely as SOURCE is the async primary; it holds no
     local ACF candidate list to maintain, so there is no per-iteration action
     beyond staying writable. Record it for observability. */
  if (is_source && !is_receiver && elected) {
    mysql_mutex_lock(&m_mutex);
    m_status.is_active_replica = false;
    set_last_action("SOURCE mode: acting as async primary; no local ACF action.");
    mysql_mutex_unlock(&m_mutex);
  }
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

  /* Apply the configured policy to a non-OK verdict: WARN logs and proceeds,
     ENFORCE logs and refuses to (re)start the replica. */
  auto verdict_fail = [&](const char *v, const std::string &detail) -> bool {
    my_stpcpy(verdict, v);
    if (pol == WSREP_ACF_GTID_WARN) {
      WSREP_WARN(
          "[wsrep-acf] GTID consistency check found %s (%s) for channel '%s'; "
          "proceeding (WARN mode)",
          v, detail.c_str(), channel.c_str());
      return true;
    }
    WSREP_WARN(
        "[wsrep-acf] GTID consistency check failed (%s: %s) for channel '%s'; "
        "replica not started",
        v, detail.c_str(), channel.c_str());
    return false;
  };

  /*
    (a) Gap detection (FR-G2a): a hole in @@GLOBAL.gtid_executed means this node
    is missing transactions in the middle of a range and must not become the
    async replica until Galera fills the hole. On query failure we do not block.
  */
  std::string executed;
  if (!acf_query_scalar(thd, "SELECT @@GLOBAL.gtid_executed", executed) &&
      acf_gtid_has_gap(executed)) {
    return verdict_fail("GAP", executed);
  }

  /*
    (c) Xid <-> GTID_NEXT agreement (FR-G2c), local half. An in-doubt (prepared
    but not committed) transaction means the node's Xid/GTID state is mid-flight;
    (re)starting a DR replica against it risks the historical PXC GTID
    inconsistency. XA RECOVER returns one row per in-doubt transaction. The full
    cross-member comparison is documented as follow-up in
    DESIGN/low_level_design.md §7. On query failure we do not block.
  */
  std::vector<std::vector<std::string>> indoubt;
  if (!acf_query_rows(thd, "XA RECOVER", indoubt) && !indoubt.empty()) {
    return verdict_fail("XID_MISMATCH", "in-doubt XA transaction present");
  }

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
    errant check has nothing to verify and reports OK. Real errant-transaction
    checking starts once replication has delivered transactions.
  */
  if (received.empty()) {
    my_stpcpy(verdict, "OK");
    return true;
  }

  /*
    (b) errant = gtid_executed
               - received_via_channel
               - locally generated.
    Anything left originates from neither the managed source nor this node and
    is therefore an errant transaction.

    "Locally generated" in PXC is NOT just @@server_uuid. Galera stamps the
    node's replicated workload with the cluster-wide wsrep GTID UUID
    (wsrep_cluster_state_uuid), which is distinct from every node's server_uuid;
    server_uuid only carries the odd non-wsrep housekeeping GTID. Both must be
    treated as local origin, otherwise the cluster's OWN transactions are
    mis-classified as errant the moment the channel has delivered anything (the
    active-async-replica / circular case) and, under ENFORCE, failover is
    blocked. (There is no wsrep_gtid_mode in 8.4; the cluster GTID UUID is
    always separate from server_uuid.) The interval upper bound is the maximum
    representable GNO (2^63-2); the absolute max 2^63-1 is rejected by the GTID
    set parser.
  */
  std::string local_tail = ":1-9223372036854775806";
  if (wsrep_cluster_state_uuid && wsrep_cluster_state_uuid[0] &&
      strcmp(wsrep_cluster_state_uuid,
             "00000000-0000-0000-0000-000000000000") != 0) {
    local_tail += "," + std::string(wsrep_cluster_state_uuid) +
                  ":1-9223372036854775806";
  }
  std::string errant;
  const std::string q =
      "SELECT GTID_SUBTRACT(GTID_SUBTRACT(@@GLOBAL.gtid_executed, '" +
      acf_sql_quote(received) + "'), CONCAT(@@GLOBAL.server_uuid, '" +
      local_tail + "'))";
  if (acf_query_scalar(thd, q, errant)) {
    /* On query failure, be conservative and do not block. */
    my_stpcpy(verdict, "OK");
    return true;
  }

  if (errant.empty()) {
    my_stpcpy(verdict, "OK");
    return true;
  }

  return verdict_fail("ERRANT", errant);
}

void Wsrep_async_failover::process_receiver(THD *thd, bool elected) {
  const std::string channel =
      wsrep_async_failover_channel ? wsrep_async_failover_channel : "";
  const char *ch_c = channel.c_str();

  /* Split-brain protection. When the knob is ON, keep the DR cluster in
     super_read_only. When the operator turns it OFF, release only the bit the
     coordinator itself set, leaving any DBA-set super_read_only untouched
     (FR-S1, FR-S3). */
  if (wsrep_async_failover_read_only)
    manage_super_read_only(thd, true);
  else
    manage_super_read_only(thd, false);

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
         configured for it (CHANGE REPLICATION SOURCE TO ... was run), and only
         with GTID auto-positioning (FR-A4). Reading HOST and AUTO_POSITION in a
         single round-trip avoids noisy "server is not configured as replica"
         errors when the coordinator is enabled before the channel is
         provisioned, and refuses to start a channel that would lose binlog
         coordinates. */
      std::vector<std::vector<std::string>> cfg;
      acf_query_rows(
          thd,
          "SELECT HOST, AUTO_POSITION FROM "
          "performance_schema.replication_connection_configuration WHERE "
          "CHANNEL_NAME='" +
              acf_sql_quote(channel) + "'",
          cfg);
      bool configured = false, auto_position = false;
      for (const auto &r : cfg) {
        if (r.size() >= 2 && !r[0].empty()) {
          configured = true;
          auto_position = (r[1] == "1");
        }
      }
      if (!configured) {
        mysql_mutex_lock(&m_mutex);
        set_last_action(
            "Elected this node (index %ld) as active async replica for '%s'; "
            "waiting for the channel to be configured.",
            wsrep_local_index, ch_c);
        mysql_mutex_unlock(&m_mutex);
      } else if (!auto_position) {
        mysql_mutex_lock(&m_mutex);
        set_last_action(
            "Channel '%s' is not configured with SOURCE_AUTO_POSITION=1; "
            "replica not started (FR-A4).",
            ch_c);
        mysql_mutex_unlock(&m_mutex);
        WSREP_WARN(
            "[wsrep-acf] channel '%s' lacks SOURCE_AUTO_POSITION=1; refusing to "
            "auto-start to avoid losing binlog coordinates",
            ch_c);
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

    /* The elected replica also owns the ACF candidate source list (Scenario B):
       keep it aligned with the managed GaleraCluster source group(s). */
    refresh_source_list(thd, channel);
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

void Wsrep_async_failover::refresh_source_list(THD *thd,
                                               const std::string &channel) {
  /*
    Scenario B: keep the ACF candidate source list aligned with the healthy
    members of the primary cluster, which is registered as one or more managed
    groups of type 'GaleraCluster'. The actual reconnect on source failure is
    performed by the server's ACF receiver thread
    (SOURCE_CONNECTION_AUTO_FAILOVER=1); this function only maintains the
    candidate rows it chooses from.

    Membership source: the primary cluster publishes its live members in
    wsrep_incoming_addresses. For a co-located deployment (BOTH mode) that list
    is read locally and the candidate rows are fully reconciled (added and
    pruned). The cross-datacenter pull from the currently-connected source is
    documented in DESIGN/low_level_design.md §8 as follow-up; until then the
    pure-RECEIVER path is add-only (it keeps every registered seed present as a
    candidate but does not prune rows whose membership it cannot observe).
  */
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

  /* Desired candidate set. Every registered seed is always desired. */
  std::vector<std::pair<std::string, std::string>> desired;
  for (const auto &g : managed) {
    if (g.size() < 3 || g[1].empty()) continue;
    desired.emplace_back(g[1], g[2]);
  }

  /* In a co-located (BOTH) deployment fold in the live membership published in
     wsrep_incoming_addresses; this is the reconcilable, add-and-prune case. */
  bool membership_known = false;
  if (wsrep_async_failover_mode == WSREP_ACF_MODE_BOTH) {
    std::string csv;
    if (!acf_query_scalar(
            thd,
            "SELECT VARIABLE_VALUE FROM performance_schema.global_status "
            "WHERE VARIABLE_NAME='wsrep_incoming_addresses'",
            csv) &&
        !csv.empty()) {
      acf_parse_incoming_addresses(csv, desired);
      membership_known = true;
    }
  }

  auto in_desired = [&](const std::string &h, const std::string &p) {
    for (const auto &d : desired)
      if (d.first == h && d.second == p) return true;
    return false;
  };

  /* Add every desired candidate that is missing (idempotent). */
  for (const auto &d : desired) {
    const std::string add =
        "SELECT asynchronous_connection_failover_add_source('" +
        acf_sql_quote(channel) + "', '" + acf_sql_quote(d.first) + "', " +
        d.second + ", '', 80)";
    acf_exec(thd, add);
  }

  /* Prune departed members, but only when the live membership is actually
     known, so we never remove a candidate we simply could not observe. */
  int pruned = 0;
  if (membership_known) {
    std::vector<std::vector<std::string>> current;
    acf_query_rows(
        thd,
        "SELECT HOST, PORT FROM "
        "performance_schema.replication_asynchronous_connection_failover WHERE "
        "CHANNEL_NAME='" +
            acf_sql_quote(channel) + "'",
        current);
    for (const auto &r : current) {
      if (r.size() < 2) continue;
      if (!in_desired(r[0], r[1])) {
        const std::string del =
            "SELECT asynchronous_connection_failover_delete_source('" +
            acf_sql_quote(channel) + "', '" + acf_sql_quote(r[0]) + "', " +
            r[1] + ", '')";
        if (!acf_exec(thd, del)) ++pruned;
      }
    }
  }

  mysql_mutex_lock(&m_mutex);
  set_last_action(
      "Refreshed ACF source list for '%s': %d candidate(s), %d pruned.",
      channel.c_str(), static_cast<int>(desired.size()), pruned);
  mysql_mutex_unlock(&m_mutex);
}
