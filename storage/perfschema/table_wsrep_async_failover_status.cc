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
  @file storage/perfschema/table_wsrep_async_failover_status.cc
  Table wsrep_async_failover_status (implementation). See PXC-5201.
*/

#include "storage/perfschema/table_wsrep_async_failover_status.h"

#include <cstring>

#include "my_dbug.h"
#include "sql/field.h"
#include "sql/plugin_table.h"
#include "sql/table.h"
#include "storage/perfschema/pfs_instr.h"
#include "storage/perfschema/pfs_instr_class.h"

THR_LOCK table_wsrep_async_failover_status::m_table_lock;

Plugin_table table_wsrep_async_failover_status::m_table_def(
    /* Schema name */
    "performance_schema",
    /* Name */
    "wsrep_async_failover_status",
    /* Definition */
    "  ENABLED ENUM('YES','NO') NOT NULL COMMENT 'Whether the PXC "
    "cluster-aware asynchronous replication failover coordinator is enabled on "
    "this node.',\n"
    "  MODE VARCHAR(16) NOT NULL COMMENT 'Role this cluster plays in the async "
    "link: OFF, RECEIVER, SOURCE or BOTH.',\n"
    "  CHANNEL_NAME CHAR(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci "
    "NOT NULL COMMENT 'The asynchronous replication channel managed by the "
    "coordinator on the receiver side.',\n"
    "  IS_ACTIVE_REPLICA ENUM('YES','NO') NOT NULL COMMENT 'Whether this node "
    "is the currently elected active asynchronous replica.',\n"
    "  ELECTED_INDEX BIGINT NOT NULL COMMENT 'wsrep cluster index of the "
    "elected active replica node, or -1 if none.',\n"
    "  CLUSTER_SIZE BIGINT NOT NULL COMMENT 'Number of members in the current "
    "primary component.',\n"
    "  GTID_CONSISTENCY VARCHAR(16) NOT NULL COMMENT 'Result of the last GTID "
    "consistency gate: OK, ERRANT, GAP, XID_MISMATCH or SKIPPED.',\n"
    "  SUPER_READ_ONLY_MANAGED ENUM('YES','NO') NOT NULL COMMENT 'Whether the "
    "coordinator is currently holding super_read_only on the DR cluster.',\n"
    "  LAST_ACTION VARCHAR(512) NOT NULL COMMENT 'Human readable description of "
    "the last decision taken by the coordinator.',\n"
    "  LAST_ACTION_TIMESTAMP TIMESTAMP(6) NULL COMMENT 'When the last "
    "action was taken.'\n",
    /* Options */
    " ENGINE=PERFORMANCE_SCHEMA",
    /* Tablespace */
    nullptr);

PFS_engine_table_share table_wsrep_async_failover_status::m_share{
    &pfs_readonly_acl,
    table_wsrep_async_failover_status::create,
    nullptr, /* write_row */
    nullptr, /* delete_all_rows */
    table_wsrep_async_failover_status::get_row_count,
    sizeof(pos_t), /* ref length */
    &m_table_lock,
    &m_table_def,
    true, /* perpetual */
    PFS_engine_table_proxy(),
    {0},
    false /* m_in_purgatory */
};

PFS_engine_table *table_wsrep_async_failover_status::create(
    PFS_engine_table_share *) {
  return new table_wsrep_async_failover_status();
}

table_wsrep_async_failover_status::table_wsrep_async_failover_status()
    : PFS_engine_table(&m_share, &m_pos), m_pos(0), m_next_pos(0) {}

table_wsrep_async_failover_status::~table_wsrep_async_failover_status() =
    default;

void table_wsrep_async_failover_status::reset_position() {
  DBUG_TRACE;
  m_pos.m_index = 0;
  m_next_pos.m_index = 0;
}

ha_rows table_wsrep_async_failover_status::get_row_count() { return 1; }

int table_wsrep_async_failover_status::rnd_init(bool) {
  DBUG_TRACE;
  return 0;
}

int table_wsrep_async_failover_status::rnd_next() {
  DBUG_TRACE;
  m_pos.set_at(&m_next_pos);
  if (m_pos.m_index >= 1) return HA_ERR_END_OF_FILE;
  m_next_pos.set_after(&m_pos);
  return make_row();
}

int table_wsrep_async_failover_status::rnd_pos(const void *pos) {
  DBUG_TRACE;
  set_position(pos);
  if (m_pos.m_index >= 1) return HA_ERR_END_OF_FILE;
  return make_row();
}

int table_wsrep_async_failover_status::make_row() {
  DBUG_TRACE;
  m_row.st = Wsrep_async_failover::instance().snapshot();
  return 0;
}

static const char *acf_mode_name(int mode) {
  switch (mode) {
    case WSREP_ACF_MODE_RECEIVER:
      return "RECEIVER";
    case WSREP_ACF_MODE_SOURCE:
      return "SOURCE";
    case WSREP_ACF_MODE_BOTH:
      return "BOTH";
    default:
      return "OFF";
  }
}

int table_wsrep_async_failover_status::read_row_values(TABLE *table,
                                                       unsigned char *buf,
                                                       Field **fields,
                                                       bool read_all) {
  DBUG_TRACE;
  assert(table->s->null_bytes == 1);
  buf[0] = 0;

  const Wsrep_async_failover::Status &st = m_row.st;
  const char *mode = acf_mode_name(st.mode);

  for (Field *f = nullptr; (f = *fields); fields++) {
    if (read_all || bitmap_is_set(table->read_set, f->field_index())) {
      switch (f->field_index()) {
        case 0: /* ENABLED */
          set_field_enum(f, st.enabled ? 1 : 2);
          break;
        case 1: /* MODE */
          set_field_varchar_utf8mb4(f, mode,
                                    static_cast<uint>(strlen(mode)));
          break;
        case 2: /* CHANNEL_NAME */
          set_field_char_utf8mb4(f, st.channel,
                                 static_cast<uint>(strlen(st.channel)));
          break;
        case 3: /* IS_ACTIVE_REPLICA */
          set_field_enum(f, st.is_active_replica ? 1 : 2);
          break;
        case 4: /* ELECTED_INDEX */
          set_field_longlong(f, st.elected_index);
          break;
        case 5: /* CLUSTER_SIZE */
          set_field_longlong(f, st.cluster_size);
          break;
        case 6: /* GTID_CONSISTENCY */
          set_field_varchar_utf8mb4(
              f, st.gtid_verdict,
              static_cast<uint>(strlen(st.gtid_verdict)));
          break;
        case 7: /* SUPER_READ_ONLY_MANAGED */
          set_field_enum(f, st.super_read_only_managed ? 1 : 2);
          break;
        case 8: /* LAST_ACTION */
          set_field_varchar_utf8mb4(
              f, st.last_action,
              static_cast<uint>(strlen(st.last_action)));
          break;
        case 9: /* LAST_ACTION_TIMESTAMP */
          if (st.last_action_time == 0)
            f->set_null();
          else
            set_field_timestamp(f, st.last_action_time);
          break;
        default:
          assert(false);
      }
    }
  }
  return 0;
}
