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

#ifndef TABLE_WSREP_ASYNC_FAILOVER_STATUS_H
#define TABLE_WSREP_ASYNC_FAILOVER_STATUS_H

/**
  @file storage/perfschema/table_wsrep_async_failover_status.h
  Table wsrep_async_failover_status (declarations).

  PXC-5201: exposes the state of the Cluster-Aware Asynchronous Replication
  Failover Coordinator (one row per node).
*/

#include <sys/types.h>

#include "my_base.h"
#include "sql/wsrep_async_failover.h"
#include "storage/perfschema/pfs_engine_table.h"
#include "storage/perfschema/table_helper.h"

class Field;
class Plugin_table;
struct TABLE;
struct THR_LOCK;

/**
  @addtogroup performance_schema_tables
  @{
*/

/** A row of PERFORMANCE_SCHEMA.WSREP_ASYNC_FAILOVER_STATUS. */
struct st_row_wsrep_acf {
  Wsrep_async_failover::Status st;
};

class table_wsrep_async_failover_status : public PFS_engine_table {
  typedef PFS_simple_index pos_t;

 private:
  int make_row();

  static THR_LOCK m_table_lock;
  static Plugin_table m_table_def;

  st_row_wsrep_acf m_row;
  pos_t m_pos;
  pos_t m_next_pos;

 protected:
  int read_row_values(TABLE *table, unsigned char *buf, Field **fields,
                      bool read_all) override;
  table_wsrep_async_failover_status();

 public:
  ~table_wsrep_async_failover_status() override;

  static PFS_engine_table_share m_share;
  static PFS_engine_table *create(PFS_engine_table_share *tbs);
  static ha_rows get_row_count();

  void reset_position() override;
  int rnd_init(bool scan) override;
  int rnd_next() override;
  int rnd_pos(const void *pos) override;
};

/** @} */
#endif
