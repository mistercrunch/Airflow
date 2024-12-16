#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
remove pickled data from dagrun table.

Revision ID: e39a26ac59f6
Revises: 038dc8bc6284
Create Date: 2024-12-01 08:33:15.425141

"""

from __future__ import annotations

import json
import pickle
from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "e39a26ac59f6"
down_revision = "038dc8bc6284"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply remove pickled data from dagrun table."""
    conn = op.get_bind()
    conf_type = sa.JSON().with_variant(postgresql.JSONB, "postgresql")
    op.add_column("dag_run", sa.Column("conf_json", conf_type, nullable=True))

    if context.is_offline_mode():
        # Update the dag_run.conf column value to NULL
        print(
            dedent("""
             ------------
             --  WARNING: Unable to migrate data of conf column in offline mode!
             --  Column conf will be set to NULL in offline mode!.
             --  DO not use Offline mode if data if you want to keep conf values.
             ------------
             """)
        )
        conn.execute(text("UPDATE dag_run set conf=null WHERE conf IS NOT NULL"))
    else:
        BATCH_SIZE = 2
        total_processed = 0
        offset = 0
        while True:
            rows = conn.execute(
                text(
                    f"SELECT id,conf FROM dag_run WHERE conf IS not NULL order by id LIMIT {BATCH_SIZE} OFFSET {offset}"
                )
            ).fetchall()
            if not rows:
                break
            for row in rows:
                row_id, pickle_data = row

                try:
                    original_data = pickle.loads(pickle_data)
                    json_data = json.dumps(original_data)
                    conn.execute(text(f"UPDATE dag_run SET conf_json ='{json_data}' WHERE id = {row_id}"))
                except Exception as e:
                    print(f"Error processing row ID {row_id}: {e}")
                    continue
            offset += BATCH_SIZE

            # Update total processed count
            total_processed += len(rows)

    op.drop_column("dag_run", "conf")

    op.alter_column("dag_run", "conf_json", existing_type=conf_type, new_column_name="conf")


def downgrade():
    """Unapply Remove pickled data from dagrun table."""
    conn = op.get_bind()
    conf_type = sa.LargeBinary().with_variant(postgresql.BYTEA, "postgresql")
    op.add_column("dag_run", sa.Column("conf_pickle", conf_type, nullable=True))

    if context.is_offline_mode():
        # Update the dag_run.conf column value to NULL
        print(
            dedent("""
             ------------
             --  WARNING: Unable to migrate data of conf column in offline mode!
             --  Column conf will be set to NULL in offline mode!.
             --  DO not use Offline mode if data if you want to keep conf values.
             ------------
             """)
        )
        conn.execute(text("UPDATE dag_run set conf=null WHERE conf IS NOT NULL"))
    else:
        BATCH_SIZE = 2
        total_processed = 0
        offset = 0
        while True:
            rows = conn.execute(
                text(
                    f"SELECT id,conf FROM dag_run WHERE conf IS not NULL order by id LIMIT {BATCH_SIZE} OFFSET {offset}"
                )
            ).fetchall()
            if not rows:
                break  #
            for row in rows:
                row_id, json_data = row

                try:
                    pickled_data = pickle.dumps(json_data, protocol=pickle.HIGHEST_PROTOCOL)
                    conn.execute(
                        text("""
                            UPDATE dag_run
                            SET conf_pickle = :pickle_data
                            WHERE id = :id
                        """),
                        {"pickle_data": pickled_data, "id": row_id},
                    )
                except Exception as e:
                    print(f"Error processing row ID {row_id}: {e}")
                    continue
            offset += BATCH_SIZE

            # Update total processed count
            total_processed += len(rows)

    op.drop_column("dag_run", "conf")

    op.alter_column("dag_run", "conf_pickle", existing_type=conf_type, new_column_name="conf")
