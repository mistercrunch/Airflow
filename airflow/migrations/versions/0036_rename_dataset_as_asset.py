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
Rename dataset as asset.

Revision ID: 05234396c6fc
Revises: 0d9e73a75ee4
Create Date: 2024-10-02 08:10:01.697128

"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic import op

# revision identifiers, used by Alembic.
revision = "05234396c6fc"
down_revision = "0d9e73a75ee4"
branch_labels = None
depends_on = None

if TYPE_CHECKING:
    from alembic.operations.base import BatchOperations


def _rename_index(
    batch_op: BatchOperations, original_name: str, new_name: str, columns: list[str], unique: bool
) -> None:
    batch_op.drop_index(original_name)
    batch_op.create_index(new_name, columns, unique=unique)


def _rename_fk_constraint(
    batch_op: BatchOperations,
    original_name: str,
    new_name: str,
    reference_table: str,
    local_cols: list[str],
    remote_cols: list[str],
    ondelete: str,
) -> None:
    batch_op.drop_constraint(original_name)
    batch_op.create_foreign_key(
        constraint_name=new_name,
        referent_table=reference_table,
        local_cols=local_cols,
        remote_cols=remote_cols,
        ondelete=ondelete,
    )


def upgrade():
    """Rename dataset as asset."""
    op.rename_table("dataset_alias", "asset_alias")
    with op.batch_alter_table("asset_alias", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_name_unique",
            new_name="idx_asset_alias_name_unique",
            columns=["name"],
            unique=True,
        )

        # batch_op.drop_constraint("dataset_alias_pkey")
        # batch_op.drop_constraint("dataset_alias_pkey")

    op.rename_table("dataset_event", "asset_event")
    with op.batch_alter_table("asset_event", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_id_timestamp",
            new_name="idx_asset_id_timestamp",
            columns=["dataset_id", "timestamp"],
            unique=False,
        )

    op.rename_table("dataset_alias_dataset", "asset_alias_asset")
    with op.batch_alter_table("asset_alias_asset", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_alias_id",
            new_name="idx_asset_alias_asset_alias_id",
            columns=["alias_id"],
            unique=False,
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_dataset_id",
            new_name="idx_asset_alias_asset_asset_id",
            columns=["dataset_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="ds_dsa_alias_id",
            new_name="a_aa_alias_id",
            reference_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="ds_dsa_dataset_id",
            new_name="a_aa_asset_id",
            reference_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dataset_alias_dataset_dataset_id_fkey",
            new_name="asset_alias_asset_dataset_id_fkey",
            reference_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        batch_op.drop_constraint(op.f("dataset_alias_dataset_pkey"))
        batch_op.create_primary_key(
            constraint_name=op.f("asset_alias_asset_pkey"),
            columns=["alias_id", "dataset_id"],
        )

    op.rename_table("dataset_alias_dataset_event", "asset_alias_asset_event")
    with op.batch_alter_table("asset_alias_asset_event", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_datasett_event_alias_id",
            new_name="idx_asset_alias_asset_event_alias_id",
            columns=["alias_id"],
            unique=False,
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_alias_dataset_event_event_id",
            new_name="idx_asset_alias_asset_event_event_id",
            columns=["event_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dss_de_dataset_id",
            new_name="dss_de_alias_id",
            reference_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dss_de_event_id",
            new_name="dss_de_event_id",
            reference_table="asset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        _rename_fk_constraint(
            batch_op=batch_op,
            original_name=op.f("dataset_alias_dataset_event_event_id_fkey"),
            new_name=op.f("asset_alias_asset_event_event_id_fkey"),
            reference_table="asset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        _rename_fk_constraint(
            batch_op=batch_op,
            original_name=op.f("dataset_alias_dataset_event_alias_id_fkey"),
            new_name=op.f("asset_alias_asset_event_alias_id_fkey"),
            reference_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        batch_op.drop_constraint(op.f("dataset_alias_dataset_event_pkey"))
        batch_op.create_primary_key(
            constraint_name=op.f("asset_alias_asset_event_pkey"),
            columns=["alias_id", "event_id"],
        )

    op.rename_table("dataset_dag_run_queue", "asset_dag_run_queue")
    with op.batch_alter_table("asset_dag_run_queue", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_dag_run_queue_target_dag_id",
            new_name="idx_asset_dag_run_queue_target_dag_id",
            columns=["target_dag_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="ddrq_dataset_fkey",
            new_name="adrq_asset_fkey",
            reference_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="ddrq_dag_fkey",
            new_name="adrq_dag_fkey",
            reference_table="dag",
            local_cols=["target_dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )

        batch_op.drop_constraint("datasetdagrunqueue_pkey")
        batch_op.create_primary_key(
            constraint_name="assetdagrunqueue_pkey",
            columns=["dataset_id", "target_dag_id"],
        )

    op.rename_table("dag_schedule_dataset_alias_reference", "dag_schedule_asset_alias_reference")
    with op.batch_alter_table("dag_schedule_asset_alias_reference", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_dataset_alias_reference_dag_id",
            new_name="idx_dag_schedule_asset_alias_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dsdar_dataset_alias_fkey",
            new_name="dsaar_asset_alias_fkey",
            reference_table="asset_alias",
            local_cols=["alias_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


    op.rename_table("dag_schedule_dataset_reference", "dag_schedule_asset_reference")
    with op.batch_alter_table("dag_schedule_asset_reference", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dag_schedule_dataset_reference_dag_id",
            new_name="idx_dag_schedule_asset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dsdr_dag_id_fkey",
            new_name="dsar_dag_id_fkey",
            reference_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )


        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="dsdr_dataset_fkey",
            new_name="dsar_asset_fkey",
            reference_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        batch_op.drop_constraint("dsdr_pkey")
        batch_op.create_primary_key(
            constraint_name="dsar_pkey",
            columns=["dataset_id", "dag_id"],
        )

    op.rename_table("task_outlet_dataset_reference", "task_outlet_asset_reference")
    with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_task_outlet_dataset_reference_dag_id",
            new_name="idx_task_outlet_asset_reference_dag_id",
            columns=["dag_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="todr_dag_id_fkey",
            new_name="todr_dag_id_fkey",
            reference_table="dag",
            local_cols=["dag_id"],
            remote_cols=["dag_id"],
            ondelete="CASCADE",
        )


        _rename_fk_constraint(
            batch_op=batch_op,
            original_name="todr_dataset_fkey",
            new_name="todr_asset_fkey",
            reference_table="dataset",
            local_cols=["dataset_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        batch_op.drop_constraint("todr_pkey")
        batch_op.create_primary_key(
            constraint_name="todr_pkey",
            columns=["dataset_id", "dag_id", "task_id"],
        )

    op.rename_table("dagrun_dataset_event", "dagrun_asset_event")
    with op.batch_alter_table("dagrun_asset_event", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_dataset_events_dag_run_id",
            new_name="idx_dagrun_asset_events_dag_run_id",
            columns=["dag_run_id"],
            unique=False,
        )

        _rename_index(
            batch_op=batch_op,
            original_name="idx_dagrun_dataset_events_event_id",
            new_name="idx_dagrun_asset_events_event_id",
            columns=["event_id"],
            unique=False,
        )

        _rename_fk_constraint(
            batch_op=batch_op,
            original_name=op.f("dagrun_dataset_event_dag_run_id_fkey"),
            new_name=op.f("dagrun_asset_event_dag_run_id_fkey"),
            reference_table="dag_run",
            local_cols=["dag_run_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        _rename_fk_constraint(
            batch_op=batch_op,
            original_name=op.f("dagrun_dataset_event_event_id_fkey"),
            new_name=op.f("dagrun_asset_event_event_id_fkey"),
            reference_table="asset_event",
            local_cols=["event_id"],
            remote_cols=["id"],
            ondelete="CASCADE",
        )


        batch_op.drop_constraint(op.f("dagrun_dataset_event_pkey"))
        batch_op.create_primary_key(
            constraint_name=op.f("dagrun_asset_event_pkey"),
            columns=["dag_run_id", "event_id"],
        )

    with op.batch_alter_table("dataset", schema=None) as batch_op:
        _rename_index(
            batch_op=batch_op,
            original_name="idx_dataset_name_uri_unique",
            new_name="idx_asset_name_uri_unique",
            columns=["name", "uri"],
            unique=True,
        )


def downgrade():
    """Unapply Rename dataset as asset."""
    # with op.batch_alter_table("dataset", schema=None) as batch_op:
    #     batch_op.drop_index("idx_asset_name_uri_unique")
    #     batch_op.create_index("idx_dataset_name_uri_unique", ["name", "uri"], unique=True)
    #
    # with op.batch_alter_table("dagrun_asset_event", schema=None) as batch_op:
    #     batch_op.drop_index("idx_dagrun_asset_events_event_id")
    #     batch_op.drop_index("idx_dagrun_asset_events_dag_run_id")
    #
    # op.drop_table("dagrun_asset_event")
    # with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
    #     batch_op.drop_index("idx_task_outlet_asset_reference_dag_id")
    #
    # op.drop_table("task_outlet_asset_reference")
    # with op.batch_alter_table("dag_schedule_asset_reference", schema=None) as batch_op:
    #     batch_op.drop_index("idx_dag_schedule_asset_reference_dag_id")
    #
    # op.drop_table("dag_schedule_asset_reference")
    # with op.batch_alter_table("dag_schedule_asset_alias_reference", schema=None) as batch_op:
    #     batch_op.drop_index("idx_dag_schedule_asset_alias_reference_dag_id")
    #
    # op.drop_table("dag_schedule_asset_alias_reference")
    # with op.batch_alter_table("asset_dag_run_queue", schema=None) as batch_op:
    #     batch_op.drop_index("idx_asset_dag_run_queue_target_dag_id")
    #
    # op.drop_table("asset_dag_run_queue")
    # with op.batch_alter_table("asset_alias_asset_event", schema=None) as batch_op:
    #     batch_op.drop_index("idx_asset_alias_asset_event_event_id")
    #     batch_op.drop_index("idx_asset_alias_asset_event_alias_id")
    #
    # op.drop_table("asset_alias_asset_event")
    # with op.batch_alter_table("asset_alias_asset", schema=None) as batch_op:
    #     batch_op.drop_index("idx_asset_alias_asset_asset_id")
    #     batch_op.drop_index("idx_asset_alias_asset_alias_id")
    #
    # op.drop_table("asset_alias_asset")
    # with op.batch_alter_table("asset_event", schema=None) as batch_op:
    #     batch_op.drop_index("idx_asset_id_timestamp")
    #
    # op.drop_table("asset_event")
    # with op.batch_alter_table("asset_alias", schema=None) as batch_op:
    #     batch_op.drop_index("idx_asset_alias_name_unique")
    #
    # op.drop_table("asset_alias")
    # # ### end Alembic commands ###
