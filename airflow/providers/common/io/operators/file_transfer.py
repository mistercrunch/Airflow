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
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.io.path import ObjectStoragePath
from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.utils.context import Context


class FileTransferOperator(BaseOperator):
    """
    Copies a file from a source to a destination.

    This streams the file from the source to the destination if required
    , so it does not need to fit into memory.

    :param src: The source file path or ObjectStoragePath object.
    :param dst: The destination file path or ObjectStoragePath object.
    :param source_conn_id: The optional source connection id.
    :param dest_conn_id: The optional destination connection id.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FileTransferOperator`
    """

    template_fields: Sequence[str] = ("src", "dst")

    def __init__(
        self,
        *,
        src: str | ObjectStoragePath,
        dst: str | ObjectStoragePath,
        source_conn_id: str | None = None,
        dest_conn_id: str | None = None,
        overwrite: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.src = src
        self.dst = dst
        self.source_conn_id = source_conn_id
        self.dst_conn_id = dest_conn_id
        self.overwrite = overwrite

    def execute(self, context: Context) -> None:
        src: ObjectStoragePath = self._get_path(self.src, self.source_conn_id)
        dst: ObjectStoragePath = self._get_path(self.dst, self.dst_conn_id)

        if not self.overwrite:
            if dst.exists() and dst.is_file():
                raise ValueError(f"Destination {dst} already exists")

        src.copy(dst)

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        from openlineage.client.run import Dataset

        from airflow.providers.openlineage.extractors import OperatorLineage

        src: ObjectStoragePath = self._get_path(self.src, self.source_conn_id)
        dst: ObjectStoragePath = self._get_path(self.dst, self.dst_conn_id)

        input_dataset = Dataset(namespace=src.namespace, name=src.key)
        output_dataset = Dataset(namespace=dst.namespace, name=dst.key)

        return OperatorLineage(
            inputs=[input_dataset],
            outputs=[output_dataset],
        )

    @staticmethod
    def _get_path(path: str | ObjectStoragePath, conn_id: str | None) -> ObjectStoragePath:
        if isinstance(path, str):
            return ObjectStoragePath(path, conn_id=conn_id)
        return path
