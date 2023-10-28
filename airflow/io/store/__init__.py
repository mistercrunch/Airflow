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

from typing import TYPE_CHECKING, ClassVar

from airflow.io import get_fs, has_fs
from airflow.utils.module_loading import qualname

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem


class ObjectStore:
    """Manages a filesystem or object storage."""

    __version__: ClassVar[int] = 1

    method: str
    conn_id: str | None
    protocol: str

    _fs: AbstractFileSystem = None

    def __init__(self, protocol: str, conn_id: str | None, fs: AbstractFileSystem | None = None):
        self.conn_id = conn_id
        self.protocol = protocol
        self._fs = fs

    def __str__(self):
        return f"{self.protocol}-{self.conn_id}" if self.conn_id else self.protocol

    @property
    def fs(self) -> AbstractFileSystem:
        self._connect()
        return self._fs

    @property
    def fsid(self) -> str:
        """
        Get the filesystem id for this store in order to be able to compare across instances.

        The underlying `fsid` is returned from the filesystem if available, otherwise it is generated
        from the protocol and connection ID.

        :return: deterministic the filesystem ID
        """
        self._connect()
        try:
            return self._fs.fsid
        except NotImplementedError:
            return f"{self.fs.protocol}-{self.conn_id or 'env'}"

    def serialize(self):
        return {
            "protocol": self.protocol,
            "conn_id": self.conn_id,
            "filesystem": qualname(self._fs) if self._fs else None,
        }

    @classmethod
    def deserialize(cls, data: dict[str, str], version: int):
        if version > cls.__version__:
            raise ValueError(f"Cannot deserialize version {version} for {cls.__name__}")

        protocol = data["protocol"]
        conn_id = data["conn_id"]

        alias = f"{protocol}-{conn_id}" if conn_id else protocol

        if store := _STORE_CACHE.get(alias, None):
            return store

        if not has_fs(protocol):
            if "filesystem" in data and data["filesystem"]:
                raise ValueError(
                    f"No attached filesystem found for {data['filesystem']} with "
                    f"protocol {data['protocol']}. Please use attach() for this protocol and filesystem."
                )

        return attach(protocol=protocol, conn_id=conn_id)

    def _connect(self):
        if self._fs is None:
            self._fs = get_fs(self.protocol, self.conn_id)

    def __eq__(self, other):
        return isinstance(other, type(self)) and other.conn_id == self.conn_id and other._fs == self._fs


_STORE_CACHE: dict[str, ObjectStore] = {}


def attach(
    protocol: str | None = None,
    conn_id: str | None = None,
    alias: str | None = None,
    encryption_type: str | None = "",
    fs: AbstractFileSystem | None = None,
) -> ObjectStore:
    """
    Attach a filesystem or object storage.

    :param alias: the alias to be used to refer to the store, autogenerated if omitted
    :param protocol: the scheme that is used without ://
    :param conn_id: the connection to use to connect to the filesystem
    :param encryption_type: the encryption type to use to connect to the filesystem
    :param fs: the filesystem type to use to connect to the filesystem
    """
    if alias:
        if store := _STORE_CACHE.get(alias, None):
            return store
        elif not protocol:
            raise ValueError(f"No registered store with alias: {alias}")

    if not protocol:
        raise ValueError("No protocol specified and no alias provided")

    if not alias:
        alias = f"{protocol}-{conn_id}" if conn_id else protocol
        if store := _STORE_CACHE.get(alias, None):
            return store

    store = ObjectStore(protocol=protocol, conn_id=conn_id, fs=fs)
    _STORE_CACHE[alias] = store

    return store
