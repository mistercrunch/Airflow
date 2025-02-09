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

import os
import shutil
from datetime import timezone, datetime
from pathlib import Path
from typing import List

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.types import ArgNotSet

try:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    raise ImportError(
        "Airflow S3DagBundle requires airflow.providers.amazon.* library, but it is not installed. "
        "Please install Apache Airflow `amazon` provider package. "
        "pip install apache-airflow-providers-amazon"
    )


class S3DagBundle(BaseDagBundle, LoggingMixin):
    """
    S3 DAG bundle - exposes a directory in S3 as a DAG bundle.  This allows
    Airflow to load DAGs directly from an S3 bucket.

    :param aws_conn_id: Airflow connection ID for AWS.  Defaults to AwsBaseHook.default_conn_name.
    :param bucket_name: The name of the S3 bucket containing the DAG files.
    :param prefix:  Optional subdirectory within the S3 bucket where the DAGs are stored.
                    If None, DAGs are assumed to be at the root of the bucket (Optional).
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        aws_conn_id: str | None | ArgNotSet = AwsBaseHook.default_conn_name,
        bucket_name: str,
        prefix: str | None = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.bare_repo_path:Path = Path(self._dag_bundle_root_storage_path).joinpath("s3").joinpath(self.name)  # Local path where DAGs are downloaded.
        try:
            self.hook: S3Hook = S3Hook(aws_conn_id=self.aws_conn_id, extra_args={})  # Initialize S3 hook.
        except AirflowException as e:
            self.log.warning("Could not create S3Hook for connection %s : %s", self.aws_conn_id, e)

    def _initialize(self):
        with self.lock():
            if not self.bare_repo_path.exists():
                self.log.info("Creating bare repo path %s", self.bare_repo_path)
                os.makedirs(self.bare_repo_path, exist_ok=True)

            if not self.bare_repo_path.is_dir():
                raise AirflowException(f"Bare repo path:{self.bare_repo_path} is not a directory.")

            if self.hook.check_for_bucket(bucket_name=self.bucket_name) is False:
                raise AirflowException(f"Given bucket_name={self.bucket_name} does not exists")

            if self.hook.check_for_prefix(bucket_name=self.bucket_name, prefix=self.prefix,
                                          delimiter="/") is False:
                raise AirflowException(f"Given prefix=s3://{self.bucket_name}/{self.prefix} does not exists")

            self._download_s3_dags()
        self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    def _delete_deleted_local_s3_files(self, local_s3_objects: List[Path]):

        local_s3_keys = {key for key in local_s3_objects}

        for item in self.bare_repo_path.iterdir():
            item:Path
            absolute_item_path = item.resolve()

            if absolute_item_path not in local_s3_keys:
                try:
                    if item.is_file():
                        item.unlink(missing_ok=True)
                        self.log.debug(f"Deleted file: {item}")
                    elif item.is_dir() and not os.listdir(item):
                        item.unlink(missing_ok=True)
                        self.log.debug(f"Deleted empty directory: {item}")
                    else:
                        self.log.debug(f"Skipping unknown item type: {item}")
                except OSError as e:
                    self.log.error(f"Error deleting {item}: {e}")
                    raise e

    def _download_if_new_or_updated(self, bucket, obj, target: Path):
        do_download = False
        if not target.exists():
            do_download = True
        else:
            local_stats = target.stat()

            if obj.size != local_stats.st_size:
                do_download = True

            s3_last_modified = obj.last_modified
            local_last_modified = datetime.fromtimestamp(local_stats.st_mtime, tz=timezone.utc)
            if s3_last_modified.replace(microsecond=0) != local_last_modified.replace(microsecond=0):
                do_download = True

        if do_download:
            bucket.download_file(obj.key, target)
            self.log.debug(f"Downloaded {obj.key} to {target.as_posix()}")
        else:
            self.log.debug(f"Dag file not changed skipping download, {obj.key}, {target.as_posix()}")

    def _download_s3_dags(self):
        """Downloads DAG files from the S3 bucket to the local directory."""
        self.log.debug(f"Downloading dags from s3://{self.bucket_name}/{self.prefix} to {self.bare_repo_path}")
        _local_s3_objects = []
        _bucket = self.hook.get_bucket(self.bucket_name)
        for obj in _bucket.objects.filter(Prefix=self.prefix):
            obj_path = Path(obj.key)
            _local_target = self.bare_repo_path.joinpath(obj_path.relative_to(self.prefix))
            if not _local_target.parent.exists():
                _local_target.parent.mkdir(parents=True, exist_ok=True)
            self._download_if_new_or_updated(bucket=_bucket, obj=obj, target=_local_target)
            _local_s3_objects.append(_local_target)

        self._delete_deleted_local_s3_files(local_s3_objects=_local_s3_objects)

    def __repr__(self):
        return (
            f"<S3DagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def get_current_version(self) -> str | None:
        """Returns the current version of the DAG bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Returns the local path to the DAG files."""
        return self.bare_repo_path  # Path where DAGs are downloaded.

    def refresh(self) -> None:
        """Refreshes the DAG bundle by re-downloading the DAGs from S3."""
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            self._download_s3_dags()

    def view_url(self, version: str | None = None) -> str | None:
        """Returns a URL for viewing the DAGs in S3. Currently, doesn't support versioning."""
        if self.version:
            raise AirflowException("S3 url with version is not supported")

        presigned_url = self.hook.generate_presigned_url(
            client_method="get_object", params={"Bucket": self.bucket_name, "Key": self.prefix}
        )
        return presigned_url
