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

import logging
import os
from datetime import datetime
from pathlib import Path

from airflow import settings
from airflow.utils import timezone
from airflow.utils.helpers import parse_template_string
from airflow.utils.log.logging_mixin import DISABLE_PROPOGATE
from airflow.utils.log.non_caching_file_handler import NonCachingFileHandler

logger = logging.getLogger(__name__)


class FileProcessorHandler(logging.Handler):
    """
    FileProcessorHandler is a python log handler that handles dag processor logs.

    It creates and delegates log handling to `logging.FileHandler`
    after receiving dag processor context.

    :param base_log_folder: Base log folder to place logs.
    :param filename_template: template filename string
    """

    def __init__(self, base_log_folder, filename_template):
        super().__init__()
        self.handler = None
        self.base_log_folder = base_log_folder
        self.dag_dir = os.path.expanduser(settings.DAGS_FOLDER)
        self.filename_template, self.filename_jinja_template = parse_template_string(
            filename_template
        )

        self._cur_date = datetime.today()
        Path(self._get_log_directory()).mkdir(parents=True, exist_ok=True)

        self._symlink_latest_log_directory()

    def set_context(self, filename):
        """
        Provide filename context to airflow task handler.

        :param filename: filename in which the dag is located
        """
        local_loc = self._init_file(filename)
        self.handler = NonCachingFileHandler(local_loc)
        self.handler.setFormatter(self.formatter)
        self.handler.setLevel(self.level)

        if self._cur_date < datetime.today():
            self._symlink_latest_log_directory()
            self._cur_date = datetime.today()

        return DISABLE_PROPOGATE

    def emit(self, record):
        if self.handler is not None:
            self.handler.emit(record)

    def flush(self):
        if self.handler is not None:
            self.handler.flush()

    def close(self):
        if self.handler is not None:
            self.handler.close()

    def _render_filename(self, filename):
        # Airflow log path used to be generated by `os.path.relpath(filename, self.dag_dir)`, however all DAGs
        # in airflow source code are not located in the DAG dir as other DAGs.
        # That will create a log filepath which is not under control since it could be outside
        # of the log dir. The change here is to make sure the log path for DAGs in airflow code
        # is always inside the log dir as other DAGs. To be differentiate with regular DAGs,
        # their logs will be in the `log_dir/native_dags`.
        import airflow

        airflow_directory = airflow.__path__[0]
        if filename.startswith(airflow_directory):
            filename = os.path.join(
                "native_dags", os.path.relpath(filename, airflow_directory)
            )
        else:
            filename = os.path.relpath(filename, self.dag_dir)
        ctx = {"filename": filename}

        if self.filename_jinja_template:
            return self.filename_jinja_template.render(**ctx)

        return self.filename_template.format(filename=ctx["filename"])

    def _get_log_directory(self):
        return os.path.join(self.base_log_folder, timezone.utcnow().strftime("%Y-%m-%d"))

    def _symlink_latest_log_directory(self):
        """
        Create symbolic link to the current day's log directory.

        Allows easy access to the latest scheduler log files.

        :return: None
        """
        log_directory = self._get_log_directory()
        latest_log_directory_path = os.path.join(self.base_log_folder, "latest")
        if os.path.isdir(log_directory):
            rel_link_target = Path(log_directory).relative_to(
                Path(latest_log_directory_path).parent
            )
            try:
                # if symlink exists but is stale, update it
                if os.path.islink(latest_log_directory_path):
                    if os.path.realpath(latest_log_directory_path) != log_directory:
                        os.unlink(latest_log_directory_path)
                        os.symlink(rel_link_target, latest_log_directory_path)
                elif os.path.isdir(latest_log_directory_path) or os.path.isfile(
                    latest_log_directory_path
                ):
                    logger.warning(
                        "%s already exists as a dir/file. Skip creating symlink.",
                        latest_log_directory_path,
                    )
                else:
                    os.symlink(rel_link_target, latest_log_directory_path)
            except OSError:
                logger.warning(
                    "OSError while attempting to symlink the latest log directory"
                )

    def _init_file(self, filename):
        """
        Create log file and directory if required.

        :param filename: task instance object
        :return: relative log path of the given task instance
        """
        relative_log_file_path = os.path.join(
            self._get_log_directory(), self._render_filename(filename)
        )
        log_file_path = os.path.abspath(relative_log_file_path)
        directory = os.path.dirname(log_file_path)

        Path(directory).mkdir(parents=True, exist_ok=True)

        if not os.path.exists(log_file_path):
            open(log_file_path, "a").close()

        return log_file_path
