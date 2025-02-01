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

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE OVERWRITTEN!
#
# IF YOU WANT TO MODIFY THIS FILE, YOU SHOULD MODIFY THE TEMPLATE
# `get_provider_info_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze/templates` DIRECTORY


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-jdbc",
        "name": "Java Database Connectivity (JDBC)",
        "description": "`Java Database Connectivity (JDBC) <https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/>`__\n",
        "state": "ready",
        "source-date-epoch": 1734535003,
        "versions": [
            "5.0.0",
            "4.5.3",
            "4.5.2",
            "4.5.1",
            "4.5.0",
            "4.4.0",
            "4.3.1",
            "4.3.0",
            "4.2.2",
            "4.2.1",
            "4.2.0",
            "4.1.0",
            "4.0.2",
            "4.0.1",
            "4.0.0",
            "3.4.0",
            "3.3.0",
            "3.2.1",
            "3.2.0",
            "3.1.0",
            "3.0.0",
            "2.1.3",
            "2.1.2",
            "2.1.1",
            "2.1.0",
            "2.0.1",
            "2.0.0",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Java Database Connectivity (JDBC)",
                "external-doc-url": "https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/",
                "how-to-guide": ["/docs/apache-airflow-providers-jdbc/operators.rst"],
                "logo": "/docs/integration-logos/JDBC.png",
                "tags": ["protocol"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Java Database Connectivity (JDBC)",
                "python-modules": ["airflow.providers.jdbc.hooks.jdbc"],
            }
        ],
        "connection-types": [
            {"hook-class-name": "airflow.providers.jdbc.hooks.jdbc.JdbcHook", "connection-type": "jdbc"}
        ],
        "config": {
            "providers.jdbc": {
                "description": "This section applies for the JDBC provider and connection type.",
                "options": {
                    "allow_driver_path_in_extra": {
                        "description": "Whether to allow using ``driver_path`` set in the connection's ``extra`` field. If set to False,\n``driver_path`` will be ignored. If enabling this functionality, you should make sure that you\ntrust the users who can edit connections to not use it maliciously.\n",
                        "version_added": "4.0.0",
                        "type": "boolean",
                        "example": None,
                        "default": "False",
                    },
                    "allow_driver_class_in_extra": {
                        "description": "Whether to allow using ``driver_class`` set in the connection's ``extra`` field. If set to False,\n``driver_class`` will be ignored. If enabling this functionality, you should make sure that you\ntrust the users who can edit connections to not use it maliciously.\n",
                        "version_added": "4.0.0",
                        "type": "boolean",
                        "example": None,
                        "default": "False",
                    },
                },
            }
        },
        "dependencies": [
            "apache-airflow>=2.9.0",
            "apache-airflow-providers-common-sql>=1.20.0",
            "jaydebeapi>=1.1.1",
        ],
    }
