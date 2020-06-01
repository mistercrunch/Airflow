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

import unittest

from airflow.api_connexion.schemas.connection import (
    connection_collection_item_schema, connection_collection_schema, connection_schema,
)
from airflow.models import Connection
from airflow.utils.session import create_session, provide_session
from tests.test_utils.db import clear_db_connections


class TestConnectionCollectionItemSchema(unittest.TestCase):

    def setUp(self) -> None:
        with create_session() as session:
            session.query(Connection).delete()

    def tearDown(self) -> None:
        clear_db_connections()

    @provide_session
    def test_serialzie(self, session):
        connection_model = Connection(
            conn_id='mysql_default',
            conn_type='mysql',
            host='mysql',
            login='login',
            schema='testschema',
            port=80
        )
        session.add(connection_model)
        session.commit()
        connection_model = session.query(Connection).first()
        deserialized_connection = connection_collection_item_schema.dump(connection_model)
        self.assertEqual(
            deserialized_connection[0],
            {
                'connection_id': "mysql_default",
                'conn_type': 'mysql',
                'host': 'mysql',
                'login': 'login',
                'schema': 'testschema',
                'port': 80
            }
        )


class TestConnectionCollectionSchema(unittest.TestCase):

    def setUp(self) -> None:
        with create_session() as session:
            session.query(Connection).delete()

    def tearDown(self) -> None:
        clear_db_connections()

    @provide_session
    def test_serialzie(self, session):
        connection_model_1 = Connection(
            conn_id='mysql_default_1'
        )
        connection_model_2 = Connection(
            conn_id='mysql_default_2',
        )
        connections = [connection_model_1, connection_model_2]
        session.add_all(connections)
        session.commit()
        deserialized_connections = connection_collection_schema.dump(connections)
        self.assertEqual(
            deserialized_connections[0],
            {
                'connections': [
                    {
                        "connection_id": "mysql_default_1",
                        "conn_type": None,
                        "host": None,
                        "login": None,
                        'schema': None,
                        'port': None
                    },
                    {
                        "connection_id": "mysql_default_2",
                        "conn_type": None,
                        "host": None,
                        "login": None,
                        'schema': None,
                        'port': None
                    }
                ],
                'total_entries': 2
            }
        )


class TestConnectionSchema(unittest.TestCase):

    def setUp(self) -> None:
        with create_session() as session:
            session.query(Connection).delete()

    def tearDown(self) -> None:
        clear_db_connections()

    @provide_session
    def test_serialzie(self, session):
        connection_model = Connection(
            conn_id='mysql_default',
            conn_type='mysql',
            host='mysql',
            login='login',
            schema='testschema',
            port=80,
            password='test-password',
            extra="{'key':'string'}"
        )
        session.add(connection_model)
        session.commit()
        connection_model = session.query(Connection).first()
        deserialized_connection = connection_schema.dump(connection_model)
        self.assertEqual(
            deserialized_connection[0],
            {
                'connection_id': "mysql_default",
                'conn_type': 'mysql',
                'host': 'mysql',
                'login': 'login',
                'schema': 'testschema',
                'port': 80,
                'extra': "{'key':'string'}"
            }
        )
