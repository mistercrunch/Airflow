# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import mock
from airflow import configuration, DAG
configuration.load_test_config()
import datetime
from airflow.contrib.hooks.dynamodb_hook import DynamoDBHook
import airflow.contrib.operators.hive_to_dynamodb

DEFAULT_DATE = datetime.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]

try:
    from moto import mock_dynamodb2
except ImportError:
    mock_dynamodb2 = None


class HiveToDynamoDBTransferTest(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG('test_dag_id', default_args=args)
        self.dag = dag
        self.nondefault_schema = "nondefault"

    @unittest.skipIf(mock_dynamodb2 is None, 'mock_dynamodb2 package not present')
    @mock_dynamodb2
    def test_get_conn_returns_a_boto3_connection(self):
        hook = DynamoDBHook(aws_conn_id='aws_default')
        self.assertIsNotNone(hook.get_conn())

    @mock.patch('airflow.hooks.hive_hooks.HiveServer2Hook.get_results', return_value={'data': [{"id": "1", "name": "siddharth"}]})
    @unittest.skipIf(mock_dynamodb2 is None, 'mock_dynamodb2 package not present')
    @mock_dynamodb2
    def test_get_records_with_schema(self, get_results_mock):

        # Configure
        sql = "SELECT 1"

        hook = DynamoDBHook(aws_conn_id='aws_default')
        # this table needs to be created in production
        table = hook.get_conn().create_table(
            TableName="test_airflow",
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "name",
                    "AttributeType": "S"
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        operator = airflow.contrib.operators.hive_to_dynamodb.HiveToDynamoDBTransfer(
            sql=sql,
            table_name="test_airflow",
            task_id='hive_to_dynamodb_check',
            table_keys=['id'],
            dag=self.dag)

        operator.execute(None)

        table = hook.get_conn().Table('test_airflow')
        table.meta.client.get_waiter(
            'table_exists').wait(TableName='test_airflow')
        self.assertEqual(table.item_count, 1)

    def pre_process(self, results):
        return results

    @mock.patch('airflow.hooks.hive_hooks.HiveServer2Hook.get_results', return_value={'data': [{"id": "1", "name": "siddharth"}, {"id": "1", "name": "gupta"}]})
    @unittest.skipIf(mock_dynamodb2 is None, 'mock_dynamodb2 package not present')
    @mock_dynamodb2
    def test_pre_process_records_with_schema(self, get_results_mock):

        # Configure
        sql = "SELECT 1"

        hook = DynamoDBHook(aws_conn_id='aws_default')
        # this table needs to be created in production
        table = hook.get_conn().create_table(
            TableName="test_airflow",
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "name",
                    "AttributeType": "S"
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        operator = airflow.contrib.operators.hive_to_dynamodb.HiveToDynamoDBTransfer(
            sql=sql,
            table_name="test_airflow",
            task_id='hive_to_dynamodb_check',
            table_keys=['id'],
            pre_process=self.pre_process,
            dag=self.dag)

        operator.execute(None)

        table = hook.get_conn().Table('test_airflow')
        table.meta.client.get_waiter(
            'table_exists').wait(TableName='test_airflow')
        self.assertEqual(table.item_count, 1)


if __name__ == '__main__':
    unittest.main()
