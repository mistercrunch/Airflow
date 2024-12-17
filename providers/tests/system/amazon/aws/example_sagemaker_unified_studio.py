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

from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.sagemaker_unified_studio import SageMakerNotebookOperator

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_sagemaker_unified_studio"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"  # must have an IAM role to run notebooks

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    
    role_arn = test_context[ROLE_ARN_KEY]
    notebook_path = "tests/amazon/aws/operators/test_notebook.ipynb"
    
    run_notebook = SageMakerNotebookOperator(
        task_id="initial",
        input_config={'input_path': notebook_path, 'input_params': {}},
        output_config={'output_formats': ['NOTEBOOK']},
        wait_for_completion=True,
        poll_interval=5 
    )
    
    chain(
        test_context,
        run_notebook,
    )
    
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()
    
from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)