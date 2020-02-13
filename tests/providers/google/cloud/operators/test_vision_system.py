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

from tests.providers.google.cloud.operators.test_vision_system_helper import GCPVisionTestHelper
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_AI_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, provide_gcp_context, skip_gcp_system
from tests.test_utils.system_tests_class import SystemTest

VISION_HELPER = GCPVisionTestHelper()


@skip_gcp_system(GCP_AI_KEY)
class CloudVisionExampleDagsSystemTest(SystemTest):
    @provide_gcp_context(GCP_AI_KEY)
    def setUp(self):
        super().setUp()
        VISION_HELPER.create_bucket()

    @provide_gcp_context(GCP_AI_KEY)
    def tearDown(self):
        VISION_HELPER.delete_bucket()
        super().tearDown()

    @provide_gcp_context(GCP_AI_KEY)
    def test_run_example_gcp_vision_autogenerated_id_dag(self):
        self.run_dag('example_gcp_vision_autogenerated_id', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_AI_KEY)
    def test_run_example_gcp_vision_explicit_id_dag(self):
        self.run_dag('example_gcp_vision_explicit_id', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_AI_KEY)
    def test_run_example_gcp_vision_annotate_image_dag(self):
        self.run_dag('example_gcp_vision_annotate_image', CLOUD_DAG_FOLDER)
