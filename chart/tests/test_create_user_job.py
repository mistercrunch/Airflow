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

import jmespath

from chart.tests.helm_template_generator import render_chart


class CreateUserJobTest(unittest.TestCase):
    def test_should_run_by_default(self):
        docs = render_chart(show_only=["templates/jobs/create-user-job.yaml"])
        assert "Job" == docs[0]["kind"]
        assert "create-user" == jmespath.search("spec.template.spec.containers[0].name", docs[0])
        assert 50000 == jmespath.search("spec.template.spec.securityContext.runAsUser", docs[0])

    def test_should_support_annotations(self):
        docs = render_chart(
            values={"createUserJob": {"annotations": {"foo": "bar"}}},
            show_only=["templates/jobs/create-user-job.yaml"],
        )
        annotations = jmespath.search("spec.template.metadata.annotations", docs[0])
        assert "foo" in annotations
        assert "bar" == annotations["foo"]
