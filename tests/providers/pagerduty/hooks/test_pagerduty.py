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
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.pagerduty.hooks.pagerduty import PagerdutyEventsHook, PagerdutyHook
from airflow.utils.session import provide_session

DEFAULT_CONN_ID = "pagerduty_default"


@pytest.fixture(scope="class")
@provide_session
def connections(request, session=None):
    connections = [
        Connection(
            conn_id=DEFAULT_CONN_ID, conn_type="http", password="token", extra='{"routing_key": "route"}'
        ),
        Connection(conn_id="pagerduty_no_extra", conn_type="http", password="pagerduty_token_without_extra"),
        Connection(conn_id="pagerduty_events_connection", conn_type="http", password="events_token"),
    ]
    for c in connections:
        session.add(c)
    session.commit()

    def cleanup():
        for c in connections:
            session.delete(c)
        session.commit()

    request.addfinalizer(cleanup)


class TestPagerdutyHook:
    def test_get_token_from_password(self, connections):
        hook = PagerdutyHook(pagerduty_conn_id=DEFAULT_CONN_ID)
        assert hook.token == "token", "token initialised."

    def test_without_routing_key_extra(self):
        hook = PagerdutyHook(pagerduty_conn_id="pagerduty_no_extra")
        assert hook.token == "pagerduty_token_without_extra", "token initialised."
        assert hook.routing_key is None, "default routing key skipped."

    def test_token_parameter_override(self):
        hook = PagerdutyHook(token="pagerduty_param_token", pagerduty_conn_id=DEFAULT_CONN_ID)
        assert hook.token == "pagerduty_param_token", "token initialised."

    def test_get_service(self, requests_mock):
        hook = PagerdutyHook(pagerduty_conn_id=DEFAULT_CONN_ID)
        mock_response_body = {
            "id": "PZYX321",
            "name": "Apache Airflow",
            "status": "active",
            "type": "service",
            "summary": "Apache Airflow",
            "self": "https://api.pagerduty.com/services/PZYX321",
        }
        requests_mock.get("https://api.pagerduty.com/services/PZYX321", json={"service": mock_response_body})
        session = hook.get_session()
        resp = session.rget("/services/PZYX321")
        assert resp == mock_response_body

    @mock.patch.object(PagerdutyEventsHook, "__init__")
    @mock.patch.object(PagerdutyEventsHook, "create_event")
    def test_create_event(self, events_hook_create_event, events_hook_init):
        events_hook_init.return_value = None
        hook = PagerdutyHook(pagerduty_conn_id=DEFAULT_CONN_ID)
        hook.create_event(
            routing_key="different_key",
            summary="test",
            source="airflow_test",
            severity="error",
        )
        events_hook_init.assert_called_with(integration_key="different_key")
        events_hook_create_event.assert_called_with(
            summary="test",
            source="airflow_test",
            severity="error",
            action="trigger",
            dedup_key=None,
            custom_details=None,
            group=None,
            component=None,
            class_type=None,
            images=None,
            links=None,
        )


class TestPagerdutyEventsHook:
    def test_get_integration_key_from_password(self, connections):
        hook = PagerdutyEventsHook(pagerduty_conn_id="pagerduty_events_connection")
        assert hook.integration_key == "events_token", "token initialised."

    def test_token_parameter_override(self):
        hook = PagerdutyEventsHook(integration_key="override_key", pagerduty_conn_id=DEFAULT_CONN_ID)
        assert hook.integration_key == "override_key", "token initialised."

    def test_create_event(self, requests_mock):
        hook = PagerdutyEventsHook(pagerduty_conn_id="pagerduty_events_connection")
        mock_response_body = {
            "status": "success",
            "message": "Event processed",
            "dedup_key": "samplekeyhere",
        }
        requests_mock.post("https://events.pagerduty.com/v2/enqueue", json=mock_response_body)
        resp = hook.create_event(
            summary="test",
            source="airflow_test",
            severity="error",
        )
        assert resp == mock_response_body
