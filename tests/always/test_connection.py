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

import json
import os
import re
from collections import namedtuple
from unittest import mock

import pytest
import sqlalchemy
from cryptography.fernet import Fernet

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection, crypto
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from tests.test_utils.config import conf_vars

ConnectionParts = namedtuple("ConnectionParts", ["conn_type", "login", "password", "host", "port", "schema"])


class UriTestCaseConfig:
    def __init__(
        self,
        test_conn_uri: str,
        test_conn_attributes: dict,
        description: str,
    ):
        """

        :param test_conn_uri: URI that we use to create connection
        :param test_conn_attributes: we expect a connection object created with `test_uri` to have these
        attributes
        :param description: human-friendly name appended to parameterized test
        """
        self.test_uri = test_conn_uri
        self.test_conn_attributes = test_conn_attributes
        self.description = description

    @staticmethod
    def uri_test_name(func, num, param):
        return f"{func.__name__}_{num}_{param.args[0].description.replace(' ', '_')}"


class TestConnection:
    def setup_method(self):
        crypto._fernet = None
        self.patcher = mock.patch("airflow.models.connection.mask_secret", autospec=True)
        self.mask_secret = self.patcher.start()

    def teardown_method(self):
        crypto._fernet = None
        self.patcher.stop()

    @conf_vars({("core", "fernet_key"): ""})
    def test_connection_extra_no_encryption(self):
        """
        Tests extras on a new connection without encryption. The fernet key
        is set to a non-base64-encoded string and the extra is stored without
        encryption.
        """
        test_connection = Connection(extra="testextra")
        assert not test_connection.is_extra_encrypted
        assert test_connection.extra == "testextra"

    @conf_vars({("core", "fernet_key"): Fernet.generate_key().decode()})
    def test_connection_extra_with_encryption(self):
        """
        Tests extras on a new connection with encryption.
        """
        test_connection = Connection(extra="testextra")
        assert test_connection.is_extra_encrypted
        assert test_connection.extra == "testextra"

    def test_connection_extra_with_encryption_rotate_fernet_key(self):
        """
        Tests rotating encrypted extras.
        """
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        with conf_vars({("core", "fernet_key"): key1.decode()}):
            test_connection = Connection(extra="testextra")
            assert test_connection.is_extra_encrypted
            assert test_connection.extra == "testextra"
            assert Fernet(key1).decrypt(test_connection._extra.encode()) == b"testextra"

        # Test decrypt of old value with new key
        with conf_vars({("core", "fernet_key"): ",".join([key2.decode(), key1.decode()])}):
            crypto._fernet = None
            assert test_connection.extra == "testextra"

            # Test decrypt of new value with new key
            test_connection.rotate_fernet_key()
            assert test_connection.is_extra_encrypted
            assert test_connection.extra == "testextra"
            assert Fernet(key2).decrypt(test_connection._extra.encode()) == b"testextra"

    test_from_uri_params = [
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password@host%2Flocation:1234/schema",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location",
                schema="schema",
                login="user",
                password="password",
                port=1234,
                extra=None,
            ),
            description="without extras",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password@host%2Flocation:1234/schema?"
            "extra1=a%20value&extra2=%2Fpath%2F",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location",
                schema="schema",
                login="user",
                password="password",
                port=1234,
                extra_dejson={"extra1": "a value", "extra2": "/path/"},
            ),
            description="with extras",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password@host%2Flocation:1234/schema?__extra__=single+value",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location",
                schema="schema",
                login="user",
                password="password",
                port=1234,
                extra="single value",
            ),
            description="with extras single value",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password@host%2Flocation:1234/schema?"
            "__extra__=arbitrary+string+%2A%29%2A%24",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location",
                schema="schema",
                login="user",
                password="password",
                port=1234,
                extra="arbitrary string *)*$",
            ),
            description="with extra non-json",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password@host%2Flocation:1234/schema?"
            "__extra__=%5B%22list%22%2C+%22of%22%2C+%22values%22%5D",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location",
                schema="schema",
                login="user",
                password="password",
                port=1234,
                extra_dejson=["list", "of", "values"],
            ),
            description="with extras list",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password@host%2Flocation:1234/schema?"
            "__extra__=%7B%22my_val%22%3A+%5B%22list%22%2C+%22of%22%2C+%22values%22%5D%2C+%22extra%22%3A+%7B%22nested%22%3A+%7B%22json%22%3A+%22val%22%7D%7D%7D",  # noqa: E501
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location",
                schema="schema",
                login="user",
                password="password",
                port=1234,
                extra_dejson={"my_val": ["list", "of", "values"], "extra": {"nested": {"json": "val"}}},
            ),
            description="with nested json",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password@host%2Flocation:1234/schema?extra1=a%20value&extra2=",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location",
                schema="schema",
                login="user",
                password="password",
                port=1234,
                extra_dejson={"extra1": "a value", "extra2": ""},
            ),
            description="with empty extras",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password@host%2Flocation%3Ax%3Ay:1234/schema?"
            "extra1=a%20value&extra2=%2Fpath%2F",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location:x:y",
                schema="schema",
                login="user",
                password="password",
                port=1234,
                extra_dejson={"extra1": "a value", "extra2": "/path/"},
            ),
            description="with colon in hostname",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password%20with%20space@host%2Flocation%3Ax%3Ay:1234/schema",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location:x:y",
                schema="schema",
                login="user",
                password="password with space",
                port=1234,
            ),
            description="with encoded password",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://domain%2Fuser:password@host%2Flocation%3Ax%3Ay:1234/schema",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host/location:x:y",
                schema="schema",
                login="domain/user",
                password="password",
                port=1234,
            ),
            description="with encoded user",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password%20with%20space@host:1234/schema%2Ftest",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host",
                schema="schema/test",
                login="user",
                password="password with space",
                port=1234,
            ),
            description="with encoded schema",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://user:password%20with%20space@host:1234",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host",
                schema="scheme",
                login="user",
                password="password with space",
                port=1234,
            ),
            description="no schema",
        ),
        UriTestCaseConfig(
            test_conn_uri="google-cloud-platform://?key_path=%2Fkeys%2Fkey.json&scope="
            "https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&project=airflow",
            test_conn_attributes=dict(
                conn_type="google_cloud_platform",
                host="",
                schema="google-cloud-platform",
                login=None,
                password=None,
                port=None,
                extra_dejson=dict(
                    key_path="/keys/key.json",
                    scope="https://www.googleapis.com/auth/cloud-platform",
                    project="airflow",
                ),
            ),
            description="with underscore",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://host:1234",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="host",
                schema="scheme",
                login=None,
                password=None,
                port=1234,
            ),
            description="without auth info",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://%2FTmP%2F:1234",
            test_conn_attributes=dict(
                conn_type="scheme",
                host="/TmP/",
                schema="scheme",
                login=None,
                password=None,
                port=1234,
            ),
            description="with path",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme:///airflow",
            test_conn_attributes=dict(
                conn_type="scheme",
                schema="airflow",
            ),
            description="schema only",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://@:1234",
            test_conn_attributes=dict(
                conn_type="scheme",
                schema="scheme",
                port=1234,
            ),
            description="port only",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://:password%2F%21%40%23%24%25%5E%26%2A%28%29%7B%7D@",
            test_conn_attributes=dict(
                conn_type="scheme",
                schema="scheme",
                password="password/!@#$%^&*(){}",
            ),
            description="password only",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://login%2F%21%40%23%24%25%5E%26%2A%28%29%7B%7D@",
            test_conn_attributes=dict(
                conn_type="scheme",
                schema="scheme",
                login="login/!@#$%^&*(){}",
            ),
            description="login only",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme://",
            test_conn_attributes=dict(
                conn_type="scheme",
                schema="scheme",
                login=None,
                host="",
            ),
            description="No Path scheme",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme:path://host",
            test_conn_attributes=dict(
                conn_type="scheme",
                schema="scheme",
                host="path://host",
            ),
            description="Scheme is delimited by :, not ://",
        ),
        UriTestCaseConfig(
            test_conn_uri="scheme:path://user:name@host:port/path?query=q#fragment",
            test_conn_attributes=dict(
                conn_type="scheme",
                schema="scheme",
                host="path://user:name@host:port/path",
                port=None,
                login=None,
                password=None,
                extra_dejson=dict(query="q"),
            ),
            description="Scheme is delimited by :, not :// but we also have query and fragment",
        ),
        UriTestCaseConfig(
            test_conn_uri="localhost",
            test_conn_attributes=dict(
                conn_type="",
                schema="",
                host="localhost",
                port=None,
                login=None,
                password=None,
            ),
            description="Just host specified as URI (non RFC3986 compliant)",
        ),
        UriTestCaseConfig(
            test_conn_uri="localhost:2345",
            test_conn_attributes=dict(
                conn_type="",
                schema="",
                host="localhost",
                port=2345,
                login=None,
                password=None,
            ),
            description="Just host:port specified as URI (non RFC3986 compliant)",
        ),
    ]

    @pytest.mark.parametrize("test_config", [x for x in test_from_uri_params])
    def test_connection_from_uri(self, test_config: UriTestCaseConfig):

        connection = Connection(uri=test_config.test_uri)
        for conn_attr, expected_val in test_config.test_conn_attributes.items():
            actual_val = getattr(connection, conn_attr)
            assert (
                expected_val == actual_val
            ), f"Connection attribute `{conn_attr}` is not as expected: `{actual_val}` vs. `{expected_val}`"

        expected_calls = []
        if test_config.test_conn_attributes.get("password"):
            expected_calls.append(mock.call(test_config.test_conn_attributes["password"]))

        if test_config.test_conn_attributes.get("extra_dejson"):
            expected_calls.append(mock.call(test_config.test_conn_attributes["extra_dejson"]))

        self.mask_secret.assert_has_calls(expected_calls)

    @pytest.mark.parametrize("test_config", [x for x in test_from_uri_params])
    def test_connection_get_uri_from_uri(self, test_config: UriTestCaseConfig):
        """
        This test verifies that when we create a conn_1 from URI, and we generate a URI from that conn, that
        when we create a conn_2 from the generated URI, we get an equivalent conn.
        1. Parse URI to create `Connection` object, `connection`.
        2. Using this connection, generate URI `generated_uri`..
        3. Using this`generated_uri`, parse and create new Connection `new_conn`.
        4. Verify that `new_conn` has same attributes as `connection`.
        """
        connection = Connection(uri=test_config.test_uri)
        generated_uri = connection.get_uri()
        new_conn = Connection(uri=generated_uri)
        assert connection.conn_type == new_conn.conn_type
        assert connection.login == new_conn.login
        assert connection.password == new_conn.password
        assert connection.host == new_conn.host
        assert connection.port == new_conn.port
        assert connection.schema == new_conn.schema
        assert connection.extra_dejson == new_conn.extra_dejson

    @pytest.mark.parametrize("test_config", [x for x in test_from_uri_params])
    def test_connection_get_uri_from_conn(self, test_config: UriTestCaseConfig):
        """
        This test verifies that if we create conn_1 from attributes (rather than from URI), and we generate a
        URI, that when we create conn_2 from this URI, we get an equivalent conn.
        1. Build conn init params using `test_conn_attributes` and store in `conn_kwargs`
        2. Instantiate conn `connection` from `conn_kwargs`.
        3. Generate uri `get_uri` from this conn.
        4. Create conn `new_conn` from this uri.
        5. Verify `new_conn` has same attributes as `connection`.
        """
        conn_kwargs = {}
        for k, v in test_config.test_conn_attributes.items():
            if k == "extra_dejson":
                conn_kwargs.update({"extra": json.dumps(v)})
            else:
                conn_kwargs.update({k: v})

        connection = Connection(conn_id="test_conn", **conn_kwargs)  # type: ignore
        gen_uri = connection.get_uri()
        new_conn = Connection(conn_id="test_conn", uri=gen_uri)
        for conn_attr, expected_val in test_config.test_conn_attributes.items():
            actual_val = getattr(new_conn, conn_attr)
            if expected_val is None:
                assert actual_val is None
            else:
                assert actual_val == expected_val

    @pytest.mark.parametrize(
        "uri,uri_parts",
        [
            (
                "http://:password@host:80/database",
                ConnectionParts(
                    conn_type="http", login="", password="password", host="host", port=80, schema="database"
                ),
            ),
            (
                "http://user:@host:80/database",
                ConnectionParts(
                    conn_type="http", login="user", password=None, host="host", port=80, schema="database"
                ),
            ),
            (
                "http://user:password@/database",
                ConnectionParts(
                    conn_type="http", login="user", password="password", host="", port=None, schema="database"
                ),
            ),
            (
                "http://user:password@host:80/",
                ConnectionParts(
                    conn_type="http", login="user", password="password", host="host", port=80, schema="http"
                ),
            ),
            (
                "http://user:password@/",
                ConnectionParts(
                    conn_type="http", login="user", password="password", host="", port=None, schema="http"
                ),
            ),
            (
                "postgresql://user:password@%2Ftmp%2Fz6rqdzqh%2Fexample%3Awest1%3Atestdb/testdb",
                ConnectionParts(
                    conn_type="postgres",
                    login="user",
                    password="password",
                    host="/tmp/z6rqdzqh/example:west1:testdb",
                    port=None,
                    schema="testdb",
                ),
            ),
            (
                "postgresql://user@%2Ftmp%2Fz6rqdzqh%2Fexample%3Aeurope-west1%3Atestdb/testdb",
                ConnectionParts(
                    conn_type="postgres",
                    login="user",
                    password=None,
                    host="/tmp/z6rqdzqh/example:europe-west1:testdb",
                    port=None,
                    schema="testdb",
                ),
            ),
            (
                "postgresql://%2Ftmp%2Fz6rqdzqh%2Fexample%3Aeurope-west1%3Atestdb",
                ConnectionParts(
                    conn_type="postgres",
                    login=None,
                    password=None,
                    host="/tmp/z6rqdzqh/example:europe-west1:testdb",
                    port=None,
                    schema="postgres",
                ),
            ),
            (
                "spark://k8s%3a%2F%2F100.68.0.1:443?deploy-mode=cluster",
                ConnectionParts(
                    conn_type="spark",
                    login=None,
                    password=None,
                    host="k8s://100.68.0.1",
                    port=443,
                    schema="spark",
                ),
            ),
            (
                "spark://user:password@k8s%3a%2F%2F100.68.0.1:443?deploy-mode=cluster",
                ConnectionParts(
                    conn_type="spark",
                    login="user",
                    password="password",
                    host="k8s://100.68.0.1",
                    port=443,
                    schema="spark",
                ),
            ),
            (
                "spark://user@k8s%3a%2F%2F100.68.0.1:443?deploy-mode=cluster",
                ConnectionParts(
                    conn_type="spark",
                    login="user",
                    password=None,
                    host="k8s://100.68.0.1",
                    port=443,
                    schema="spark",
                ),
            ),
            (
                "spark://k8s%3a%2F%2Fno.port.com?deploy-mode=cluster",
                ConnectionParts(
                    conn_type="spark",
                    login=None,
                    password=None,
                    host="k8s://no.port.com",
                    port=None,
                    schema="spark",
                ),
            ),
        ],
    )
    def test_connection_from_with_auth_info(self, uri, uri_parts):
        connection = Connection(uri=uri)

        assert connection.conn_type == uri_parts.conn_type
        assert connection.login == uri_parts.login
        assert connection.password == uri_parts.password
        assert connection.host == uri_parts.host
        assert connection.port == uri_parts.port
        assert connection.schema == uri_parts.schema

    @pytest.mark.parametrize(
        "extra,expected",
        [
            ('{"extra": null}', None),
            ('{"extra": "hi"}', "hi"),
            ('{"extra": {"yo": "hi"}}', '{"yo": "hi"}'),
            ('{"extra": "{\\"yo\\": \\"hi\\"}"}', '{"yo": "hi"}'),
        ],
    )
    def test_from_json_extra(self, extra, expected):
        """json serialization should support extra stored as object _or_ as string"""
        assert Connection.from_json(extra).extra == expected

    @pytest.mark.parametrize(
        "val,expected",
        [
            ('{"conn_type": "abc-abc"}', "abc_abc"),
            ('{"conn_type": "abc_abc"}', "abc_abc"),
            ('{"conn_type": "postgresql"}', "postgres"),
        ],
    )
    def test_from_json_conn_type(self, val, expected):
        """two conn_type normalizations are applied: replace - with _ and postgresql with postgres"""
        assert Connection.from_json(val).conn_type == expected

    @pytest.mark.parametrize(
        "val,expected",
        [
            ('{"port": 1}', 1),
            ('{"port": "1"}', 1),
            ('{"port": null}', None),
        ],
    )
    def test_from_json_port(self, val, expected):
        """two conn_type normalizations are applied: replace - with _ and postgresql with postgres"""
        assert Connection.from_json(val).port == expected

    @pytest.mark.parametrize(
        "val,expected",
        [
            ('pass :/!@#$%^&*(){}"', 'pass :/!@#$%^&*(){}"'),  # these are the same
            (None, None),
            ("", None),  # this is a consequence of the password getter
        ],
    )
    def test_from_json_special_characters(self, val, expected):
        """two conn_type normalizations are applied: replace - with _ and postgresql with postgres"""
        json_val = json.dumps(dict(password=val))
        assert Connection.from_json(json_val).password == expected

    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_TEST_URI": "postgresql://username:password@ec2.compute.com:5432/the_database",
        },
    )
    def test_using_env_var(self):
        conn = SqliteHook.get_connection(conn_id="test_uri")
        assert "ec2.compute.com" == conn.host
        assert "the_database" == conn.schema
        assert "username" == conn.login
        assert "password" == conn.password
        assert 5432 == conn.port

        self.mask_secret.assert_called_once_with("password")

    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_TEST_URI_NO_CREDS": "postgresql://ec2.compute.com/the_database",
        },
    )
    def test_using_unix_socket_env_var(self):
        conn = SqliteHook.get_connection(conn_id="test_uri_no_creds")
        assert "ec2.compute.com" == conn.host
        assert "the_database" == conn.schema
        assert conn.login is None
        assert conn.password is None
        assert conn.port is None

    def test_param_setup(self):
        conn = Connection(
            conn_id="local_mysql",
            conn_type="mysql",
            host="localhost",
            login="airflow",
            password="airflow",
            schema="airflow",
        )
        assert "localhost" == conn.host
        assert "airflow" == conn.schema
        assert "airflow" == conn.login
        assert "airflow" == conn.password
        assert conn.port is None

    def test_env_var_priority(self):
        conn = SqliteHook.get_connection(conn_id="airflow_db")
        assert "ec2.compute.com" != conn.host

        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW_CONN_AIRFLOW_DB": "postgresql://username:password@ec2.compute.com:5432/the_database",
            },
        ):
            conn = SqliteHook.get_connection(conn_id="airflow_db")
            assert "ec2.compute.com" == conn.host
            assert "the_database" == conn.schema
            assert "username" == conn.login
            assert "password" == conn.password
            assert 5432 == conn.port

    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_TEST_URI": "postgresql://username:password@ec2.compute.com:5432/the_database",
            "AIRFLOW_CONN_TEST_URI_NO_CREDS": "postgresql://ec2.compute.com/the_database",
        },
    )
    def test_dbapi_get_uri(self):
        conn = BaseHook.get_connection(conn_id="test_uri")
        hook = conn.get_hook()
        assert "postgresql://username:password@ec2.compute.com:5432/the_database" == hook.get_uri()
        conn2 = BaseHook.get_connection(conn_id="test_uri_no_creds")
        hook2 = conn2.get_hook()
        assert "postgresql://ec2.compute.com/the_database" == hook2.get_uri()

    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_TEST_URI": "postgresql://username:password@ec2.compute.com:5432/the_database",
            "AIRFLOW_CONN_TEST_URI_NO_CREDS": "postgresql://ec2.compute.com/the_database",
        },
    )
    def test_dbapi_get_sqlalchemy_engine(self):
        conn = BaseHook.get_connection(conn_id="test_uri")
        hook = conn.get_hook()
        engine = hook.get_sqlalchemy_engine()
        assert isinstance(engine, sqlalchemy.engine.Engine)
        assert "postgresql://username:password@ec2.compute.com:5432/the_database" == str(engine.url)

    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_TEST_URI": "postgresql://username:password@ec2.compute.com:5432/the_database",
            "AIRFLOW_CONN_TEST_URI_NO_CREDS": "postgresql://ec2.compute.com/the_database",
        },
    )
    def test_get_connections_env_var(self):
        conns = SqliteHook.get_connection(conn_id="test_uri")
        assert conns.host == "ec2.compute.com"
        assert conns.schema == "the_database"
        assert conns.login == "username"
        assert conns.password == "password"
        assert conns.port == 5432

    def test_connection_mixed(self):
        with pytest.raises(
            AirflowException,
            match=re.escape(
                "You must create an object using the URI or individual values (conn_type, host, login, "
                "password, schema, port or extra).You can't mix these two ways to create this object."
            ),
        ):
            Connection(conn_id="TEST_ID", uri="mysql://", schema="AAA")

    def test_masking_from_db(self):
        """Test secrets are masked when loaded directly from the DB"""
        from airflow.settings import Session

        session = Session()

        try:
            conn = Connection(
                conn_id=f"test-{os.getpid()}",
                conn_type="http",
                password="s3cr3t",
                extra='{"apikey":"masked too"}',
            )
            session.add(conn)
            session.flush()

            # Make sure we re-load it, not just get the cached object back
            session.expunge(conn)

            self.mask_secret.reset_mock()

            from_db = session.get(Connection, conn.id)
            from_db.extra_dejson

            assert self.mask_secret.mock_calls == [
                # We should have called it _again_ when loading from the DB
                mock.call("s3cr3t"),
                mock.call({"apikey": "masked too"}),
            ]
        finally:
            session.rollback()

    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_TEST_URI": "sqlite://",
        },
    )
    def test_connection_test_success(self):
        conn = Connection(conn_id="test_uri", conn_type="sqlite")
        res = conn.test_connection()
        assert res[0] is True
        assert res[1] == "Connection successfully tested"

    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_TEST_URI_NO_HOOK": "unknown://",
        },
    )
    def test_connection_test_no_hook(self):
        conn = Connection(conn_id="test_uri_no_hook", conn_type="unknown")
        res = conn.test_connection()
        assert res[0] is False
        assert res[1] == 'Unknown hook type "unknown"'

    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_TEST_URI_HOOK_METHOD_MISSING": "grpc://",
        },
    )
    def test_connection_test_hook_method_missing(self):
        conn = Connection(conn_id="test_uri_hook_method_missing", conn_type="grpc")
        res = conn.test_connection()
        assert res[0] is False
        assert res[1] == "Hook GrpcHook doesn't implement or inherit test_connection method"

    def test_extra_warnings_non_json(self):
        with pytest.warns(DeprecationWarning, match="non-JSON"):
            Connection(conn_id="test_extra", conn_type="none", extra="hi")

    def test_extra_warnings_non_dict_json(self):
        with pytest.warns(DeprecationWarning, match="not parse as a dictionary"):
            Connection(conn_id="test_extra", conn_type="none", extra='"hi"')

    def test_get_uri_no_conn_type(self):
        # no conn type --> scheme-relative URI
        assert Connection().get_uri() == "//"

    def test_get_uri_host_only(self):
        # with host, still works
        assert Connection(host="abc").get_uri() == "//abc"

    def test_host_uri_slashes_no_conn_type(self):
        assert Connection(uri="//abc").host == "abc"

    def test_host_uri_just_host(self):
        assert Connection(uri="abc").host == "abc"

    def test_host_uri_just_host_with_slashes(self):
        assert Connection(uri="//abc").host == "abc"

    def test_host_uri_just_host_and_port(self):
        conn = Connection(uri="abc:234")
        assert conn.host == "abc"
        assert conn.port == 234

    def test_host_uri_just_host_and_port_with_slashes(self):
        conn = Connection(uri="//abc:234")
        assert conn.host == "abc"
        assert conn.port == 234
