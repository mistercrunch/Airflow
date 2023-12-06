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

import asyncio
from typing import TYPE_CHECKING, Any, Callable

import aiohttp
import requests
import tenacity
from aiohttp import ClientResponseError
from asgiref.sync import sync_to_async
from requests.auth import HTTPBasicAuth
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

from airflow.compat.functools import cache
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse


DEFAULT_AUTH_TYPES = frozenset(
    {
        "request.auth.HTTPBasicAuth",
        "request.auth.HTTPProxyAuth",
        "request.auth.HTTPDigestAuth",
    }
)


@cache
def get_auth_types() -> frozenset[str]:
    """Get comma-separated extra auth_types from airflow config.

    Those auth_types can then be used in Connection configuration.
    """
    from airflow.configuration import conf

    auth_types = DEFAULT_AUTH_TYPES.copy()
    extra_auth_types = conf.get("http", "extra_auth_types", fallback=None)
    if extra_auth_types:
        auth_types |= frozenset({field.strip() for field in extra_auth_types.split(",")})
    return auth_types


class HttpHook(BaseHook):
    """Interact with HTTP servers.

    To configure the auth_type, in addition to the `auth_type` parameter, you can also:
        * set the `auth_type` parameter in the Connection settings.
        * define extra parameters passed to the `auth_type` class via the `auth_kwargs`, in the Connection
            settings. The class will be instantiated with those parameters.

    See :doc:`/connections/http` for full documentation.

    :param method: the API method to be called
    :param http_conn_id: :ref:`http connection<howto/connection:http>` that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "http_default"
    conn_type = "http"
    hook_name = "HTTP"

    def __init__(
        self,
        method: str = "POST",
        http_conn_id: str = default_conn_name,
        auth_type: Any = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
    ) -> None:
        super().__init__()
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self._is_auth_type_setup: bool = auth_type is not None
        self.auth_type: Any = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.keep_alive_idle = tcp_keep_alive_idle
        self.keep_alive_count = tcp_keep_alive_count
        self.keep_alive_interval = tcp_keep_alive_interval

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_babel import lazy_gettext
        from wtforms.fields import SelectField, TextAreaField

        auth_types_choices = frozenset({""}) | get_auth_types()
        return {
            "auth_type": SelectField(
                lazy_gettext("Auth type"),
                choices=[(clazz, clazz) for clazz in auth_types_choices]
            ),
            "auth_kwargs": TextAreaField(lazy_gettext("Auth kwargs")),
            "extra_headers": TextAreaField(lazy_gettext("Extra Headers"))
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["extra"],
            "relabeling": {}
        }

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_conn(self, headers: dict[Any, Any] | None = None) -> requests.Session:
        """Create a Requests HTTP session.

        :param headers: additional headers to be passed through as a dictionary
        """
        session = requests.Session()

        if self.http_conn_id:
            conn = self.get_connection(self.http_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = f"{schema}://{host}"

            if conn.port:
                self.base_url += f":{conn.port}"

            conn_extra: dict = conn.extra_dejson.copy()
            conn_auth_type_name: str | None = conn_extra.pop("auth_type", None)
            auth_type: Any = self.auth_type or self._load_conn_auth_type(conn_auth_type_name) or HTTPBasicAuth
            auth_args: list[str | None] = [conn.login, conn.password]
            auth_kwargs: dict[str, Any] = conn_extra.pop("auth_kwargs", {})

            if any(auth_args) or auth_kwargs:
                session.auth = auth_type(*auth_args, **auth_kwargs)
            elif self._is_auth_type_setup:
                session.auth = auth_type()

            if conn_extra:
                try:
                    session.headers.update(conn_extra)
                except TypeError:
                    self.log.warning("Connection to %s has invalid extra field.", conn.host)
        if headers:
            session.headers.update(headers)

        return session

    def _load_conn_auth_type(self, module_name: str | None) -> Any:
        """Load auth_type module from extra Connection parameters."""
        if module_name:
            if module_name in get_auth_types():
                try:
                    module = import_string(module_name)
                    self._is_auth_type_setup = True
                    self.log.info("Loaded auth_type: %s", module_name)
                    return module
                except ImportError as error:
                    self.log.debug("Cannot import auth_type '%s' due to: %s", error)
                    raise AirflowException(error)
            else:
                self.log.warning(
                    "Skipping import of auth_type '%s'. The class should be listed in "
                    "'extra_auth_types' config of the http provider.",
                    module_name,
                )
        return None

    def run(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        **request_kwargs: Any,
    ) -> Any:
        r"""Perform the request.

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :param data: payload to be uploaded or request parameters
        :param headers: additional headers to be passed through as a dictionary
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :param request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        extra_options = extra_options or {}

        session = self.get_conn(headers)

        url = self.url_from_endpoint(endpoint)

        if self.tcp_keep_alive:
            keep_alive_adapter = TCPKeepAliveAdapter(
                idle=self.keep_alive_idle, count=self.keep_alive_count, interval=self.keep_alive_interval
            )
            session.mount(url, keep_alive_adapter)
        if self.method == "GET":
            # GET uses params
            req = requests.Request(self.method, url, params=data, headers=headers, **request_kwargs)
        elif self.method == "HEAD":
            # HEAD doesn't use params
            req = requests.Request(self.method, url, headers=headers, **request_kwargs)
        else:
            # Others use data
            req = requests.Request(self.method, url, data=data, headers=headers, **request_kwargs)

        prepped_request = session.prepare_request(req)
        self.log.debug("Sending '%s' to url: %s", self.method, url)
        return self.run_and_check(session, prepped_request, extra_options)

    def check_response(self, response: requests.Response) -> None:
        """Check the status code and raise on failure.

        :param response: A requests response object.
        :raise AirflowException: If the response contains a status code not
            in the 2xx and 3xx range.
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)

    def run_and_check(
        self,
        session: requests.Session,
        prepped_request: requests.PreparedRequest,
        extra_options: dict[Any, Any],
    ) -> Any:
        """Grab extra options, actually run the request, and check the result.

        :param session: the session to be used to execute the request
        :param prepped_request: the prepared request generated in run()
        :param extra_options: additional options to be used when executing the request
            i.e. ``{'check_response': False}`` to avoid checking raising exceptions on non 2XX
            or 3XX status codes
        """
        extra_options = extra_options or {}

        settings = session.merge_environment_settings(
            prepped_request.url,
            proxies=extra_options.get("proxies", {}),
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify"),
            cert=extra_options.get("cert"),
        )

        # Send the request.
        send_kwargs: dict[str, Any] = {
            "timeout": extra_options.get("timeout"),
            "allow_redirects": extra_options.get("allow_redirects", True),
        }
        send_kwargs.update(settings)

        try:
            response = session.send(prepped_request, **send_kwargs)

            if extra_options.get("check_response", True):
                self.check_response(response)
            return response

        except requests.exceptions.ConnectionError as ex:
            self.log.warning("%s Tenacity will retry to execute the operation", ex)
            raise ex

    def run_with_advanced_retry(self, _retry_args: dict[Any, Any], *args: Any, **kwargs: Any) -> Any:
        """Run the hook with retry.

        This is useful for connectors which might be disturbed by intermittent
        issues and should not instantly fail.

        :param _retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity


        .. code-block:: python

            hook = HttpHook(http_conn_id="my_conn", method="GET")
            retry_args = dict(
                wait=tenacity.wait_exponential(),
                stop=tenacity.stop_after_attempt(10),
                retry=tenacity.retry_if_exception_type(Exception),
            )
            hook.run_with_advanced_retry(endpoint="v1/test", _retry_args=retry_args)

        """
        self._retry_obj = tenacity.Retrying(**_retry_args)

        return self._retry_obj(self.run, *args, **kwargs)

    def url_from_endpoint(self, endpoint: str | None) -> str:
        """Combine base url with endpoint."""
        if self.base_url and not self.base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
            return self.base_url + "/" + endpoint
        return (self.base_url or "") + (endpoint or "")

    def test_connection(self):
        """Test HTTP Connection."""
        try:
            self.run()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)


class HttpAsyncHook(BaseHook):
    """Interact with HTTP servers asynchronously.

    :param method: the API method to be called
    :param http_conn_id: http connection id that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "http_default"
    conn_type = "http"
    hook_name = "HTTP"

    def __init__(
        self,
        method: str = "POST",
        http_conn_id: str = default_conn_name,
        auth_type: Any = aiohttp.BasicAuth,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self.auth_type: Any = auth_type
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    async def run(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> ClientResponse:
        """Perform an asynchronous HTTP request call.

        :param endpoint: Endpoint to be called, i.e. ``resource/v1/query?``.
        :param data: Payload to be uploaded or request parameters.
        :param headers: Additional headers to be passed through as a dict.
        :param extra_options: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as
            ``aiohttp.ClientSession().get(json=obj)``.
        """
        extra_options = extra_options or {}

        # headers may be passed through directly or in the "extra" field in the connection
        # definition
        _headers = {}
        auth = None

        if self.http_conn_id:
            conn = await sync_to_async(self.get_connection)(self.http_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url += f":{conn.port}"
            if conn.login:
                auth = self.auth_type(conn.login, conn.password)
            if conn.extra:
                try:
                    _headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning("Connection to %s has invalid extra field.", conn.host)
        if headers:
            _headers.update(headers)

        base_url = (self.base_url or "").rstrip("/")
        endpoint = (endpoint or "").lstrip("/")
        url = f"{base_url}/{endpoint}"

        async with aiohttp.ClientSession() as session:
            if self.method == "GET":
                request_func = session.get
            elif self.method == "POST":
                request_func = session.post
            elif self.method == "PATCH":
                request_func = session.patch
            elif self.method == "HEAD":
                request_func = session.head
            elif self.method == "PUT":
                request_func = session.put
            elif self.method == "DELETE":
                request_func = session.delete
            elif self.method == "OPTIONS":
                request_func = session.options
            else:
                raise AirflowException(f"Unexpected HTTP Method: {self.method}")

            for attempt in range(1, 1 + self.retry_limit):
                response = await request_func(
                    url,
                    json=data if self.method in ("POST", "PUT", "PATCH") else None,
                    params=data if self.method == "GET" else None,
                    headers=_headers,
                    auth=auth,
                    **extra_options,
                )
                try:
                    response.raise_for_status()
                except ClientResponseError as e:
                    self.log.warning(
                        "[Try %d of %d] Request to %s failed.",
                        attempt,
                        self.retry_limit,
                        url,
                    )
                    if not self._retryable_error_async(e) or attempt == self.retry_limit:
                        self.log.exception("HTTP error with status: %s", e.status)
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        raise AirflowException(f"{e.status}:{e.message}")
                    else:
                        await asyncio.sleep(self.retry_delay)
                else:
                    return response
            else:
                raise NotImplementedError  # should not reach this, but makes mypy happy

    def _retryable_error_async(self, exception: ClientResponseError) -> bool:
        """Determine whether an exception may successful on a subsequent attempt.

        It considers the following to be retryable:
            - requests_exceptions.ConnectionError
            - requests_exceptions.Timeout
            - anything with a status code >= 500

        Most retryable errors are covered by status code >= 500.
        """
        if exception.status == 429:
            # don't retry for too Many Requests
            return False
        if exception.status == 413:
            # don't retry for payload Too Large
            return False

        return exception.status >= 500
