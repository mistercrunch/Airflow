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

import ast
from typing import TYPE_CHECKING

import pytest

from airflow.decorators import task

if TYPE_CHECKING:
    from airflow.decorators.base import Task

DECORATORS = set(x for x in dir(task) if not x.startswith("_")) - {"skip_if", "run_if"}


class TestDecoratorSource:
    @staticmethod
    def parse_python_source(task: Task) -> str:
        operator = task().operator
        public_methods = {x for x in dir(operator) if not x.startswith("_")}
        if "get_python_source" not in public_methods:
            pytest.skip(f"Operator {operator} does not have get_python_source method")
        return operator.get_python_source()

    @staticmethod
    def init_decorator(decorator_name: str):
        decorator_factory = getattr(task, decorator_name)

        kwargs = {}
        if "external" in decorator_name:
            kwargs["python"] = "python3"
        return decorator_factory(**kwargs)

    @classmethod
    def parse_decorator_names(cls, source: Task | str) -> list[str]:
        if not isinstance(source, str):
            source = cls.parse_python_source(source)
        node = ast.parse(source)
        func: ast.FunctionDef = node.body[0]  # type: ignore[assignment]
        decorators: list[ast.Name] = func.decorator_list  # type: ignore[assignment]
        return [decorator.id for decorator in decorators]

    def test_branch_external_python(self):
        @task.branch_virtualenv()
        def f():
            return ["some_task"]

        assert not self.parse_decorator_names(f)
        assert self.parse_python_source(f) == 'def f():\n    return ["some_task"]\n'

    def test_branch_virtualenv(self):
        @task.external_python(python="python3")
        def f():
            return "hello world"

        assert not self.parse_decorator_names(f)
        assert self.parse_python_source(f) == 'def f():\n    return "hello world"\n'

    def test_virtualenv(self):
        @task.virtualenv()
        def f():
            return "hello world"

        assert not self.parse_decorator_names(f)
        assert self.parse_python_source(f) == 'def f():\n    return "hello world"\n'

    @pytest.mark.parametrize("decorator_name", DECORATORS)
    def test_skip_if(self, decorator_name):
        decorator = self.init_decorator(decorator_name)

        @task.skip_if(lambda context: True)
        @decorator
        def f():
            return "hello world"

        source = self.parse_python_source(f)
        decorators = self.parse_decorator_names(source)

        # In `airflow.utils.decorators.remove_task_decorator`, `@decorator` should be removed,
        # but it does so using `custom_operator_name`, which is defined as a string,
        # we have to check it ourselves during testing.
        assert len(decorators) == 1
        assert decorators[0] == "decorator"

    @pytest.mark.parametrize("decorator_name", DECORATORS)
    def test_run_if(self, decorator_name):
        decorator = self.init_decorator(decorator_name)

        @task.run_if(lambda context: True)
        @decorator
        def f():
            return "hello world"

        source = self.parse_python_source(f)

        # In `airflow.utils.decorators.remove_task_decorator`, `@decorator` should be removed,
        # but it does so using `custom_operator_name`, which is defined as a string,
        # we have to check it ourselves during testing.
        decorators = self.parse_decorator_names(source)
        assert len(decorators) == 1
        assert decorators[0] == "decorator"

    def test_skip_if_and_run_if(self):
        @task.skip_if(lambda context: True)
        @task.run_if(lambda context: True)
        @task.virtualenv()
        def f():
            return "hello world"

        assert self.parse_python_source(f) == 'def f():\n    return "hello world"\n'

    def test_run_if_and_skip_if(self):
        @task.run_if(lambda context: True)
        @task.skip_if(lambda context: True)
        @task.virtualenv()
        def f():
            return "hello world"

        assert self.parse_python_source(f) == 'def f():\n    return "hello world"\n'

    def test_skip_if_allow_decorator(self):
        def decorator(func):
            return func

        @task.skip_if(lambda context: True)
        @task.virtualenv()
        @decorator
        def f():
            return "hello world"

        assert self.parse_decorator_names(f) == ["decorator"]

    def test_run_if_allow_decorator(self):
        def decorator(func):
            return func

        @task.run_if(lambda context: True)
        @task.virtualenv()
        @decorator
        def f():
            return "hello world"

        assert self.parse_decorator_names(f) == ["decorator"]
