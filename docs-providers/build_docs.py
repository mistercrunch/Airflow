#!/usr/bin/env python
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
import argparse
import logging
import os
import shlex
import shutil
import subprocess
import sys
from typing import Dict, Optional

if __name__ != "__main__":
    raise Exception(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        "To run this script, run the ./build_docs.py command"
    )


ROOT_PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
ROOT_PACKAGE_DIR = os.path.join(ROOT_PROJECT_DIR, "airflow")
CONF_DIR = os.path.join(ROOT_PROJECT_DIR, "docs-providers")
PROVIDER_INIT_FILE = os.path.join(ROOT_PACKAGE_DIR, "providers", "__init__.py")

sys.path.append(os.path.join(ROOT_PROJECT_DIR, 'docs', 'exts'))

from provider_yaml_utils import load_package_data

logging.basicConfig()


class DocsBuilder:
    def __init__(self, provider: Dict, version: Optional[str]):
        super().__init__()
        self.provider = provider
        self.version = version

    @property
    def package_name(self):
        return self.provider['package-name']

    @property
    def build_dir(self):
        return os.path.join(ROOT_PROJECT_DIR, "docs-providers", "_build", "docs", self.package_name, "latest")

    @property
    def doctree_dir(self):
        return os.path.join(
            ROOT_PROJECT_DIR, "docs-providers", "_doctree", "docs", self.package_name, "latest"
        )

    @property
    def docs_dir(self):
        return os.path.join(ROOT_PROJECT_DIR, "docs-providers", self.package_name)

    @property
    def api_dir(self):
        return os.path.join(self.docs_dir, "_api")

    def clean_files(self) -> None:
        """Cleanup all artifacts generated by previous builds."""
        shutil.rmtree(self.api_dir, ignore_errors=True)
        shutil.rmtree(self.build_dir, ignore_errors=True)
        os.makedirs(self.api_dir, exist_ok=True)
        os.makedirs(self.build_dir, exist_ok=True)
        print(f"Recreated content of the ${self.build_dir} and ${self.api_dir} folders")

    def build_sphinx_docs(self) -> None:
        """Build documentation for sphinx."""
        build_cmd = [
            "sphinx-build",
            "-b",  # builder to use
            "html",
            "-d",  # path for the cached environment and doctree files
            self.doctree_dir,
            "-c",  # path where configuration file (conf.py) is located
            CONF_DIR,
            "--color",  # do emit colored output
            self.docs_dir,  # path to documentation source files
            self.build_dir,  # path to output directory
        ]
        print("Executing cmd: ", " ".join([shlex.quote(c) for c in build_cmd]))

        env = os.environ.copy()
        env['AIRFLOW_PACKAGE_NAME'] = self.package_name
        env['AIRFLOW_PACKAGE_DIR'] = self.provider['package-dir']
        env['AIRFLOW_PACKAGE_VERSION'] = self.version

        completed_proc = subprocess.run(build_cmd, cwd=self.docs_dir, env=env)
        if completed_proc.returncode != 0:
            sys.exit(completed_proc.returncode)


def is_valid_provider_name(value):
    if not os.path.isdir(os.path.join(CONF_DIR, value)):
        return False
    if not os.path.isfile(os.path.join(CONF_DIR, value, 'index.rst')):
        return False
    return True


def get_list_providers(provider_yamls):
    packages_names = [provider['package-name'] for provider in provider_yamls]
    result = []
    for package_name in packages_names:
        if not is_valid_provider_name(package_name):
            logging.warning(f"The docs for provider could not be found: {package_name}")
            continue
        result.append(package_name)

    return result


all_provider_yamls = load_package_data()
all_providers = get_list_providers(all_provider_yamls)

parser = argparse.ArgumentParser(description='Builds documentation and runs spell checking')
parser.add_argument(
    'providers',
    help='If you want to build a docs for all providers, pass "all"',
    choices=['all', *all_providers],
    nargs='+',
)
parser.add_argument('--version', help='Package version', default='master')

# parser.add_argument('--docs-only', dest='docs_only', action='store_true', help='Only build documentation')
# parser.add_argument(
#     '--spellcheck-only', dest='spellcheck_only', action='store_true', help='Only perform spellchecking'
# )

args = parser.parse_args()


def build_docs(provider):
    builder = DocsBuilder(provider, version)
    builder.clean_files()
    builder.build_sphinx_docs()


providers = all_providers if 'all' in args.providers else args.providers
print("Building docs for: ", providers)
for name in providers:
    provider = next(
        provider_yaml for provider_yaml in all_provider_yamls if provider_yaml['package-name'] == name
    )
    build_docs(provider, args.version)
