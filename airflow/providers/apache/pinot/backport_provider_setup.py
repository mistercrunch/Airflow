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

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE\
# OVERWRITTEN WHEN RUNNING
#
# ./breeze prepare-provider-readme
#
# IF YOU WANT TO MODIFY IT, YOU SHOULD MODIFY THE TEMPLATE
# `SETUP_TEMPLATE.py.jinja2` IN the `provider_packages` DIRECTORY

"""Setup.py for the apache-airflow-backport-providers-apache-pinot package."""

import logging
import os
import sys

from os.path import dirname
from setuptools import find_packages, setup

logger = logging.getLogger(__name__)

version = '2020.10.29'

my_dir = dirname(__file__)

try:
    with open(
        os.path.join(my_dir, 'airflow/providers/apache/pinot/BACKPORT_PROVIDER_README.md'), encoding='utf-8'
    ) as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ''


def do_setup(version_suffix_for_pypi=''):
    """Perform the package apache-airflow-backport-providers-apache-pinot setup."""
    setup(
        name='apache-airflow-backport-providers-apache-pinot',
        description='Backport provider package '
        'apache-airflow-backport-providers-apache-pinot for Apache Airflow',
        long_description=long_description,
        long_description_content_type='text/markdown',
        license='Apache License 2.0',
        version=version + version_suffix_for_pypi,
        packages=find_packages(include=['airflow.providers.apache.pinot*']),
        zip_safe=False,
        install_requires=['apache-airflow~=1.10', 'pinotdb==0.1.1'],
        setup_requires=['setuptools', 'wheel'],
        extras_require={},
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Environment :: Console',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'Intended Audience :: System Administrators',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Topic :: System :: Monitoring',
        ],
        author='Apache Software Foundation',
        author_email='dev@airflow.apache.org',
        url='http://airflow.apache.org/',
        download_url=('https://archive.apache.org/dist/airflow/backport-providers'),
        python_requires='~=3.6',
        project_urls={
            'Documentation': 'https://airflow.apache.org/docs/',
            'Bug Tracker': 'https://github.com/apache/airflow/issues',
            'Source Code': 'https://github.com/apache/airflow',
        },
    )


#
# Note that --version-suffix-for-pypi should only be used in case we generate RC packages for PyPI
# Those packages should have actual RC version in order to be published even if source version
# should be the final one.
#
if __name__ == "__main__":
    suffix = ''
    if len(sys.argv) > 1 and sys.argv[1] == "--version-suffix-for-pypi":
        if len(sys.argv) < 3:
            print("ERROR! --version-suffix-for-pypi needs parameter!", file=sys.stderr)
            sys.exit(1)
        suffix = sys.argv[2]
        sys.argv = [sys.argv[0]] + sys.argv[3:]
    do_setup(version_suffix_for_pypi=suffix)
