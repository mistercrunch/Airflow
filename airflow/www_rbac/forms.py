# -*- coding: utf-8 -*-
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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json

from flask_appbuilder.fieldwidgets import (
    BS3PasswordFieldWidget, BS3TextAreaFieldWidget, BS3TextFieldWidget, Select2Widget,
)
from flask_appbuilder.forms import DynamicForm
from flask_babel import lazy_gettext
from flask_wtf import FlaskForm

from wtforms import validators
from wtforms.fields import (IntegerField, SelectField, TextAreaField, PasswordField,
                            StringField, DateTimeField, BooleanField)
from airflow.models import Connection
from airflow.utils import timezone
from airflow.www_rbac.validators import ValidJson
from airflow.www_rbac.widgets import AirflowDateTimePickerWidget


class DateTimeForm(FlaskForm):
    # Date filter form needed for task views
    execution_date = DateTimeField(
        "Execution date", widget=AirflowDateTimePickerWidget())


class DateTimeWithNumRunsForm(FlaskForm):
    # Date time and number of runs form for tree view, task duration
    # and landing times
    base_date = DateTimeField(
        "Anchor date", widget=AirflowDateTimePickerWidget(), default=timezone.utcnow())
    num_runs = SelectField("Number of runs", default=25, choices=(
        (5, "5"),
        (25, "25"),
        (50, "50"),
        (100, "100"),
        (365, "365"),
    ))


class DateTimeWithNumRunsWithDagRunsForm(DateTimeWithNumRunsForm):
    # Date time and number of runs and dag runs form for graph and gantt view
    execution_date = SelectField("DAG run")


from wtforms.compat import with_metaclass, iteritems, itervalues


class VariableForm(DynamicForm):
    key = StringField(
        lazy_gettext('Key'),
        validators=[validators.DataRequired()],
        widget=BS3TextFieldWidget())

    val = StringField(
        lazy_gettext('Value'),
        widget=BS3TextFieldWidget())

    is_curve_template = BooleanField(
        lazy_gettext('Is Curve Template'),
        default='checked'
    )


class DagRunForm(DynamicForm):
    dag_id = StringField(
        lazy_gettext('Dag Id'),
        validators=[validators.DataRequired()],
        widget=BS3TextFieldWidget())
    start_date = DateTimeField(
        lazy_gettext('Start Date'),
        widget=AirflowDateTimePickerWidget())
    end_date = DateTimeField(
        lazy_gettext('End Date'),
        widget=AirflowDateTimePickerWidget())
    run_id = StringField(
        lazy_gettext('Run Id'),
        validators=[validators.DataRequired()],
        widget=BS3TextFieldWidget())
    state = SelectField(
        lazy_gettext('State'),
        choices=(('success', 'success'), ('running', 'running'), ('failed', 'failed'),),
        widget=Select2Widget())
    execution_date = DateTimeField(
        lazy_gettext('Execution Date'),
        widget=AirflowDateTimePickerWidget())
    external_trigger = BooleanField(
        lazy_gettext('External Trigger'))
    conf = TextAreaField(
        lazy_gettext('Conf'),
        validators=[ValidJson(), validators.Optional()],
        widget=BS3TextAreaFieldWidget())

    def populate_obj(self, item):
        # TODO: This is probably better done as a custom field type so we can
        # set TZ at parse time
        super(DagRunForm, self).populate_obj(item)
        item.execution_date = timezone.make_aware(item.execution_date)
        if item.conf:
            item.conf = json.loads(item.conf)


class ConnectionForm(DynamicForm):
    conn_id = StringField(
        lazy_gettext('Conn Id'),
        validators=[validators.DataRequired()],
        widget=BS3TextFieldWidget())
    conn_type = SelectField(
        lazy_gettext('Conn Type'),
        choices=Connection._types,
        widget=Select2Widget())
    host = StringField(
        lazy_gettext('Host'),
        widget=BS3TextFieldWidget())
    schema = StringField(
        lazy_gettext('Schema'),
        widget=BS3TextFieldWidget())
    login = StringField(
        lazy_gettext('Login'),
        widget=BS3TextFieldWidget())
    password = PasswordField(
        lazy_gettext('Password'),
        widget=BS3PasswordFieldWidget())
    port = IntegerField(
        lazy_gettext('Port'),
        validators=[validators.Optional()],
        widget=BS3TextFieldWidget())
    extra = TextAreaField(
        lazy_gettext('Extra'),
        widget=BS3TextAreaFieldWidget())

    # Used to customized the form, the forms elements get rendered
    # and results are stored in the extra field as json. All of these
    # need to be prefixed with extra__ and then the conn_type ___ as in
    # extra__{conn_type}__name. You can also hide form elements and rename
    # others from the connection_form.js file
    extra__jdbc__drv_path = StringField(
        lazy_gettext('Driver Path'),
        widget=BS3TextFieldWidget())
    extra__jdbc__drv_clsname = StringField(
        lazy_gettext('Driver Class'),
        widget=BS3TextFieldWidget())
    extra__google_cloud_platform__project = StringField(
        lazy_gettext('Project Id'),
        widget=BS3TextFieldWidget())
    extra__google_cloud_platform__key_path = StringField(
        lazy_gettext('Keyfile Path'),
        widget=BS3TextFieldWidget())
    extra__google_cloud_platform__keyfile_dict = PasswordField(
        lazy_gettext('Keyfile JSON'),
        widget=BS3PasswordFieldWidget())
    extra__google_cloud_platform__scope = StringField(
        lazy_gettext('Scopes (comma separated)'),
        widget=BS3TextFieldWidget())
    extra__google_cloud_platform__num_retries = IntegerField(
        lazy_gettext('Number of Retries'),
        validators=[validators.NumberRange(min=0)],
        widget=BS3TextFieldWidget(),
        default=5)
    extra__grpc__auth_type = StringField(
        lazy_gettext('Grpc Auth Type'),
        widget=BS3TextFieldWidget())
    extra__grpc__credential_pem_file = StringField(
        lazy_gettext('Credential Keyfile Path'),
        widget=BS3TextFieldWidget())
    extra__grpc__scopes = StringField(
        lazy_gettext('Scopes (comma separated)'),
        widget=BS3TextFieldWidget())


class ErrorTagForm(DynamicForm):
    value = StringField(
        lazy_gettext('Value'),
        widget=BS3TextFieldWidget())
    label = StringField(
        lazy_gettext('Label'),
        widget=BS3TextFieldWidget())


class TighteningControllerForm(DynamicForm):
    controller_name = StringField(
        lazy_gettext('Controller_name'),
        widget=BS3TextFieldWidget())
    line_code = StringField(
        lazy_gettext('Line_code'),
        widget=BS3TextFieldWidget())
    line_name = StringField(
        lazy_gettext('line_name'),
        widget=BS3TextFieldWidget())
    work_center_code = StringField(
        lazy_gettext('Work_center_code'),
        widget=BS3TextFieldWidget())
    work_center_name = StringField(
        lazy_gettext('Work_center_name'),
        widget=BS3TextFieldWidget())
