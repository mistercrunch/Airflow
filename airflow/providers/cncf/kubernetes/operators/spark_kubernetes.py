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
import re
import sys
from typing import Any, Optional

from typing import TYPE_CHECKING
import jinja2

from kubernetes.client import CoreV1Api, CustomObjectsApi, models as k8s

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import _suppress
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook, _load_body_to_dict
from airflow.utils.helpers import prune_dict

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property
from airflow.kubernetes import kube_client, pod_generator
from airflow.kubernetes.pod_generator import MAX_LABEL_LEN, PodGenerator
from airflow.models import BaseOperator

from airflow.providers.cncf.kubernetes.operators.custom_object_launcher import CustomObjectLauncher
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, PodPhase

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SparkKubernetesOperator(BaseOperator):
    """
    Creates sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.3.3-3.1.1/docs/api-docs.md#sparkapplication

    :param application_file: filepath to kubernetes custom_resource_definition of sparkApplication
    :param kubernetes_conn_id: the connection to Kubernetes cluster
    :param image: Docker image you wish to launch. Defaults to hub.docker.com,
    :param code_path: path to the spark code in image,
    :param namespace: kubernetes namespace to put sparkApplication
    :param cluster_context: context of the cluster
    :param application_file: yaml file if passed
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    :param success_run_history_limit: Number of past successful runs of the application to keep.
    :param delete_on_termination: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
    :param startup_timeout_seconds: timeout in seconds to startup the pod.
    :param log_events_on_failure: Log the pod's events if a failure occurs
    :param reattach_on_restart: if the scheduler dies while the pod is running, reattach and monitor
    """

    template_fields = ['application_file', 'namespace', 'template_spec']
    template_fields_renderers = {"template_spec": "py"}
    template_ext = ('yaml', 'yml', 'json')
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        image: str,
        code_path: str,
        namespace: str = 'default',
        name = None,
        application_file: Optional[str] = None,
        template_spec=None,
        cluster_context: Optional[str] = None,
        get_logs: bool = True,
        do_xcom_push: bool = False,
        success_run_history_limit: int = 1,
        startup_timeout_seconds=600,
        log_events_on_failure: bool = False,
        reattach_on_restart: bool = True,
        delete_on_termination: bool = True,
        kubernetes_conn_id: str = 'kubernetes_default',
        **kwargs,
    ) -> None:
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(**kwargs)
        self.image = image
        self.code_path = code_path
        self.application_file = application_file
        self.template_spec = template_spec
        self.name = self.create_job_name()
        self.kubernetes_conn_id = kubernetes_conn_id
        self.startup_timeout_seconds = startup_timeout_seconds
        self.reattach_on_restart = reattach_on_restart
        self.delete_on_termination = delete_on_termination
        self.do_xcom_push = do_xcom_push
        self.cluster_context = cluster_context
        self.namespace = namespace
        self.get_logs = get_logs
        self.log_events_on_failure = log_events_on_failure
        self.success_run_history_limit = success_run_history_limit
        self.template_body = self.manage_template_specs()

    def _render_nested_template_fields(
            self,
            content: Any,
            context: Context,
            jinja_env: jinja2.Environment,
            seen_oids: set,
        ) -> None:
            if id(content) not in seen_oids and isinstance(content, k8s.V1EnvVar):
                seen_oids.add(id(content))
                self._do_render_template_fields(content, ('value', 'name'), context, jinja_env, seen_oids)
                return

            super()._render_nested_template_fields(content, context, jinja_env, seen_oids)

    def manage_template_specs(self):
        if self.application_file:
            template_body = _load_body_to_dict(open(self.application_file))
        elif self.template_spec:
            template_body = self.template_spec
        else:
            raise AirflowException('either application_file or template_spec should be passed')
        return template_body

    def create_job_name(self):
        initial_name = PodGenerator.make_unique_pod_id(self.task_id)[:MAX_LABEL_LEN]
        return re.sub(r'[^a-z0-9-]+', '-', initial_name.lower())

    @staticmethod
    def _get_pod_identifying_label_string(labels) -> str:
        filtered_labels = {label_id: label for label_id, label in labels.items() if label_id != 'try_number'}
        return ','.join([label_id + '=' + label for label_id, label in sorted(filtered_labels.items())])

    @staticmethod
    def create_labels_for_pod(context: Optional[dict] = None, include_try_number: bool = True) -> dict:
        """
        Generate labels for the pod to track the pod in case of Operator crash
        :param include_try_number: add try number to labels
        :param context: task context provided by airflow DAG
        :return: dict
        """
        if not context:
            return {}

        ti = context['ti']
        run_id = context['run_id']

        labels = {
            'dag_id': ti.dag_id,
            'task_id': ti.task_id,
            'run_id': run_id,
            'spark_kubernetes_operator': 'True',
            # 'execution_date': context['ts'],
            # 'try_number': context['ti'].try_number,
        }

        # If running on Airflow 2.3+:
        map_index = getattr(ti, 'map_index', -1)
        if map_index >= 0:
            labels['map_index'] = map_index

        if include_try_number:
            labels.update(try_number=ti.try_number)

        # In the case of sub dags this is just useful
        if context['dag'].is_subdag:
            labels['parent_dag_id'] = context['dag'].parent_dag.dag_id
        # Ensure that label is valid for Kube,
        # and if not truncate/remove invalid chars and replace with short hash.
        for label_id, label in labels.items():
            safe_label = pod_generator.make_safe_label_value(str(label))
            labels[label_id] = safe_label
        return labels

    @cached_property
    def pod_manager(self) -> PodManager:
        return PodManager(kube_client=self.client)

    @staticmethod
    def _try_numbers_match(context, pod) -> bool:
        return pod.metadata.labels['try_number'] == context['ti'].try_number

    def find_spark_job(self, context):
        labels = self.create_labels_for_pod(context, include_try_number=False)
        label_selector = self._get_pod_identifying_label_string(labels) + ',spark-role=driver'
        pod_list = self.client.list_namespaced_pod(self.namespace, label_selector=label_selector).items

        pod = None
        if len(pod_list) > 1:  # and self.reattach_on_restart:
            raise AirflowException(f'More than one pod running with labels: {label_selector}')
        elif len(pod_list) == 1:
            pod = pod_list[0]
            self.log.info(
                "Found matching driver pod %s with labels %s", pod.metadata.name, pod.metadata.labels
            )
            self.log.info("`try_number` of task_instance: %s", context['ti'].try_number)
            self.log.info("`try_number` of pod: %s", pod.metadata.labels['try_number'])
        return pod

    def get_or_create_spark_crd(self, launcher: CustomObjectLauncher, context) -> k8s.V1Pod:
        if self.reattach_on_restart:
            driver_pod = self.find_spark_job(context)
            if driver_pod:
                return driver_pod

        # self.log.debug("Starting spark job:\n%s", yaml.safe_dump(launcher.body.to_dict()))
        driver_pod, spark_obj_spec = launcher.start_spark_job(
            image=self.image,
            code_path=self.code_path,
            startup_timeout=self.startup_timeout_seconds
        )
        return driver_pod

    def extract_xcom(self, pod):
        """Retrieves xcom value and kills xcom sidecar container"""
        result = self.pod_manager.extract_xcom(pod)
        self.log.info("xcom result: \n%s", result)
        return json.loads(result)

    def cleanup(self, pod: k8s.V1Pod, remote_pod: k8s.V1Pod):
        pod_phase = remote_pod.status.phase if hasattr(remote_pod, 'status') else None
        if not self.delete_on_termination:
            with _suppress(Exception):
                self.patch_already_checked(pod)
        if pod_phase != PodPhase.SUCCEEDED:
            if self.log_events_on_failure:
                with _suppress(Exception):
                    for event in self.pod_manager.read_pod_events(pod).items:
                        self.log.error("Pod Event: %s - %s", event.reason, event.message)
            with _suppress(Exception):
                self.process_spark_job_deletion(pod)
        else:
            with _suppress(Exception):
                self.process_spark_job_deletion(pod)

    def process_spark_job_deletion(self, pod):
        if self.delete_on_termination:
            self.log.info("Deleting spark job: %s", pod.metadata)
            self.launcher.delete_spark_job(pod.metadata.name.replace('-driver', ''))
        else:
            self.log.info("skipping deleting spark job: %s", pod.metadata.name)

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.template_body['kubernetes']['in_cluster'],
            config_file=self.template_body['kubernetes']['kube_config_file'],
            cluster_context=self.cluster_context,
        )
        return hook

    @cached_property
    def client(self) -> CoreV1Api:
        return self.hook.core_v1_client

    @cached_property
    def custom_obj_api(self) -> CustomObjectsApi:
        return CustomObjectsApi()

    def execute(self, context: Context):
        self.log.info('Creating sparkApplication.')
        # If yaml file used to create spark application
        remote_pod = None
        driver_pod = None
        try:
            self.launcher = CustomObjectLauncher(
                name=self.name,
                namespace=self.namespace,
                kube_client=self.client,
                custom_obj_api=self.custom_obj_api,
                template_body=self.template_body,
            )
            driver_pod = self.get_or_create_spark_crd(self.launcher, context)

            if self.get_logs:
                self.pod_manager.fetch_container_logs(
                    pod=driver_pod,
                    container_name='spark-kubernetes-driver',
                    follow=True,
                )
            else:
                self.pod_manager.await_container_completion(
                    pod=driver_pod, container_name='spark-kubernetes-driver'
                )

            if self.do_xcom_push:
                result = self.extract_xcom(pod=driver_pod)
            remote_pod = self.pod_manager.await_pod_completion(driver_pod)
        finally:
            self.cleanup(
                pod=driver_pod or self.launcher.pod_spec,
                remote_pod=remote_pod,
            )
        ti = context['ti']
        ti.xcom_push(key='pod_name', value=driver_pod.metadata.name)
        ti.xcom_push(key='pod_namespace', value=driver_pod.metadata.namespace)
        if self.do_xcom_push:
            return result

    def on_kill(self) -> None:
        if self.launcher:
            self.log.debug("Deleting spark job for task %s", self.task_id)
            self.launcher.delete_spark_job()

    def patch_already_checked(self, pod: k8s.V1Pod):
        """Add an "already checked" annotation to ensure we don't reattach on retries"""
        pod.metadata.labels["already_checked"] = "True"
        body = PodGenerator.serialize_pod(pod)
        self.client.patch_namespaced_pod(pod.metadata.name, pod.metadata.namespace, body)

    def dry_run(self) -> None:
        """Prints out the spark job that would be created by this operator."""
        print(prune_dict(self.launcher.body, mode='strict'))
