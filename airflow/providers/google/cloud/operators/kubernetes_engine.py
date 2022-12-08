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
"""This module contains Google Kubernetes Engine operators."""
from __future__ import annotations

import os
import warnings
from typing import TYPE_CHECKING, Sequence

from google.cloud.container_v1.types import Cluster

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.google.cloud.hooks.kubernetes_engine import GKEHook
from airflow.providers.google.cloud.links.kubernetes_engine import (
    KubernetesEngineClusterLink,
    KubernetesEnginePodLink,
)
from airflow.providers.google.cloud.utils.kubernetes_engine_config import (
    temporary_gke_config_file,
    write_permanent_gke_config_file,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GKEDeleteClusterOperator(BaseOperator):
    """
    Deletes the cluster, including the Kubernetes endpoint and all worker nodes.

    To delete a certain cluster, you must specify the ``project_id``, the ``name``
    of the cluster, the ``location`` that the cluster is in, and the ``task_id``.

    **Operator Creation**: ::

        operator = GKEClusterDeleteOperator(
                    task_id='cluster_delete',
                    project_id='my-project',
                    location='cluster-location'
                    name='cluster-name')

    .. seealso::
        For more detail about deleting clusters have a look at the reference:
        https://google-cloud-python.readthedocs.io/en/latest/container/gapic/v1/api.html#google.cloud.container_v1.ClusterManagerClient.delete_cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEDeleteClusterOperator`

    :param project_id: The Google Developers Console [project ID or project number]
    :param name: The name of the resource to delete, in this case cluster name
    :param location: The name of the Google Kubernetes Engine zone or region in which the cluster
        resides.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param api_version: The api version to use
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "gcp_conn_id",
        "name",
        "location",
        "api_version",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        name: str,
        location: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v2",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.api_version = api_version
        self.name = name
        self.impersonation_chain = impersonation_chain
        self._check_input()

    def _check_input(self) -> None:
        if not all([self.project_id, self.name, self.location]):
            self.log.error("One of (project_id, name, location) is missing or incorrect")
            raise AirflowException("Operator has incorrect or missing input.")

    def execute(self, context: Context) -> str | None:
        hook = GKEHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        delete_result = hook.delete_cluster(name=self.name, project_id=self.project_id)
        return delete_result


class GKECreateClusterOperator(BaseOperator):
    """
    Create a Google Kubernetes Engine Cluster of specified dimensions
    The operator will wait until the cluster is created.

    The **minimum** required to define a cluster to create is:

    ``dict()`` ::
        cluster_def = {'name': 'my-cluster-name',
                       'initial_node_count': 1}

    or

    ``Cluster`` proto ::
        from google.cloud.container_v1.types import Cluster

        cluster_def = Cluster(name='my-cluster-name', initial_node_count=1)

    **Operator Creation**: ::

        operator = GKEClusterCreateOperator(
                    task_id='cluster_create',
                    project_id='my-project',
                    location='my-location'
                    body=cluster_def)

    .. seealso::
        For more detail on about creating clusters have a look at the reference:
        :class:`google.cloud.container_v1.types.Cluster`

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKECreateClusterOperator`

    :param project_id: The Google Developers Console [project ID or project number]
    :param location: The name of the Google Kubernetes Engine zone or region in which the cluster
        resides.
    :param body: The Cluster definition to create, can be protobuf or python dict, if
        dict it must match protobuf message Cluster
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param api_version: The api version to use
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "gcp_conn_id",
        "location",
        "api_version",
        "body",
        "impersonation_chain",
    )
    operator_extra_links = (KubernetesEngineClusterLink(),)

    def __init__(
        self,
        *,
        location: str,
        body: dict | Cluster | None,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v2",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.api_version = api_version
        self.body = body
        self.impersonation_chain = impersonation_chain
        self._check_input()

    def _check_input(self) -> None:
        if (
            not all([self.project_id, self.location, self.body])
            or (isinstance(self.body, dict) and "name" not in self.body)
            or (
                isinstance(self.body, dict)
                and ("initial_node_count" not in self.body and "node_pools" not in self.body)
            )
            or (not (isinstance(self.body, dict)) and not (getattr(self.body, "name", None)))
            or (
                not (isinstance(self.body, dict))
                and (
                    not (getattr(self.body, "initial_node_count", None))
                    and not (getattr(self.body, "node_pools", None))
                )
            )
        ):
            self.log.error(
                "One of (project_id, location, body, body['name'], "
                "body['initial_node_count']), body['node_pools'] is missing or incorrect"
            )
            raise AirflowException("Operator has incorrect or missing input.")
        elif (
            isinstance(self.body, dict) and ("initial_node_count" in self.body and "node_pools" in self.body)
        ) or (
            not (isinstance(self.body, dict))
            and (getattr(self.body, "initial_node_count", None) and getattr(self.body, "node_pools", None))
        ):
            self.log.error("Only one of body['initial_node_count']) and body['node_pools'] may be specified")
            raise AirflowException("Operator has incorrect or missing input.")

    def execute(self, context: Context) -> str:
        hook = GKEHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        create_op = hook.create_cluster(cluster=self.body, project_id=self.project_id)
        KubernetesEngineClusterLink.persist(context=context, task_instance=self, cluster=self.body)
        return create_op


class GKEStartPodOperator(KubernetesPodOperator):
    """
    Executes a task in a Kubernetes pod in the specified Google Kubernetes
    Engine cluster

    This Operator assumes that the system has gcloud installed and has configured a
    connection id with a service account.

    The **minimum** required to define a cluster to create are the variables
    ``task_id``, ``project_id``, ``location``, ``cluster_name``, ``name``,
    ``namespace``, and ``image``

    .. seealso::
        For more detail about Kubernetes Engine authentication have a look at the reference:
        https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#internal_ip

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEStartPodOperator`

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster the pod
        should be spawned in
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param regional: The location param is region name.
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True, delete the
        pod; if False, leave the pod.  Current default is False, but this will be
        changed in the next major release of this provider.
    :param deferrable: Run operator in the deferrable mode.
    """

    template_fields: Sequence[str] = tuple(
        {"project_id", "location", "cluster_name"} | set(KubernetesPodOperator.template_fields)
    )
    operator_extra_links = (KubernetesEnginePodLink(),)

    def __init__(
        self,
        *,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        regional: bool = False,
        is_delete_operator_pod: bool | None = None,
        **kwargs,
    ) -> None:
        if is_delete_operator_pod is None:
            warnings.warn(
                f"You have not set parameter `is_delete_operator_pod` in class {self.__class__.__name__}. "
                "Currently the default for this parameter is `False` but in a future release the default "
                "will be changed to `True`. To ensure pods are not deleted in the future you will need to "
                "set `is_delete_operator_pod=False` explicitly.",
                DeprecationWarning,
                stacklevel=2,
            )
            is_delete_operator_pod = False

        super().__init__(is_delete_operator_pod=is_delete_operator_pod, **kwargs)
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain
        self.regional = regional

        if self.gcp_conn_id is None:
            raise AirflowException(
                "The gcp_conn_id parameter has become required. If you want to use Application Default "
                "Credentials (ADC) strategy for authorization, create an empty connection "
                "called `google_cloud_default`.",
            )
        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the GKEStartPodOperator.
        if self.config_file:
            raise AirflowException("config_file is not an allowed parameter for the GKEStartPodOperator.")

    def execute(self, context: Context) -> None:
        """Look for a pod, if not found then create one and defer"""
        config_args = {
            "gcp_conn_id": self.gcp_conn_id,
            "project_id": self.project_id,
            "cluster_name": self.cluster_name,
            "impersonation_chain": self.impersonation_chain,
            "regional": self.regional,
            "location": self.location,
            "use_internal_ip": self.use_internal_ip,
        }

        if self.deferrable:
            config_file = write_permanent_gke_config_file(**config_args)  # type: ignore[arg-type]
            self.config_file = config_file
            super().execute(context)
        else:
            with temporary_gke_config_file(**config_args) as config_file:  # type: ignore[arg-type]
                self.config_file = config_file
                super().execute(context)

    def execute_complete(self, context: Context, event: dict):
        # Config file should be set to successfully use Kubernetes API after triggers work
        config_file = event["config_file"]
        if config_file is not None:
            self.config_file = config_file

        return super().execute_complete(context, event)

    def post_complete_action(self, **kwargs):
        super().post_complete_action(**kwargs)
        # Remove config file which was created in execute method
        self._remove_config_file()

    def on_kill(self) -> None:
        self._remove_config_file()

    def _remove_config_file(self):
        if self.config_file is not None and os.path.exists(self.config_file):
            os.remove(self.config_file)
