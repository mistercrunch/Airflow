// This file is auto-generated by @hey-api/openapi-ts
import type { CancelablePromise } from "./core/CancelablePromise";
import { OpenAPI } from "./core/OpenAPI";
import { request as __request } from "./core/request";
import type {
  NextRunAssetsData,
  NextRunAssetsResponse,
  HistoricalMetricsData,
  HistoricalMetricsResponse,
  RecentDagRunsData,
  RecentDagRunsResponse,
  GetDagsData,
  GetDagsResponse,
  PatchDagsData,
  PatchDagsResponse,
  GetDagTagsData,
  GetDagTagsResponse,
  GetDagData,
  GetDagResponse,
  PatchDagData,
  PatchDagResponse,
  DeleteDagData,
  DeleteDagResponse,
  GetDagDetailsData,
  GetDagDetailsResponse,
  DeleteConnectionData,
  DeleteConnectionResponse,
  GetConnectionData,
  GetConnectionResponse,
  GetConnectionsData,
  GetConnectionsResponse,
  DeleteVariableData,
  DeleteVariableResponse,
  GetVariableData,
  GetVariableResponse,
  PatchVariableData,
  PatchVariableResponse,
  GetVariablesData,
  GetVariablesResponse,
  PostVariableData,
  PostVariableResponse,
  GetDagRunData,
  GetDagRunResponse,
  DeleteDagRunData,
  DeleteDagRunResponse,
  UpdateDagRunStateData,
  UpdateDagRunStateResponse,
  GetHealthResponse,
  DeletePoolData,
  DeletePoolResponse,
  GetPoolData,
  GetPoolResponse,
  PatchPoolData,
  PatchPoolResponse,
  GetPoolsData,
  GetPoolsResponse,
  PostPoolData,
  PostPoolResponse,
  GetProvidersData,
  GetProvidersResponse,
  GetPluginsData,
  GetPluginsResponse,
  GetVersionResponse,
} from "./types.gen";

export class AssetService {
  /**
   * Next Run Assets
   * @param data The data for the request.
   * @param data.dagId
   * @returns unknown Successful Response
   * @throws ApiError
   */
  public static nextRunAssets(
    data: NextRunAssetsData,
  ): CancelablePromise<NextRunAssetsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/ui/next_run_assets/{dag_id}",
      path: {
        dag_id: data.dagId,
      },
      errors: {
        422: "Validation Error",
      },
    });
  }
}

export class DashboardService {
  /**
   * Historical Metrics
   * Return cluster activity historical metrics.
   * @param data The data for the request.
   * @param data.startDate
   * @param data.endDate
   * @returns HistoricalMetricDataResponse Successful Response
   * @throws ApiError
   */
  public static historicalMetrics(
    data: HistoricalMetricsData,
  ): CancelablePromise<HistoricalMetricsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/ui/dashboard/historical_metrics_data",
      query: {
        start_date: data.startDate,
        end_date: data.endDate,
      },
      errors: {
        400: "Bad Request",
        422: "Validation Error",
      },
    });
  }
}

export class DagsService {
  /**
   * Recent Dag Runs
   * Get recent DAG runs.
   * @param data The data for the request.
   * @param data.dagRunsLimit
   * @param data.limit
   * @param data.offset
   * @param data.tags
   * @param data.owners
   * @param data.dagIdPattern
   * @param data.dagDisplayNamePattern
   * @param data.onlyActive
   * @param data.paused
   * @param data.lastDagRunState
   * @returns DAGWithLatestDagRunsCollectionResponse Successful Response
   * @throws ApiError
   */
  public static recentDagRuns(
    data: RecentDagRunsData = {},
  ): CancelablePromise<RecentDagRunsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/ui/dags/recent_dag_runs",
      query: {
        dag_runs_limit: data.dagRunsLimit,
        limit: data.limit,
        offset: data.offset,
        tags: data.tags,
        owners: data.owners,
        dag_id_pattern: data.dagIdPattern,
        dag_display_name_pattern: data.dagDisplayNamePattern,
        only_active: data.onlyActive,
        paused: data.paused,
        last_dag_run_state: data.lastDagRunState,
      },
      errors: {
        422: "Validation Error",
      },
    });
  }
}

export class DagService {
  /**
   * Get Dags
   * Get all DAGs.
   * @param data The data for the request.
   * @param data.limit
   * @param data.offset
   * @param data.tags
   * @param data.owners
   * @param data.dagIdPattern
   * @param data.dagDisplayNamePattern
   * @param data.onlyActive
   * @param data.paused
   * @param data.lastDagRunState
   * @param data.orderBy
   * @returns DAGCollectionResponse Successful Response
   * @throws ApiError
   */
  public static getDags(
    data: GetDagsData = {},
  ): CancelablePromise<GetDagsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/dags/",
      query: {
        limit: data.limit,
        offset: data.offset,
        tags: data.tags,
        owners: data.owners,
        dag_id_pattern: data.dagIdPattern,
        dag_display_name_pattern: data.dagDisplayNamePattern,
        only_active: data.onlyActive,
        paused: data.paused,
        last_dag_run_state: data.lastDagRunState,
        order_by: data.orderBy,
      },
      errors: {
        422: "Validation Error",
      },
    });
  }

  /**
   * Patch Dags
   * Patch multiple DAGs.
   * @param data The data for the request.
   * @param data.requestBody
   * @param data.updateMask
   * @param data.limit
   * @param data.offset
   * @param data.tags
   * @param data.owners
   * @param data.dagIdPattern
   * @param data.onlyActive
   * @param data.paused
   * @param data.lastDagRunState
   * @returns DAGCollectionResponse Successful Response
   * @throws ApiError
   */
  public static patchDags(
    data: PatchDagsData,
  ): CancelablePromise<PatchDagsResponse> {
    return __request(OpenAPI, {
      method: "PATCH",
      url: "/public/dags/",
      query: {
        update_mask: data.updateMask,
        limit: data.limit,
        offset: data.offset,
        tags: data.tags,
        owners: data.owners,
        dag_id_pattern: data.dagIdPattern,
        only_active: data.onlyActive,
        paused: data.paused,
        last_dag_run_state: data.lastDagRunState,
      },
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Get Dag Tags
   * Get all DAG tags.
   * @param data The data for the request.
   * @param data.limit
   * @param data.offset
   * @param data.orderBy
   * @param data.tagNamePattern
   * @returns DAGTagCollectionResponse Successful Response
   * @throws ApiError
   */
  public static getDagTags(
    data: GetDagTagsData = {},
  ): CancelablePromise<GetDagTagsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/dags/tags",
      query: {
        limit: data.limit,
        offset: data.offset,
        order_by: data.orderBy,
        tag_name_pattern: data.tagNamePattern,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        422: "Validation Error",
      },
    });
  }

  /**
   * Get Dag
   * Get basic information about a DAG.
   * @param data The data for the request.
   * @param data.dagId
   * @returns DAGResponse Successful Response
   * @throws ApiError
   */
  public static getDag(data: GetDagData): CancelablePromise<GetDagResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/dags/{dag_id}",
      path: {
        dag_id: data.dagId,
      },
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Unprocessable Entity",
      },
    });
  }

  /**
   * Patch Dag
   * Patch the specific DAG.
   * @param data The data for the request.
   * @param data.dagId
   * @param data.requestBody
   * @param data.updateMask
   * @returns DAGResponse Successful Response
   * @throws ApiError
   */
  public static patchDag(
    data: PatchDagData,
  ): CancelablePromise<PatchDagResponse> {
    return __request(OpenAPI, {
      method: "PATCH",
      url: "/public/dags/{dag_id}",
      path: {
        dag_id: data.dagId,
      },
      query: {
        update_mask: data.updateMask,
      },
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Delete Dag
   * Delete the specific DAG.
   * @param data The data for the request.
   * @param data.dagId
   * @returns unknown Successful Response
   * @throws ApiError
   */
  public static deleteDag(
    data: DeleteDagData,
  ): CancelablePromise<DeleteDagResponse> {
    return __request(OpenAPI, {
      method: "DELETE",
      url: "/public/dags/{dag_id}",
      path: {
        dag_id: data.dagId,
      },
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Unprocessable Entity",
      },
    });
  }

  /**
   * Get Dag Details
   * Get details of DAG.
   * @param data The data for the request.
   * @param data.dagId
   * @returns DAGDetailsResponse Successful Response
   * @throws ApiError
   */
  public static getDagDetails(
    data: GetDagDetailsData,
  ): CancelablePromise<GetDagDetailsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/dags/{dag_id}/details",
      path: {
        dag_id: data.dagId,
      },
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Unprocessable Entity",
      },
    });
  }
}

export class ConnectionService {
  /**
   * Delete Connection
   * Delete a connection entry.
   * @param data The data for the request.
   * @param data.connectionId
   * @returns void Successful Response
   * @throws ApiError
   */
  public static deleteConnection(
    data: DeleteConnectionData,
  ): CancelablePromise<DeleteConnectionResponse> {
    return __request(OpenAPI, {
      method: "DELETE",
      url: "/public/connections/{connection_id}",
      path: {
        connection_id: data.connectionId,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Get Connection
   * Get a connection entry.
   * @param data The data for the request.
   * @param data.connectionId
   * @returns ConnectionResponse Successful Response
   * @throws ApiError
   */
  public static getConnection(
    data: GetConnectionData,
  ): CancelablePromise<GetConnectionResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/connections/{connection_id}",
      path: {
        connection_id: data.connectionId,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Get Connections
   * Get all connection entries.
   * @param data The data for the request.
   * @param data.limit
   * @param data.offset
   * @param data.orderBy
   * @returns ConnectionCollectionResponse Successful Response
   * @throws ApiError
   */
  public static getConnections(
    data: GetConnectionsData = {},
  ): CancelablePromise<GetConnectionsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/connections/",
      query: {
        limit: data.limit,
        offset: data.offset,
        order_by: data.orderBy,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }
}

export class VariableService {
  /**
   * Delete Variable
   * Delete a variable entry.
   * @param data The data for the request.
   * @param data.variableKey
   * @returns void Successful Response
   * @throws ApiError
   */
  public static deleteVariable(
    data: DeleteVariableData,
  ): CancelablePromise<DeleteVariableResponse> {
    return __request(OpenAPI, {
      method: "DELETE",
      url: "/public/variables/{variable_key}",
      path: {
        variable_key: data.variableKey,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Get Variable
   * Get a variable entry.
   * @param data The data for the request.
   * @param data.variableKey
   * @returns VariableResponse Successful Response
   * @throws ApiError
   */
  public static getVariable(
    data: GetVariableData,
  ): CancelablePromise<GetVariableResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/variables/{variable_key}",
      path: {
        variable_key: data.variableKey,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Patch Variable
   * Update a variable by key.
   * @param data The data for the request.
   * @param data.variableKey
   * @param data.requestBody
   * @param data.updateMask
   * @returns VariableResponse Successful Response
   * @throws ApiError
   */
  public static patchVariable(
    data: PatchVariableData,
  ): CancelablePromise<PatchVariableResponse> {
    return __request(OpenAPI, {
      method: "PATCH",
      url: "/public/variables/{variable_key}",
      path: {
        variable_key: data.variableKey,
      },
      query: {
        update_mask: data.updateMask,
      },
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Get Variables
   * Get all Variables entries.
   * @param data The data for the request.
   * @param data.limit
   * @param data.offset
   * @param data.orderBy
   * @returns VariableCollectionResponse Successful Response
   * @throws ApiError
   */
  public static getVariables(
    data: GetVariablesData = {},
  ): CancelablePromise<GetVariablesResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/variables/",
      query: {
        limit: data.limit,
        offset: data.offset,
        order_by: data.orderBy,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        422: "Validation Error",
      },
    });
  }

  /**
   * Post Variable
   * Create a variable.
   * @param data The data for the request.
   * @param data.requestBody
   * @returns VariableResponse Successful Response
   * @throws ApiError
   */
  public static postVariable(
    data: PostVariableData,
  ): CancelablePromise<PostVariableResponse> {
    return __request(OpenAPI, {
      method: "POST",
      url: "/public/variables/",
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        422: "Validation Error",
      },
    });
  }
}

export class DagRunService {
  /**
   * Get Dag Run
   * @param data The data for the request.
   * @param data.dagId
   * @param data.dagRunId
   * @returns DAGRunResponse Successful Response
   * @throws ApiError
   */
  public static getDagRun(
    data: GetDagRunData,
  ): CancelablePromise<GetDagRunResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/dags/{dag_id}/dagRuns/{dag_run_id}",
      path: {
        dag_id: data.dagId,
        dag_run_id: data.dagRunId,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Delete Dag Run
   * Delete a DAG Run entry.
   * @param data The data for the request.
   * @param data.dagId
   * @param data.dagRunId
   * @returns void Successful Response
   * @throws ApiError
   */
  public static deleteDagRun(
    data: DeleteDagRunData,
  ): CancelablePromise<DeleteDagRunResponse> {
    return __request(OpenAPI, {
      method: "DELETE",
      url: "/public/dags/{dag_id}/dagRuns/{dag_run_id}",
      path: {
        dag_id: data.dagId,
        dag_run_id: data.dagRunId,
      },
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Update Dag Run State
   * Modify a DAG Run.
   * @param data The data for the request.
   * @param data.dagId
   * @param data.dagRunId
   * @param data.requestBody
   * @returns DAGRunResponse Successful Response
   * @throws ApiError
   */
  public static updateDagRunState(
    data: UpdateDagRunStateData,
  ): CancelablePromise<UpdateDagRunStateResponse> {
    return __request(OpenAPI, {
      method: "PATCH",
      url: "/public/dags/{dag_id}/dagRuns/{dag_run_id}",
      path: {
        dag_id: data.dagId,
        dag_run_id: data.dagRunId,
      },
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }
}

export class MonitorService {
  /**
   * Get Health
   * @returns HealthInfoSchema Successful Response
   * @throws ApiError
   */
  public static getHealth(): CancelablePromise<GetHealthResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/monitor/health",
    });
  }
}

export class PoolService {
  /**
   * Delete Pool
   * Delete a pool entry.
   * @param data The data for the request.
   * @param data.poolName
   * @returns void Successful Response
   * @throws ApiError
   */
  public static deletePool(
    data: DeletePoolData,
  ): CancelablePromise<DeletePoolResponse> {
    return __request(OpenAPI, {
      method: "DELETE",
      url: "/public/pools/{pool_name}",
      path: {
        pool_name: data.poolName,
      },
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Get Pool
   * Get a pool.
   * @param data The data for the request.
   * @param data.poolName
   * @returns PoolResponse Successful Response
   * @throws ApiError
   */
  public static getPool(data: GetPoolData): CancelablePromise<GetPoolResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/pools/{pool_name}",
      path: {
        pool_name: data.poolName,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Patch Pool
   * Update a Pool.
   * @param data The data for the request.
   * @param data.poolName
   * @param data.requestBody
   * @param data.updateMask
   * @returns PoolResponse Successful Response
   * @throws ApiError
   */
  public static patchPool(
    data: PatchPoolData,
  ): CancelablePromise<PatchPoolResponse> {
    return __request(OpenAPI, {
      method: "PATCH",
      url: "/public/pools/{pool_name}",
      path: {
        pool_name: data.poolName,
      },
      query: {
        update_mask: data.updateMask,
      },
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Get Pools
   * Get all pools entries.
   * @param data The data for the request.
   * @param data.limit
   * @param data.offset
   * @param data.orderBy
   * @returns PoolCollectionResponse Successful Response
   * @throws ApiError
   */
  public static getPools(
    data: GetPoolsData = {},
  ): CancelablePromise<GetPoolsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/pools/",
      query: {
        limit: data.limit,
        offset: data.offset,
        order_by: data.orderBy,
      },
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        422: "Validation Error",
      },
    });
  }

  /**
   * Post Pool
   * Create a Pool.
   * @param data The data for the request.
   * @param data.requestBody
   * @returns PoolResponse Successful Response
   * @throws ApiError
   */
  public static postPool(
    data: PostPoolData,
  ): CancelablePromise<PostPoolResponse> {
    return __request(OpenAPI, {
      method: "POST",
      url: "/public/pools/",
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        401: "Unauthorized",
        403: "Forbidden",
        422: "Validation Error",
      },
    });
  }
}

export class ProviderService {
  /**
   * Get Providers
   * Get providers.
   * @param data The data for the request.
   * @param data.limit
   * @param data.offset
   * @returns ProviderCollectionResponse Successful Response
   * @throws ApiError
   */
  public static getProviders(
    data: GetProvidersData = {},
  ): CancelablePromise<GetProvidersResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/providers/",
      query: {
        limit: data.limit,
        offset: data.offset,
      },
      errors: {
        422: "Validation Error",
      },
    });
  }
}

export class PluginService {
  /**
   * Get Plugins
   * @param data The data for the request.
   * @param data.limit
   * @param data.offset
   * @returns PluginCollectionResponse Successful Response
   * @throws ApiError
   */
  public static getPlugins(
    data: GetPluginsData = {},
  ): CancelablePromise<GetPluginsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/plugins/",
      query: {
        limit: data.limit,
        offset: data.offset,
      },
      errors: {
        422: "Validation Error",
      },
    });
  }
}

export class VersionService {
  /**
   * Get Version
   * Get version information.
   * @returns VersionInfo Successful Response
   * @throws ApiError
   */
  public static getVersion(): CancelablePromise<GetVersionResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/public/version/",
    });
  }
}
