/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { Box } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useParams, useSearchParams } from "react-router-dom";

import { useXcomServiceGetXcomEntries } from "openapi/queries";
import type { XComResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";

import { XComEntry } from "./XComEntry";

const XComColumn = ({
  runId,
}: {
  runId: string;
}): Array<ColumnDef<XComResponse>> => [
  {
    accessorKey: "key",
    enableSorting: true,
    header: "Key",
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    cell: ({ row: { original } }) => (
      <XComEntry
        dagId={original.dag_id}
        mapIndex={original.map_index}
        runId={runId}
        taskId={original.task_id}
        xcomKey={original.key}
      />
    ),
    header: "Value",
    meta: {
      skeletonWidth: 10,
    },
  },
];

export const XCom = () => {
  const { dagId = "", runId = "", taskId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const mapIndexParam = searchParams.get("map_index");
  const mapIndex = parseInt(mapIndexParam ?? "-1", 10);

  const { data, error, isFetching, isLoading } = useXcomServiceGetXcomEntries({
    dagId,
    dagRunId: runId,
    mapIndex,
    taskId,
  });

  return (
    <Box>
      <ErrorAlert error={error} />
      <DataTable
        columns={XComColumn({ runId })}
        data={data ? data.xcom_entries : []}
        displayMode="table"
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="XCom"
        skeletonCount={undefined}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
