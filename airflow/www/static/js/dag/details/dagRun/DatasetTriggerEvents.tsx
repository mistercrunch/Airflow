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
import React, { useMemo } from "react";
import { Box, Text } from "@chakra-ui/react";

import {
  CodeCell,
  DatasetLink,
  Table,
  TaskInstanceLink,
  TimeCell,
} from "src/components/Table";
import { useUpstreamDatasetEvents } from "src/api";
import type { DagRun as DagRunType } from "src/types";

interface Props {
  runId: DagRunType["runId"];
}

const DatasetTriggerEvents = ({ runId }: Props) => {
  const {
    data: { datasetEvents = [] },
    isLoading,
  } = useUpstreamDatasetEvents({ runId });

  const columns = useMemo(
    () => [
      {
        Header: "Dataset URI",
        accessor: "datasetUri",
        Cell: DatasetLink,
      },
      {
        Header: "Source Task Instance",
        accessor: "sourceTaskId",
        Cell: TaskInstanceLink,
      },
      {
        Header: "When",
        accessor: "timestamp",
        Cell: TimeCell,
      },
      {
        Header: "Extra",
        accessor: "extra",
        Cell: CodeCell,
        disableSortBy: true,
      },
    ],
    []
  );

  const data = useMemo(() => datasetEvents, [datasetEvents]);

  return (
    <Box mt={3} flexGrow={1}>
      <Text as="strong" mb={3}>
        Dataset Events
      </Text>
      <Text>Dataset updates that triggered this DAG run.</Text>
      <Table data={data} columns={columns} isLoading={isLoading} />
    </Box>
  );
};

export default DatasetTriggerEvents;
