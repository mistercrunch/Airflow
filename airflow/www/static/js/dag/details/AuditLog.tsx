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
import React, { useMemo, useRef, useState } from "react";
import { Box, Text } from "@chakra-ui/react";

import { CodeCell, Table, TimeCell } from "src/components/Table";
import { useEventLogs } from "src/api";
import { getMetaValue, useOffsetTop } from "src/utils";
import type { DagRun } from "src/types";
import Time from "src/components/Time";
import type { SortingRule } from "react-table";
import { snakeCase } from "lodash";

interface Props {
  taskId?: string;
  run?: DagRun;
}

const dagId = getMetaValue("dag_id") || undefined;

const AuditLog = ({ taskId, run }: Props) => {
  const logRef = useRef<HTMLDivElement>(null);
  const offsetTop = useOffsetTop(logRef);
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState<SortingRule<object>[]>([
    { id: "when", desc: true },
  ]);

  const sort = sortBy[0];
  const orderBy = sort ? `${sort.desc ? "-" : ""}${snakeCase(sort.id)}` : "";

  const { data, isLoading } = useEventLogs({
    dagId,
    taskId,
    before: run?.lastSchedulingDecision || undefined,
    after: run?.queuedAt || undefined,
    orderBy,
    limit,
    offset,
  });

  const columns = useMemo(() => {
    const when = {
      Header: "When",
      accessor: "when",
      Cell: TimeCell,
    };
    const task = {
      Header: "Task ID",
      accessor: "taskId",
    };
    const rest = [
      {
        Header: "Event",
        accessor: "event",
      },
      {
        Header: "Owner",
        accessor: "owner",
      },
      {
        Header: "Extra",
        accessor: "extra",
        Cell: CodeCell,
      },
    ];
    return !taskId ? [when, task, ...rest] : [when, ...rest];
  }, [taskId]);

  const memoData = useMemo(() => data?.eventLogs, [data?.eventLogs]);
  const memoSort = useMemo(() => sortBy, [sortBy]);

  return (
    <Box
      height="100%"
      maxHeight={`calc(100% - ${offsetTop}px)`}
      ref={logRef}
      overflowY="auto"
    >
      {!!run && (
        <Text>
          Showing logs between Dag Run Queued at (
          <Time dateTime={run.queuedAt} />) and Last Scheduling Decision (
          <Time dateTime={run.lastSchedulingDecision} />)
        </Text>
      )}
      <Table
        data={memoData || []}
        columns={columns}
        isLoading={isLoading}
        manualPagination={{
          offset,
          setOffset,
          totalEntries: data?.totalEntries || 0,
        }}
        manualSort={{
          setSortBy,
          sortBy,
          initialSortBy: memoSort,
        }}
        pageSize={limit}
      />
    </Box>
  );
};

export default AuditLog;
