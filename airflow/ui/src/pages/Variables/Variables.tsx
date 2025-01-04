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
import { Box, Flex, HStack, Spacer, VStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useMemo, useState } from "react";
import { FiShare, FiTrash2 } from "react-icons/fi";
import { useSearchParams } from "react-router-dom";

import { useVariableServiceGetVariables } from "openapi/queries";
import type { VariableResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { Button } from "src/components/ui";
import { ActionBar } from "src/components/ui/ActionBar";
import { Checkbox } from "src/components/ui/Checkbox";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useRowSelection, type GetColumnsParams } from "src/queries/useRowSelection";

import ImportVariablesButton from "./ImportVariablesButton";
import AddVariableButton from "./ManageVariable/AddVariableButton";
import DeleteVariableButton from "./ManageVariable/DeleteVariableButton";
import EditVariableButton from "./ManageVariable/EditVariableButton";

const getColumns = ({
  allRowsSelected,
  onRowSelect,
  onSelectAll,
  selectedRows,
}: GetColumnsParams): Array<ColumnDef<VariableResponse>> => [
  {
    accessorKey: "select",
    cell: ({ row }) => (
      <Checkbox
        checked={selectedRows.get(row.original.key)}
        onCheckedChange={(event) => onRowSelect(row.original.key, Boolean(event.checked))}
      />
    ),
    enableSorting: false,
    header: () => (
      <Checkbox checked={allRowsSelected} onCheckedChange={(event) => onSelectAll(Boolean(event.checked))} />
    ),
    meta: {
      skeletonWidth: 10,
    },
  },
  {
    accessorKey: "key",
    header: "Key",
  },
  {
    accessorKey: "value",
    header: "Value",
  },
  {
    accessorKey: "description",
    header: "Description",
  },
  {
    accessorKey: "is_encrypted",
    header: "Is Encrypted",
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) => (
      <Flex justifyContent="end">
        <EditVariableButton variable={original} />
        <DeleteVariableButton deleteKey={original.key} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
    meta: {
      skeletonWidth: 10,
    },
  },
];

export const Variables = () => {
  const { setTableURLState, tableURLState } = useTableURLState();
  const [searchParams, setSearchParams] = useSearchParams();
  const { NAME_PATTERN: NAME_PATTERN_PARAM }: SearchParamsKeysType = SearchParamsKeys;
  const [variableKeyPattern, setVariableKeyPattern] = useState(
    searchParams.get(NAME_PATTERN_PARAM) ?? undefined,
  );
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id === "value" ? "_val" : sort.id}` : "-key";

  const { data, error, isFetching, isLoading } = useVariableServiceGetVariables({
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
    variableKeyPattern: variableKeyPattern ?? undefined,
  });

  const { allRowsSelected, clearSelections, handleRowSelect, handleSelectAll, selectedRows } =
    useRowSelection({
      data: data?.variables,
      getKey: (variable) => variable.key,
    });

  const columns = useMemo(
    () =>
      getColumns({
        allRowsSelected,
        onRowSelect: handleRowSelect,
        onSelectAll: handleSelectAll,
        selectedRows,
      }),
    [allRowsSelected, handleRowSelect, handleSelectAll, selectedRows],
  );

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setSearchParams(searchParams);
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setVariableKeyPattern(value);
  };

  return (
    <>
      <VStack alignItems="none">
        <SearchBar
          buttonProps={{ disabled: true }}
          defaultValue={variableKeyPattern ?? ""}
          onChange={handleSearchChange}
          placeHolder="Search Keys"
        />
        <HStack gap={4} mt={2}>
          <ImportVariablesButton />
          <Spacer />
          <AddVariableButton />
        </HStack>
      </VStack>
      <Box overflow="auto">
        <DataTable
          columns={columns}
          data={data?.variables ?? []}
          errorMessage={<ErrorAlert error={error} />}
          initialState={tableURLState}
          isFetching={isFetching}
          isLoading={isLoading}
          modelName="Variable"
          onStateChange={setTableURLState}
          total={data?.total_entries ?? 0}
        />
      </Box>
      <ActionBar.Root closeOnInteractOutside={false} open={Boolean(selectedRows.size)}>
        <ActionBar.Content>
          <ActionBar.SelectionTrigger>{selectedRows.size} selected</ActionBar.SelectionTrigger>
          <ActionBar.Separator />
          <Button disabled size="sm" variant="outline">
            <FiTrash2 />
            Delete
          </Button>
          <Button disabled size="sm" variant="outline">
            <FiShare />
            Export
          </Button>
          <ActionBar.CloseTrigger onClick={clearSelections} />
        </ActionBar.Content>
      </ActionBar.Root>
    </>
  );
};
