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
import {
  Box,
  Flex,
  Heading,
  HStack,
  SimpleGrid,
  Text,
  VStack,
} from "@chakra-ui/react";
import { FiCalendar, FiPlay } from "react-icons/fi";

import type { DAGResponse, DAGRunResponse } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import Time from "src/components/Time";
import { TogglePause } from "src/components/TogglePause";
import { Tooltip } from "src/components/ui";
import TriggerDAGTextButton from "src/components/TriggerDag/TriggerDAGTextButton";

import { DagTags } from "../DagTags";
import { LatestRun } from "../LatestRun";

export const Header = ({
  dag,
  dagId,
  latestRun,
}: {
  readonly dag?: DAGResponse;
  readonly dagId?: string;
  readonly latestRun?: DAGRunResponse;
}) => (
  <Box borderColor="border" borderRadius={8} borderWidth={1} overflow="hidden">
    <Box p={2}>
      <Flex alignItems="center" justifyContent="space-between">
        <HStack alignItems="center" gap={2}>
          <DagIcon height={8} width={8} />
          <Heading size="lg">{dag?.dag_display_name ?? dagId}</Heading>
          {dag !== undefined && (
            <TogglePause dagId={dag.dag_id} isPaused={dag.is_paused} />
          )}
        </HStack>
        <Flex>
          <TriggerDAGTextButton
            dagDisplayName={dag?.dag_display_name ?? ""}
            dagId={dag?.dag_id ?? ""}
          />
        </Flex>
      </Flex>
      <SimpleGrid columns={4} gap={4} my={2}>
        <VStack align="flex-start" gap={1}>
          <Heading color="fg.muted" fontSize="xs">
            Schedule
          </Heading>
          {Boolean(dag?.timetable_summary) ? (
            <Tooltip content={dag?.timetable_description} showArrow>
              <Text fontSize="sm">
                <FiCalendar style={{ display: "inline" }} />{" "}
                {dag?.timetable_summary}
              </Text>
            </Tooltip>
          ) : undefined}
        </VStack>
        <VStack align="flex-start" gap={1}>
          <Heading color="fg.muted" fontSize="xs">
            Last Run
          </Heading>
          <LatestRun latestRun={latestRun} />
          <LatestRun />
        </VStack>
        <VStack align="flex-start" gap={1}>
          <Heading color="fg.muted" fontSize="xs">
            Next Run
          </Heading>
          {Boolean(dag?.next_dagrun) ? (
            <Text fontSize="sm">
              <Time datetime={dag?.next_dagrun} />
            </Text>
          ) : undefined}
        </VStack>
        <div />
        <div />
      </SimpleGrid>
    </Box>
    <Flex
      alignItems="center"
      bg="bg.muted"
      borderTopColor="border"
      borderTopWidth={1}
      color="fg.subtle"
      fontSize="sm"
      justifyContent="space-between"
      px={2}
      py={1}
    >
      <Text>Owner: {dag?.owners.join(", ")}</Text>
      <DagTags tags={dag?.tags ?? []} />
    </Flex>
  </Box>
);
