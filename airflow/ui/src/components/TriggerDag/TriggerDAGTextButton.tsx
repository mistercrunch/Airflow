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
import { Box, Button } from "@chakra-ui/react";
import { useDisclosure } from "@chakra-ui/react";
import { FiPlay } from "react-icons/fi";

import TriggerDAGModal from "./TriggerDAGModal";
import type { TriggerDAGButtonProps } from "./TriggerDag";

const TriggerDAGIconButton: React.FC<TriggerDAGButtonProps> = ({
  dagDisplayName,
  dagId,
}) => {
  const { onClose, onOpen, open } = useDisclosure();

  return (
    <Box>
      <Button colorPalette="blue" onClick={onOpen}>
        <FiPlay />
        Trigger
      </Button>

      <TriggerDAGModal
        dagDisplayName={dagDisplayName}
        dagId={dagId}
        onClose={onClose}
        open={open}
      />
    </Box>
  );
};

export default TriggerDAGIconButton;
