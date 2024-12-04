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
import { FiBookOpen } from "react-icons/fi";

import { Button } from "src/components/ui";

const DagDocumentationButton = ({
  setIsDocsOpen,
}: {
  readonly setIsDocsOpen: React.Dispatch<React.SetStateAction<boolean>>;
}) => {
  const onOpen = () => {
    setIsDocsOpen(true);
  };

  return (
    <Button onClick={onOpen} variant="outline">
      <FiBookOpen height={5} width={5} />
      Dag Docs
    </Button>
  );
};

export default DagDocumentationButton;
