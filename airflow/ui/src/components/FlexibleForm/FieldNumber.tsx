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
import type { FlexibleFormElementProps } from ".";
import { useParamStore } from "../TriggerDag/useParamStore";
import { NumberInputField, NumberInputRoot } from "../ui/NumberInput";

export const FieldNumber = ({ name, param }: FlexibleFormElementProps) => {
  const { paramsDict, setParamsDict } = useParamStore();
  const handleChange = (value: string) => {
    if (paramsDict[name] && paramsDict[name].value !== undefined) {
      paramsDict[name].value = Number(value);
    }

    setParamsDict(paramsDict);
  };

  return (
    <NumberInputRoot
      allowMouseWheel
      id={`element_${name}`}
      max={param.schema.maximum ?? undefined}
      min={param.schema.minimum ?? undefined}
      name={`element_${name}`}
      onValueChange={(event) => handleChange(event.value)}
      size="sm"
      value={String(param.value ?? "")}
    >
      <NumberInputField />
    </NumberInputRoot>
  );
};
