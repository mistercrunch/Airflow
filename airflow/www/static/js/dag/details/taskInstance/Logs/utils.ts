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

/* global moment */

import { AnsiUp } from "ansi_up";
import { defaultFormatWithTZ } from "src/datetime_utils";

import sanitizeHtml from "sanitize-html";

export enum LogLevel {
  DEBUG = "DEBUG",
  INFO = "INFO",
  WARNING = "WARNING",
  ERROR = "ERROR",
  CRITICAL = "CRITICAL",
}

export const logLevelColorMapping = {
  [LogLevel.DEBUG]: "gray.300",
  [LogLevel.INFO]: "green.200",
  [LogLevel.WARNING]: "yellow.200",
  [LogLevel.ERROR]: "red.200",
  [LogLevel.CRITICAL]: "red.400",
};

export const parseLogs = (
  data: string | undefined,
  timezone: string | null,
  logLevelFilters: Array<LogLevel>,
  fileSourceFilters: Array<string>
) => {
  if (!data) {
    return {};
  }
  let lines;

  let warning;

  try {
    lines = data.split("\n");
  } catch (err) {
    warning = "Unable to show logs. There was an error parsing logs.";
    return { warning };
  }

  const parsedLines: Array<string> = [];
  const fileSources: Set<string> = new Set();
  const ansiUp = new AnsiUp();
  ansiUp.escape_html = true;
  ansiUp.url_allowlist = { http: 1, https: 1 };

  lines.forEach((line) => {
    let parsedLine = line;

    // Apply log level filter.
    if (
      logLevelFilters.length > 0 &&
      logLevelFilters.every((level) => !line.includes(level))
    ) {
      return;
    }

    const regExp = /\[(.*?)\] \{(.*?)\}/;
    const matches = line.match(regExp);
    let logGroup = "";
    if (matches) {
      // Replace UTC with the local timezone.
      const dateTime = matches[1];
      [logGroup] = matches[2].split(":");
      if (dateTime && timezone) {
        // @ts-ignore
        const localDateTime = moment
          .utc(dateTime)
          // @ts-ignore
          .tz(timezone)
          .format(defaultFormatWithTZ);
        parsedLine = line.replace(dateTime, localDateTime);
      }

      fileSources.add(logGroup);
    }

    if (
      fileSourceFilters.length === 0 ||
      fileSourceFilters.some((fileSourceFilter) =>
        line.includes(fileSourceFilter)
      )
    ) {
      // sanitize the lines to remove any tags that may cause HTML injection
      const sanitizedLine = sanitizeHtml(parsedLine, {
        allowedTags: ["a"],
        allowedAttributes: {
          a: ["href", "target", "style"],
        },
        transformTags: {
          a: (tagName, attribs) => {
            attribs.style = "color: blue; text-decoration: underline;";
            return {
              tagName: "a",
              attribs,
            };
          },
        },
      });

      // for lines with color, links, transform to hyperlinks
      const coloredLine = ansiUp.ansi_to_html(sanitizedLine);
      parsedLines.push(coloredLine);
    }
  });

  return {
    parsedLogs: parsedLines
      .map((l) => {
        if (l.length >= 1000000) {
          warning =
            "Large log file. Some lines have been truncated. Download logs in order to see everything.";
          return `${l.slice(0, 1000000)}...`;
        }
        return l;
      })
      .join("\n"),
    fileSources: Array.from(fileSources).sort(),
    warning,
  };
};
