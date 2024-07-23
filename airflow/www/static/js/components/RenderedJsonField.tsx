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

import React from "react";

import ReactJson from "react-json-view";

import {
  Flex,
  Button,
  Code,
  Spacer,
  useClipboard,
  useTheme,
  FlexProps,
} from "@chakra-ui/react";

interface Props extends FlexProps {
  content: string;
}

const JsonParse = (content: string) => {
  let contentJson = null;
  let contentFormatted = "";
  let isJson = false;
  try {
    contentJson = JSON.parse(content);
    contentFormatted = JSON.stringify(contentJson, null, 4);
    isJson = true;
  } catch (e) {
    // skip
  }
  return [isJson, contentJson, contentFormatted];
};

const RenderedJsonField = ({ content, ...rest }: Props) => {
  const [isJson, contentJson, contentFormatted] = JsonParse(content);
  const { onCopy, hasCopied } = useClipboard(contentFormatted);
  const theme = useTheme();

  return isJson ? (
    <Flex {...rest} p={2}>
      <ReactJson
        src={contentJson}
        name={false}
        theme="rjv-default"
        iconStyle="triangle"
        indentWidth={2}
        displayDataTypes={false}
        enableClipboard={false}
        style={{
          backgroundColor: "inherit",
          fontSize: theme.fontSizes.md,
          font: theme.fonts.mono,
        }}
      />
      <Spacer />
      <Button aria-label="Copy" onClick={onCopy}>
        {hasCopied ? "Copied!" : "Copy"}
      </Button>
    </Flex>
  ) : (
    <Code fontSize="md">{content}</Code>
  );
};

export default RenderedJsonField;