/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_BINARY;

import org.junit.jupiter.api.Test;

public class OpenSearchExprBinaryValueTest {

  @Test
  public void compare() {
    assertEquals(
        0,
        new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==")
            .compare(new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==")));
  }

  @Test
  public void equal() {
    OpenSearchExprBinaryValue value =
        new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==");
    assertTrue(value.equal(new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==")));
  }

  @Test
  public void value() {
    OpenSearchExprBinaryValue value =
        new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==");
    assertEquals("U29tZSBiaW5hcnkgYmxvYg==", value.value());
  }

  @Test
  public void type() {
    OpenSearchExprBinaryValue value =
        new OpenSearchExprBinaryValue("U29tZSBiaW5hcnkgYmxvYg==");
    assertEquals(OPENSEARCH_BINARY, value.type());
  }

}
