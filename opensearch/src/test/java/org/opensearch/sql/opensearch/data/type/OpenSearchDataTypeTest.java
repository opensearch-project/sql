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
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import org.junit.jupiter.api.Test;

class OpenSearchDataTypeTest {
  @Test
  public void testIsCompatible() {
    assertTrue(STRING.isCompatible(OPENSEARCH_TEXT));
    assertFalse(OPENSEARCH_TEXT.isCompatible(STRING));

    assertTrue(STRING.isCompatible(OPENSEARCH_TEXT_KEYWORD));
    assertTrue(OPENSEARCH_TEXT.isCompatible(OPENSEARCH_TEXT_KEYWORD));
  }

  @Test
  public void testTypeName() {
    assertEquals("string", OPENSEARCH_TEXT.typeName());
    assertEquals("string", OPENSEARCH_TEXT_KEYWORD.typeName());
  }

  @Test
  public void legacyTypeName() {
    assertEquals("text", OPENSEARCH_TEXT.legacyTypeName());
    assertEquals("text", OPENSEARCH_TEXT_KEYWORD.legacyTypeName());
  }

  @Test
  public void testShouldCast() {
    assertFalse(OPENSEARCH_TEXT.shouldCast(STRING));
    assertFalse(OPENSEARCH_TEXT_KEYWORD.shouldCast(STRING));
  }
}
