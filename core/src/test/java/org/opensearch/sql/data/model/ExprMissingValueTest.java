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
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.utils.ComparisonUtil.compare;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;


class ExprMissingValueTest {

  @Test
  public void test_is_missing() {
    assertTrue(LITERAL_MISSING.isMissing());
  }

  @Test
  public void getValue() {
    assertNull(LITERAL_MISSING.value());
  }

  @Test
  public void getType() {
    assertEquals(ExprCoreType.UNDEFINED, LITERAL_MISSING.type());
  }

  @Test
  public void toStringTest() {
    assertEquals("MISSING", LITERAL_MISSING.toString());
  }

  @Test
  public void equal() {
    assertTrue(LITERAL_MISSING.equals(LITERAL_MISSING));
    assertFalse(LITERAL_FALSE.equals(LITERAL_MISSING));
    assertFalse(LITERAL_MISSING.equals(LITERAL_FALSE));
  }

  @Test
  public void comparabilityTest() {
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> compare(LITERAL_MISSING, LITERAL_MISSING));
    assertEquals("invalid to call compare operation on missing value", exception.getMessage());
  }
}
