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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.utils.ComparisonUtil.compare;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

class ExprTupleValueTest {
  @Test
  public void equal_to_itself() {
    ExprValue tupleValue = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    assertTrue(tupleValue.equals(tupleValue));
  }

  @Test
  public void tuple_compare_int() {
    ExprValue tupleValue = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    ExprValue intValue = ExprValueUtils.integerValue(10);
    assertFalse(tupleValue.equals(intValue));
  }

  @Test
  public void compare_tuple_with_different_key() {
    ExprValue tupleValue1 = ExprValueUtils.tupleValue(ImmutableMap.of("value", 2));
    ExprValue tupleValue2 =
        ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2, "float_value", 1f));
    assertNotEquals(tupleValue1, tupleValue2);
    assertNotEquals(tupleValue2, tupleValue1);
  }

  @Test
  public void compare_tuple_with_different_size() {
    ExprValue tupleValue1 = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    ExprValue tupleValue2 =
        ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2, "float_value", 1f));
    assertFalse(tupleValue1.equals(tupleValue2));
    assertFalse(tupleValue2.equals(tupleValue1));
  }

  @Test
  public void comparabilityTest() {
    ExprValue tupleValue = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> compare(tupleValue, tupleValue));
    assertEquals("ExprTupleValue instances are not comparable", exception.getMessage());
  }
}
