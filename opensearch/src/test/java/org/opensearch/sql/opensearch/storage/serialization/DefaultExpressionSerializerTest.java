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
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.storage.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.ref;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DefaultExpressionSerializerTest {

  /**
   * Initialize function repository manually to avoid dependency on Spring container.
   */
  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());

  private final ExpressionSerializer serializer = new DefaultExpressionSerializer();

  @Test
  public void can_serialize_and_deserialize_literals() {
    Expression original = literal(10);
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void can_serialize_and_deserialize_references() {
    Expression original = ref("name", STRING);
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void can_serialize_and_deserialize_predicates() {
    Expression original = dsl.or(literal(true), dsl.less(literal(1), literal(2)));
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Disabled("Bypass until all functions become serializable")
  @Test
  public void can_serialize_and_deserialize_functions() {
    Expression original = dsl.abs(literal(30.0));
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void cannot_serialize_illegal_expression() {
    Expression illegalExpr = new Expression() {
      private final Object object = new Object(); // non-serializable
      @Override
      public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
        return null;
      }

      @Override
      public ExprType type() {
        return null;
      }

      @Override
      public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
        return null;
      }
    };
    assertThrows(IllegalStateException.class, () -> serializer.serialize(illegalExpr));
  }

  @Test
  public void cannot_deserialize_illegal_expression_code() {
    assertThrows(IllegalStateException.class, () -> serializer.deserialize("hello world"));
  }

}
