/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.config.TestConfig.BOOL_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.BOOL_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.floatValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.DSL.ref;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReferenceExpressionTest extends ExpressionTestBase {

  @Test
  public void resolve_value() {
    assertEquals(integerValue(1), DSL.ref("integer_value", INTEGER).valueOf(valueEnv()));
    assertEquals(longValue(1L), DSL.ref("long_value", LONG).valueOf(valueEnv()));
    assertEquals(floatValue(1f), DSL.ref("float_value", FLOAT).valueOf(valueEnv()));
    assertEquals(doubleValue(1d), DSL.ref("double_value", DOUBLE).valueOf(valueEnv()));
    assertEquals(booleanValue(true), DSL.ref("boolean_value", BOOLEAN).valueOf(valueEnv()));
    assertEquals(stringValue("str"), DSL.ref("string_value", STRING).valueOf(valueEnv()));
    assertEquals(
        tupleValue(ImmutableMap.of("str", 1)), DSL.ref("struct_value", STRUCT).valueOf(valueEnv()));
    assertEquals(
        collectionValue(ImmutableList.of(1)), DSL.ref("array_value", ARRAY).valueOf(valueEnv()));
    assertEquals(LITERAL_NULL, DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN).valueOf(valueEnv()));
    assertEquals(
        LITERAL_MISSING, DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN).valueOf(valueEnv()));
  }

  @Test
  public void resolve_type() {
    assertEquals(ExprCoreType.INTEGER, DSL.ref("integer_value", INTEGER).type());
    assertEquals(ExprCoreType.LONG, DSL.ref("long_value", LONG).type());
    assertEquals(ExprCoreType.FLOAT, DSL.ref("float_value", FLOAT).type());
    assertEquals(ExprCoreType.DOUBLE, DSL.ref("double_value", DOUBLE).type());
    assertEquals(ExprCoreType.BOOLEAN, DSL.ref("boolean_value", BOOLEAN).type());
    assertEquals(ExprCoreType.STRING, DSL.ref("string_value", STRING).type());
    assertEquals(ExprCoreType.STRUCT, DSL.ref("struct_value", STRUCT).type());
    assertEquals(ExprCoreType.ARRAY, DSL.ref("array_value", ARRAY).type());
  }

  @Test
  public void path_as_whole_has_highest_priority() {
    ReferenceExpression expr = new ReferenceExpression("project.year", INTEGER);
    ExprValue actualValue = expr.resolve(tuple());

    assertEquals(INTEGER, actualValue.type());
    assertEquals(1990, actualValue.integerValue());
  }

  @Test
  public void one_path_value() {
    ReferenceExpression expr = ref("name", STRING);
    ExprValue actualValue = expr.resolve(tuple());

    assertEquals(STRING, actualValue.type());
    assertEquals("bob smith", actualValue.stringValue());
  }

  @Test
  public void multiple_path_value() {
    ReferenceExpression expr = new ReferenceExpression("address.state", STRING);
    ExprValue actualValue = expr.resolve(tuple());

    assertEquals(STRING, actualValue.type());
    assertEquals("WA", actualValue.stringValue());
  }

  @Test
  public void not_exist_path() {
    ReferenceExpression expr = new ReferenceExpression("missing_field", STRING);
    ExprValue actualValue = expr.resolve(tuple());

    assertTrue(actualValue.isMissing());
  }

  @Test
  public void object_field_contain_dot() {
    ReferenceExpression expr = new ReferenceExpression("address.local.state", STRING);
    ExprValue actualValue = expr.resolve(tuple());

    assertTrue(actualValue.isMissing());
  }

  @Test
  public void innner_none_object_field_contain_dot() {
    ReferenceExpression expr = new ReferenceExpression("address.project.year", INTEGER);
    ExprValue actualValue = expr.resolve(tuple());

    assertEquals(INTEGER, actualValue.type());
    assertEquals(1990, actualValue.integerValue());
  }

  @Test
  public void array_with_multiple_path_value() {
    ReferenceExpression expr = new ReferenceExpression("message.info", STRING);
    ExprValue actualValue = expr.resolve(tuple());

    assertEquals(STRING, actualValue.type());
    // Array of object, only first index is used
    assertEquals("First message in array", actualValue.stringValue());
  }

  /**
   *
   *
   * <pre>
   * {
   *   "name": "bob smith"
   *   "project.year": 1990,
   *   "project": {
   *     "year": 2020
   *   },
   *   "address": {
   *     "state": "WA",
   *     "city": "seattle"
   *     "project.year": 1990
   *   },
   *   "address.local": {
   *     "state": "WA",
   *   },
   *   "message": [
   *     { "info": "message in array" },
   *     { "info": "Only first index of array used" }
   *   ]
   * }
   * </pre>
   */
  private ExprTupleValue tuple() {
    ExprValue address =
        ExprValueUtils.tupleValue(
            ImmutableMap.of("state", "WA", "city", "seattle", "project" + ".year", 1990));
    ExprValue project = ExprValueUtils.tupleValue(ImmutableMap.of("year", 2020));
    ExprValue addressLocal = ExprValueUtils.tupleValue(ImmutableMap.of("state", "WA"));
    ExprValue messageCollectionValue =
        new ExprCollectionValue(
            ImmutableList.of(
                ExprValueUtils.tupleValue(
                    ImmutableMap.of("info", stringValue("First message in array"))),
                ExprValueUtils.tupleValue(
                    ImmutableMap.of("info", stringValue("Only first index of array used")))));

    ExprTupleValue tuple =
        ExprTupleValue.fromExprValueMap(
            ImmutableMap.of(
                "name", new ExprStringValue("bob smith"),
                "project.year", new ExprIntegerValue(1990),
                "project", project,
                "address", address,
                "address.local", addressLocal,
                "message", messageCollectionValue));
    return tuple;
  }
}
