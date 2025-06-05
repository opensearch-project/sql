/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.function.jsonUDF.JsonUtils;

@ExtendWith(MockitoExtension.class)
public class JsonFunctionsTest {
  @Test
  public void json_valid_throws_ExpressionEvaluationException() {
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.jsonValid(DSL.literal((ExprValueUtils.booleanValue(true)))).valueOf());
  }

  @Test
  void json_returnsSemanticCheckException() {
    List<LiteralExpression> expressions =
        List.of(
            DSL.literal("invalid"), // invalid type
            DSL.literal("{{[}}"), // missing bracket
            DSL.literal("[}"), // missing bracket
            DSL.literal("}"), // missing bracket
            DSL.literal("\"missing quote"), // missing quote
            DSL.literal("abc"), // not a type
            DSL.literal("97ab"), // not a type
            DSL.literal("{1, 2, 3, 4}"), // invalid object
            DSL.literal("{123: 1, true: 2, null: 3}"), // invalid object
            DSL.literal("{\"invalid\":\"json\", \"string\"}"), // invalid object
            DSL.literal("[\"a\": 1, \"b\": 2]") // invalid array
            );

    expressions.stream()
        .forEach(
            expr ->
                assertThrows(
                    SemanticCheckException.class,
                    () -> DSL.castJson(expr).valueOf(),
                    "Expected to throw SemanticCheckException when calling castJson with " + expr));
  }

  @Test
  void test_convertToJsonPath() {
    List<String> originalJsonPath = List.of("{}", "a.b.c", "a{2}.c", "{3}.bc{}.d{1}");
    List<String> targetJsonPath = List.of("$.[*]", "$.a.b.c", "$.a[2].c", "$.[3].bc[*].d[1]");
    List<String> convertedJsonPath =
        originalJsonPath.stream().map(JsonUtils::convertToJsonPath).toList();
    assertEquals(targetJsonPath, convertedJsonPath);
  }

  @Test
  void test_convertToJsonPathWithWrongPath() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> convertToJsonPath("a.{"));
    assertEquals(e.getMessage(), "Unmatched { in input when converting json path");
  }

  @Test
  void test_jsonPathExpand() {
    String jsonStr =
        "{\"a\": {\"b\": {\"c\": 1}}, \"a2\": [{\"b2\": [{\"c2\": 1}, {\"c2\": 2}]}, {\"b2\":"
            + " [{\"c2\": 1}, {\"c2\": 2}]}, {\"b2\": [{\"c2\": 1}]}], \"a3\": [{\"b3\": [{\"c2\":"
            + " 1}, {\"c2\": 2}]}, {\"b4\": [{\"c2\": 1}, {\"c2\": 2}]}, {\"b5\": [{\"c2\": 1},"
            + " {\"c2\": 2}]}]}";
    JsonNode node = convertInputToJsonNode(jsonStr);
    String candidate1 = "$.a.b.c";
    List<String> target1 = List.of("$.a.b.c");
    assertEquals(expandJsonPath(node, candidate1), target1);
    String candidate2 = "$.a2[*].b2[*].c2";
    List<String> target2 =
        List.of(
            "$.a2[0].b2[0].c2",
            "$.a2[0].b2[1].c2",
            "$.a2[1].b2[0].c2",
            "$.a2[1].b2[1].c2",
            "$.a2[2].b2[0].c2");
    assertEquals(expandJsonPath(node, candidate2), target2);
    String candidate3 = "$.a3[*].b3[*].c2";
    List<String> target3 = List.of("$.a3[0].b3[0].c2", "$.a3[0].b3[1].c2");
    assertEquals(expandJsonPath(node, candidate3), target3);
    String candidate4 = "$.a2[*].b2[1].c2";
    List<String> target4 = List.of("$.a2[0].b2[1].c2", "$.a2[1].b2[1].c2", "$.a2[2].b2[1]");
    assertEquals(expandJsonPath(node, candidate4), target4);
  }

  @Test
  void test_jsonPathExpandAtArray() {
    String jsonStr = "[{\"c\": 1}, {\"c\": 1}, {\"c\": 1}]";
    JsonNode node = convertInputToJsonNode(jsonStr);
    String candidate1 = "$.[*]";
    List<String> target1 = List.of("$.[0]", "$.[1]", "$.[2]");
    assertEquals(expandJsonPath(node, candidate1), target1);
  }
}
