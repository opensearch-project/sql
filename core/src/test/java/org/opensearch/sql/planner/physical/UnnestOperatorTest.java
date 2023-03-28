/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.ReferenceExpression;

@ExtendWith(MockitoExtension.class)
class UnnestOperatorTest extends PhysicalPlanTestBase {
  @Mock
  private PhysicalPlan inputPlan;

  private final ExprValue testData = tupleValue(
      Map.of(
          "message",
          collectionValue(
              List.of(
                  Map.of("info", "a"),
                  Map.of("info", "b"),
                  Map.of("info", "c")
              )
          ),
          "comment",
          collectionValue(
              List.of(
                  Map.of("data", "1"),
                  Map.of("data", "2"),
                  Map.of("data", "3")
              )
          )
      )
  );

  private final ExprValue testDataWithSamePath = tupleValue(
      Map.of(
          "message",
          collectionValue(
              List.of(
                  Map.of("info", "a"),
                  Map.of("info", "b"),
                  Map.of("info", "c"),
                  Map.of("id", "1"),
                  Map.of("id", "2"),
                  Map.of("id", "3")
              )
          )
      )
  );

  private final ExprValue nonNestedTestData = tupleValue(
      Map.of(
          "message", "val"
      )
  );

  private final ExprValue missingTupleData = tupleValue(
      Map.of(
          "tuple",
          tupleValue(
              Map.of()
          )
      )
  );

  private final ExprValue missingArrayData = tupleValue(
      Map.of(
          "missing",
          collectionValue(
              List.of("value")
          )
      )
  );

  @Test
  public void nested_one_nested_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(testData);

    Set<String> fields = Set.of("message.info");
    Map<String, List<String>> groupedFieldsByPath =
        Map.of("message", List.of("message.info"));
    assertThat(
        execute(new UnnestOperator(inputPlan, fields, groupedFieldsByPath)),
        contains(
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "a", "comment.data", "1"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "b", "comment.data", "1"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "c", "comment.data", "1")))
        )
    );
  }

  @Test
  public void nested_two_nested_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(testData);

    List<Map<String, ReferenceExpression>> fields =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)),
            Map.of(
                "field", new ReferenceExpression("comment.data", STRING),
                "path", new ReferenceExpression("comment", STRING))
        );

    assertThat(
        execute(new UnnestOperator(inputPlan, fields)),
        contains(
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "a", "comment.data", "1"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "a", "comment.data", "2"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "a", "comment.data", "3"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "b", "comment.data", "1"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "b", "comment.data", "2"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "b", "comment.data", "3"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "c", "comment.data", "1"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "c", "comment.data", "2"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "c", "comment.data", "3")))
        )
    );
  }

  @Test
  public void nested_two_nested_fields_with_same_path() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(testDataWithSamePath);

    List<Map<String, ReferenceExpression>> fields =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)),
            Map.of(
                "field", new ReferenceExpression("message.id", STRING),
                "path", new ReferenceExpression("message", STRING))
        );
    assertThat(
        execute(new UnnestOperator(inputPlan, fields)),
        contains(
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "a", "message.id", "1"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "b", "message.id", "2"))),
            tupleValue(new LinkedHashMap<>(Map.of("message.info", "c", "message.id", "3")))
        )
    );
  }

  @Test
  public void non_nested_field_tests() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(nonNestedTestData);

    Set<String> fields = Set.of("message");
    Map<String, List<String>> groupedFieldsByPath =
        Map.of("message", List.of("message.info"));
    assertThat(
        execute(new UnnestOperator(inputPlan, fields, groupedFieldsByPath)),
        contains(
            tupleValue(new LinkedHashMap<>(Map.of("message", "val")))
        )
    );
  }

  @Test
  public void nested_missing_tuple_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(missingTupleData);
    Set<String> fields = Set.of("message.val");
    Map<String, List<String>> groupedFieldsByPath =
        Map.of("message", List.of("message.val"));
    assertTrue(
        execute(new UnnestOperator(inputPlan, fields, groupedFieldsByPath))
            .get(0)
            .tupleValue()
            .size() == 0
    );
  }

  @Test
  public void nested_missing_array_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(missingArrayData);
    Set<String> fields = Set.of("missing.data");
    Map<String, List<String>> groupedFieldsByPath =
        Map.of("message", List.of("message.data"));
    assertTrue(
        execute(new UnnestOperator(inputPlan, fields, groupedFieldsByPath))
            .get(0)
            .tupleValue()
            .size() == 0
    );
  }
}
