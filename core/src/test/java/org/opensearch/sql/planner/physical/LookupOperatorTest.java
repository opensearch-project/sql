/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.utils.MatcherUtils.containsNull;
import static org.opensearch.sql.utils.MatcherUtils.containsValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ReferenceExpression;

@ExtendWith(MockitoExtension.class)
public class LookupOperatorTest extends PhysicalPlanTestBase {

  public static final String LOOKUP_INDEX = "lookup_index";

  public static final ImmutableList<Map<String, Object>> LOOKUP_TABLE =
      ImmutableList.of(
          ImmutableMap.of(
              "id", 1, "ip", "v4", "ip_v4", "112.111.162.4", "region", "USA", "class", "A"),
          ImmutableMap.of(
              "id", 2, "ip", "4", "ip_v4", "74.125.19.106", "region", "EU", "class", "A"));

  public static final ImmutableList<Map<String, Object>> LOOKUP_TABLE_WITH_NULLS =
      ImmutableList.of(
          new HashMap<>(
              ImmutableMap.of(
                  "id", 1, "ip", "v4", "ip_v4", "112.111.162.4", "region", "USA", "class", "A")) {
            {
              put("class", null);
            }
          },
          ImmutableMap.of(
              "id", 2, "ip", "4", "ip_v4", "74.125.19.106", "region", "EU", "class", "A"));

  @Mock private BiFunction<String, Map<String, Object>, Map<String, Object>> lookupFunction;

  @Test
  public void lookup_empty_table() {
    when(lookupFunction.apply(eq(LOOKUP_INDEX), anyMap()))
        .thenAnswer(lookupTableQueryResults("ip", Collections.emptyList()));
    PhysicalPlan plan =
        new LookupOperator(
            new TestScan(),
            LOOKUP_INDEX,
            ImmutableMap.of(
                new ReferenceExpression("ip_v4", STRING), new ReferenceExpression("ip", STRING)),
            true,
            ImmutableMap.of(),
            lookupFunction);

    List<ExprValue> result = execute(plan);

    assertThat(result, hasSize(5));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "112.111.162.4",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "74.125.19.106",
                    "action",
                    "POST",
                    "response",
                    200,
                    "referer",
                    "www.google.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("ip", "74.125.19.106", "action", "POST", "response", 500))));
  }

  @Test
  public void lookup_append_only_true() {
    when(lookupFunction.apply(eq(LOOKUP_INDEX), anyMap()))
        .thenAnswer(lookupTableQueryResults("ip_v4", LOOKUP_TABLE));
    PhysicalPlan plan =
        new LookupOperator(
            new TestScan(),
            LOOKUP_INDEX,
            ImmutableMap.of(
                new ReferenceExpression("ip_v4", STRING), new ReferenceExpression("ip", STRING)),
            true,
            ImmutableMap.of(),
            lookupFunction);

    List<ExprValue> result = execute(plan);

    assertThat(result, hasSize(5));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("ip", "112.111.162.4"),
                containsValue("ip_v4", "112.111.162.4"),
                containsValue("region", "USA"),
                containsValue("class", "A"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 200),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"),
                containsValue("class", "A"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 500),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"),
                containsValue("class", "A"))));
  }

  @Test
  public void lookup_append_only_false() {
    when(lookupFunction.apply(eq(LOOKUP_INDEX), anyMap()))
        .thenAnswer(lookupTableQueryResults("ip_v4", LOOKUP_TABLE));
    PhysicalPlan plan =
        new LookupOperator(
            new TestScan(),
            LOOKUP_INDEX,
            ImmutableMap.of(
                new ReferenceExpression("ip_v4", STRING), new ReferenceExpression("ip", STRING)),
            false,
            ImmutableMap.of(),
            lookupFunction);

    List<ExprValue> result = execute(plan);

    assertThat(result, hasSize(5));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("ip", "v4"),
                containsValue("ip_v4", "112.111.162.4"),
                containsValue("region", "USA"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 200),
                containsValue("ip", "4"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 500),
                containsValue("ip", "4"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"))));
  }

  @Test
  public void lookup_copy_one_field() {
    when(lookupFunction.apply(eq(LOOKUP_INDEX), anyMap()))
        .thenAnswer(lookupTableQueryResults("ip_v4", LOOKUP_TABLE));
    PhysicalPlan plan =
        new LookupOperator(
            new TestScan(),
            LOOKUP_INDEX,
            ImmutableMap.of(
                new ReferenceExpression("ip_v4", STRING), new ReferenceExpression("ip", STRING)),
            true,
            ImmutableMap.of(
                new ReferenceExpression("class", STRING),
                new ReferenceExpression("ip_address_class", STRING)),
            lookupFunction);

    List<ExprValue> result = execute(plan);

    assertThat(result, hasSize(5));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("ip", "112.111.162.4"),
                containsValue("ip_v4", "112.111.162.4"),
                containsValue("ip_address_class", "A"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 200),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("ip_address_class", "A"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 500),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("ip_address_class", "A"))));
  }

  @Test
  public void lookup_copy_multiple_fields() {
    when(lookupFunction.apply(eq(LOOKUP_INDEX), anyMap()))
        .thenAnswer(lookupTableQueryResults("ip_v4", LOOKUP_TABLE));
    PhysicalPlan plan =
        new LookupOperator(
            new TestScan(),
            LOOKUP_INDEX,
            ImmutableMap.of(
                new ReferenceExpression("ip_v4", STRING), new ReferenceExpression("ip", STRING)),
            true,
            ImmutableMap.of(
                new ReferenceExpression("class", STRING),
                new ReferenceExpression("class", STRING),
                new ReferenceExpression("id", INTEGER),
                new ReferenceExpression("address_id", INTEGER)),
            lookupFunction);

    List<ExprValue> result = execute(plan);

    assertThat(result, hasSize(5));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("ip", "112.111.162.4"),
                containsValue("ip_v4", "112.111.162.4"),
                containsValue("class", "A"),
                containsValue("address_id", 1))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 200),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("class", "A"),
                containsValue("address_id", 2))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 500),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("class", "A"),
                containsValue("address_id", 2))));
  }

  @Test
  public void lookup_empty_join_field() {
    when(lookupFunction.apply(eq(LOOKUP_INDEX), anyMap()))
        .thenAnswer(lookupTableQueryResults("ip_v4", LOOKUP_TABLE));
    List<ExprValue> queryResults =
        new ImmutableList.Builder<ExprValue>()
            .addAll(inputs)
            .add(
                ExprValueUtils.tupleValue(
                    ImmutableMap.of("action", "GET", "response", 200, "referer", "www.amazon.com")))
            .build();
    PhysicalPlan plan =
        new LookupOperator(
            testScan(queryResults),
            LOOKUP_INDEX,
            ImmutableMap.of(
                new ReferenceExpression("ip_v4", STRING), new ReferenceExpression("ip", STRING)),
            true,
            ImmutableMap.of(),
            lookupFunction);

    List<ExprValue> result = execute(plan);

    assertThat(result, hasSize(6));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("ip", "112.111.162.4"),
                containsValue("ip_v4", "112.111.162.4"),
                containsValue("region", "USA"),
                containsValue("class", "A"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 200),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"),
                containsValue("class", "A"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 500),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"),
                containsValue("class", "A"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("action", "GET", "response", 200, "referer", "www.amazon.com"))));
  }

  @Test
  public void lookup_ignore_non_struct_input() {
    when(lookupFunction.apply(eq(LOOKUP_INDEX), anyMap()))
        .thenAnswer(lookupTableQueryResults("ip_v4", LOOKUP_TABLE));
    ExprStringValue stringExpression =
        new ExprStringValue("Expression of string type should be ignored");
    List<ExprValue> queryResults =
        new ImmutableList.Builder<ExprValue>().addAll(inputs).add(stringExpression).build();
    PhysicalPlan plan =
        new LookupOperator(
            testScan(queryResults),
            LOOKUP_INDEX,
            ImmutableMap.of(
                new ReferenceExpression("ip_v4", STRING), new ReferenceExpression("ip", STRING)),
            true,
            ImmutableMap.of(),
            lookupFunction);

    List<ExprValue> result = execute(plan);

    assertThat(result, hasSize(6));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("ip", "112.111.162.4"),
                containsValue("ip_v4", "112.111.162.4"),
                containsValue("region", "USA"),
                containsValue("class", "A"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 200),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"),
                containsValue("class", "A"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 500),
                containsValue("ip", "74.125.19.106"),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"),
                containsValue("class", "A"))));
    assertThat(result, hasItem(stringExpression));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void lookup_table_with_nulls(boolean overwrite) {
    when(lookupFunction.apply(eq(LOOKUP_INDEX), anyMap()))
        .thenAnswer(lookupTableQueryResults("ip_v4", LOOKUP_TABLE_WITH_NULLS));
    PhysicalPlan plan =
        new LookupOperator(
            new TestScan(),
            LOOKUP_INDEX,
            ImmutableMap.of(
                new ReferenceExpression("ip_v4", STRING), new ReferenceExpression("ip", STRING)),
            overwrite,
            ImmutableMap.of(),
            lookupFunction);

    List<ExprValue> result = execute(plan);

    assertThat(result, hasSize(5));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    200,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("ip_v4", "112.111.162.4"),
                containsValue("region", "USA"),
                containsNull("class"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 200),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"),
                containsValue("class", "A"))));
    assertThat(
        result,
        hasItem(
            allOf(
                containsValue("response", 500),
                containsValue("ip_v4", "74.125.19.106"),
                containsValue("region", "EU"),
                containsValue("class", "A"))));
  }

  private static @NotNull Answer<Map<String, Object>> lookupTableQueryResults(
      String lookupTableFieldName, List<Map<String, Object>> lookupTableContent) {
    return invocationOnMock -> {
      String lookupTableName = invocationOnMock.getArgument(0);
      if (!LOOKUP_INDEX.equals(lookupTableName)) {
        return ImmutableMap.of();
      }
      HashMap<String, HashMap<String, Object>> parameters = invocationOnMock.getArgument(1);
      String valueOfJoinFieldInLookupTable =
          (String) parameters.get("_match").get(lookupTableFieldName);
      if (Objects.isNull(valueOfJoinFieldInLookupTable)) {
        return null;
      }
      return lookupTableContent.stream()
          .filter(map -> valueOfJoinFieldInLookupTable.equals(map.get(lookupTableFieldName)))
          .findAny()
          .orElse(ImmutableMap.of());
    };
  }
}
