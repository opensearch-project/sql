/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.executor.format;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.opensearch.sql.legacy.util.MatcherUtils.featureValueOf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.opensearch.sql.legacy.executor.format.BindingTupleResultSet;
import org.opensearch.sql.legacy.executor.format.DataRows;
import org.opensearch.sql.legacy.executor.format.Schema;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.query.planner.core.ColumnNode;

public class BindingTupleResultSetTest {

  @Test
  public void buildDataRowsFromBindingTupleShouldPass() {
    assertThat(
        row(
            Arrays.asList(
                ColumnNode.builder().name("age").type(Schema.Type.INTEGER).build(),
                ColumnNode.builder().name("gender").type(Schema.Type.TEXT).build()),
            Arrays.asList(
                BindingTuple.from(ImmutableMap.of("age", 31, "gender", "m")),
                BindingTuple.from(ImmutableMap.of("age", 31, "gender", "f")),
                BindingTuple.from(ImmutableMap.of("age", 39, "gender", "m")),
                BindingTuple.from(ImmutableMap.of("age", 39, "gender", "f")))),
        containsInAnyOrder(
            rowContents(allOf(hasEntry("age", 31), hasEntry("gender", (Object) "m"))),
            rowContents(allOf(hasEntry("age", 31), hasEntry("gender", (Object) "f"))),
            rowContents(allOf(hasEntry("age", 39), hasEntry("gender", (Object) "m"))),
            rowContents(allOf(hasEntry("age", 39), hasEntry("gender", (Object) "f")))));
  }

  @Test
  public void buildDataRowsFromBindingTupleIncludeLongValueShouldPass() {
    assertThat(
        row(
            Arrays.asList(
                ColumnNode.builder().name("longValue").type(Schema.Type.LONG).build(),
                ColumnNode.builder().name("gender").type(Schema.Type.TEXT).build()),
            Arrays.asList(
                BindingTuple.from(ImmutableMap.of("longValue", Long.MAX_VALUE, "gender", "m")),
                BindingTuple.from(ImmutableMap.of("longValue", Long.MIN_VALUE, "gender", "f")))),
        containsInAnyOrder(
            rowContents(
                allOf(hasEntry("longValue", Long.MAX_VALUE), hasEntry("gender", (Object) "m"))),
            rowContents(
                allOf(hasEntry("longValue", Long.MIN_VALUE), hasEntry("gender", (Object) "f")))));
  }

  @Test
  public void buildDataRowsFromBindingTupleIncludeDateShouldPass() {
    assertThat(
        row(
            Arrays.asList(
                ColumnNode.builder().alias("dateValue").type(Schema.Type.DATE).build(),
                ColumnNode.builder().alias("gender").type(Schema.Type.TEXT).build()),
            Collections.singletonList(
                BindingTuple.from(ImmutableMap.of("dateValue", 1529712000000L, "gender", "m")))),
        containsInAnyOrder(
            rowContents(
                allOf(
                    hasEntry("dateValue", "2018-06-23 00:00:00.000"),
                    hasEntry("gender", (Object) "m")))));
  }

  private static Matcher<DataRows.Row> rowContents(Matcher<Map<String, Object>> matcher) {
    return featureValueOf("DataRows.Row", matcher, DataRows.Row::getContents);
  }

  private List<DataRows.Row> row(
      List<ColumnNode> columnNodes, List<BindingTuple> bindingTupleList) {
    return ImmutableList.copyOf(
        BindingTupleResultSet.buildDataRows(columnNodes, bindingTupleList).iterator());
  }
}
