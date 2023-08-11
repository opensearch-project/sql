/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.LiteralExpression;

class NoFieldQueryTest {
  NoFieldQuery query;
  private final String testQueryName = "test_query";
  private final Map<String, RelevanceQuery.QueryBuilderStep> actionMap =
      ImmutableMap.of("paramA", (o, v) -> o);

  @BeforeEach
  void setUp() {
    query =
        mock(
            NoFieldQuery.class,
            Mockito.withSettings()
                .useConstructor(actionMap)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    when(query.getQueryName()).thenReturn(testQueryName);
  }

  @Test
  void createQueryBuilderTest() {
    String sampleQuery = "field:query";

    query.createQueryBuilder(
        List.of(
            DSL.namedArgument(
                "query", new LiteralExpression(ExprValueUtils.stringValue(sampleQuery)))));

    verify(query).createBuilder(eq(sampleQuery));
  }
}
