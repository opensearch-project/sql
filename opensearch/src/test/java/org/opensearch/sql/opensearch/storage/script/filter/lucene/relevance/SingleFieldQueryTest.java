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
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

class SingleFieldQueryTest {
  SingleFieldQuery query;
  private final String testQueryName = "test_query";
  private final Map<String, RelevanceQuery.QueryBuilderStep> actionMap =
      ImmutableMap.of("paramA", (o, v) -> o);

  @BeforeEach
  void setUp() {
    query =
        mock(
            SingleFieldQuery.class,
            Mockito.withSettings()
                .useConstructor(actionMap)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));
    when(query.getQueryName()).thenReturn(testQueryName);
  }

  @Test
  void createQueryBuilderTestTypeTextKeyword() {
    String sampleQuery = "sample query";
    String sampleField = "fieldA";

    query.createQueryBuilder(
        List.of(
            DSL.namedArgument(
                "field",
                new ReferenceExpression(
                    sampleField,
                    OpenSearchTextType.of(
                        Map.of(
                            "words",
                            OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword))))),
            DSL.namedArgument(
                "query", new LiteralExpression(ExprValueUtils.stringValue(sampleQuery)))));

    verify(query).createBuilder(eq(sampleField), eq(sampleQuery));
  }

  @Test
  void createQueryBuilderTestTypeText() {
    String sampleQuery = "sample query";
    String sampleField = "fieldA";

    query.createQueryBuilder(
        List.of(
            DSL.namedArgument(
                "field", new ReferenceExpression(sampleField, OpenSearchTextType.of())),
            DSL.namedArgument(
                "query", new LiteralExpression(ExprValueUtils.stringValue(sampleQuery)))));

    verify(query).createBuilder(eq(sampleField), eq(sampleQuery));
  }
}
