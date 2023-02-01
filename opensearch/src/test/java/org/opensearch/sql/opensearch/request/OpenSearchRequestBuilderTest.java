/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;

@ExtendWith(MockitoExtension.class)
public class OpenSearchRequestBuilderTest {

  private static final TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);
  private static final Integer DEFAULT_OFFSET = 0;
  private static final Integer DEFAULT_LIMIT = 200;
  private static final Integer MAX_RESULT_WINDOW = 500;

  @Mock
  private Settings settings;

  @Mock
  private OpenSearchExprValueFactory exprValueFactory;

  private OpenSearchRequestBuilder requestBuilder;

  @BeforeEach
  void setup() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    requestBuilder = new OpenSearchRequestBuilder(
        "test", MAX_RESULT_WINDOW, settings, exprValueFactory);
  }

  @Test
  void buildQueryRequest() {
    Integer limit = 200;
    Integer offset = 0;
    requestBuilder.pushDownLimit(limit, offset);

    assertEquals(
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            new SearchSourceBuilder()
                .from(offset)
                .size(limit)
                .timeout(DEFAULT_QUERY_TIMEOUT),
            exprValueFactory),
        requestBuilder.build());
  }

  @Test
  void buildScrollRequestWithCorrectSize() {
    Integer limit = 800;
    Integer offset = 10;
    requestBuilder.pushDownLimit(limit, offset);

    assertEquals(
        new OpenSearchScrollRequest(
            new OpenSearchRequest.IndexName("test"),
            new SearchSourceBuilder()
                .from(offset)
                .size(MAX_RESULT_WINDOW - offset)
                .timeout(DEFAULT_QUERY_TIMEOUT),
        exprValueFactory),
        requestBuilder.build());
  }

  @Test
  void testPushDownQuery() {
    QueryBuilder query = QueryBuilders.termQuery("intA", 1);
    requestBuilder.pushDown(query);

    assertEquals(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .query(query)
            .sort(DOC_FIELD_NAME, ASC),
        requestBuilder.getSourceBuilder()
    );
  }

  @Test
  void testPushDownAggregation() {
    AggregationBuilder aggBuilder = AggregationBuilders.composite(
        "composite_buckets",
        Collections.singletonList(new TermsValuesSourceBuilder("longA")));
    OpenSearchAggregationResponseParser responseParser =
        new CompositeAggregationParser(
            new SingleValueParser("AVG(intA)"));
    requestBuilder.pushDownAggregation(Pair.of(List.of(aggBuilder), responseParser));

    assertEquals(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(0)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .aggregation(aggBuilder),
        requestBuilder.getSourceBuilder()
    );
    verify(exprValueFactory).setParser(responseParser);
  }

  @Test
  void testPushDownQueryAndSort() {
    QueryBuilder query = QueryBuilders.termQuery("intA", 1);
    requestBuilder.pushDown(query);

    FieldSortBuilder sortBuilder = SortBuilders.fieldSort("intA");
    requestBuilder.pushDownSort(List.of(sortBuilder));

    assertEquals(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .query(query)
            .sort(sortBuilder),
        requestBuilder.getSourceBuilder());
  }

  @Test
  void testPushDownSort() {
    FieldSortBuilder sortBuilder = SortBuilders.fieldSort("intA");
    requestBuilder.pushDownSort(List.of(sortBuilder));

    assertEquals(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .sort(sortBuilder),
        requestBuilder.getSourceBuilder());
  }

  @Test
  void testPushDownNonFieldSort() {
    ScoreSortBuilder sortBuilder = SortBuilders.scoreSort();
    requestBuilder.pushDownSort(List.of(sortBuilder));

    assertEquals(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .sort(sortBuilder),
        requestBuilder.getSourceBuilder());
  }

  @Test
  void testPushDownMultipleSort() {
    requestBuilder.pushDownSort(List.of(
        SortBuilders.fieldSort("intA"),
        SortBuilders.fieldSort("intB")));

    assertEquals(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .sort(SortBuilders.fieldSort("intA"))
            .sort(SortBuilders.fieldSort("intB")),
        requestBuilder.getSourceBuilder());
  }

  @Test
  void testPushDownProject() {
    Set<ReferenceExpression> references = Set.of(DSL.ref("intA", INTEGER));
    requestBuilder.pushDownProjects(references);

    assertEquals(
        new SearchSourceBuilder()
            .from(DEFAULT_OFFSET)
            .size(DEFAULT_LIMIT)
            .timeout(DEFAULT_QUERY_TIMEOUT)
            .fetchSource(new String[]{"intA"}, new String[0]),
        requestBuilder.getSourceBuilder());
  }

  @Test
  void testPushTypeMapping() {
    Map<String, OpenSearchDataType> typeMapping = Map.of("intA", OpenSearchDataType.of(INTEGER));
    requestBuilder.pushTypeMapping(typeMapping);

    verify(exprValueFactory).extendTypeMapping(typeMapping);
  }
}
