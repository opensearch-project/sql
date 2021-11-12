/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import java.util.Arrays;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class BucketAggregationBuilderTest {

  @Mock
  private ExpressionSerializer serializer;

  private BucketAggregationBuilder aggregationBuilder;

  @BeforeEach
  void set_up() {
    aggregationBuilder = new BucketAggregationBuilder(serializer);
  }

  @Test
  void should_build_bucket_with_field() {
    assertEquals(
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"field\" : \"age\",\n"
            + "    \"missing_bucket\" : true,\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(
            Arrays.asList(
                asc(named("age", ref("age", INTEGER))))));
  }

  @Test
  void should_build_bucket_with_keyword_field() {
    assertEquals(
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"field\" : \"name.keyword\",\n"
            + "    \"missing_bucket\" : true,\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(
            Arrays.asList(
                asc(named("name", ref("name", OPENSEARCH_TEXT_KEYWORD))))));
  }

  @SneakyThrows
  private String buildQuery(List<Pair<NamedExpression, SortOrder>> groupByExpressions) {
    XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
    builder.startObject();
    CompositeValuesSourceBuilder<?> sourceBuilder =
        aggregationBuilder.build(groupByExpressions).get(0);
    sourceBuilder.toXContent(builder, EMPTY_PARAMS);
    builder.endObject();
    return BytesReference.bytes(builder).utf8ToString();
  }

  private Pair<NamedExpression, SortOrder> asc(NamedExpression expression) {
    return Pair.of(expression, SortOrder.ASC);
  }
}
