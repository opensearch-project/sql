/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class BucketAggregationBuilderTest {

  @Mock private ExpressionSerializer serializer;

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
            + "    \"missing_order\" : \"first\",\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(Arrays.asList(asc(named("age", ref("age", INTEGER))))));
  }

  @Test
  void should_build_bucket_with_literal() {
    var literal = literal(1);
    when(serializer.serialize(literal)).thenReturn("mock-serialize");
    assertEquals(
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"script\" : {\n"
            + "      \"source\" : \"mock-serialize\",\n"
            + "      \"lang\" : \"opensearch_query_expression\"\n"
            + "    },\n"
            + "    \"missing_bucket\" : true,\n"
            + "    \"missing_order\" : \"first\",\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(Arrays.asList(asc(named(literal)))));
  }

  @Test
  void should_build_bucket_with_keyword_field() {
    assertEquals(
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"field\" : \"name.keyword\",\n"
            + "    \"missing_bucket\" : true,\n"
            + "    \"missing_order\" : \"first\",\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(
            Arrays.asList(
                asc(
                    named(
                        "name",
                        ref(
                            "name",
                            OpenSearchTextType.of(
                                Map.of(
                                    "words",
                                    OpenSearchDataType.of(
                                        OpenSearchDataType.MappingType.Keyword)))))))));
  }

  @Test
  void should_build_bucket_with_parse_expression() {
    ParseExpression parseExpression =
        DSL.regex(ref("name.keyword", STRING), DSL.literal("(?<name>\\w+)"), DSL.literal("name"));
    when(serializer.serialize(parseExpression)).thenReturn("mock-serialize");
    assertEquals(
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"script\" : {\n"
            + "      \"source\" : \"mock-serialize\",\n"
            + "      \"lang\" : \"opensearch_query_expression\"\n"
            + "    },\n"
            + "    \"missing_bucket\" : true,\n"
            + "    \"missing_order\" : \"first\",\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(Arrays.asList(asc(named("name", parseExpression)))));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = ExprCoreType.class,
      names = {"TIMESTAMP", "TIME", "DATE"})
  void terms_bucket_for_datetime_types_uses_long(ExprType dataType) {
    assertEquals(
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"field\" : \"date\",\n"
            + "    \"missing_bucket\" : true,\n"
            + "    \"value_type\" : \"long\",\n"
            + "    \"missing_order\" : \"first\",\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(Arrays.asList(asc(named("date", ref("date", dataType))))));
  }

  /** Add this unit test in case build bucket aggregation with unsupported expression in future */
  @Test
  void should_throw_exception_for_unsupported_expression() {
    Expression unsupportedExpression =
        new Expression() {
          @Override
          public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
            return ExprValueUtils.nullValue();
          }

          @Override
          public ExprType type() {
            return UNKNOWN;
          }

          @Override
          public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
            return null;
          }

          @Override
          public String toString() {
            return "unknown";
          }
        };
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> buildQuery(Arrays.asList(asc(named("UNKNOWN", unsupportedExpression)))));
    assertEquals("bucket aggregation doesn't support expression unknown", exception.getMessage());
  }

  @SneakyThrows
  private String buildQuery(
      List<Triple<NamedExpression, SortOrder, MissingOrder>> groupByExpressions) {
    XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
    builder.startObject();
    CompositeValuesSourceBuilder<?> sourceBuilder =
        aggregationBuilder.build(groupByExpressions).get(0);
    sourceBuilder.toXContent(builder, EMPTY_PARAMS);
    builder.endObject();
    return BytesReference.bytes(builder).utf8ToString();
  }

  private Triple<NamedExpression, SortOrder, MissingOrder> asc(NamedExpression expression) {
    return Triple.of(expression, SortOrder.ASC, MissingOrder.FIRST);
  }
}
