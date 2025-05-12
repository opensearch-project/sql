/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;

import java.util.Map;
import lombok.SneakyThrows;
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
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
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
            + "  \"age\" : {\n"
            + "    \"terms\" : {\n"
            + "      \"field\" : \"age\",\n"
            + "      \"size\" : 1000,\n"
            + "      \"min_doc_count\" : 1,\n"
            + "      \"shard_min_doc_count\" : 0,\n"
            + "      \"show_term_doc_count_error\" : false,\n"
            + "      \"order\" : [\n"
            + "        {\n"
            + "          \"_count\" : \"desc\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"_key\" : \"asc\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(named("age", ref("age", INTEGER))));
  }

  @Test
  void should_build_bucket_with_literal() {
    var literal = literal(1);
    when(serializer.serialize(literal)).thenReturn("mock-serialize");
    assertEquals(
        "{\n"
            + "  \"1\" : {\n"
            + "    \"terms\" : {\n"
            + "      \"script\" : {\n"
            + "        \"source\" : \"mock-serialize\",\n"
            + "        \"lang\" : \"opensearch_query_expression\"\n"
            + "      },\n"
            + "      \"size\" : 1000,\n"
            + "      \"min_doc_count\" : 1,\n"
            + "      \"shard_min_doc_count\" : 0,\n"
            + "      \"show_term_doc_count_error\" : false,\n"
            + "      \"order\" : [\n"
            + "        {\n"
            + "          \"_count\" : \"desc\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"_key\" : \"asc\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(named(literal)));
  }

  @Test
  void should_build_bucket_with_keyword_field() {
    assertEquals(
        "{\n"
            + "  \"name\" : {\n"
            + "    \"terms\" : {\n"
            + "      \"field\" : \"name.keyword\",\n"
            + "      \"size\" : 1000,\n"
            + "      \"min_doc_count\" : 1,\n"
            + "      \"shard_min_doc_count\" : 0,\n"
            + "      \"show_term_doc_count_error\" : false,\n"
            + "      \"order\" : [\n"
            + "        {\n"
            + "          \"_count\" : \"desc\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"_key\" : \"asc\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            named(
                "name",
                ref(
                    "name",
                    OpenSearchTextType.of(
                        Map.of(
                            "words",
                            OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)))))));
  }

  @Test
  void should_build_bucket_with_parse_expression() {
    ParseExpression parseExpression =
        DSL.regex(ref("name.keyword", STRING), DSL.literal("(?<name>\\w+)"), DSL.literal("name"));
    when(serializer.serialize(parseExpression)).thenReturn("mock-serialize");
    assertEquals(
        "{\n"
            + "  \"name\" : {\n"
            + "    \"terms\" : {\n"
            + "      \"script\" : {\n"
            + "        \"source\" : \"mock-serialize\",\n"
            + "        \"lang\" : \"opensearch_query_expression\"\n"
            + "      },\n"
            + "      \"size\" : 1000,\n"
            + "      \"min_doc_count\" : 1,\n"
            + "      \"shard_min_doc_count\" : 0,\n"
            + "      \"show_term_doc_count_error\" : false,\n"
            + "      \"order\" : [\n"
            + "        {\n"
            + "          \"_count\" : \"desc\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"_key\" : \"asc\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(named("name", parseExpression)));
  }

  @Test
  void terms_bucket_for_opensearchdate_type_uses_long() {
    OpenSearchDateType dataType = OpenSearchDateType.of(ExprCoreType.TIMESTAMP);

    assertEquals(
        "{\n"
            + "  \"date\" : {\n"
            + "    \"terms\" : {\n"
            + "      \"field\" : \"date\",\n"
            + "      \"value_type\" : \"long\",\n"
            + "      \"size\" : 1000,\n"
            + "      \"min_doc_count\" : 1,\n"
            + "      \"shard_min_doc_count\" : 0,\n"
            + "      \"show_term_doc_count_error\" : false,\n"
            + "      \"order\" : [\n"
            + "        {\n"
            + "          \"_count\" : \"desc\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"_key\" : \"asc\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(named("date", ref("date", dataType))));
  }

  @Test
  void terms_bucket_for_opensearchdate_type_uses_long_false() {
    OpenSearchDateType dataType = OpenSearchDateType.of(STRING);

    assertEquals(
        "{\n"
            + "  \"date\" : {\n"
            + "    \"terms\" : {\n"
            + "      \"field\" : \"date\",\n"
            + "      \"size\" : 1000,\n"
            + "      \"min_doc_count\" : 1,\n"
            + "      \"shard_min_doc_count\" : 0,\n"
            + "      \"show_term_doc_count_error\" : false,\n"
            + "      \"order\" : [\n"
            + "        {\n"
            + "          \"_count\" : \"desc\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"_key\" : \"asc\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(named("date", ref("date", dataType))));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = ExprCoreType.class,
      names = {"TIMESTAMP", "TIME", "DATE"})
  void terms_bucket_for_datetime_types_uses_long(ExprType dataType) {
    assertEquals(
        "{\n"
            + "  \"date\" : {\n"
            + "    \"terms\" : {\n"
            + "      \"field\" : \"date\",\n"
            + "      \"value_type\" : \"long\",\n"
            + "      \"size\" : 1000,\n"
            + "      \"min_doc_count\" : 1,\n"
            + "      \"shard_min_doc_count\" : 0,\n"
            + "      \"show_term_doc_count_error\" : false,\n"
            + "      \"order\" : [\n"
            + "        {\n"
            + "          \"_count\" : \"desc\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"_key\" : \"asc\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(named("date", ref("date", dataType))));
  }

  @SneakyThrows
  private String buildQuery(NamedExpression groupByExpression) {
    XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
    builder.startObject();
    ValuesSourceAggregationBuilder<?> sourceBuilder = aggregationBuilder.build(groupByExpression);
    sourceBuilder.toXContent(builder, EMPTY_PARAMS);
    builder.endObject();
    return BytesReference.bytes(builder).utf8ToString();
  }
}
