/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.expression.DSL.span;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.expression.NamedExpression;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class SpanAggregationBuilderTest {
  private final SpanAggregationBuilder aggregationBuilder = new SpanAggregationBuilder();

  @Test
  void fixed_interval_time_span() {
    assertEquals(
        "{\n"
            + "  \"SpanExpression(field=timestamp, value=1, unit=H)\" : {\n"
            + "    \"date_histogram\" : {\n"
            + "      \"field\" : \"timestamp\",\n"
            + "      \"fixed_interval\" : \"1h\",\n"
            + "      \"offset\" : 0,\n"
            + "      \"order\" : {\n"
            + "        \"_key\" : \"asc\"\n"
            + "      },\n"
            + "      \"keyed\" : false,\n"
            + "      \"min_doc_count\" : 0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(named(span(ref("timestamp", TIMESTAMP), literal(1), "h")))
    );
  }

  @Test
  void calendar_interval_time_span() {
    assertEquals(
        "{\n"
            + "  \"SpanExpression(field=date, value=1, unit=W)\" : {\n"
            + "    \"date_histogram\" : {\n"
            + "      \"field\" : \"date\",\n"
            + "      \"calendar_interval\" : \"1w\",\n"
            + "      \"offset\" : 0,\n"
            + "      \"order\" : {\n"
            + "        \"_key\" : \"asc\"\n"
            + "      },\n"
            + "      \"keyed\" : false,\n"
            + "      \"min_doc_count\" : 0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(named(span(ref("date", DATE), literal(1), "w")))
    );
  }

  @Test
  void general_span() {
    assertEquals(
        "{\n"
            + "  \"SpanExpression(field=age, value=1, unit=NONE)\" : {\n"
            + "    \"histogram\" : {\n"
            + "      \"field\" : \"age\",\n"
            + "      \"interval\" : 1.0,\n"
            + "      \"offset\" : 0.0,\n"
            + "      \"order\" : {\n"
            + "        \"_key\" : \"asc\"\n"
            + "      },\n"
            + "      \"keyed\" : false,\n"
            + "      \"min_doc_count\" : 0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(named(span(ref("age", INTEGER), literal(1), "")))
    );
  }

  @Test
  void invalid_unit() {
    NamedExpression namedSpan = named(span(ref("age", INTEGER), literal(1), "invalid_unit"));
    assertThrows(IllegalStateException.class, () -> buildQuery(namedSpan));
  }

  @SneakyThrows
  private String buildQuery(NamedExpression namedExpression) {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readTree(
        aggregationBuilder.build(namedExpression).toString())
        .toPrettyString();
  }

}
