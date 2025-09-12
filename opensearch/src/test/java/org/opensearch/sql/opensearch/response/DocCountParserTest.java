/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.opensearch.response.agg.DocCountParser;

class DocCountParserTest {

  @Test
  void should_parse_doc_count_from_bucket() {
    DocCountParser parser = new DocCountParser("count_field");
    Map<String, Object> bucket = new HashMap<>();
    bucket.put("doc_count", 42);
    
    Map<String, Object> result = parser.parseBucket(bucket);
    assertEquals(42, result.get("count_field"));
  }

  @Test
  void should_return_zero_when_doc_count_missing() {
    DocCountParser parser = new DocCountParser("count_field");
    Map<String, Object> bucket = new HashMap<>();
    
    Map<String, Object> result = parser.parseBucket(bucket);
    assertEquals(0, result.get("count_field"));
  }

  @Test
  void should_return_name() {
    DocCountParser parser = new DocCountParser("test_name");
    assertEquals("test_name", parser.getName());
  }

  @Test
  void should_throw_exception_for_aggregations_parse() {
    DocCountParser parser = new DocCountParser("count_field");
    
    assertThrows(UnsupportedOperationException.class, () -> parser.parse((org.opensearch.search.aggregations.Aggregation) null));
  }
}