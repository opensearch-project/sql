/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.rest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.client.Request;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/** Sends source JSON directly so the YAML client cannot normalize numeric tokens. */
public class FlatObjectLexicalReviewIT extends OpenSearchRestTestCase {
  private Map<String, Object> request(String method, String path, String json) throws Exception {
    Request request = new Request(method, path);
    request.setJsonEntity(json);
    return entityAsMap(client().performRequest(request));
  }

  public void testOverflowedNumericTextMustNotChangeWithPushdown() throws Exception {
    request(
        "PUT",
        "/review_overflow_5830",
        "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},"
            + "\"mappings\":{\"properties\":{\"attributes\":{\"type\":\"flat_object\"}}}}");
    try {
      request("PUT", "/review_overflow_5830/_doc/1?refresh=true", "{\"attributes\":{\"n\":1e309}}");
      request(
          "PUT",
          "/_cluster/settings",
          "{\"transient\":{\"plugins.calcite.enabled\":true,"
              + "\"plugins.calcite.fallback.allowed\":false,"
              + "\"plugins.calcite.pushdown.enabled\":true}}");
      Object read =
          request(
                  "POST",
                  "/_plugins/_ppl",
                  "{\"query\":\"source=review_overflow_5830 | fields attributes.n\"}")
              .get("datarows");
      String query =
          "{\"query\":\"source=review_overflow_5830 "
              + "| where attributes.n = 'Infinity' | fields attributes.n\"}";
      Object withPushdown = request("POST", "/_plugins/_ppl", query).get("datarows");
      request(
          "PUT",
          "/_cluster/settings",
          "{\"transient\":{\"plugins.calcite.pushdown.enabled\":false}}");
      Object withoutPushdown = request("POST", "/_plugins/_ppl", query).get("datarows");
      System.out.println(
          "B3 overflow read="
              + read
              + " pushdown=true "
              + withPushdown
              + " pushdown=false "
              + withoutPushdown);
      assertEquals(List.of(List.of("Infinity")), read);
      assertEquals(read, withoutPushdown);
      assertEquals(
          "Rendered numeric values outside the numeric-token alphabet must preserve results",
          withoutPushdown,
          withPushdown);
    } finally {
      request(
          "PUT",
          "/_cluster/settings",
          "{\"transient\":{\"plugins.calcite.pushdown.enabled\":null,"
              + "\"plugins.calcite.enabled\":null,"
              + "\"plugins.calcite.fallback.allowed\":null}}");
      request("DELETE", "/review_overflow_5830", "{}");
    }
  }

  public void testWildcardPatternsMustNotChangeWithPushdown() throws Exception {
    request(
        "PUT",
        "/review_like_5830",
        "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},"
            + "\"mappings\":{\"properties\":{\"attributes\":{\"type\":\"flat_object\"}}}}");
    try {
      request("PUT", "/review_like_5830/_doc/1?refresh=true", "{\"attributes\":{\"n\":1e3}}");
      List<String> patterns = List.of("%e%", "%E%", "_e_", "______", "1000%");
      Map<String, Object> withPushdown = new LinkedHashMap<>();
      Map<String, Object> withoutPushdown = new LinkedHashMap<>();
      request(
          "PUT",
          "/_cluster/settings",
          "{\"transient\":{\"plugins.calcite.enabled\":true,"
              + "\"plugins.calcite.fallback.allowed\":false,"
              + "\"plugins.calcite.pushdown.enabled\":true}}");
      for (String pattern : patterns) {
        String query =
            "{\"query\":\"source=review_like_5830 | where like(attributes.n, '"
                + pattern
                + "') | fields attributes.n\"}";
        withPushdown.put(pattern, request("POST", "/_plugins/_ppl", query).get("datarows"));
      }
      request(
          "PUT",
          "/_cluster/settings",
          "{\"transient\":{\"plugins.calcite.pushdown.enabled\":false}}");
      for (String pattern : patterns) {
        String query =
            "{\"query\":\"source=review_like_5830 | where like(attributes.n, '"
                + pattern
                + "') | fields attributes.n\"}";
        withoutPushdown.put(pattern, request("POST", "/_plugins/_ppl", query).get("datarows"));
      }
      System.out.println("B3 LIKE pushdown=true " + withPushdown);
      System.out.println("B3 LIKE pushdown=false " + withoutPushdown);
      assertEquals(List.of(List.of("1000.0")), withoutPushdown.get("______"));
      assertEquals(List.of(List.of("1000.0")), withPushdown.get("1000%"));
      assertEquals(
          "Wildcard patterns without literal digits must preserve results across plans",
          withoutPushdown,
          withPushdown);
    } finally {
      request(
          "PUT",
          "/_cluster/settings",
          "{\"transient\":{\"plugins.calcite.pushdown.enabled\":null,"
              + "\"plugins.calcite.enabled\":null,"
              + "\"plugins.calcite.fallback.allowed\":null}}");
      request("DELETE", "/review_like_5830", "{}");
    }
  }

  public void testTextEqualityMustNotChangeWithPushdown() throws Exception {
    request(
        "PUT",
        "/review_plan_5830",
        "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},"
            + "\"mappings\":{\"properties\":{\"attributes\":{\"type\":\"flat_object\"}}}}");
    try {
      request("PUT", "/review_plan_5830/_doc/1?refresh=true", "{\"attributes\":{\"n\":1e3}}");
      String canonicalQuery =
          "{\"query\":\"source=review_plan_5830 "
              + "| where attributes.n = '1000.0' | fields attributes.n\"}";
      String tokenQuery =
          "{\"query\":\"source=review_plan_5830 "
              + "| where attributes.n = '1e3' | fields attributes.n\"}";
      String numericQuery =
          "{\"query\":\"source=review_plan_5830 "
              + "| where cast(attributes.n as double) = 1000 | fields attributes.n\"}";
      request(
          "PUT",
          "/_cluster/settings",
          "{\"transient\":{\"plugins.calcite.enabled\":true,"
              + "\"plugins.calcite.fallback.allowed\":false,"
              + "\"plugins.calcite.pushdown.enabled\":true}}");
      Object canonicalWithPushdown =
          request("POST", "/_plugins/_ppl", canonicalQuery).get("datarows");
      Object tokenWithPushdown = request("POST", "/_plugins/_ppl", tokenQuery).get("datarows");
      Object numericWithPushdown = request("POST", "/_plugins/_ppl", numericQuery).get("datarows");
      System.out.println(
          "B3 pushdown=true canonical="
              + canonicalWithPushdown
              + " token="
              + tokenWithPushdown
              + " numericCast="
              + numericWithPushdown);

      request(
          "PUT",
          "/_cluster/settings",
          "{\"transient\":{\"plugins.calcite.pushdown.enabled\":false}}");
      Object canonicalWithoutPushdown =
          request("POST", "/_plugins/_ppl", canonicalQuery).get("datarows");
      Object tokenWithoutPushdown = request("POST", "/_plugins/_ppl", tokenQuery).get("datarows");
      Object numericWithoutPushdown =
          request("POST", "/_plugins/_ppl", numericQuery).get("datarows");
      System.out.println(
          "B3 pushdown=false canonical="
              + canonicalWithoutPushdown
              + " token="
              + tokenWithoutPushdown
              + " numericCast="
              + numericWithoutPushdown);

      assertEquals(List.of(List.of("1000.0")), canonicalWithoutPushdown);
      assertEquals(List.of(List.of("1000.0")), numericWithPushdown);
      assertEquals(numericWithoutPushdown, numericWithPushdown);
      assertEquals(
          "Changing the execution plan must preserve text equality results",
          canonicalWithoutPushdown,
          canonicalWithPushdown);
      assertEquals(tokenWithoutPushdown, tokenWithPushdown);
    } finally {
      request(
          "PUT",
          "/_cluster/settings",
          "{\"transient\":{\"plugins.calcite.pushdown.enabled\":null,"
              + "\"plugins.calcite.enabled\":null,"
              + "\"plugins.calcite.fallback.allowed\":null}}");
      request("DELETE", "/review_plan_5830", "{}");
    }
  }

  public void testDisplayedNumericTextMustMatchExactTerm() throws Exception {
    request(
        "PUT",
        "/review_numeric_5830",
        "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},"
            + "\"mappings\":{\"properties\":{\"attributes\":{\"type\":\"flat_object\"}}}}");
    request("PUT", "/review_numeric_5830/_doc/1?refresh=true", "{\"attributes\":{\"n\":1e3}}");
    request(
        "PUT",
        "/_cluster/settings",
        "{\"transient\":{\"plugins.calcite.enabled\":true,"
            + "\"plugins.calcite.fallback.allowed\":false}}");
    Map<String, Object> indexed =
        request(
            "POST",
            "/review_numeric_5830/_search",
            "{\"query\":{\"term\":{\"attributes.n\":\"1e3\"}}}");
    assertEquals(
        1,
        ((Number) ((Map<?, ?>) ((Map<?, ?>) indexed.get("hits")).get("total")).get("value"))
            .intValue());
    Map<String, Object> read =
        request(
            "POST",
            "/_plugins/_ppl",
            "{\"query\":\"source=review_numeric_5830 | fields attributes.n\"}");
    assertEquals(List.of(List.of("1000.0")), read.get("datarows"));
    Map<String, Object> filtered =
        request(
            "POST",
            "/_plugins/_ppl",
            "{\"query\":\"source=review_numeric_5830 "
                + "| where attributes.n = '1000.0' | fields attributes.n\"}");
    assertEquals(List.of(List.of("1000.0")), filtered.get("datarows"));
  }
}
