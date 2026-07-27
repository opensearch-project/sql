/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.spi.rest.ColumnType.STRING;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spi.rest.ArgSpec;
import org.opensearch.sql.spi.rest.Column;
import org.opensearch.sql.spi.rest.RedactionClass;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.sql.spi.rest.RestEndpointDefinition;
import org.opensearch.sql.spi.rest.RestEndpointProvider;

/**
 * Proves the core property of class-based redaction: a sensitivity class is declared ONCE on a
 * column and one platform {@link org.opensearch.sql.spi.rest.Redactor} registered for that class is
 * reused by EVERY endpoint that has such a column, endpoint- and column-name-agnostic. Two
 * unrelated fake endpoints, each with an {@link RedactionClass#IP} column of a different name, are
 * both masked by the single registered IP redactor; a column of another class is untouched; and an
 * empty registry (the OSS default) masks nothing.
 */
class RedactionByClassTest {

  private static final String MASK = "x.x.x.x";

  /**
   * Two unrelated endpoints, each with an IP-classed column (different names) plus a NONE column.
   */
  private static final class TwoIpEndpointsProvider implements RestEndpointProvider {
    @Override
    public List<RestEndpointDefinition> getEndpoints() {
      return List.of(
          RestEndpointDefinition.builder()
              .name("/_fake/alpha")
              .schema(
                  List.of(
                      Column.of("addr", STRING, RedactionClass.IP),
                      Column.of("label", STRING, RedactionClass.NONE)))
              .argSpec(ArgSpec.NONE)
              .handler(ctx -> List.of(Map.of("addr", "10.0.0.7", "label", "10.0.0.7")))
              .build(),
          RestEndpointDefinition.builder()
              .name("/_fake/beta")
              .schema(
                  List.of(
                      Column.of("host_ip", STRING, RedactionClass.IP), Column.of("note", STRING)))
              .argSpec(ArgSpec.NONE)
              .handler(ctx -> List.of(Map.of("host_ip", "192.168.1.1", "note", "192.168.1.1")))
              .build());
    }
  }

  private static RestEndpointRegistry registry() {
    return new RestEndpointRegistry(List.of(new TwoIpEndpointsProvider()));
  }

  private static RestEndpointContext ctx() {
    return RestEndpointContext.of(Map.of(), null);
  }

  @Test
  void oneRedactorMasksTheIpColumnOfEveryEndpoint() {
    // Declare the IP masker ONCE; it must apply to both endpoints' IP columns regardless of name.
    RedactionRegistry redaction = new RedactionRegistry();
    redaction.register(RedactionClass.IP, value -> MASK);

    RestEndpointRegistry registry = registry();

    Map<String, ExprValue> alpha =
        registry.resolve("/_fake/alpha").toRows(ctx(), redaction).get(0).tupleValue();
    assertEquals(MASK, alpha.get("addr").stringValue(), "alpha IP column masked");
    // A NONE-class column is never masked, even though its value is an address.
    assertEquals("10.0.0.7", alpha.get("label").stringValue(), "NONE-class column untouched");

    Map<String, ExprValue> beta =
        registry.resolve("/_fake/beta").toRows(ctx(), redaction).get(0).tupleValue();
    assertEquals(
        MASK, beta.get("host_ip").stringValue(), "beta IP column masked by the same redactor");
    assertEquals("192.168.1.1", beta.get("note").stringValue(), "default NONE column untouched");
  }

  @Test
  void emptyRegistryIsANoOp() {
    RedactionRegistry empty = new RedactionRegistry();
    RestEndpointRegistry registry = registry();

    Map<String, ExprValue> alpha =
        registry.resolve("/_fake/alpha").toRows(ctx(), empty).get(0).tupleValue();
    // No redactor registered for IP: the value passes through unchanged.
    assertEquals("10.0.0.7", alpha.get("addr").stringValue());
    assertEquals("10.0.0.7", alpha.get("label").stringValue());
  }
}
