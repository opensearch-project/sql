/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Base64;
import org.junit.jupiter.api.Test;

class QueryJobIdTest {

  @Test
  void create_generatesDistinctContextIdsForSameOwner() {
    QueryJobId a = QueryJobId.create("node-1");
    QueryJobId b = QueryJobId.create("node-1");
    assertEquals("node-1", a.ownerNodeId());
    assertEquals("node-1", b.ownerNodeId());
    assertNotEquals(a.contextId(), b.contextId());
  }

  @Test
  void encode_roundTripsThroughParse() {
    QueryJobId original = new QueryJobId("node-abc", "ctx-42");
    QueryJobId parsed = QueryJobId.parse(original.encode());
    assertEquals(original, parsed);
  }

  @Test
  void constructor_rejectsBlankComponents() {
    assertThrows(IllegalArgumentException.class, () -> new QueryJobId(" ", "ctx"));
    assertThrows(IllegalArgumentException.class, () -> new QueryJobId("node", ""));
    assertThrows(IllegalArgumentException.class, () -> new QueryJobId(null, "ctx"));
  }

  @Test
  void parse_rejectsNullOrEmpty() {
    assertThrows(IllegalArgumentException.class, () -> QueryJobId.parse(null));
    assertThrows(IllegalArgumentException.class, () -> QueryJobId.parse(""));
    assertThrows(IllegalArgumentException.class, () -> QueryJobId.parse("   "));
  }

  @Test
  void parse_rejectsMalformedBase64() {
    assertThrows(IllegalArgumentException.class, () -> QueryJobId.parse("!!not-base64!!"));
  }

  @Test
  void parse_rejectsTruncatedPayload() {
    String almost = QueryJobId.create("node-x").encode();
    byte[] bytes = Base64.getUrlDecoder().decode(almost);
    String truncated =
        Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[bytes.length - 1]);
    assertThrows(IllegalArgumentException.class, () -> QueryJobId.parse(truncated));
  }

  @Test
  void parse_rejectsUnsupportedVersion() {
    // A payload with a version byte other than 1 encoded via a well-formed but wrong format.
    String wrongVersion =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(new byte[] {0, 0, 0, 99, 0, 0, 0, 1, 65, 0, 0, 0, 1, 66});
    assertThrows(IllegalArgumentException.class, () -> QueryJobId.parse(wrongVersion));
  }
}
