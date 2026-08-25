/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ObjectInputFilter;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class DeserializationFilterUtilTest {

  @Test
  void allowlisted_class_is_allowed() {
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter("");
    assertEquals(
        ObjectInputFilter.Status.ALLOWED,
        filter.checkInput(info(String.class, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 100)));
  }

  @Test
  void disallowed_class_is_rejected() {
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter("");
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(java.net.URL.class, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 100)));
  }

  @Test
  void depth_exceeding_limit_is_rejected() {
    // Structural limit: maxdepth=20 → depth 21 must be rejected regardless of class.
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter("");
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(/*class*/ null, /*depth*/ 21, /*refs*/ 1, /*bytes*/ 100)));
  }

  @Test
  void refs_exceeding_limit_is_rejected() {
    // Structural limit: maxrefs=300 → refs 301 must be rejected regardless of class.
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter("");
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(/*class*/ null, /*depth*/ 1, /*refs*/ 301, /*bytes*/ 100)));
  }

  @Test
  void bytes_exceeding_limit_are_rejected() {
    // Structural limit: maxbytes=15000 → 15001 bytes must be rejected regardless of class.
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter("");
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(/*class*/ null, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 15001)));
  }

  @Test
  void additional_pattern_is_honored() {
    // Patterns passed in should extend the base allowlist.
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter("java.net.URI;");
    assertEquals(
        ObjectInputFilter.Status.ALLOWED,
        filter.checkInput(info(java.net.URI.class, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 100)));
  }

  private static ObjectInputFilter.FilterInfo info(
      Class<?> cls, long depth, long refs, long bytes) {
    return new ObjectInputFilter.FilterInfo() {
      @Override
      public Class<?> serialClass() {
        return cls;
      }

      @Override
      public long arrayLength() {
        return -1;
      }

      @Override
      public long depth() {
        return depth;
      }

      @Override
      public long references() {
        return refs;
      }

      @Override
      public long streamBytes() {
        return bytes;
      }
    };
  }
}
