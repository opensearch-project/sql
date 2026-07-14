/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

/**
 * Computes a deterministic document {@code _id} from the {@code key_field} values of a row, so
 * re-running an upsert updates the same document instead of duplicating it.
 *
 * <p>The id is {@code base64url(SHA-256(canonical(keyValues)))}: a fixed 43-character string, well
 * under the 512-byte id limit regardless of key size. The canonical encoding is a sequence of
 * length-prefixed, type-tagged tokens (one per key field), so multi-field keys cannot collide
 * across different field boundaries, and empty string, null, and a missing field are all distinct.
 * Multivalue key fields are rejected. The raw key values still live in {@code _source}, so the
 * lookup stays queryable by value.
 */
public final class LookupIdEncoder {

  private LookupIdEncoder() {}

  /** Marker for a key field that is not present in the row schema (distinct from a null value). */
  private static final Object MISSING = new Object();

  public static String encode(List<String> keyFields, List<String> fields, Object[] row) {
    MessageDigest digest = sha256();
    for (String keyField : keyFields) {
      int idx = fields.indexOf(keyField);
      Object value = (idx >= 0 && idx < row.length) ? row[idx] : MISSING;
      if (value == null || value == MISSING) {
        digest.update((byte) 'N'); // null and missing both encode as N, distinct from any V token
        continue;
      }
      if (value instanceof Object[] || value instanceof Collection) {
        throw new IllegalArgumentException(
            "outputlookup key_field [" + keyField + "] must not be multivalue");
      }
      byte[] bytes = canonical(value).getBytes(StandardCharsets.UTF_8);
      digest.update((byte) 'V');
      digest.update(typeTag(value));
      digest.update(intToBytes(bytes.length)); // length prefix makes multi-field keys unambiguous
      digest.update(bytes);
    }
    return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
  }

  private static String canonical(Object value) {
    if (value instanceof Boolean) {
      return value.toString();
    }
    if (value instanceof Float || value instanceof Double) {
      return Double.toString(((Number) value).doubleValue());
    }
    if (value instanceof Number) {
      return Long.toString(((Number) value).longValue());
    }
    return String.valueOf(value);
  }

  private static byte typeTag(Object value) {
    if (value instanceof Boolean) {
      return 'b';
    }
    if (value instanceof Float || value instanceof Double) {
      return 'd';
    }
    if (value instanceof Number) {
      return 'i';
    }
    return 's';
  }

  private static byte[] intToBytes(int v) {
    return new byte[] {(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
