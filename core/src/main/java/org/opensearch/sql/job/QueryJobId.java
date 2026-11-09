/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

/**
 * Opaque, URL-safe, node-routable identifier for a query job.
 *
 * <p>The encoded form carries a format version, the owner node ID used for cluster routing, and a
 * per-job random context. Clients treat the encoded value as opaque; the internal fields are hidden
 * from the public API surface. The ID is a routing token, not an authorization credential — GET and
 * DELETE still authorize the current caller against the job's stored owner.
 *
 * <p>Encoding layout (little-endian, wrapped in URL-safe Base64 without padding):
 *
 * <pre>
 *   int  formatVersion
 *   int  ownerNodeIdLength + UTF-8 ownerNodeId
 *   int  contextIdLength   + UTF-8 contextId
 * </pre>
 *
 * @param ownerNodeId node that owns the in-memory job
 * @param contextId random identifier for one job on the owner node
 */
public record QueryJobId(String ownerNodeId, String contextId) {

  private static final int FORMAT_VERSION = 1;
  private static final int MAX_ENCODED_LENGTH = 2_048;
  private static final int MAX_OWNER_NODE_ID_BYTES = 1_024;
  private static final int MAX_CONTEXT_ID_BYTES = 128;

  /**
   * Validates that both components are present and non-blank. Called by every path that
   * materializes an id, including {@link #create(String)} and {@link #parse(String)}.
   *
   * @throws IllegalArgumentException when either component is {@code null} or blank
   */
  public QueryJobId {
    if (ownerNodeId == null || ownerNodeId.isBlank()) {
      throw new IllegalArgumentException("Query job owner node must not be empty");
    }
    if (contextId == null || contextId.isBlank()) {
      throw new IllegalArgumentException("Query job context must not be empty");
    }
  }

  /** Creates a fresh job ID with a random context. */
  public static QueryJobId create(String ownerNodeId) {
    return new QueryJobId(ownerNodeId, UUID.randomUUID().toString());
  }

  /**
   * Attempts to parse an opaque encoded id. Unlike {@link #parse(String)}, returns {@link
   * Optional#empty()} instead of throwing when the value is not a valid encoded {@code QueryJobId}
   * — useful for callers that treat non-matching ids as belonging to a different backend.
   */
  public static Optional<QueryJobId> tryParse(String encoded) {
    if (encoded == null || encoded.isBlank()) {
      return Optional.empty();
    }
    try {
      return Optional.of(parse(encoded));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  /** Returns the versioned URL-safe encoded form used on the wire. */
  public String encode() {
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(FORMAT_VERSION);
      writeString(out, ownerNodeId, MAX_OWNER_NODE_ID_BYTES);
      writeString(out, contextId, MAX_CONTEXT_ID_BYTES);
      out.flush();
      return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to encode query job ID", e);
    }
  }

  /**
   * Decodes an opaque job ID. All parse failures surface as a single generic {@link
   * IllegalArgumentException} to avoid leaking internal encoding details.
   */
  public static QueryJobId parse(String encoded) {
    try {
      if (encoded == null || encoded.isBlank() || encoded.length() > MAX_ENCODED_LENGTH) {
        throw new IllegalArgumentException("Invalid query job ID length");
      }
      byte[] bytes = Base64.getUrlDecoder().decode(encoded);
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
        int version = in.readInt();
        if (version != FORMAT_VERSION) {
          throw new IllegalArgumentException("Unsupported query job ID version [" + version + "]");
        }
        QueryJobId id =
            new QueryJobId(
                readString(in, MAX_OWNER_NODE_ID_BYTES), readString(in, MAX_CONTEXT_ID_BYTES));
        if (in.available() != 0) {
          throw new IllegalArgumentException("Unexpected trailing bytes in query job ID");
        }
        return id;
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid query job ID", e);
    }
  }

  /**
   * Writes {@code value} to {@code out} with a length-prefixed UTF-8 encoding. Rejects values whose
   * UTF-8 form exceeds {@code maxLength} bytes so a corrupt or hostile id cannot force an
   * over-large allocation.
   *
   * @param out target stream, positioned at the field's write offset
   * @param value UTF-8 payload; must fit in {@code maxLength} bytes
   * @param maxLength inclusive upper bound on the encoded byte length
   * @throws IOException if {@code out} rejects the write
   * @throws IllegalArgumentException if the encoded byte length exceeds {@code maxLength}
   */
  private static void writeString(DataOutputStream out, String value, int maxLength)
      throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > maxLength) {
      throw new IllegalArgumentException("Query job ID component is too long");
    }
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  /**
   * Reads a length-prefixed UTF-8 string written by {@link #writeString}. Rejects negative or
   * over-large declared lengths, as well as truncated payloads.
   *
   * @param in source stream, positioned at the field's length prefix
   * @param maxLength inclusive upper bound on the declared byte length
   * @return the decoded UTF-8 string
   * @throws IOException if {@code in} cannot supply the declared bytes
   * @throws IllegalArgumentException if the declared length is negative, exceeds {@code maxLength},
   *     or the payload is truncated
   */
  private static String readString(DataInputStream in, int maxLength) throws IOException {
    int length = in.readInt();
    if (length < 0 || length > maxLength) {
      throw new IllegalArgumentException("Invalid query job ID component length");
    }
    byte[] bytes = in.readNBytes(length);
    if (bytes.length != length) {
      throw new IllegalArgumentException("Truncated query job ID");
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
