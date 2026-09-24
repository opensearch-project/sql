/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

/**
 * Opaque, URL-safe identifier for an asynchronous PPL job.
 *
 * <p>The encoded value contains a format version, the owner node ID used for request routing, and a
 * random per-job context ID. The complete encoded value is the key in the owner node's job
 * registry. It contains no query text, user identity, or result data, and clients must treat it as
 * opaque.
 *
 * <p>Before Base64 URL encoding, the binary layout is:
 *
 * <pre>
 * int formatVersion
 * int ownerNodeIdLength + UTF-8 ownerNodeId
 * int contextIdLength   + UTF-8 contextId
 * </pre>
 *
 * <p>The ID provides routing, not authorization. GET and DELETE still authorize the current caller
 * against the owner stored in the job.
 *
 * @param ownerNodeId node that owns the in-memory job
 * @param contextId random identifier for one job on the owner node
 */
record PPLAsyncQueryJobId(String ownerNodeId, String contextId) {
  private static final int FORMAT_VERSION = 1;
  private static final int MAX_ENCODED_LENGTH = 2_048;
  private static final int MAX_OWNER_NODE_ID_BYTES = 1_024;
  private static final int MAX_CONTEXT_ID_BYTES = 128;

  PPLAsyncQueryJobId {
    if (ownerNodeId == null || ownerNodeId.isBlank()) {
      throw new IllegalArgumentException("PPL asynchronous query owner node must not be empty");
    }
    if (contextId == null || contextId.isBlank()) {
      throw new IllegalArgumentException("PPL asynchronous query context must not be empty");
    }
  }

  /**
   * Creates a new job ID for the given owner node.
   *
   * @param ownerNodeId node that will own the job
   * @return unencoded job ID with a random context ID
   */
  static PPLAsyncQueryJobId create(String ownerNodeId) {
    return new PPLAsyncQueryJobId(ownerNodeId, UUID.randomUUID().toString());
  }

  /**
   * Serializes this ID using the versioned binary format and URL-safe Base64 without padding.
   *
   * @return opaque value returned by the asynchronous PPL API
   */
  String encode() {
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeInt(FORMAT_VERSION);
      writeString(output, ownerNodeId, MAX_OWNER_NODE_ID_BYTES);
      writeString(output, contextId, MAX_CONTEXT_ID_BYTES);
      output.flush();
      return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to encode PPL asynchronous query ID", e);
    }
  }

  /**
   * Decodes and validates an opaque job ID.
   *
   * <p>Parsing rejects empty or oversized input, unsupported versions, invalid component lengths,
   * truncated data, and trailing bytes. All malformed input is reported uniformly to avoid exposing
   * details of the internal encoding.
   *
   * @param encoded opaque value supplied by the client
   * @return decoded owner node and context IDs
   * @throws IllegalArgumentException when the value is not a valid job ID
   */
  static PPLAsyncQueryJobId parse(String encoded) {
    try {
      if (encoded == null || encoded.isBlank() || encoded.length() > MAX_ENCODED_LENGTH) {
        throw new IllegalArgumentException("Invalid PPL asynchronous query ID length");
      }
      byte[] bytes = Base64.getUrlDecoder().decode(encoded);
      try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
        int version = input.readInt();
        if (version != FORMAT_VERSION) {
          throw new IllegalArgumentException(
              "Unsupported PPL asynchronous query ID version [" + version + "]");
        }
        PPLAsyncQueryJobId id =
            new PPLAsyncQueryJobId(
                readString(input, MAX_OWNER_NODE_ID_BYTES),
                readString(input, MAX_CONTEXT_ID_BYTES));
        if (input.available() != 0) {
          throw new IllegalArgumentException(
              "Unexpected trailing bytes in PPL asynchronous query ID");
        }
        return id;
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid PPL asynchronous query ID", e);
    }
  }

  private static void writeString(DataOutputStream output, String value, int maxLength)
      throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > maxLength) {
      throw new IllegalArgumentException("PPL asynchronous query ID component is too long");
    }
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  private static String readString(DataInputStream input, int maxLength) throws IOException {
    int length = input.readInt();
    if (length < 0 || length > maxLength) {
      throw new IllegalArgumentException("Invalid PPL asynchronous query ID component length");
    }
    byte[] bytes = input.readNBytes(length);
    if (bytes.length != length) {
      throw new IllegalArgumentException("Truncated PPL asynchronous query ID");
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
