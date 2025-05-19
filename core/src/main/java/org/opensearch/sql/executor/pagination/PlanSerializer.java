/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import com.google.common.hash.HashCode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;

/**
 * This class is entry point to paged requests. It is responsible to cursor serialization and
 * deserialization.
 */
@RequiredArgsConstructor
public class PlanSerializer {
  public static final String CURSOR_PREFIX = "n:";

  private final StorageEngine engine;

  /** Converts a physical plan tree to a cursor. */
  public Cursor convertToCursor(PhysicalPlan plan) {
    try {
      val b = ((SerializablePlan) plan).getPlanForSerialization();
      val a =  serialize(b);
      return new Cursor(
          CURSOR_PREFIX + a);
      // ClassCastException thrown when a plan in the tree doesn't implement SerializablePlan
    } catch (NotSerializableException | ClassCastException | NoCursorException e) {
      return Cursor.None;
    }
  }

  /**
   * Serializes and compresses the object.
   *
   * @param object The object.
   * @return Encoded binary data.
   */
  protected String serialize(Serializable object) throws NotSerializableException {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(object);
      objectOutput.flush();

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      // GZIP provides 35-45%, lzma from apache commons-compress has few % better compression
      GZIPOutputStream gzip =
          new GZIPOutputStream(out) {
            {
              this.def.setLevel(Deflater.BEST_COMPRESSION);
            }
          };
      gzip.write(output.toByteArray());
      gzip.close();

      return HashCode.fromBytes(out.toByteArray()).toString();
    } catch (NotSerializableException e) {
      throw e;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize: " + object, e);
    }
  }

  /**
   * Decompresses and deserializes the binary data.
   *
   * @param code Encoded binary data.
   * @return An object.
   */
  protected Serializable deserialize(String code) {
    try {
      GZIPInputStream gzip =
          new GZIPInputStream(new ByteArrayInputStream(HashCode.fromString(code).asBytes()));
      ObjectInputStream objectInput =
          new CursorDeserializationStream(new ByteArrayInputStream(gzip.readAllBytes()));
      return (Serializable) objectInput.readObject();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize object", e);
    }
  }

  /** Converts a cursor to a physical plan tree. */
  public PhysicalPlan convertToPlan(String cursor) {
    if (!cursor.startsWith(CURSOR_PREFIX)) {
      throw new UnsupportedOperationException("Unsupported cursor");
    }
    try {
      return (PhysicalPlan) deserialize(cursor.substring(CURSOR_PREFIX.length()));
    } catch (Exception e) {
      throw new UnsupportedOperationException("Unsupported cursor", e);
    }
  }

  /**
   * This function is used in testing only, to get access to {@link CursorDeserializationStream}.
   */
  public CursorDeserializationStream getCursorDeserializationStream(InputStream in)
      throws IOException {
    return new CursorDeserializationStream(in);
  }

  public class CursorDeserializationStream extends ObjectInputStream {
    public CursorDeserializationStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    public Object resolveObject(Object obj) throws IOException {
      return obj.equals("engine") ? engine : obj;
    }
  }
}
