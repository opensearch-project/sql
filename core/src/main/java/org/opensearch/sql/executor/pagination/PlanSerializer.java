/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import java.io.IOException;
import java.util.Base64;
import java.util.Objects;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.serde.SerializablePlanNode;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.utils.DeserializationFilterUtil;

/** Serializes paged physical plans with a closed, versioned Jackson Smile schema. */
public class PlanSerializer {
  public static final String CURSOR_PREFIX = "n:v2:";

  private static final ObjectMapper WRITE_MAPPER = new ObjectMapper(new SmileFactory());
  private static final Base64.Encoder CURSOR_ENCODER = Base64.getUrlEncoder().withoutPadding();
  private static final Base64.Decoder CURSOR_DECODER = Base64.getUrlDecoder();

  private final StorageEngine engine;

  /** Cluster settings supplying deserialization structural limits; null falls back to defaults. */
  private final Settings settings;

  private final ObjectMapper writeMapper;
  private final PlanFlattenFunction flattener;
  private final PlanRebuildFunction rebuilder;

  public PlanSerializer(StorageEngine engine) {
    this(engine, null);
  }

  public PlanSerializer(StorageEngine engine, Settings settings) {
    this(engine, settings, WRITE_MAPPER, null, null);
  }

  PlanSerializer(
      StorageEngine engine,
      Settings settings,
      ObjectMapper writeMapper,
      PlanFlattenFunction flattener,
      PlanRebuildFunction rebuilder) {
    this.engine = engine;
    this.settings = settings;
    this.writeMapper = Objects.requireNonNull(writeMapper, "writeMapper");
    this.flattener = flattener;
    this.rebuilder = rebuilder;
  }

  /**
   * Converts a physical plan tree to a cursor. PIT ownership transfers only after the complete
   * cursor has been encoded and validated successfully.
   */
  public Cursor convertToCursor(PhysicalPlan plan) {
    try {
      FlattenResult flattened = flattener().flatten(plan, settings);
      ReadLimits limits = readLimits();
      byte[] smileBytes = writeMapper.writeValueAsBytes(flattened.node());
      checkSmileSize(smileBytes, limits);
      readMapper(limits).readValue(smileBytes, SerializablePlanNode.class);
      String encoded = CURSOR_ENCODER.encodeToString(smileBytes);
      checkBase64Size(encoded, limits);
      Cursor cursor = new Cursor(CURSOR_PREFIX + encoded);
      flattened.onCursorEncoded().run();
      return cursor;
    } catch (NoCursorException | ClassCastException e) {
      return Cursor.None;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize cursor", e);
    }
  }

  /** Converts a cursor to a physical plan tree. */
  public PhysicalPlan convertToPlan(String cursor) {
    if (cursor == null || !cursor.startsWith(CURSOR_PREFIX)) {
      throw new UnsupportedOperationException("Unsupported cursor");
    }
    try {
      ReadLimits limits = readLimits();
      String encoded = cursor.substring(CURSOR_PREFIX.length());
      checkBase64Size(encoded, limits);
      byte[] smileBytes = CURSOR_DECODER.decode(encoded);
      checkSmileSize(smileBytes, limits);
      SerializablePlanNode node =
          readMapper(limits).readValue(smileBytes, SerializablePlanNode.class);
      return rebuilder().rebuild(node, engine);
    } catch (Exception e) {
      throw new UnsupportedOperationException("Unsupported cursor", e);
    }
  }

  private static void checkBase64Size(String encoded, ReadLimits limits) {
    if (encoded.length() > limits.maxBase64Length()) {
      throw new IllegalArgumentException("Cursor exceeds the configured byte limit");
    }
  }

  private static void checkSmileSize(byte[] smileBytes, ReadLimits limits) {
    if (smileBytes.length > limits.maxBytes()) {
      throw new IllegalArgumentException("Cursor exceeds the configured byte limit");
    }
  }

  private ReadLimits readLimits() {
    int maxDepth =
        settingOrDefault(
            Settings.Key.DESERIALIZATION_MAX_DEPTH, DeserializationFilterUtil.DEFAULT_MAX_DEPTH);
    int maxRefs =
        settingOrDefault(
            Settings.Key.DESERIALIZATION_MAX_REFS, DeserializationFilterUtil.DEFAULT_MAX_REFS);
    int maxBytes =
        settingOrDefault(
            Settings.Key.DESERIALIZATION_MAX_BYTES, DeserializationFilterUtil.DEFAULT_MAX_BYTES);
    return new ReadLimits(maxDepth, maxRefs, maxBytes);
  }

  private int settingOrDefault(Settings.Key key, int defaultValue) {
    if (settings == null) {
      return defaultValue;
    }
    Integer value = settings.getSettingValue(key);
    return value == null ? defaultValue : value;
  }

  private static ObjectMapper readMapper(ReadLimits limits) {
    StreamReadConstraints constraints =
        StreamReadConstraints.builder()
            .maxNestingDepth(limits.maxDepth())
            .maxTokenCount(limits.maxRefs())
            .maxDocumentLength(limits.maxBytes())
            .maxStringLength(limits.maxBytes())
            .maxNameLength(limits.maxBytes())
            .build();
    SmileFactory factory = SmileFactory.builder().streamReadConstraints(constraints).build();
    return new ObjectMapper(factory);
  }

  private PlanFlattenFunction flattener() {
    return flattener == null ? getFlattener() : flattener;
  }

  private PlanRebuildFunction rebuilder() {
    return rebuilder == null ? getRebuilder() : rebuilder;
  }

  private record ReadLimits(int maxDepth, int maxRefs, int maxBytes) {
    private long maxBase64Length() {
      return 4L * ((maxBytes + 2L) / 3L);
    }
  }

  /** A flattened plan and the ownership transfer to commit after successful cursor encoding. */
  public record FlattenResult(SerializablePlanNode node, Runnable onCursorEncoded) {
    public FlattenResult {
      Objects.requireNonNull(node, "node");
      Objects.requireNonNull(onCursorEncoded, "onCursorEncoded");
    }
  }

  /** Flattens engine-specific physical plans into the closed cursor schema. */
  public interface PlanFlattenFunction {
    FlattenResult flatten(PhysicalPlan plan, Settings settings) throws NoCursorException;
  }

  /** Rebuilds engine-specific physical plans from the closed cursor schema. */
  public interface PlanRebuildFunction {
    PhysicalPlan rebuild(SerializablePlanNode node, StorageEngine engine);
  }

  private static volatile PlanFlattenFunction cachedFlattener;
  private static volatile PlanRebuildFunction cachedRebuilder;

  private static PlanFlattenFunction getFlattener() {
    PlanFlattenFunction local = cachedFlattener;
    if (local == null) {
      synchronized (PlanSerializer.class) {
        local = cachedFlattener;
        if (local == null) {
          cachedFlattener = local = loadService(PlanFlattenFunction.class);
        }
      }
    }
    return local;
  }

  private static PlanRebuildFunction getRebuilder() {
    PlanRebuildFunction local = cachedRebuilder;
    if (local == null) {
      synchronized (PlanSerializer.class) {
        local = cachedRebuilder;
        if (local == null) {
          cachedRebuilder = local = loadService(PlanRebuildFunction.class);
        }
      }
    }
    return local;
  }

  private static <T> T loadService(Class<T> serviceClass) {
    return java.util.ServiceLoader.load(serviceClass, PlanSerializer.class.getClassLoader())
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "No ServiceLoader provider found for " + serviceClass.getName()));
  }
}
