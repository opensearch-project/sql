/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.io.ObjectInputFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.setting.Settings;

/** Utility class for creating deserialization filters with logging. */
public class DeserializationFilterUtil {
  private static final Logger LOG = LogManager.getLogger(DeserializationFilterUtil.class);

  /** Base allowlist shared across all serializers. */
  private static final String BASE_ALLOWLIST =
      "org.opensearch.sql.expression.**;"
          + "org.opensearch.sql.data.**;"
          + "org.opensearch.sql.executor.QueryType;"
          + "org.opensearch.sql.opensearch.data.type.*;"
          + "java.lang.Object;"
          + "java.lang.String;"
          + "java.lang.Number;"
          + "java.lang.Integer;"
          + "java.lang.Long;"
          + "java.lang.Double;"
          + "java.lang.Float;"
          + "java.lang.Short;"
          + "java.lang.Byte;"
          + "java.lang.Boolean;"
          + "java.lang.Character;"
          + "java.lang.Enum;"
          + "java.util.ArrayList;"
          + "java.util.Arrays$ArrayList;"
          + "java.util.LinkedHashMap;"
          + "java.util.HashMap;"
          + "java.util.Collections$*;"
          + "java.util.ImmutableCollections$*;"
          + "java.util.CollSer;"
          + "java.util.Map$Entry;"
          + "java.io.Serializable;"
          + "java.lang.invoke.SerializedLambda;"
          + "java.math.BigDecimal;"
          + "java.math.BigInteger;"
          + "java.time.**;"
          + "com.google.common.collect.**;";

  /**
   * Default structural limits on the deserialized object graph, used when a setting is unset or no
   * {@link Settings} is available (serialize-only call sites and tests).
   *
   * <p>Together with the class allowlist, these limits bound the resources a single payload can
   * consume. They are sized to admit legitimate pushed-down expressions with headroom, and no
   * looser than limits OpenSearch already enforces:
   *
   * <ul>
   *   <li>Depth: each level of expression nesting costs 3 to 4 levels of serialization depth on top
   *       of about 13 for the enclosing CASE/WHEN and lambda wrappers; IN-lists become a balanced
   *       OR tree, so they add depth only logarithmically. The deepest legitimate expression
   *       measured (20 nested AND/OR levels) reaches 72. 150 leaves 2x headroom over that and stays
   *       far below the depth of about 1200 at which cold deserialization overflows a 1 MB thread
   *       stack.
   *   <li>References and bytes: pushed-down scripts are capped by {@code script.max_size_in_bytes}
   *       (65535 by default) before they are deserialized, so a script decodes to at most about 49
   *       KB and, for legitimate expressions, about 7200 references. 10000 references and 65536
   *       bytes therefore never reject a script that OpenSearch accepted, while still bounding the
   *       cursor path, which that setting does not cover.
   * </ul>
   */
  public static final int DEFAULT_MAX_DEPTH = 150;

  public static final int DEFAULT_MAX_REFS = 10000;
  public static final int DEFAULT_MAX_BYTES = 65536;

  /**
   * Creates a logging filter that wraps the provided filter and logs rejected classes.
   *
   * @param filter The underlying filter to wrap.
   * @return A filter that logs rejections.
   */
  public static ObjectInputFilter createLoggingFilter(ObjectInputFilter filter) {
    return info -> {
      ObjectInputFilter.Status status = filter.checkInput(info);
      if (status == ObjectInputFilter.Status.REJECTED) {
        if (info.serialClass() != null) {
          LOG.warn("Deserialization filter rejected class: {}", info.serialClass().getName());
        } else {
          LOG.warn(
              "Deserialization filter rejected: depth={}, refs={}, bytes={}",
              info.depth(),
              info.references(),
              info.streamBytes());
        }
      }
      return status;
    };
  }

  /**
   * Creates a filter with the base allowlist, the built-in default structural limits, and
   * additional patterns. Used by serialize-only call sites and tests that have no {@link Settings}.
   *
   * @param additionalPatterns Additional patterns to append to the base allowlist.
   * @return A logging filter with the combined allowlist and default structural limits.
   */
  public static ObjectInputFilter createFilter(String additionalPatterns) {
    return createFilter(DEFAULT_MAX_DEPTH, DEFAULT_MAX_REFS, DEFAULT_MAX_BYTES, additionalPatterns);
  }

  /**
   * Creates a filter with the base allowlist, the structural limits from the {@code
   * plugins.query.deserialization.*} cluster settings, and additional patterns.
   *
   * @param settings cluster settings supplying the structural limits (must be non-null)
   * @param additionalPatterns Additional patterns to append to the base allowlist.
   * @return A logging filter with the combined allowlist and configured structural limits.
   */
  public static ObjectInputFilter createFilter(Settings settings, String additionalPatterns) {
    return createFilter(
        settings.getSettingValue(Settings.Key.DESERIALIZATION_MAX_DEPTH),
        settings.getSettingValue(Settings.Key.DESERIALIZATION_MAX_REFS),
        settings.getSettingValue(Settings.Key.DESERIALIZATION_MAX_BYTES),
        additionalPatterns);
  }

  private static ObjectInputFilter createFilter(
      int maxDepth, int maxRefs, int maxBytes, String additionalPatterns) {
    String structuralLimits =
        String.format("maxdepth=%d;maxrefs=%d;maxbytes=%d;", maxDepth, maxRefs, maxBytes);
    String fullPattern = BASE_ALLOWLIST + additionalPatterns + structuralLimits + "!*";
    return createLoggingFilter(ObjectInputFilter.Config.createFilter(fullPattern));
  }
}
