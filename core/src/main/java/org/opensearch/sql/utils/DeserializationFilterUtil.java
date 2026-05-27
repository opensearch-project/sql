/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.io.ObjectInputFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
   * Creates a logging filter that wraps the provided filter and logs rejected classes.
   *
   * @param filter The underlying filter to wrap.
   * @return A filter that logs rejections.
   */
  public static ObjectInputFilter createLoggingFilter(ObjectInputFilter filter) {
    return info -> {
      ObjectInputFilter.Status status = filter.checkInput(info);
      if (status == ObjectInputFilter.Status.REJECTED && info.serialClass() != null) {
        LOG.warn("Deserialization filter rejected class: {}", info.serialClass().getName());
      }
      return status;
    };
  }

  /**
   * Creates a filter with the base allowlist plus additional patterns.
   *
   * @param additionalPatterns Additional patterns to append to the base allowlist.
   * @return A logging filter with the combined allowlist.
   */
  public static ObjectInputFilter createFilter(String additionalPatterns) {
    String fullPattern = BASE_ALLOWLIST + additionalPatterns + "!*";
    return createLoggingFilter(ObjectInputFilter.Config.createFilter(fullPattern));
  }
}
