/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.lang.LangSpec;

/** System Index Utils. Todo. Find the better name for this class. */
@UtilityClass
public class SystemIndexUtils {

  public static final String TABLE_NAME_FOR_TABLES_INFO = "tables";

  /** The suffix of all the system tables. */
  private static final String SYS_TABLES_SUFFIX = "ODFE_SYS_TABLE";

  /** The suffix of all the meta tables. */
  private static final String SYS_META_SUFFIX = "META_" + SYS_TABLES_SUFFIX;

  /** The suffix of all the table mappings. */
  private static final String SYS_MAPPINGS_SUFFIX = "MAPPINGS_" + SYS_TABLES_SUFFIX;

  /** The ALL.META_ODFE_SYS_TABLE contain all the table info. */
  public static final String TABLE_INFO = "ALL." + SYS_META_SUFFIX;

  public static final String DATASOURCES_TABLE_NAME = ".DATASOURCES";

  public static Boolean isSystemIndex(String indexName) {
    return indexName.endsWith(SYS_TABLES_SUFFIX);
  }

  /**
   * Reserved suffix marking a {@code rest} source table. Distinct from {@link #SYS_TABLES_SUFFIX}
   * so {@link #isSystemIndex} and {@link #isRestSource} never overlap. The whole reserved name is a
   * single Calcite identifier (REST + lowercase hex + this suffix), so it survives name resolution
   * the same way the uppercase system-mapping names do.
   */
  private static final String REST_SOURCE_SUFFIX = "__REST_SOURCE";

  private static final String REST_SOURCE_PREFIX = "REST";

  /** True if the resolved table name is a {@code rest} source token. */
  public static boolean isRestSource(String indexName) {
    return indexName.endsWith(REST_SOURCE_SUFFIX);
  }

  /**
   * Encode a validated {@link RestSpec} into a single reserved table name. Mirrors {@link
   * #mappingTable}: structured metadata travels inside a reserved name rather than a side channel.
   * The endpoint/args have already been allow-list-validated before this is called.
   */
  public static String restTable(RestSpec spec) {
    StringBuilder sb = new StringBuilder();
    sb.append("endpoint=").append(spec.getEndpoint());
    if (spec.getCount() != null) {
      sb.append('\n').append("count=").append(spec.getCount());
    }
    if (spec.getTimeout() != null) {
      sb.append('\n').append("timeout=").append(spec.getTimeout());
    }
    if (spec.getArgs() != null) {
      for (Map.Entry<String, String> e : spec.getArgs().entrySet()) {
        sb.append('\n').append("arg.").append(e.getKey()).append('=').append(e.getValue());
      }
    }
    return REST_SOURCE_PREFIX + toHex(sb.toString()) + REST_SOURCE_SUFFIX;
  }

  /** Decode a reserved {@code rest} table name back into its {@link RestSpec}. */
  public static RestSpec decodeRestSpec(String indexName) {
    String body =
        indexName.substring(
            REST_SOURCE_PREFIX.length(), indexName.length() - REST_SOURCE_SUFFIX.length());
    String decoded = fromHex(body);
    String endpoint = null;
    Integer count = null;
    String timeout = null;
    LinkedHashMap<String, String> args = new LinkedHashMap<>();
    for (String line : decoded.split("\n")) {
      if (line.isEmpty()) {
        continue;
      }
      int eq = line.indexOf('=');
      if (eq < 0) {
        continue;
      }
      String k = line.substring(0, eq);
      String v = line.substring(eq + 1);
      if (k.equals("endpoint")) {
        endpoint = v;
      } else if (k.equals("count")) {
        count = Integer.parseInt(v);
      } else if (k.equals("timeout")) {
        timeout = v;
      } else if (k.startsWith("arg.")) {
        args.put(k.substring("arg.".length()), v);
      }
    }
    if (endpoint == null) {
      throw new IllegalArgumentException("rest source token is missing the endpoint: " + indexName);
    }
    return new RestSpec(endpoint, args, count, timeout);
  }

  private static String toHex(String s) {
    StringBuilder h = new StringBuilder();
    for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
      h.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return h.toString();
  }

  private static String fromHex(String h) {
    byte[] bytes = new byte[h.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] =
          (byte)
              ((Character.digit(h.charAt(2 * i), 16) << 4)
                  + Character.digit(h.charAt(2 * i + 1), 16));
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * The validated spec for a {@code rest} command: an allow-listed read-only endpoint plus optional
   * count/timeout/query args. Lives in core so the parser (encode) and the storage engine (decode)
   * share it without a cross-module dependency.
   */
  @Getter
  @RequiredArgsConstructor
  public static class RestSpec {
    private final String endpoint;
    private final Map<String, String> args;
    private final Integer count;
    private final String timeout;
  }

  /**
   * Compose system mapping table.
   *
   * @return system mapping table.
   */
  public static String mappingTable(String indexName) {
    return mappingTable(indexName, LangSpec.SQL_SPEC);
  }

  public static String mappingTable(String indexName, LangSpec langSpec) {

    return String.join(".", indexName, encodeLangSpec(langSpec));
  }

  /**
   * Encodes the language specification into a system mappings suffix.
   *
   * <p>The returned suffix is composed of the language name (e.g., "SQL" or "PPL") concatenated
   * with an underscore and the system mappings suffix constant. For example:
   * "SQL_MAPPINGS_ODFE_SYS_TABLE".
   *
   * @param spec the language specification.
   * @return the encoded system mappings suffix.
   */
  public static String encodeLangSpec(LangSpec spec) {
    return spec.language().name() + "_" + SYS_MAPPINGS_SUFFIX;
  }

  /**
   * Extracts the language specification from a given system mappings suffix.
   *
   * <p>This method expects the suffix to start with the language name followed by an underscore.
   * For example, given "SQL_MAPPINGS_ODFE_SYS_TABLE", it extracts "SQL" and returns the
   * corresponding language specification via {@link LangSpec#fromLanguage(String)}. If the expected
   * format is not met, the default SQL specification is returned.
   *
   * @param systemMappingsSuffix the system mappings suffix.
   * @return the language specification extracted from the suffix, or {@link LangSpec#SQL_SPEC} if
   *     the format is invalid.
   */
  public static LangSpec extractLangSpec(String systemMappingsSuffix) {
    int underscoreIndex = systemMappingsSuffix.indexOf('_');
    if (underscoreIndex <= 0) {
      return LangSpec.SQL_SPEC;
    }
    String langName = systemMappingsSuffix.substring(0, underscoreIndex);
    return LangSpec.fromLanguage(langName);
  }

  /**
   * Build the {@link SystemTable}.
   *
   * @return {@link SystemTable}
   */
  public static SystemTable systemTable(String indexName) {
    final int lastDot = indexName.lastIndexOf(".");
    String suffix = indexName.substring(lastDot + 1);
    String tableName = indexName.substring(0, lastDot).replace("%", "*");

    if (suffix.endsWith(SYS_META_SUFFIX)) {
      return new SystemInfoTable(tableName);
    } else if (suffix.endsWith(SYS_MAPPINGS_SUFFIX)) {
      return new MetaInfoTable(tableName, extractLangSpec(suffix));
    } else {
      throw new IllegalStateException("Invalid system index name: " + indexName);
    }
  }

  /** System Table. */
  public interface SystemTable {

    String getTableName();

    default LangSpec getLangSpec() {
      return LangSpec.SQL_SPEC;
    }

    default boolean isSystemInfoTable() {
      return false;
    }

    default boolean isMetaInfoTable() {
      return false;
    }
  }

  /** System Info Table. */
  @Getter
  @RequiredArgsConstructor
  public static class SystemInfoTable implements SystemTable {

    private final String tableName;

    public boolean isSystemInfoTable() {
      return true;
    }
  }

  /** System Table. */
  @Getter
  @RequiredArgsConstructor
  public static class MetaInfoTable implements SystemTable {

    private final String tableName;
    private final LangSpec langSpec;

    public boolean isMetaInfoTable() {
      return true;
    }
  }
}
