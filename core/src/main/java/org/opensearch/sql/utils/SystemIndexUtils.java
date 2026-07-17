/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

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
