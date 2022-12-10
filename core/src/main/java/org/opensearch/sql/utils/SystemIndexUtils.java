/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.utils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;

/**
 * System Index Utils.
 * Todo. Find the better name for this class.
 */
@UtilityClass
public class SystemIndexUtils {

  public static final String TABLE_NAME_FOR_TABLES_INFO = "tables";
  /**
   * The suffix of all the system tables.
   */
  private static final String SYS_TABLES_SUFFIX = "ODFE_SYS_TABLE";

  /**
   * The suffix of all the meta tables.
   */
  private static final String SYS_META_SUFFIX = "META_" + SYS_TABLES_SUFFIX;

  /**
   * The suffix of all the table mappings.
   */
  private static final String SYS_MAPPINGS_SUFFIX =  "MAPPINGS_" + SYS_TABLES_SUFFIX;

  /**
   * The ALL.META_ODFE_SYS_TABLE contain all the table info.
   */
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
    return String.join(".", indexName, SYS_MAPPINGS_SUFFIX);
  }

  /**
   * Build the {@link SystemTable}.
   *
   * @return {@link SystemTable}
   */
  public static SystemTable systemTable(String indexName) {
    final int lastDot = indexName.lastIndexOf(".");
    String suffix = indexName.substring(lastDot + 1);
    String tableName = indexName.substring(0, lastDot)
        .replace("%", "*");

    if (suffix.equalsIgnoreCase(SYS_META_SUFFIX)) {
      return new SystemInfoTable(tableName);
    } else if (suffix.equalsIgnoreCase(SYS_MAPPINGS_SUFFIX)) {
      return new MetaInfoTable(tableName);
    } else {
      throw new IllegalStateException("Invalid system index name: " + indexName);
    }
  }

  /**
   * System Table.
   */
  public interface SystemTable {

    String getTableName();

    default boolean isSystemInfoTable() {
      return false;
    }

    default boolean isMetaInfoTable() {
      return false;
    }
  }

  /**
   * System Info Table.
   */
  @Getter
  @RequiredArgsConstructor
  public static class SystemInfoTable implements SystemTable {

    private final String tableName;

    public boolean isSystemInfoTable() {
      return true;
    }
  }

  /**
   * System Table.
   */
  @Getter
  @RequiredArgsConstructor
  public static class MetaInfoTable implements SystemTable {

    private final String tableName;

    public boolean isMetaInfoTable() {
      return true;
    }
  }
}
