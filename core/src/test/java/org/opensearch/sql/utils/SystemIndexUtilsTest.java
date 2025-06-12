/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;
import static org.opensearch.sql.utils.SystemIndexUtils.systemTable;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.lang.LangSpec;
import org.opensearch.sql.lang.PPLLangSpec;

class SystemIndexUtilsTest {

  @Test
  void test_system_index() {
    assertTrue(isSystemIndex("ALL.META_ODFE_SYS_TABLE"));
    assertFalse(isSystemIndex(".opensearch_dashboards"));
  }

  @Test
  void test_compose_mapping_table() {
    assertEquals("employee.SQL_MAPPINGS_ODFE_SYS_TABLE", mappingTable("employee"));
  }

  @Test
  void test_system_info_table() {
    final SystemIndexUtils.SystemTable table = systemTable("ALL.META_ODFE_SYS_TABLE");

    assertTrue(table.isSystemInfoTable());
    assertFalse(table.isMetaInfoTable());
    assertEquals("ALL", table.getTableName());
  }

  @Test
  void test_mapping_info_table() {
    final SystemIndexUtils.SystemTable table = systemTable("employee.MAPPINGS_ODFE_SYS_TABLE");

    assertTrue(table.isMetaInfoTable());
    assertFalse(table.isSystemInfoTable());
    assertEquals("employee", table.getTableName());
  }

  @Test
  void test_mapping_info_table_with_special_index_name() {
    final SystemIndexUtils.SystemTable table =
        systemTable("logs-2021.01.11.MAPPINGS_ODFE_SYS_TABLE");
    assertTrue(table.isMetaInfoTable());
    assertFalse(table.isSystemInfoTable());
    assertEquals("logs-2021.01.11", table.getTableName());
  }

  @Test
  void throw_exception_for_invalid_index() {
    final IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> systemTable("employee._ODFE_SYS_TABLE"));
    assertEquals("Invalid system index name: employee._ODFE_SYS_TABLE", exception.getMessage());
  }

  /** Tests that encoding the SQL specification produces the expected suffix. */
  @Test
  public void testEncodeLangSpecSQL() {
    String expected = "SQL_MAPPINGS_ODFE_SYS_TABLE";
    String encoded = SystemIndexUtils.encodeLangSpec(LangSpec.SQL_SPEC);
    assertEquals(expected, encoded, "Encoded SQL lang spec should match expected suffix.");
  }

  /** Tests that encoding the PPL specification produces the expected suffix. */
  @Test
  public void testEncodeLangSpecPPL() {
    String expected = "PPL_MAPPINGS_ODFE_SYS_TABLE";
    String encoded = SystemIndexUtils.encodeLangSpec(PPLLangSpec.PPL_SPEC);
    assertEquals(expected, encoded, "Encoded PPL lang spec should match expected suffix.");
  }

  /** Tests that extracting the language specification from a valid SQL suffix returns SQL_SPEC. */
  @Test
  public void testExtractLangSpecValidSQL() {
    LangSpec spec = SystemIndexUtils.extractLangSpec("SQL_MAPPINGS_ODFE_SYS_TABLE");
    assertEquals(LangSpec.SQL_SPEC, spec, "Extracting from SQL suffix should return SQL_SPEC.");
  }

  /** Tests that extracting the language specification from a valid PPL suffix returns PPL_SPEC. */
  @Test
  public void testExtractLangSpecValidPPL() {
    LangSpec spec = SystemIndexUtils.extractLangSpec("PPL_MAPPINGS_ODFE_SYS_TABLE");
    assertEquals(PPLLangSpec.PPL_SPEC, spec, "Extracting from PPL suffix should return PPL_SPEC.");
  }

  /** Tests that an improperly formatted suffix defaults to SQL_SPEC. */
  @Test
  public void testExtractLangSpecInvalidFormat() {
    assertEquals(
        LangSpec.SQL_SPEC,
        SystemIndexUtils.extractLangSpec("FORMAT"),
        "Invalid format should default to SQL_SPEC.");
  }

  @Test
  public void testGetLangSpec() {
    assertEquals(
        LangSpec.SQL_SPEC,
        new SystemIndexUtils.SystemInfoTable("employee.MAPPINGS_ODFE_SYS_TABLE").getLangSpec());
  }
}
