/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

public class StringUtilsTest {
  @Test
  public void test_escaping_sql_wildcards() {
    assertEquals("%", StringUtils.convertSqlWildcardToLucene("\\%"));
    assertEquals("\\*", StringUtils.convertSqlWildcardToLucene("\\*"));
    assertEquals("_", StringUtils.convertSqlWildcardToLucene("\\_"));
    assertEquals("\\?", StringUtils.convertSqlWildcardToLucene("\\?"));
    assertEquals("%*", StringUtils.convertSqlWildcardToLucene("\\%%"));
    assertEquals("*%", StringUtils.convertSqlWildcardToLucene("%\\%"));
    assertEquals("%*%", StringUtils.convertSqlWildcardToLucene("\\%%\\%"));
    assertEquals("*%*", StringUtils.convertSqlWildcardToLucene("%\\%%"));
    assertEquals("_?", StringUtils.convertSqlWildcardToLucene("\\__"));
    assertEquals("?_", StringUtils.convertSqlWildcardToLucene("_\\_"));
    assertEquals("_?_", StringUtils.convertSqlWildcardToLucene("\\__\\_"));
    assertEquals("?_?", StringUtils.convertSqlWildcardToLucene("_\\__"));
    assertEquals("%\\*_\\?", StringUtils.convertSqlWildcardToLucene("\\%\\*\\_\\?"));
  }
}
