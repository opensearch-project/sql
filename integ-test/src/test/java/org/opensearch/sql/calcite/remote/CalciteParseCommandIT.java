/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.sql.ppl.ParseCommandIT;

public class CalciteParseCommandIT extends ParseCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void testParseErrorInvalidGroupNameUnderscore() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | parse email '.+@(?<host_name>.+)' | fields email", TEST_INDEX_BANK));
      fail("Should have thrown an exception for underscore in named capture group");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid capture group name 'host_name'"));
      assertTrue(
          e.getMessage().contains("must start with a letter and contain only letters and digits"));
    }
  }

  @Test
  public void testParseErrorInvalidGroupNameHyphen() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | parse email '.+@(?<host-name>.+)' | fields email", TEST_INDEX_BANK));
      fail("Should have thrown an exception for hyphen in named capture group");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid capture group name 'host-name'"));
      assertTrue(
          e.getMessage().contains("must start with a letter and contain only letters and digits"));
    }
  }

  @Test
  public void testParseErrorInvalidGroupNameStartingWithDigit() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | parse email '.+@(?<1host>.+)' | fields email", TEST_INDEX_BANK));
      fail("Should have thrown an exception for group name starting with digit");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid capture group name '1host'"));
      assertTrue(
          e.getMessage().contains("must start with a letter and contain only letters and digits"));
    }
  }

  @Test
  public void testParseErrorInvalidGroupNameSpecialCharacter() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | parse email '.+@(?<host@name>.+)' | fields email", TEST_INDEX_BANK));
      fail("Should have thrown an exception for special character in named capture group");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid capture group name 'host@name'"));
      assertTrue(
          e.getMessage().contains("must start with a letter and contain only letters and digits"));
    }
  }
}
