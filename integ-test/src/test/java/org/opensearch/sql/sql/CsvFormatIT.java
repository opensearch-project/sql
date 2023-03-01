/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_CSV_SANITIZE;

import java.io.IOException;
import java.util.Locale;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class CsvFormatIT extends SQLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK_CSV_SANITIZE);
  }

  @Test
  public void sanitizeTest() {
    String result = executeQuery(
        String.format(Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_CSV_SANITIZE), "csv");
    assertEquals(
        "firstname,lastname" + System.lineSeparator()
            + "'+Amber JOHnny,Duke Willmington+" + System.lineSeparator()
            + "'-Hattie,Bond-" + System.lineSeparator()
            + "'=Nanette,Bates=" + System.lineSeparator()
            + "'@Dale,Adams@" + System.lineSeparator()
            + "\",Elinor\",\"Ratliff,,,\"" + System.lineSeparator(),
        result);
  }

  @Test
  public void escapeSanitizeTest() {
    String result = executeQuery(
        String.format(Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_CSV_SANITIZE),
        "csv&sanitize=false");
    assertEquals(
        "firstname,lastname" + System.lineSeparator()
            + "+Amber JOHnny,Duke Willmington+" + System.lineSeparator()
            + "-Hattie,Bond-" + System.lineSeparator()
            + "=Nanette,Bates=" + System.lineSeparator()
            + "@Dale,Adams@" + System.lineSeparator()
            + "\",Elinor\",\"Ratliff,,,\"" + System.lineSeparator(),
        result);
  }
}
