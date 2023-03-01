/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_CSV_SANITIZE;

import java.io.IOException;
import java.util.Locale;
import org.junit.Test;

public class CsvFormatIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK_CSV_SANITIZE);
  }

  @Test
  public void sanitizeTest() throws IOException {
    String result = executeCsvQuery(
        String.format(Locale.ROOT, "source=%s | fields firstname, lastname", TEST_INDEX_BANK_CSV_SANITIZE));
    assertEquals(
        "firstname,lastname" + System.lineSeparator()
            + "'+Amber JOHnny,Duke Willmington+"+ System.lineSeparator()
            + "'-Hattie,Bond-"+System.lineSeparator()
            + "'=Nanette,Bates="+System.lineSeparator()
            + "'@Dale,Adams@"+System.lineSeparator()
            + "\",Elinor\",\"Ratliff,,,\""+System.lineSeparator(),
        result);
  }

  @Test
  public void escapeSanitizeTest() throws IOException {
    String result = executeCsvQuery(
        String.format(Locale.ROOT, "source=%s | fields firstname, lastname", TEST_INDEX_BANK_CSV_SANITIZE), false);
    assertEquals(
        "firstname,lastname" + System.lineSeparator()
            + "+Amber JOHnny,Duke Willmington+" + System.lineSeparator()
            + "-Hattie,Bond-" + System.lineSeparator()
            + "=Nanette,Bates="+ System.lineSeparator()
            + "@Dale,Adams@" + System.lineSeparator()
            + "\",Elinor\",\"Ratliff,,,\"" + System.lineSeparator(),
        result);
  }
}
