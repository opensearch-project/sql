/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_CSV_SANITIZE;
import static org.opensearch.sql.util.Capability.RESPONSE_FORMAT;
import static org.opensearch.sql.util.TestUtils.assertRowsEqual;

import java.io.IOException;
import java.util.Locale;
import org.junit.Test;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.util.RequiresCapability;

public class CsvFormatIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK_CSV_SANITIZE);
  }

  @Test
  @RequiresCapability(RESPONSE_FORMAT)
  public void sanitizeTest() throws IOException {
    String result =
        executeCsvQuery(
            String.format(
                Locale.ROOT,
                "source=%s | fields firstname, lastname",
                TEST_INDEX_BANK_CSV_SANITIZE));
    assertRowsEqual(
        StringUtils.format(
            "firstname,lastname%n"
                + "'+Amber JOHnny,Duke Willmington+%n"
                + "'-Hattie,Bond-%n"
                + "'=Nanette,Bates=%n"
                + "'@Dale,Adams@%n"
                + "\",Elinor\",\"Ratliff,,,\"%n"),
        result);
  }

  @Test
  @RequiresCapability(RESPONSE_FORMAT)
  public void escapeSanitizeTest() throws IOException {
    String result =
        executeCsvQuery(
            String.format(
                Locale.ROOT,
                "source=%s | fields firstname, lastname",
                TEST_INDEX_BANK_CSV_SANITIZE),
            false);
    assertRowsEqual(
        StringUtils.format(
            "firstname,lastname%n"
                + "+Amber JOHnny,Duke Willmington+%n"
                + "-Hattie,Bond-%n"
                + "=Nanette,Bates=%n"
                + "@Dale,Adams@%n"
                + "\",Elinor\",\"Ratliff,,,\"%n"),
        result);
  }
}
