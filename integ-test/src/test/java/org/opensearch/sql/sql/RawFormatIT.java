/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_RAW_SANITIZE;

import java.io.IOException;
import java.util.Locale;
import org.junit.Test;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class RawFormatIT extends SQLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK_RAW_SANITIZE);
  }

  @Test
  public void rawFormatWithPipeFieldTest() {
    String result = executeQuery(
        String.format(Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_RAW_SANITIZE), "raw");
    assertEquals(StringUtils.format(
        "firstname|lastname%n"
            + "+Amber JOHnny|Duke Willmington+%n"
            + "-Hattie|Bond-%n"
            + "=Nanette|Bates=%n"
            + "@Dale|Adams@%n"
            + "@Elinor|\"Ratliff|||\"%n"),
        result);
  }

}
