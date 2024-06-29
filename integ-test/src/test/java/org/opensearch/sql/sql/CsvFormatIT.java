/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_CSV_SANITIZE;
import static org.opensearch.sql.protocol.response.format.CsvResponseFormatter.CONTENT_TYPE;

import java.io.IOException;
import java.util.Locale;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class CsvFormatIT extends SQLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK_CSV_SANITIZE);
  }

  @Test
  public void sanitizeTest() {
    String result =
        executeQuery(
            String.format(
                Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_CSV_SANITIZE),
            "csv");
    assertEquals(
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
  public void escapeSanitizeTest() {
    String result =
        executeQuery(
            String.format(
                Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_CSV_SANITIZE),
            "csv&sanitize=false");
    assertEquals(
        StringUtils.format(
            "firstname,lastname%n"
                + "+Amber JOHnny,Duke Willmington+%n"
                + "-Hattie,Bond-%n"
                + "=Nanette,Bates=%n"
                + "@Dale,Adams@%n"
                + "\",Elinor\",\"Ratliff,,,\"%n"),
        result);
  }

  @Test
  public void contentHeaderTest() throws IOException {
    String query =
        String.format(
            Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_CSV_SANITIZE);
    String requestBody = makeRequest(query);

    Request sqlRequest = new Request("POST", "/_plugins/_sql?format=csv");
    sqlRequest.setJsonEntity(requestBody);

    Response response = client().performRequest(sqlRequest);

    assertEquals(response.getEntity().getContentType(), CONTENT_TYPE);
  }
}
