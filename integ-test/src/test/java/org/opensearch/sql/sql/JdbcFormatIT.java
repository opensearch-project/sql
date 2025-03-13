/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class JdbcFormatIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BANK);
  }

  @Test
  public void testSimpleDataTypesInSchema() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                "SELECT account_number, address, age, birthdate, city, male, state "
                    + "FROM "
                    + TEST_INDEX_BANK,
                "jdbc"));

    verifySchema(
        response,
        schema("account_number", "long"),
        schema("address", "text"),
        schema("age", "integer"),
        schema("birthdate", "timestamp"),
        schema("city", "keyword"),
        schema("male", "boolean"),
        schema("state", "text"));
  }

  @Test
  public void testAliasInSchema() {
    JSONObject response =
        new JSONObject(
            executeQuery("SELECT account_number AS acc FROM " + TEST_INDEX_BANK, "jdbc"));

    verifySchema(response, schema("account_number", "acc", "long"));
  }
}
