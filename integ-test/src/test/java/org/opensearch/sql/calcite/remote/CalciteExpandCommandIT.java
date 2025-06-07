/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ARRAY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_SIMPLE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteExpandCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.NESTED_SIMPLE);
    loadIndex(Index.ARRAY);
    enableCalcite();
    disallowCalciteFallback();
  }

  @Test
  public void testExpandOnNested() throws Exception {
    JSONObject response =
        executeQuery(String.format("source=%s | expand address", TEST_INDEX_NESTED_SIMPLE));
    verifySchema(
        response,
        schema("name", "string"),
        schema("age", "bigint"),
        schema("id", "bigint"),
        schema("address", "struct"));
    verifyNumOfRows(response, 11);
    verifyDataRows(
        response,
        rows(
            "abbas",
            null,
            24,
            new JSONObject()
                .put("city", "New york city")
                .put("state", "NY")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1984-04-12 09:07:42"))),
        rows(
            "abbas",
            null,
            24,
            new JSONObject()
                .put("city", "bellevue")
                .put("state", "WA")
                .put(
                    "moveInDate",
                    new JSONArray()
                        .put(new JSONObject().put("dateAndTime", "2023-05-03 08:07:42"))
                        .put(new JSONObject().put("dateAndTime", "2001-11-11 04:07:44")))),
        rows(
            "abbas",
            null,
            24,
            new JSONObject()
                .put("city", "seattle")
                .put("state", "WA")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1966-03-19 03:04:55"))),
        rows(
            "abbas",
            null,
            24,
            new JSONObject()
                .put("city", "chicago")
                .put("state", "IL")
                .put("moveInDate", new JSONObject().put("dateAndTime", "2011-06-01 01:01:42"))),
        rows(
            "chen",
            null,
            32,
            new JSONObject()
                .put("city", "Miami")
                .put("state", "Florida")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1901-08-11 04:03:33"))),
        rows(
            "chen",
            null,
            32,
            new JSONObject()
                .put("city", "los angeles")
                .put("state", "CA")
                .put("moveInDate", new JSONObject().put("dateAndTime", "2023-05-03 08:07:42"))),
        rows(
            "peng",
            null,
            26,
            new JSONObject()
                .put("city", "san diego")
                .put("state", "CA")
                .put("moveInDate", new JSONObject().put("dateAndTime", "2001-11-11 04:07:44"))),
        rows(
            "peng",
            null,
            26,
            new JSONObject()
                .put("city", "austin")
                .put("state", "TX")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1977-07-13 09:04:41"))),
        rows(
            "andy",
            4,
            19,
            new JSONObject()
                .put("city", "houston")
                .put("state", "TX")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1933-12-12 05:05:45"))),
        rows(
            "david",
            null,
            25,
            new JSONObject()
                .put("city", "raleigh")
                .put("state", "NC")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1909-06-17 01:04:21"))),
        rows(
            "david",
            null,
            25,
            new JSONObject()
                .put("city", "charlotte")
                .put("state", "SC")
                .put(
                    "moveInDate",
                    new JSONArray()
                        .put(new JSONObject().put("dateAndTime", "2001-11-11 04:07:44")))));
  }

  // To consider in future releases: will expand on array (instead of nested) be supported.
  //  In Opensearch, a string field can store either a single string or an array of strings.
  //  This makes it difficult to implement expand on arries.
  @Ignore
  @Test
  public void testExpandOnArray() throws Exception {
    JSONObject response =
        executeQuery(String.format("source=%s | expand strings", TEST_INDEX_ARRAY));
    verifySchema(response, schema("numbers", "array"), schema("strings", "string"));
    verifyNumOfRows(response, 5);
  }

  @Test
  public void testExpandWithAlias() throws Exception {
    JSONObject response =
        executeQuery(String.format("source=%s | expand address as addr", TEST_INDEX_NESTED_SIMPLE));
    verifySchema(
        response,
        schema("name", "string"),
        schema("age", "bigint"),
        schema("id", "bigint"),
        schema("addr", "struct"));
    verifyDataRows(
        response,
        rows(
            "abbas",
            null,
            24,
            new JSONObject()
                .put("city", "New york city")
                .put("state", "NY")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1984-04-12 09:07:42"))),
        rows(
            "abbas",
            null,
            24,
            new JSONObject()
                .put("city", "bellevue")
                .put("state", "WA")
                .put(
                    "moveInDate",
                    new JSONArray()
                        .put(new JSONObject().put("dateAndTime", "2023-05-03 08:07:42"))
                        .put(new JSONObject().put("dateAndTime", "2001-11-11 04:07:44")))),
        rows(
            "abbas",
            null,
            24,
            new JSONObject()
                .put("city", "seattle")
                .put("state", "WA")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1966-03-19 03:04:55"))),
        rows(
            "abbas",
            null,
            24,
            new JSONObject()
                .put("city", "chicago")
                .put("state", "IL")
                .put("moveInDate", new JSONObject().put("dateAndTime", "2011-06-01 01:01:42"))),
        rows(
            "chen",
            null,
            32,
            new JSONObject()
                .put("city", "Miami")
                .put("state", "Florida")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1901-08-11 04:03:33"))),
        rows(
            "chen",
            null,
            32,
            new JSONObject()
                .put("city", "los angeles")
                .put("state", "CA")
                .put("moveInDate", new JSONObject().put("dateAndTime", "2023-05-03 08:07:42"))),
        rows(
            "peng",
            null,
            26,
            new JSONObject()
                .put("city", "san diego")
                .put("state", "CA")
                .put("moveInDate", new JSONObject().put("dateAndTime", "2001-11-11 04:07:44"))),
        rows(
            "peng",
            null,
            26,
            new JSONObject()
                .put("city", "austin")
                .put("state", "TX")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1977-07-13 09:04:41"))),
        rows(
            "andy",
            4,
            19,
            new JSONObject()
                .put("city", "houston")
                .put("state", "TX")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1933-12-12 05:05:45"))),
        rows(
            "david",
            null,
            25,
            new JSONObject()
                .put("city", "raleigh")
                .put("state", "NC")
                .put("moveInDate", new JSONObject().put("dateAndTime", "1909-06-17 01:04:21"))),
        rows(
            "david",
            null,
            25,
            new JSONObject()
                .put("city", "charlotte")
                .put("state", "SC")
                .put(
                    "moveInDate",
                    new JSONArray()
                        .put(new JSONObject().put("dateAndTime", "2001-11-11 04:07:44")))));
  }

  @Test
  public void testExpandWithEval() throws Exception {
    JSONObject response =
        executeQuery(
            String.format("source=%s | eval addr=address | expand addr", TEST_INDEX_NESTED_SIMPLE));
    verifySchema(
        response,
        schema("name", "string"),
        schema("age", "bigint"),
        schema("address", "array"),
        schema("id", "bigint"),
        schema("addr", "struct"));
    verifyNumOfRows(response, 11);
  }
}
