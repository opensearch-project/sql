/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

public class ScoreQueryIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  /**
   *
   *
   * <pre>
   * "query" : {
   *   "from": 0,
   *   "size": 3,
   *   "timeout": "1m",
   *   "query": {
   *     "bool": {
   *       "should": [
   *         {
   *           "match": {
   *             "address": {
   *               "query": "Lane",
   *               "operator": "OR",
   *               "prefix_length": 0,
   *               "max_expansions": 50,
   *               "fuzzy_transpositions": true,
   *               "lenient": false,
   *               "zero_terms_query": "NONE",
   *               "auto_generate_synonyms_phrase_query": true,
   *               "boost": 100.0
   *             }
   *           }
   *         },
   *         {
   *           "match": {
   *             "address": {
   *               "query": "Street",
   *               "operator": "OR",
   *               "prefix_length": 0,
   *               "max_expansions": 50,
   *               "fuzzy_transpositions": true,
   *               "lenient": false,
   *               "zero_terms_query": "NONE",
   *               "auto_generate_synonyms_phrase_query": true,
   *               "boost": 0.5
   *             }
   *           }
   *         }
   *       ],
   *       "adjust_pure_negative": true,
   *       "boost": 1.0
   *     }
   *   },
   *   "_source": {
   *     "includes": [
   *       "address"
   *     ],
   *     "excludes": []
   *   },
   *   "sort": [
   *     {
   *       "_score": {
   *         "order": "desc"
   *       }
   *     }
   *   ],
   *   "track_scores": true
   * }
   * </pre>
   *
   * @throws IOException
   */
  @Test
  public void scoreQueryExplainTest() throws IOException {
    final String result =
        explainQuery(
            String.format(
                Locale.ROOT,
                "select address from %s "
                    + "where score(matchQuery(address, 'Douglass'), 100) "
                    + "or score(matchQuery(address, 'Hall'), 0.5) order by _score desc limit 2",
                TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(
        result, containsString("\\\"match\\\":{\\\"address\\\":{\\\"query\\\":\\\"Douglass\\\""));
    Assert.assertThat(result, containsString("\\\"boost\\\":100.0"));
    Assert.assertThat(
        result, containsString("\\\"match\\\":{\\\"address\\\":{\\\"query\\\":\\\"Hall\\\""));
    Assert.assertThat(result, containsString("\\\"boost\\\":0.5"));
    Assert.assertThat(result, containsString("\\\"sort\\\":[{\\\"_score\\\""));
    Assert.assertThat(result, containsString("\\\"track_scores\\\":true"));
  }

  @Test
  public void scoreQueryTest() throws IOException {
    final JSONObject result =
        new JSONObject(
            executeQuery(
                String.format(
                    Locale.ROOT,
                    "select address, _score from %s "
                        + "where score(matchQuery(address, 'Douglass'), 100) "
                        + "or score(matchQuery(address, 'Hall'), 0.5) order by _score desc limit 2",
                    TestsConstants.TEST_INDEX_ACCOUNT),
                "jdbc"));
    verifySchema(result, schema("address", null, "text"), schema("_score", null, "float"));
    verifyDataRows(
        result, rows("154 Douglass Street", 650.1515), rows("565 Hall Street", 3.2507575));
  }

  @Test
  public void scoreQueryDefaultBoostExplainTest() throws IOException {
    final String result =
        explainQuery(
            String.format(
                Locale.ROOT,
                "select address from %s "
                    + "where score(matchQuery(address, 'Lane')) order by _score desc limit 2",
                TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(
        result, containsString("\\\"match\\\":{\\\"address\\\":{\\\"query\\\":\\\"Lane\\\""));
    Assert.assertThat(result, containsString("\\\"boost\\\":1.0"));
    Assert.assertThat(result, containsString("\\\"sort\\\":[{\\\"_score\\\""));
    Assert.assertThat(result, containsString("\\\"track_scores\\\":true"));
  }

  @Test
  public void scoreQueryDefaultBoostQueryTest() throws IOException {
    final JSONObject result =
        new JSONObject(
            executeQuery(
                String.format(
                    Locale.ROOT,
                    "select address, _score from %s "
                        + "where score(matchQuery(address, 'Powell')) order by _score desc limit 2",
                    TestsConstants.TEST_INDEX_ACCOUNT),
                "jdbc"));
    verifySchema(result, schema("address", null, "text"), schema("_score", null, "float"));
    verifyDataRows(result, rows("305 Powell Street", 6.501515));
  }
}
