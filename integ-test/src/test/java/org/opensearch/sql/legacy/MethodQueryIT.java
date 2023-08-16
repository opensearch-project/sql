/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import java.util.Locale;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 定製方法查詢．
 *
 * @author ansj
 */
public class MethodQueryIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  /**
   * <pre>
   * query
   * "query" : {
   *   query_string" : {
   *     "query" : "address:880 Holmes Lane"
   *   }
   * }
   * </pre>
   * @throws IOException
   */
  @Test
  public void queryTest() throws IOException {
    final String result =
        explainQuery(
            String.format(
                Locale.ROOT,
                "select address from %s where query('address:880 Holmes Lane') limit 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(
        result, containsString("query_string\\\":{\\\"query\\\":\\\"address:880 Holmes Lane"));
  }

  /**
   * <pre>
   * matchQuery
   * "query" : {
   *   "match" : {
   *     "address" : {
   *       "query" : "880 Holmes Lane",
   *       "type" : "boolean"
   *     }
   *   }
   * }
   * </pre>
   * @throws IOException
   */
  @Test
  public void matchQueryTest() throws IOException {
    final String result =
        explainQuery(
            String.format(
                Locale.ROOT,
                "select address from %s where address= matchQuery('880 Holmes Lane') limit 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(
        result,
        containsString("{\\\"match\\\":{\\\"address\\\":{\\\"query\\\":\\\"880 Holmes Lane\\\""));
  }

  /**
   * <pre>
   * matchQuery
   * {
   *   "query": {
   *     "bool": {
   *       "must": {
   *         "bool": {
   *           "should": [
   *             {
   *               "constant_score": {
   *                 "query": {
   *                   "match": {
   *                     "address": {
   *                       "query": "Lane",
   *                       "type": "boolean"
   *                     }
   *                   }
   *                 },
   *                 "boost": 100
   *               }
   *             },
   *             {
   *               "constant_score": {
   *                 "query": {
   *                   "match": {
   *                     "address": {
   *                       "query": "Street",
   *                       "type": "boolean"
   *                     }
   *                   }
   *                 },
   *                 "boost": 0.5
   *               }
   *             }
   *           ]
   *         }
   *       }
   *     }
   *   }
   * }
   * </pre>
   * @throws IOException
   */
  @Test
  @Ignore(
      "score query no longer maps to constant_score in the V2 engine - @see"
          + " org.opensearch.sql.sql.ScoreQueryIT")
  public void scoreQueryTest() throws IOException {
    final String result =
        explainQuery(
            String.format(
                Locale.ROOT,
                "select address from %s "
                    + "where score(matchQuery(address, 'Lane'),100) "
                    + "or score(matchQuery(address,'Street'),0.5) order by _score desc limit 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(
        result,
        both(containsString(
                "{\"constant_score\":" + "{\"filter\":{\"match\":{\"address\":{\"query\":\"Lane\""))
            .and(
                containsString(
                    "{\"constant_score\":"
                        + "{\"filter\":{\"match\":{\"address\":{\"query\":\"Street\"")));
  }

  @Test
  public void regexpQueryTest() throws IOException {
    final String result =
        explainQuery(
            String.format(
                Locale.ROOT,
                "SELECT * FROM %s WHERE address=REGEXP_QUERY('.*')",
                TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(
        result,
        containsString(
            "{\"bool\":{\"must\":[{\"regexp\":"
                + "{\"address\":{\"value\":\".*\",\"flags_value\":255,\"max_determinized_states\":10000,\"boost\":1.0}}}"));
  }

  @Test
  public void negativeRegexpQueryTest() throws IOException {
    final String result =
        explainQuery(
            String.format(
                Locale.ROOT,
                "SELECT * FROM %s WHERE NOT(address=REGEXP_QUERY('.*'))",
                TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(
        result,
        containsString(
            "{\"bool\":{\"must_not\":[{\"regexp\":"
                + "{\"address\":{\"value\":\".*\",\"flags_value\":255,\"max_determinized_states\":10000,\"boost\":1.0}}}"));
  }

  /**
   * <pre>
   * wildcardQuery
   * l*e means leae ltae ...
   * "wildcard": {
   *   "address" : {
   *     "wildcard" : "l*e"
   *   }
   * }
   * </pre>
   * @throws IOException
   */
  @Test
  public void wildcardQueryTest() throws IOException {
    final String result =
        explainQuery(
            String.format(
                Locale.ROOT,
                "select address from %s where address= wildcardQuery('l*e')  order by _score desc"
                    + " limit 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(result, containsString("{\"wildcard\":{\"address\":{\"wildcard\":\"l*e\""));
  }

  /**
   * <pre>
   * matchPhraseQuery
   * "address" : {
   *   "query" : "671 Bristol Street",
   *   "type" : "phrase"
   * }
   * </pre>
   * @throws IOException
   */
  @Test
  @Ignore(
      "score query no longer handled by legacy engine - @see org.opensearch.sql.sql.ScoreQueryIT")
  public void matchPhraseQueryTest() throws IOException {
    final String result =
        explainQuery(
            String.format(
                Locale.ROOT,
                "select address from %s where address= matchPhrase('671 Bristol Street')  order by"
                    + " _score desc limit 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    Assert.assertThat(
        result, containsString("{\"match_phrase\":{\"address\":{\"query\":\"671 Bristol Street\""));
  }
}
