/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import org.hamcrest.Matcher;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class EscapeCharacterIT extends SQLIntegTestCase {

  private static final String SELECT_FROM_WHERE_GROUP_BY =
          "SELECT * " +
                  "FROM " + TestsConstants.TEST_INDEX_ACCOUNT + ";";


  private static final String SELECT_FROM_WHERE_GROUP_BY_2 =
          "SELECT state, COUNT(*) cnt " +
                  "FROM " + TestsConstants.TEST_INDEX_ACCOUNT + " " +
                  "WHERE age = 30 " +
                  "GROUP BY state ";

//  private static final Set<Matcher<Object[]>> states1 = rowSet(1, Arrays.asList(
//          "AK", "AR", "CT", "DE", "HI", "IA", "IL", "IN", "LA", "MA", "MD", "MN",
//          "MO", "MT", "NC", "ND", "NE", "NH", "NJ", "NV", "SD", "VT", "WV", "WY"
//  ));
//  private static final Set<Matcher<Object[]>> states2 =
//          rowSet(2, Arrays.asList("AZ", "DC", "KS", "ME"));
//  private static final Set<Matcher<Object[]>> states3 =
//          rowSet(3, Arrays.asList("AL", "ID", "KY", "OR", "TN"));
//
//  @Override
//  protected void init() throws Exception {
//    loadIndex(Index.ACCOUNT);
//  }
//
//  @Test
//  public void equalsTo() throws IOException {
//
//    var temp = query(SELECT_FROM_WHERE_GROUP_BY);
//
//  }
//
//  @Test
//  public void lessThanOrEqual() throws IOException {
//    assertThat(
//            query(SELECT_FROM_WHERE_GROUP_BY_2),
//            resultSet(
//                    states1,
//                    states2
//            )
//    );
//  }

  public void escapeCharacterIT() throws IOException {
    JSONObject response = new JSONObject(
            executeQuery("SELECT 'I\\\'m';", "jdbc"));
  }



  private Set<Object[]> query(String query) throws IOException {
    JSONObject response = executeQuery(query);
    return getResult(response, "state.keyword", "cnt");
  }

  private Set<Object[]> getResult(JSONObject response, String aggName, String aggFunc) {

    String bucketsPath = String.format(Locale.ROOT, "/aggregations/%s/buckets", aggName);
    JSONArray buckets = (JSONArray) response.query(bucketsPath);

    Set<Object[]> result = new HashSet<>();
    for (int i = 0; i < buckets.length(); i++) {
      JSONObject bucket = buckets.getJSONObject(i);
      result.add(new Object[] {
          bucket.get("key"),
          ((JSONObject) bucket.get(aggFunc)).getLong("value")
      });
    }

    return result;
  }

  @SafeVarargs
  private final Matcher<Iterable<? extends Object[]>> resultSet(Set<Matcher<Object[]>>... rowSets) {
    return containsInAnyOrder(Arrays.stream(rowSets)
        .flatMap(Collection::stream)
        .collect(Collectors.toList()));
  }

  private static Set<Matcher<Object[]>> rowSet(long count, List<String> states) {
    return states.stream()
        .map(state -> row(state, count))
        .collect(Collectors.toSet());
  }

  private static Matcher<Object[]> row(String state, long count) {
    return arrayContaining(is(state), is(count));
  }
}
