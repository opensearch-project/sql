/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import java.util.Set;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

public class SourceFieldIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void includeTest() throws IOException {
    SearchHits response =
        query(
            String.format(
                "SELECT include('*name','*ge'),include('b*'),include('*ddre*'),include('gender')"
                    + " FROM %s LIMIT 1000",
                TEST_INDEX_ACCOUNT));
    for (SearchHit hit : response.getHits()) {
      Set<String> keySet = hit.getSourceAsMap().keySet();
      for (String field : keySet) {
        Assert.assertTrue(
            field.endsWith("name")
                || field.endsWith("ge")
                || field.startsWith("b")
                || field.contains("ddre")
                || field.equals("gender"));
      }
    }
  }

  @Test
  public void excludeTest() throws IOException {

    SearchHits response =
        query(
            String.format(
                "SELECT exclude('*name','*ge'),exclude('b*'),exclude('*ddre*'),exclude('gender')"
                    + " FROM %s LIMIT 1000",
                TEST_INDEX_ACCOUNT));

    for (SearchHit hit : response.getHits()) {
      Set<String> keySet = hit.getSourceAsMap().keySet();
      for (String field : keySet) {
        Assert.assertFalse(
            field.endsWith("name")
                || field.endsWith("ge")
                || field.startsWith("b")
                || field.contains("ddre")
                || field.equals("gender"));
      }
    }
  }

  @Test
  public void allTest() throws IOException {

    SearchHits response =
        query(
            String.format(
                "SELECT exclude('*name','*ge'),include('b*'),exclude('*ddre*'),include('gender')"
                    + " FROM %s LIMIT 1000",
                TEST_INDEX_ACCOUNT));

    for (SearchHit hit : response.getHits()) {
      Set<String> keySet = hit.getSourceAsMap().keySet();
      for (String field : keySet) {
        Assert.assertFalse(
            field.endsWith("name") || field.endsWith("ge") || field.contains("ddre"));
        Assert.assertTrue(field.startsWith("b") || field.equals("gender"));
      }
    }
  }

  private SearchHits query(String query) throws IOException {
    final JSONObject jsonObject = executeQuery(query);

    final XContentParser parser =
        new JsonXContentParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            new JsonFactory().createParser(jsonObject.toString()));
    return SearchResponse.fromXContent(parser).getHits();
  }
}
