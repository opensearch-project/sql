package org.opensearch.sql.sql;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

public class HighlightFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BEER);
  }

  @Test
  public void defaultParameters() {
    String query = "SELECT Tags, highlight('Tags') from %s WHERE match(Tags, 'yeast') limit 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    assertEquals(1, response.getInt("total"));
  }
}
