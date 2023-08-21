/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.executor.join;

import java.io.IOException;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.legacy.executor.join.ElasticUtils;
import org.opensearch.sql.legacy.executor.join.MetaSearchResult;

@RunWith(MockitoJUnitRunner.class)
public class ElasticUtilsTest {

  @Mock MetaSearchResult metaSearchResult;

  /** test handling {@link TotalHits} correctly. */
  @Test
  public void hitsAsStringResult() throws IOException {
    final SearchHits searchHits =
        new SearchHits(new SearchHit[] {}, new TotalHits(1, Relation.EQUAL_TO), 0);
    final String result = ElasticUtils.hitsAsStringResult(searchHits, metaSearchResult);

    Assert.assertEquals(1, new JSONObject(result).query("/hits/total/value"));
    Assert.assertEquals(
        Relation.EQUAL_TO.toString(), new JSONObject(result).query("/hits/total/relation"));
  }

  /** test handling {@link TotalHits} with null value correctly. */
  @Test
  public void test_hitsAsStringResult_withNullTotalHits() throws IOException {
    final SearchHits searchHits = new SearchHits(new SearchHit[] {}, null, 0);
    final String result = ElasticUtils.hitsAsStringResult(searchHits, metaSearchResult);

    Assert.assertEquals(0, new JSONObject(result).query("/hits/total/value"));
    Assert.assertEquals(
        Relation.EQUAL_TO.toString(), new JSONObject(result).query("/hits/total/relation"));
  }
}
