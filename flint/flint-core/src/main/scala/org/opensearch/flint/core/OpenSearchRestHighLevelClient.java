/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.storage.FlintReader;
import org.opensearch.flint.core.storage.OpenSearchScrollReader;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;

import static org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

public class OpenSearchRestHighLevelClient implements FlintClient {

  /**
   * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link QueryBuilder} from DSL query string.
   */
  private final static NamedXContentRegistry
      xContentRegistry =
      new NamedXContentRegistry(new SearchModule(Settings.builder().build(), new ArrayList<>()).getNamedXContents());

  /**
   * Todo. Migrate to https://opensearch.org/docs/latest/clients/java/ in future.
   * The reason we use {@link RestHighLevelClient} is https://github.com/opensearch-project/opensearch-java/issues/257
   */
  private final RestHighLevelClient client;

  public OpenSearchRestHighLevelClient(RestHighLevelClient client) {
    this.client = client;
  }

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query DSL query. DSL query is null means match_all.
   * @return {@link FlintReader}.
   */
  @Override public FlintReader createReader(String indexName, String query, FlintOptions options) {
    try {
      QueryBuilder queryBuilder = new MatchAllQueryBuilder();
      if (!Strings.isNullOrEmpty(query)) {
        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, IGNORE_DEPRECATIONS, query);
        queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
      }
      return new OpenSearchScrollReader(client, indexName, new SearchSourceBuilder().query(queryBuilder), options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
