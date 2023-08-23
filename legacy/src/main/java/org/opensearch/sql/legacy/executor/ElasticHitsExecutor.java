/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import java.io.IOException;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.legacy.exception.SqlParseException;

/** Created by Eliran on 21/8/2016. */
public interface ElasticHitsExecutor {
  void run() throws IOException, SqlParseException;

  SearchHits getHits();
}
