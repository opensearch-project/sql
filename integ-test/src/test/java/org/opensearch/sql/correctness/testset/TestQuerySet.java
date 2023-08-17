/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.testset;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/** Test query set including SQL queries for comparison testing. */
public class TestQuerySet implements Iterable<String> {

  private List<String> queries;

  /**
   * Construct by a test query file.
   *
   * @param queryFileContent file content with query per line
   */
  public TestQuerySet(String queryFileContent) {
    queries = lines(queryFileContent);
  }

  /**
   * Construct by a test query array.
   *
   * @param queries query in array
   */
  public TestQuerySet(String[] queries) {
    this.queries = Arrays.asList(queries);
  }

  @Override
  public Iterator<String> iterator() {
    return queries.iterator();
  }

  private List<String> lines(String content) {
    return Arrays.asList(content.split("\\r?\\n"));
  }

  @Override
  public String toString() {
    int total = queries.size();
    return "SQL queries (first 5 in "
        + total
        + "):"
        + queries.stream().limit(5).collect(joining("\n ", "\n ", "\n"));
  }
}
