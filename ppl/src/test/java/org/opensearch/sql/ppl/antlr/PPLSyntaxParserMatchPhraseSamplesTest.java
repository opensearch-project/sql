/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.antlr;

import static org.junit.Assert.assertNotEquals;

import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PPLSyntaxParserMatchPhraseSamplesTest {

  /**
   * Returns sample queries that the PPLSyntaxParser is expected to parse successfully.
   *
   * @return an Iterable of sample queries.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object> sampleQueries() {
    return List.of(
        "source=t a= 1 | where match_phrase(a, 'hello world')",
        "source=t a = 1 | where match_phrase(a, 'hello world', slop = 3)",
        "source=t a = 1 | where match_phrase(a, 'hello world', analyzer = 'standard',"
            + "zero_terms_query = 'none', slop = 3)",
        "source=t a = 1 | where match_phrase(a, 'hello world', zero_terms_query = all)");
  }

  private final String query;

  public PPLSyntaxParserMatchPhraseSamplesTest(String query) {
    this.query = query;
  }

  @Test
  public void test() {
    ParseTree tree = new PPLSyntaxParser().parse(query);
    assertNotEquals(null, tree);
  }
}
