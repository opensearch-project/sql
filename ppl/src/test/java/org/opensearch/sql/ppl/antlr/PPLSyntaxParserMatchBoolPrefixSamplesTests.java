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
public class PPLSyntaxParserMatchBoolPrefixSamplesTests {

  /**
   * Returns sample queries that the PPLSyntaxParser is expected to parse successfully.
   *
   * @return an Iterable of sample queries.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object> sampleQueries() {
    return List.of(
        "source=t a= 1 | where match_bool_prefix(a, 'hello world')",
        "source=t a = 1 | where match_bool_prefix(a, 'hello world'," + " minimum_should_match = 3)",
        "source=t a = 1 | where match_bool_prefix(a, 'hello world', fuzziness='AUTO')",
        "source=t a = 1 | where match_bool_prefix(a, 'hello world', fuzziness='AUTO:4,6')",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world', prefix_length=0)",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world', max_expansions=1)",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world'," + " fuzzy_transpositions=true)",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world',"
            + " fuzzy_rewrite=constant_score)",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world',"
            + " fuzzy_rewrite=constant_score_boolean)",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world',"
            + " fuzzy_rewrite=scoring_boolean)",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world',"
            + " fuzzy_rewrite=top_terms_blended_freqs_1)",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world',"
            + " fuzzy_rewrite=top_terms_boost_1)",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world'," + " fuzzy_rewrite=top_terms_1)",
        "source=t a= 1 | where match_bool_prefix(a, 'hello world', boost=1)",
        "source=t a = 1 | where match_bool_prefix(a, 'hello world', analyzer = 'standard',"
            + "prefix_length = '0', boost = 1)");
  }

  private final String query;

  public PPLSyntaxParserMatchBoolPrefixSamplesTests(String query) {
    this.query = query;
  }

  @Test
  public void test() {
    ParseTree tree = new PPLSyntaxParser().parse(query);
    assertNotEquals(null, tree);
  }
}
