/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.antlr;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class MatchBoolPrefixParserTest extends SQLParserTest {

  @Test
  public void testDefaultArguments() {
    acceptQuery("SELECT * FROM T WHERE MATCH_BOOL_PREFIX(message, 'query')");
  }

  static Stream<String> generateValidArguments() {
    return Stream.of(
        new String("minimum_should_match=3"),
        new String("fuzziness=AUTO"),
        new String("prefix_length=0"),
        new String("max_expansions=50"),
        new String("fuzzy_transpositions=true"),
        new String("fuzzy_rewrite=constant_score"),
        new String("boost=1")
    );
  }

  @ParameterizedTest
  @MethodSource("generateValidArguments")
  public void testValidArguments(String arg) {
    acceptQuery("SELECT * FROM T WHERE MATCH_BOOL_PREFIX(message, 'query', " + arg  + ")");
  }

  @Test
  public void testOneParameter() {
    rejectQuery("SELECT * FROM T WHERE MATCH_BOOL_PREFIX(message)");
  }
}
