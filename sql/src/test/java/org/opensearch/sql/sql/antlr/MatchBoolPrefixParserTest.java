package org.opensearch.sql.sql.antlr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class MatchBoolPrefixParserTest extends SQLParserTest {

  @Test
  public void testDefaultParameters() {
    acceptQuery("SELECT * FROM T WHERE MATCH_BOOL_PREFIX(message, 'query')");
  }

  static Stream<String> generateAvailableParameters() {
    return Stream.of(
        new String("minimum_should_match=3"),
        new String("fuzziness=AUTO"),
        new String("prefix_length=0"),
        new String("max_expansions=50"),
        new String("fuzzy_transpositions=true"),
        new String("fuzzy_rewrite=constant_score")
    );
  }

  @ParameterizedTest
  @MethodSource("generateAvailableParameters")
  public void testAvailableParameters(String arg) {
    acceptQuery("SELECT * FROM T WHERE MATCH_BOOL_PREFIX(message, 'query', " + arg  + ")");
  }

  @Test
  public void testOneParameter() {
    rejectQuery("SELECT * FROM T WHERE MATCH_BOOL_PREFIX(message)");
  }
}
