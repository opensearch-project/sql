package org.opensearch.sql.sql.antlr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.opensearch.sql.common.antlr.SyntaxParserTestBase;

@EnabledIfEnvironmentVariable(named = "INCLUDE_WIP", matches = ".*")
public class MatchBoolPrefixParserTest extends SQLParserTest {

  @Test
  public void testDefaultParameters() {
    acceptQuery("SELECT * FROM T WHERE MATCH_BOOL_PREFIX(message, 'query')");
  }

  @Test public void testOneParameter() {
    rejectQuery("SELECT * FROM T WHERE MATCH_BOOL_PREFIX(message)");
  }
}
