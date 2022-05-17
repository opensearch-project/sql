package org.opensearch.sql.sql.antlr;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import lombok.NoArgsConstructor;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

@NoArgsConstructor
public abstract class SQLSyntaxParserTestBase {
  private final SQLSyntaxParser parser = new SQLSyntaxParser();

  /**
   * A helper function that fails a test if the parser rejects a given query.
   * @param query Query to test.
   */
  protected void acceptQuery(String query) {
    assertNotNull(parser.parse(query));
  }

  /**
   * A helper function that fails a test if the parser accepts a given query.
   * @param query Query to test.
   */
  protected void rejectQuery(String query) {
    assertThrows(SyntaxCheckException.class, () -> parser.parse(query));
  }
}
