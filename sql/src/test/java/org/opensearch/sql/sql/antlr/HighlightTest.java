package org.opensearch.sql.sql.antlr;

import org.junit.jupiter.api.Test;

public class HighlightTest  extends SQLParserTest {
  @Test
  void single_field() {
    acceptQuery("SELECT HIGHLIGHT(Tags) FROM Index WHERE MATCH(Tags, 'Time')");
  }
}
