package org.opensearch.sql.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.common.utils.StringUtils.unquoteText;
import static org.opensearch.sql.common.utils.StringUtils.whichQuote;

import org.junit.jupiter.api.Test;

class StringUtilsTest {
  @Test
  void unquoteTest() {
    assertEquals("test", unquoteText("test"));
    assertEquals("test", unquoteText("'test'"));
    assertEquals("test", unquoteText("`test`"));

    assertEquals("test'", unquoteText("'test'''"));
    assertEquals("test\"", unquoteText("\"test\"\"\""));


    assertEquals("te``st", unquoteText("'te``st'"));
    assertEquals("te``st", unquoteText("\"te``st\""));
    assertEquals("te``st", unquoteText("`te``st`"));

    assertEquals("te'st", unquoteText("'te''st'"));
    assertEquals("te''st", unquoteText("\"te''st\""));
    assertEquals("te''st", unquoteText("`te''st`"));

    assertEquals("te\"\"st", unquoteText("'te\"\"st'"));
    assertEquals("te\"st", unquoteText("\"te\"\"st\""));
    assertEquals("te\"\"st", unquoteText("`te\"\"st`"));

  }

  @Test
  void whichQuoteTest() {
    assertEquals('\'', whichQuote("'hello'"));
    assertEquals('"', whichQuote("\"hello\""));
    assertEquals('`', whichQuote("`hello`"));
    assertEquals(0, whichQuote("\"hello'"));
    assertEquals(0, whichQuote("hello'"));
    assertEquals(0, whichQuote("hello"));
  }
}
