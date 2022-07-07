package org.opensearch.sql.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.common.utils.StringUtils.unquoteText;
import static org.opensearch.sql.common.utils.StringUtils.whichQuote;

import org.junit.jupiter.api.Test;

class StringUtilsTest {
  @Test
  void unquoteTest() {
    assertEquals("test\\'s", unquoteText("'test\\\\'s'"));
    assertEquals("test\\'s", unquoteText("'test\\\\\\'s'"));
    assertEquals("test\\\\'s", unquoteText("'test\\\\\\\\'s'"));
    assertEquals("test\\\\'s", unquoteText("'test\\\\\\\\\\'s'"));

    assertEquals("test's", unquoteText("`test\\'s`"));
    assertEquals("test\\'s", unquoteText("`test\\\\'s`"));
    assertEquals("test\\'s", unquoteText("`test\\\\\\'s`"));
    assertEquals("test\\\\'s", unquoteText("`test\\\\\\\\'s`"));

    assertEquals("test's", unquoteText("\"test\\'s\""));
    assertEquals("test\\'s", unquoteText("\"test\\\\'s\""));
    assertEquals("test\\\\'s", unquoteText("\"test\\\\\\\\'s\""));
    assertEquals("test\\\\'s", unquoteText("\"test\\\\\\\\'s\""));

    assertEquals("'test'", unquoteText("\"'test'\""));
    assertEquals("\"test\"", unquoteText("'\"test\"'"));
    assertEquals("test", unquoteText("'test'"));
    assertEquals("test", unquoteText("\"test\""));

    assertEquals("`test`", unquoteText("``test``"));
    assertEquals("//test//", unquoteText("//test//"));

    assertEquals("test's", unquoteText("'test''s'"));
    assertEquals("test''s", unquoteText("'test''''s'"));
    assertEquals("test\"s", unquoteText("\"test\"\"s\""));
    assertEquals("test\"\"s", unquoteText("\"test\"\"\"\"s\""));

    assertEquals("test''s", unquoteText("\"test''s\""));
    assertEquals("test''''s", unquoteText("\"test''''s\""));
    assertEquals("test\"\"s", unquoteText("'test\"\"s'"));
    assertEquals("test\"\"\"\"s", unquoteText("'test\"\"\"\"s'"));

    assertEquals("\"test''s", unquoteText("\"test''s"));
    assertEquals("'/\"/`test'/\"/`", unquoteText("'/\"/`test'/\"/`"));

    // Tests case of unquoted string sent to unquoteText function
    assertEquals("test", unquoteText("test"));
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
