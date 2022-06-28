package org.opensearch.sql.common.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.common.utils.StringUtils.unquoteText;

class StringUtilsTest {
    @Test
    void unquoteTest() {
        assertEquals("test\\'s", unquoteText("'test\\\\'s'"));
        assertEquals("test\\'s", unquoteText("'test\\\\\\'s'"));
        assertEquals("test\\\\'s", unquoteText("'test\\\\\\\\'s'"));
        assertEquals("test\\\\'s", unquoteText("'test\\\\\\\\\\'s'"));

        assertEquals("test\\'s", unquoteText("`test\\'s`"));
        assertEquals("test\\'s", unquoteText("`test\\\\'s`"));
        assertEquals("test\\\\'s", unquoteText("`test\\\\\\'s`"));
        assertEquals("test\\\\'s", unquoteText("`test\\\\\\\\'s`"));

        assertEquals("test\\'s", unquoteText("\"test\\'s\""));
        assertEquals("test\\'s", unquoteText("\"test\\\\'s\""));
        assertEquals("test\\\\'s", unquoteText("\"test\\\\\\'s\""));
        assertEquals("test\\\\'s", unquoteText("\"test\\\\\\\\'s\""));
    }
}