package org.opensearch.sql.common.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.common.utils.StringUtils.unescapeBackslashes;
import static org.opensearch.sql.common.utils.StringUtils.unquoteText;
import static org.hamcrest.MatcherAssert.assertThat;

class StringUtilsTest {

//    @Test
//    void unquoteTest() {
//
//    }
//
//    @Test
//    void unquoteTextTest() {
//    //    unquoteText("Hello world\\\\\\\'s");
//    }

    @Test
    void unescapeBackslashesTest() {
        assertEquals("test\\'s", unescapeBackslashes("test\\\\'s"));
        assertEquals("test\\'s", unescapeBackslashes("test\\\\\\'s"));
        assertEquals("test\\\\'s", unescapeBackslashes("test\\\\\\\\'s"));
        assertEquals("test\\\\'s", unescapeBackslashes("test\\\\\\\\\\'s"));
    }
}