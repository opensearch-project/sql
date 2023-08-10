/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.unittest.cursor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import org.junit.Test;
import org.opensearch.sql.legacy.cursor.CursorType;
import org.opensearch.sql.legacy.cursor.DefaultCursor;

public class DefaultCursorTest {

    @Test
    public void checkCursorType() {
        DefaultCursor cursor = new DefaultCursor();
        assertEquals(cursor.getType(), CursorType.DEFAULT);
    }


    @Test
    public void cursorShouldStartWithCursorTypeID() {
        DefaultCursor cursor = new DefaultCursor();
        cursor.setRowsLeft(50);
        cursor.setScrollId("dbdskbcdjksbcjkdsbcjk+//");
        cursor.setIndexPattern("myIndex");
        cursor.setFetchSize(500);
        cursor.setFieldAliasMap(Collections.emptyMap());
        cursor.setColumns(new ArrayList<>());
        assertThat(cursor.generateCursorId(), startsWith(cursor.getType().getId()+ ":") );
    }

    @Test
    public void nullCursorWhenRowLeftIsLessThanEqualZero() {
        DefaultCursor cursor = new DefaultCursor();
        assertThat(cursor.generateCursorId(), emptyOrNullString());

        cursor.setRowsLeft(-10);
        assertThat(cursor.generateCursorId(), emptyOrNullString());
    }

    @Test
    public void nullCursorWhenScrollIDIsNullOrEmpty() {
        DefaultCursor cursor = new DefaultCursor();
        assertThat(cursor.generateCursorId(), emptyOrNullString());

        cursor.setScrollId("");
        assertThat(cursor.generateCursorId(), emptyOrNullString());
    }
}
