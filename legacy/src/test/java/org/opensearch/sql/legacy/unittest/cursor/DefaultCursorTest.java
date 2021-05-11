/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.unittest.cursor;

import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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
