/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

package org.opensearch.sql.legacy.unittest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.junit.Test;
import org.opensearch.sql.legacy.executor.Format;

public class FormatTest {

    @Test
    public void ofJdbcShouldReturnJDBCFormat() {
        Optional<Format> format = Format.of(Format.JDBC.getFormatName());
        assertTrue(format.isPresent());
        assertEquals(Format.JDBC, format.get());
    }

    @Test
    public void ofUnknownFormatShouldReturnEmpty() {
        assertFalse(Format.of("xml").isPresent());
    }
}
