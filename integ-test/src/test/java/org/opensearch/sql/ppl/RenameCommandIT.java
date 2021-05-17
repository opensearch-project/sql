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
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.columnPattern;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

public class RenameCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testRenameOneField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | rename firstname as first_name",
                TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("first_name"));
  }

  @Test
  public void testRenameMultiField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname, age | rename firstname as FIRSTNAME, age as AGE",
                TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("FIRSTNAME"), columnName("AGE"));
  }

  @Ignore("Wildcard is unsupported yet")
  @Test
  public void testRenameWildcardFields() throws IOException {
    JSONObject result = executeQuery("source=" + TEST_INDEX_ACCOUNT + " | rename %name as %NAME");
    verifyColumn(result, columnPattern(".*name$"));
  }
}
