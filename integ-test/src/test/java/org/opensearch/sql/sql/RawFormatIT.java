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
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_RAW_SANITIZE;

import java.io.IOException;
import java.util.Locale;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class RawFormatIT extends SQLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK_RAW_SANITIZE);
  }

  @Test
  public void rawFormatWithPipeFieldTest() {
    String result = executeQuery(
        String.format(Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_RAW_SANITIZE), "raw");
    assertEquals(
        "firstname|lastname\n"
            + "+Amber JOHnny|Duke Willmington+\n"
            + "-Hattie|Bond-\n"
            + "=Nanette|Bates=\n"
            + "@Dale|Adams@\n"
            + "@Elinor|\"Ratliff|||\"\n",
        result);
  }

}
