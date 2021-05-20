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

import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class DataTypeIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(DATA_TYPE_NUMERIC);
    loadIndex(DATA_TYPE_NONNUMERIC);
  }

  @Test
  public void test_numeric_data_types() throws IOException {
    JSONObject result = executeQuery(
        String.format("source=%s", TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(result,
        schema("long_number", "long"),
        schema("integer_number", "integer"),
        schema("short_number", "short"),
        schema("byte_number", "byte"),
        schema("double_number", "double"),
        schema("float_number", "float"),
        schema("half_float_number", "float"),
        schema("scaled_float_number", "double"));
  }

  @Test
  public void test_nonnumeric_data_types() throws IOException {
    JSONObject result = executeQuery(
        String.format("source=%s", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result,
        schema("boolean_value", "boolean"),
        schema("keyword_value", "string"),
        schema("text_value", "string"),
        schema("binary_value", "binary"),
        schema("date_value", "timestamp"),
        schema("ip_value", "ip"),
        schema("object_value", "struct"),
        schema("nested_value", "array"));
  }

  @Test
  public void test_long_integer_data_type() throws IOException {
    JSONObject result = executeQuery(
        String.format("source=%s | eval "
                + " int1 = 2147483647,"
                + " int2 = -2147483648,"
                + " long1 = 2147483648,"
                + " long2 = -2147483649 | "
                + "fields int1, int2, long1, long2 ",
            TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(result,
        schema("int1", "integer"),
        schema("int2", "integer"),
        schema("long1", "long"),
        schema("long2", "long"));
  }

}
