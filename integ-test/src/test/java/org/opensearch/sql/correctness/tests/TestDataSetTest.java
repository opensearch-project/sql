/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.tests;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.opensearch.sql.correctness.testset.TestDataSet;

/** Tests for {@link TestDataSet} */
public class TestDataSetTest {

  @Test
  public void testDataSetWithSingleColumnData() {
    String mappings =
        "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"field\": {\n"
            + "        \"type\": \"text\"\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

    TestDataSet dataSet = new TestDataSet("test", mappings, "field\nhello\nworld\n123");
    assertEquals("test", dataSet.getTableName());
    assertEquals(mappings, dataSet.getSchema());
    assertThat(
        dataSet.getDataRows(),
        contains(
            new Object[] {"field"},
            new Object[] {"hello"},
            new Object[] {"world"},
            new Object[] {"123"}));
  }

  @Test
  public void testDataSetWithMultiColumnsData() {
    String mappings =
        "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"field1\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"field2\": {\n"
            + "        \"type\": \"integer\"\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

    TestDataSet dataSet = new TestDataSet("test", mappings, "field1,field2\nhello,123\nworld,456");
    assertThat(
        dataSet.getDataRows(),
        contains(
            new Object[] {"field1", "field2"},
            new Object[] {"hello", 123},
            new Object[] {"world", 456}));
  }

  @Test
  public void testDataSetWithEscapedComma() {
    String mappings =
        "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"field\": {\n"
            + "        \"type\": \"text\"\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

    TestDataSet dataSet =
        new TestDataSet("test", mappings, "field\n\"hello,world,123\"\n123\n\"[abc,def,ghi]\"");
    assertThat(
        dataSet.getDataRows(),
        contains(
            new Object[] {"field"},
            new Object[] {"hello,world,123"},
            new Object[] {"123"},
            new Object[] {"[abc,def,ghi]"}));
  }

  @Test
  public void testDataSetWithNullData() {
    String mappings =
        "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"field1\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"field2\": {\n"
            + "        \"type\": \"integer\"\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

    TestDataSet dataSet = new TestDataSet("test", mappings, "field1,field2\n,123\nworld,\n,");
    assertThat(
        dataSet.getDataRows(),
        contains(
            new Object[] {"field1", "field2"},
            new Object[] {null, 123},
            new Object[] {"world", null},
            new Object[] {null, null}));
  }
}
