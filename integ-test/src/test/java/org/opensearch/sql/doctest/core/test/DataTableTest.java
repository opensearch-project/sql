/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.opensearch.sql.doctest.core.response.DataTable;

/**
 * Test cases for {@link DataTable}
 */
public class DataTableTest {

  @Test
  public void testSingleColumnTable() {
    DataTable table = new DataTable(new Object[] {"Test Table"});
    table.addRow(new Object[] {"this is a very long line"});

    assertThat(
        table.toString(),
        is(
            "+------------------------+\n" +
                "|              Test Table|\n" +
                "+========================+\n" +
                "|this is a very long line|\n" +
                "+------------------------+\n"
        )
    );
  }

  @Test
  public void testTwoColumnsTable() {
    DataTable table = new DataTable(new Object[] {"Test Table", "Very Long Title"});
    table.addRow(new Object[] {"this is a very long line", "short"});

    assertThat(
        table.toString(),
        is(
            "+------------------------+---------------+\n" +
                "|              Test Table|Very Long Title|\n" +
                "+========================+===============+\n" +
                "|this is a very long line|          short|\n" +
                "+------------------------+---------------+\n"
        )
    );
  }

}
