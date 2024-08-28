/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.opensearch.sql.legacy.utils.StringUtils.unquoteFullColumn;
import static org.opensearch.sql.legacy.utils.StringUtils.unquoteSingleField;

import org.junit.Test;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * To test the functionality of {@link StringUtils#unquoteSingleField} and {@link
 * StringUtils#unquoteFullColumn(String, String)}
 */
public class BackticksUnquoterTest {

  @Test
  public void assertNotQuotedStringShouldKeepTheSame() {
    assertThat(unquoteSingleField("identifier"), equalTo("identifier"));
    assertThat(unquoteFullColumn("identifier"), equalTo("identifier"));
    assertThat(unquoteFullColumn(".identifier"), equalTo(".identifier"));
  }

  @Test
  public void assertStringWithOneBackTickShouldKeepTheSame() {
    assertThat(unquoteSingleField("`identifier"), equalTo("`identifier"));
    assertThat(unquoteFullColumn("`identifier"), equalTo("`identifier"));
    assertThat(unquoteFullColumn("`.identifier"), equalTo("`.identifier"));
  }

  @Test
  public void assertBackticksQuotedStringShouldBeUnquoted() {
    assertThat("identifier", equalTo(unquoteSingleField("`identifier`")));
    assertThat(".identifier", equalTo(unquoteSingleField("`.identifier`")));

    assertThat(
        "identifier1.identifier2", equalTo(unquoteFullColumn("`identifier1`.`identifier2`")));
    assertThat("identifier1.identifier2", equalTo(unquoteFullColumn("`identifier1`.identifier2")));
    assertThat(
        ".identifier1.identifier2", equalTo(unquoteFullColumn("`.identifier1`.`identifier2`")));
  }
}
