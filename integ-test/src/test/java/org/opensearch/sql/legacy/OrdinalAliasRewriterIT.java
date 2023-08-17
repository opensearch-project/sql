/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.legacy.utils.StringUtils;

public class OrdinalAliasRewriterIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  // tests query results with jdbc output
  @Test
  public void simpleGroupByOrdinal() {
    String expected =
        executeQuery(
            StringUtils.format(
                "SELECT lastname FROM %s AS b GROUP BY lastname LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    String actual =
        executeQuery(
            StringUtils.format(
                "SELECT lastname FROM %s AS b GROUP BY 1 LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void multipleGroupByOrdinal() {
    String expected =
        executeQuery(
            StringUtils.format(
                "SELECT lastname, firstname, age FROM %s AS b GROUP BY firstname, age, lastname"
                    + " LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    String actual =
        executeQuery(
            StringUtils.format(
                "SELECT lastname, firstname, age FROM %s AS b GROUP BY 2, 3, 1 LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void selectFieldiWithBacticksGroupByOrdinal() {
    String expected =
        executeQuery(
            StringUtils.format(
                "SELECT `lastname` FROM %s AS b GROUP BY `lastname` LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    String actual =
        executeQuery(
            StringUtils.format(
                "SELECT `lastname` FROM %s AS b GROUP BY 1 LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void selectFieldiWithBacticksAndTableAliasGroupByOrdinal() {
    String expected =
        executeQuery(
            StringUtils.format(
                "SELECT `b`.`lastname`, `age`, firstname FROM %s AS b GROUP BY `age`,"
                    + " `b`.`lastname` , firstname LIMIT 10",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    String actual =
        executeQuery(
            StringUtils.format(
                "SELECT `b`.`lastname`, `age`, firstname  FROM %s AS b GROUP BY 2, 1, 3 LIMIT 10",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void simpleOrderByOrdinal() {
    String expected =
        executeQuery(
            StringUtils.format(
                "SELECT lastname FROM %s AS b ORDER BY lastname LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    String actual =
        executeQuery(
            StringUtils.format(
                "SELECT lastname FROM %s AS b ORDER BY 1 LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void multipleOrderByOrdinal() {
    String expected =
        executeQuery(
            StringUtils.format(
                "SELECT lastname, firstname, age FROM %s AS b ORDER BY firstname, age, lastname"
                    + " LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    String actual =
        executeQuery(
            StringUtils.format(
                "SELECT lastname, firstname, age FROM %s AS b ORDER BY 2, 3, 1 LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void selectFieldiWithBacticksOrderByOrdinal() {
    String expected =
        executeQuery(
            StringUtils.format(
                "SELECT `lastname` FROM %s AS b ORDER BY `lastname` LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    String actual =
        executeQuery(
            StringUtils.format(
                "SELECT `lastname` FROM %s AS b ORDER BY 1 LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void selectFieldiWithBacticksAndTableAliasOrderByOrdinal() {
    String expected =
        executeQuery(
            StringUtils.format(
                "SELECT `b`.`lastname` FROM %s AS b ORDER BY `b`.`lastname` LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    String actual =
        executeQuery(
            StringUtils.format(
                "SELECT `b`.`lastname` FROM %s AS b ORDER BY 1 LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    assertThat(actual, equalTo(expected));
  }

  // ORDER BY IS NULL/NOT NULL
  @Test
  public void selectFieldiWithBacticksAndTableAliasOrderByOrdinalAndNull() {
    String expected =
        executeQuery(
            StringUtils.format(
                "SELECT `b`.`lastname`, age FROM %s AS b ORDER BY `b`.`lastname` IS NOT NULL DESC,"
                    + " age is NULL LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    String actual =
        executeQuery(
            StringUtils.format(
                "SELECT `b`.`lastname`, age FROM %s AS b ORDER BY 1 IS NOT NULL DESC, 2 IS NULL"
                    + " LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT),
            "jdbc");
    assertThat(actual, equalTo(expected));
  }

  // explain
  @Test
  public void explainSelectFieldiWithBacticksAndTableAliasGroupByOrdinal() throws IOException {
    String expected =
        explainQuery(
            StringUtils.format(
                "SELECT `b`.`lastname` FROM %s AS b GROUP BY `b`.`lastname` LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    String actual =
        explainQuery(
            StringUtils.format(
                "SELECT `b`.`lastname` FROM %s AS b GROUP BY 1 LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void explainSelectFieldiWithBacticksAndTableAliasOrderByOrdinal() throws IOException {
    String expected =
        explainQuery(
            StringUtils.format(
                "SELECT `b`.`lastname` FROM %s AS b ORDER BY `b`.`lastname` LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    String actual =
        explainQuery(
            StringUtils.format(
                "SELECT `b`.`lastname` FROM %s AS b ORDER BY 1 LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    assertThat(actual, equalTo(expected));
  }

  // explain ORDER BY IS NULL/NOT NULL
  @Ignore("only work for legacy engine")
  public void explainSelectFieldiWithBacticksAndTableAliasOrderByOrdinalAndNull()
      throws IOException {
    String expected =
        explainQuery(
            StringUtils.format(
                "SELECT `b`.`lastname`, age FROM %s AS b ORDER BY `b`.`lastname` IS NOT NULL DESC,"
                    + " age is NULL LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    String actual =
        explainQuery(
            StringUtils.format(
                "SELECT `b`.`lastname`, age FROM %s AS b ORDER BY 1 IS NOT NULL DESC, 2 IS NULL"
                    + " LIMIT 3",
                TestsConstants.TEST_INDEX_ACCOUNT));
    assertThat(actual, equalTo(expected));
  }
}
