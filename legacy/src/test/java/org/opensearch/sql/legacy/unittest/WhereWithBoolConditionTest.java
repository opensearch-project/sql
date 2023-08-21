/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLFeatureNotSupportedException;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.client.Client;
import org.opensearch.sql.legacy.exception.SQLFeatureDisabledException;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.OpenSearchActionFactory;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.util.CheckScriptContents;
import org.opensearch.sql.legacy.util.TestsConstants;
import org.opensearch.sql.legacy.utils.StringUtils;

public class WhereWithBoolConditionTest {

  @Test
  public void whereWithBoolCompilationTest()
      throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {
    query(
        StringUtils.format("SELECT * FROM %s WHERE male = false", TestsConstants.TEST_INDEX_BANK));
  }

  @Test
  public void selectAllTest()
      throws SQLFeatureNotSupportedException,
          SqlParseException,
          IOException,
          SQLFeatureDisabledException {
    String expectedOutput =
        Files.toString(
                new File(
                    getResourcePath() + "src/test/resources/expectedOutput/select_where_true.json"),
                StandardCharsets.UTF_8)
            .replaceAll("\r", "");

    assertThat(
        removeSpaces(
            query(
                StringUtils.format(
                    "SELECT * " + "FROM %s " + "WHERE male = true",
                    TestsConstants.TEST_INDEX_BANK))),
        equalTo(removeSpaces(expectedOutput)));
  }

  private String query(String query)
      throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {
    return explain(query);
  }

  private String explain(String sql)
      throws SQLFeatureNotSupportedException, SqlParseException, SQLFeatureDisabledException {
    Client mockClient = Mockito.mock(Client.class);
    CheckScriptContents.stubMockClient(mockClient);
    QueryAction queryAction = OpenSearchActionFactory.create(mockClient, sql);
    return queryAction.explain().explain();
  }

  private String removeSpaces(String s) {
    return s.replaceAll("\\s+", "");
  }

  private String getResourcePath() {
    String projectRoot = System.getProperty("project.root");
    if (projectRoot != null && projectRoot.trim().length() > 0) {
      return projectRoot.trim() + "/";
    } else {
      return "";
    }
  }
}
