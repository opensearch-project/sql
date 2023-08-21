/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.opensearch.sql.legacy.util.TestUtils;
import org.opensearch.sql.legacy.utils.JsonPrettyFormatter;

public class PrettyFormatterTest {

  @Test
  public void assertFormatterWithoutContentInside() throws IOException {
    String noContentInput = "{ }";
    String expectedOutput = "{ }";
    String result = JsonPrettyFormatter.format(noContentInput);
    assertThat(result, equalTo(expectedOutput));
  }

  @Test
  public void assertFormatterOutputsPrettyJson() throws IOException {
    String explainFormattedPrettyFilePath =
        TestUtils.getResourceFilePath(
            "/src/test/resources/expectedOutput/explain_format_pretty.json");
    String explainFormattedPretty =
        Files.toString(new File(explainFormattedPrettyFilePath), StandardCharsets.UTF_8)
            .replaceAll("\r", "");

    String explainFormattedOnelineFilePath =
        TestUtils.getResourceFilePath("/src/test/resources/explain_format_oneline.json");
    String explainFormattedOneline =
        Files.toString(new File(explainFormattedOnelineFilePath), StandardCharsets.UTF_8)
            .replaceAll("\r", "");
    String result = JsonPrettyFormatter.format(explainFormattedOneline);

    assertThat(result, equalTo(explainFormattedPretty));
  }

  @Test(expected = IOException.class)
  public void illegalInputOfNull() throws IOException {
    JsonPrettyFormatter.format("");
  }

  @Test(expected = IOException.class)
  public void illegalInputOfUnpairedBrace() throws IOException {
    JsonPrettyFormatter.format("{\"key\" : \"value\"");
  }

  @Test(expected = IOException.class)
  public void illegalInputOfWrongBraces() throws IOException {
    JsonPrettyFormatter.format("<\"key\" : \"value\">");
  }
}
