/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import org.junit.Test;

/** SQL integration test automated by comparison test framework. */
public class SQLCorrectnessIT extends CorrectnessTestBase {

  private static final String ROOT_DIR = "correctness/";
  private static final String[] EXPR_TEST_DIR = {"expressions"};
  private static final String[] QUERY_TEST_DIR = {"queries", "bugfixes"};

  @Override
  protected void init() throws Exception {
    super.init();
  }

  @Test
  public void runAllTests() throws Exception {
    verifyQueries(EXPR_TEST_DIR, expr -> "SELECT " + expr);
    verifyQueries(QUERY_TEST_DIR, Function.identity());
  }

  /**
   * Verify queries in files in directories with a converter to preprocess query. For example, for
   * expressions it is converted to a SELECT clause before testing.
   */
  @SuppressWarnings("UnstableApiUsage")
  private void verifyQueries(String[] dirs, Function<String, String> converter) throws Exception {
    for (String dir : dirs) {
      Path dirPath = Paths.get(Resources.getResource(ROOT_DIR + dir).toURI());
      Files.walk(dirPath)
          .filter(Files::isRegularFile)
          .forEach(file -> verifyQueries(file, converter));
    }
  }

  /** Comment start with # */
  private void verifyQueries(Path file, Function<String, String> converter) {
    try {
      String[] queries =
          Files.lines(file)
              .filter(line -> !line.startsWith("#"))
              .map(converter)
              .toArray(String[]::new);
      verify(queries);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read file: " + file, e);
    }
  }
}
