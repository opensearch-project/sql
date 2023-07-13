/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import java.io.IOException;
import java.util.Objects;

public class TestUtils {

  /**
   * Get Json document from the files in resources folder.
   * @param filename filename.
   * @return String.
   * @throws IOException IOException.
   */
  public static String getJson(String filename) throws IOException {
    ClassLoader classLoader = TestUtils.class.getClassLoader();
    return new String(
        Objects.requireNonNull(classLoader.getResourceAsStream(filename)).readAllBytes());
  }

}

