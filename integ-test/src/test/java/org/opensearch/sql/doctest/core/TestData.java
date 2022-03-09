/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core;

import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.getResourceFilePath;
import static org.opensearch.sql.util.TestUtils.loadDataByRestClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Test data for document generation
 */
public class TestData {

  public static final String MAPPINGS_FOLDER_ROOT = "src/test/resources/doctest/mappings/";
  public static final String TEST_DATA_FOLDER_ROOT = "src/test/resources/doctest/testdata/";

  private final String[] testFilePaths;

  public TestData(String[] testFilePaths) {
    this.testFilePaths = testFilePaths;
  }

  /**
   * Load test data in file to Elaticsearch cluster via client.
   *
   * @param test current test instance
   */
  public void loadToES(DocTest test) {
    for (String filePath : testFilePaths) {
      String indexName = indexName(filePath);
      try {
        createIndexByRestClient(test.restClient(), indexName, getIndexMapping(filePath));
        loadDataByRestClient(test.restClient(), indexName, TEST_DATA_FOLDER_ROOT + filePath);
      } catch (Exception e) {
        throw new IllegalStateException(StringUtils.format(
            "Failed to load mapping and test filePath from %s", filePath), e);
      }
      test.ensureGreen(indexName);
    }
  }

  /**
   * Use file name (without file extension) as index name implicitly.
   * For example, for 'testdata/accounts.json', 'accounts' will be used.
   */
  private String indexName(String filePath) {
    return filePath.substring(
        filePath.lastIndexOf(File.separatorChar) + 1,
        filePath.lastIndexOf('.')
    );
  }

  private String getIndexMapping(String filePath) throws IOException {
    Path path = Paths.get(getResourceFilePath(MAPPINGS_FOLDER_ROOT + filePath));
    if (Files.notExists(path)) {
      return "";
    }
    return new String(Files.readAllBytes(path));
  }

}
