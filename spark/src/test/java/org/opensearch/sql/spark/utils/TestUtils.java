/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.util.Objects;
import lombok.SneakyThrows;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentType;

public class TestUtils {

  /**
   * Get Json document from the files in resources folder.
   *
   * @param filename filename.
   * @return String.
   * @throws IOException IOException.
   */
  public static String getJson(String filename) throws IOException {
    ClassLoader classLoader = TestUtils.class.getClassLoader();
    return new String(
        Objects.requireNonNull(classLoader.getResourceAsStream(filename)).readAllBytes());
  }

  @SneakyThrows
  public static String loadMappings(String path) {
    URL url = Resources.getResource(path);
    return Resources.toString(url, Charsets.UTF_8);
  }

  public static void createIndexWithMappings(
      Client client, String indexName, String metadataFileLocation) {
    CreateIndexRequest request = new CreateIndexRequest(indexName);
    request.mapping(loadMappings(metadataFileLocation), XContentType.JSON);
    client.admin().indices().create(request).actionGet();
  }
}
