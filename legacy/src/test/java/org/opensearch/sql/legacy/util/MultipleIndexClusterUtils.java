/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.util;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.legacy.util.CheckScriptContents.createParser;
import static org.opensearch.sql.legacy.util.CheckScriptContents.mockIndexNameExpressionResolver;
import static org.opensearch.sql.legacy.util.CheckScriptContents.mockPluginSettings;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;

/** Test Utility which provide the cluster have 2 indices. */
public class MultipleIndexClusterUtils {
  public static final String INDEX_ACCOUNT_1 = "account1";
  public static final String INDEX_ACCOUNT_2 = "account2";
  public static final String INDEX_ACCOUNT_ALL = "account*";

  public static String INDEX_ACCOUNT_1_MAPPING =
      "{\n"
          + "  \"field_mappings\": {\n"
          + "    \"mappings\": {\n"
          + "      \"account1\": {\n"
          + "        \"properties\": {\n"
          + "          \"id\": {\n"
          + "            \"type\": \"long\"\n"
          + "          },\n"
          + "          \"address\": {\n"
          + "            \"type\": \"text\",\n"
          + "            \"fields\": {\n"
          + "              \"keyword\": {\n"
          + "                \"type\": \"keyword\"\n"
          + "              }\n"
          + "            },\n"
          + "            \"fielddata\": true\n"
          + "          },\n"
          + "          \"age\": {\n"
          + "            \"type\": \"integer\"\n"
          + "          },\n"
          + "          \"projects\": {\n"
          + "            \"type\": \"nested\",\n"
          + "            \"properties\": {\n"
          + "              \"name\": {\n"
          + "                \"type\": \"text\",\n"
          + "                \"fields\": {\n"
          + "                  \"keyword\": {\n"
          + "                    \"type\": \"keyword\"\n"
          + "                  }\n"
          + "                },\n"
          + "                \"fielddata\": true\n"
          + "              },\n"
          + "              \"started_year\": {\n"
          + "                \"type\": \"int\"\n"
          + "              }\n"
          + "            }\n"
          + "          }\n"
          + "        }\n"
          + "      }\n"
          + "    },\n"
          + "    \"settings\": {\n"
          + "      \"index\": {\n"
          + "        \"number_of_shards\": 1,\n"
          + "        \"number_of_replicas\": 0,\n"
          + "        \"version\": {\n"
          + "          \"created\": \"6050399\"\n"
          + "        }\n"
          + "      }\n"
          + "    },\n"
          + "    \"mapping_version\": \"1\",\n"
          + "    \"settings_version\": \"1\",\n"
          + "    \"aliases_version\": \"1\"\n"
          + "  }\n"
          + "}";

  /**
   * The difference with account1. 1. missing address. 2. age has different type. 3.
   * projects.started_year has different type.
   */
  public static String INDEX_ACCOUNT_2_MAPPING =
      "{\n"
          + "  \"field_mappings\": {\n"
          + "    \"mappings\": {\n"
          + "      \"account2\": {\n"
          + "        \"properties\": {\n"
          + "          \"id\": {\n"
          + "            \"type\": \"long\"\n"
          + "          },\n"
          + "          \"age\": {\n"
          + "            \"type\": \"long\"\n"
          + "          },\n"
          + "          \"projects\": {\n"
          + "            \"type\": \"nested\",\n"
          + "            \"properties\": {\n"
          + "              \"name\": {\n"
          + "                \"type\": \"text\",\n"
          + "                \"fields\": {\n"
          + "                  \"keyword\": {\n"
          + "                    \"type\": \"keyword\"\n"
          + "                  }\n"
          + "                },\n"
          + "                \"fielddata\": true\n"
          + "              },\n"
          + "              \"started_year\": {\n"
          + "                \"type\": \"long\"\n"
          + "              }\n"
          + "            }\n"
          + "          }\n"
          + "        }\n"
          + "      }\n"
          + "    },\n"
          + "    \"settings\": {\n"
          + "      \"index\": {\n"
          + "        \"number_of_shards\": 1,\n"
          + "        \"number_of_replicas\": 0,\n"
          + "        \"version\": {\n"
          + "          \"created\": \"6050399\"\n"
          + "        }\n"
          + "      }\n"
          + "    },\n"
          + "    \"mapping_version\": \"1\",\n"
          + "    \"settings_version\": \"1\",\n"
          + "    \"aliases_version\": \"1\"\n"
          + "  }\n"
          + "}";

  public static void mockMultipleIndexEnv() {
    mockLocalClusterState(
        Map.of(
            INDEX_ACCOUNT_1,
            buildIndexMapping(INDEX_ACCOUNT_1, INDEX_ACCOUNT_1_MAPPING),
            INDEX_ACCOUNT_2,
            buildIndexMapping(INDEX_ACCOUNT_2, INDEX_ACCOUNT_2_MAPPING),
            INDEX_ACCOUNT_ALL,
            buildIndexMapping(
                Map.of(
                    INDEX_ACCOUNT_1,
                    INDEX_ACCOUNT_1_MAPPING,
                    INDEX_ACCOUNT_2,
                    INDEX_ACCOUNT_2_MAPPING))));
  }

  public static void mockLocalClusterState(Map<String, Map<String, MappingMetadata>> indexMapping) {
    LocalClusterState.state().setClusterService(mockClusterService(indexMapping));
    LocalClusterState.state().setResolver(mockIndexNameExpressionResolver());
    LocalClusterState.state().setPluginSettings(mockPluginSettings());
  }

  public static ClusterService mockClusterService(
      Map<String, Map<String, MappingMetadata>> indexMapping) {
    ClusterService mockService = mock(ClusterService.class);
    ClusterState mockState = mock(ClusterState.class);
    Metadata mockMetaData = mock(Metadata.class);

    when(mockService.state()).thenReturn(mockState);
    when(mockState.metadata()).thenReturn(mockMetaData);
    try {
      for (var entry : indexMapping.entrySet()) {
        when(mockMetaData.findMappings(eq(new String[] {entry.getKey()}), any()))
            .thenReturn(entry.getValue());
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return mockService;
  }

  private static Map<String, MappingMetadata> buildIndexMapping(Map<String, String> indexMapping) {
    return indexMapping.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                e -> {
                  try {
                    return IndexMetadata.fromXContent(createParser(e.getValue())).mapping();
                  } catch (IOException ex) {
                    throw new IllegalStateException(ex);
                  }
                }));
  }

  private static Map<String, MappingMetadata> buildIndexMapping(String index, String mapping) {
    try {
      return Map.of(index, IndexMetadata.fromXContent(createParser(mapping)).mapping());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
