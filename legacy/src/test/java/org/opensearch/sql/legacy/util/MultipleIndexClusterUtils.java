/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.legacy.util.CheckScriptContents.createParser;
import static org.opensearch.sql.legacy.util.CheckScriptContents.mockPluginSettings;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
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

  @SneakyThrows
  public static void mockLocalClusterState(Map<String, Map<String, MappingMetadata>> indexMapping) {

    Client client = Mockito.mock(Client.class, Mockito.RETURNS_DEEP_STUBS);

    ThreadLocal<String> callerIndexExpression = new ThreadLocal<>();
    ArgumentMatcher<Object> preserveIndexMappingsFromCaller =
        arg -> {
          callerIndexExpression.set((String) arg);
          return true;
        };
    Answer<Map<String, MappingMetadata>> getIndexMappingsForCaller =
        invoke -> {
          return indexMapping.get(callerIndexExpression.get());
        };

    when(client
            .admin()
            .indices()
            .prepareGetMappings((String[]) argThat(preserveIndexMappingsFromCaller))
            .setLocal(anyBoolean())
            .setIndicesOptions(any())
            .execute()
            .actionGet(anyLong(), any())
            .mappings())
        .thenAnswer(getIndexMappingsForCaller);

    LocalClusterState.state().setClusterService(mock(ClusterService.class));
    LocalClusterState.state().setPluginSettings(mockPluginSettings());
    LocalClusterState.state().setClient(client);
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
