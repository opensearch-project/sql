/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.legacy.util.CheckScriptContents.mockClusterService;
import static org.opensearch.sql.legacy.util.CheckScriptContents.mockLocalClusterState;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.esdomain.mapping.FieldMappings;
import org.opensearch.sql.legacy.esdomain.mapping.IndexMappings;
import org.opensearch.sql.legacy.util.TestsConstants;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;

/** Local cluster state testing without covering OpenSearch logic, ex. resolve index pattern. */
public class LocalClusterStateTest {

  private static final String INDEX_NAME = TestsConstants.TEST_INDEX_BANK;
  private static final String TYPE_NAME = "account";

  private static final String MAPPING =
      "{\n"
          + "  \"opensearch-sql_test_index_bank\": {\n"
          + "    \"mappings\": {\n"
          + "      \"account\": {\n"
          + "        \"properties\": {\n"
          + "          \"address\": {\n"
          + "            \"type\": \"text\"\n"
          + "          },\n"
          + "          \"age\": {\n"
          + "            \"type\": \"integer\"\n"
          + "          },\n"
          + "          \"city\": {\n"
          + "            \"type\": \"keyword\"\n"
          + "          },\n"
          + "          \"employer\": {\n"
          + "            \"type\": \"text\",\n"
          + "            \"fields\": {\n"
          + "              \"keyword\": {\n"
          + "                \"type\": \"keyword\",\n"
          + "                \"ignore_above\": 256\n"
          + "              }\n"
          + "            }\n"
          + "          },\n"
          + "          \"state\": {\n"
          + "            \"type\": \"text\",\n"
          + "            \"fields\": {\n"
          + "              \"raw\": {\n"
          + "                \"type\": \"keyword\",\n"
          + "                \"ignore_above\": 256\n"
          + "              }\n"
          + "            }\n"
          + "          },\n"
          + "          \"manager\": {\n"
          + "            \"properties\": {\n"
          + "              \"name\": {\n"
          + "                \"type\": \"text\",\n"
          + "                \"fields\": {\n"
          + "                  \"keyword\": {\n"
          + "                    \"type\": \"keyword\",\n"
          + "                    \"ignore_above\": 256\n"
          + "                  }\n"
          + "                }\n"
          + "              },\n"
          + "              \"address\": {\n"
          + "                \"type\": \"keyword\"\n"
          + "              }\n"
          + "            }\n"
          + "          }\n"
          + "        }\n"
          + "      }\n"
          + "    },\n"
          +
          // ==== All required by IndexMetaData.fromXContent() ====
          "    \"settings\": {\n"
          + "      \"index\": {\n"
          + "        \"number_of_shards\": 5,\n"
          + "        \"number_of_replicas\": 0,\n"
          + "        \"version\": {\n"
          + "          \"created\": \"6050399\"\n"
          + "        }\n"
          + "      }\n"
          + "    },\n"
          + "    \"mapping_version\": \"1\",\n"
          + "    \"settings_version\": \"1\",\n"
          + "    \"aliases_version\": \"1\"\n"
          +
          // =======================================================
          "  }\n"
          + "}";

  @Mock private ClusterSettings clusterSettings;

  @Before
  public void init() {
    MockitoAnnotations.openMocks(this);
    LocalClusterState.state(null);
    mockLocalClusterState(MAPPING);
  }

  @Test
  public void getMappingForExistingField() {
    IndexMappings indexMappings =
        LocalClusterState.state().getFieldMappings(new String[] {INDEX_NAME});
    Assert.assertNotNull(indexMappings);

    FieldMappings fieldMappings = indexMappings.mapping(INDEX_NAME);
    Assert.assertNotNull(fieldMappings);

    Assert.assertEquals("text", fieldMappings.mapping("address").get("type"));
    Assert.assertEquals("integer", fieldMappings.mapping("age").get("type"));
    Assert.assertEquals("keyword", fieldMappings.mapping("city").get("type"));
    Assert.assertEquals("text", fieldMappings.mapping("employer").get("type"));

    Assert.assertEquals("text", fieldMappings.mapping("manager.name").get("type"));
    Assert.assertEquals("keyword", fieldMappings.mapping("manager.address").get("type"));
  }

  @Test
  public void getMappingForInvalidField() {
    IndexMappings indexMappings =
        LocalClusterState.state().getFieldMappings(new String[] {INDEX_NAME});
    FieldMappings fieldMappings = indexMappings.mapping(INDEX_NAME);

    Assert.assertNull(fieldMappings.mapping("work-email"));
    Assert.assertNull(fieldMappings.mapping("manager.home-address"));
    Assert.assertNull(fieldMappings.mapping("manager.name.first"));
    Assert.assertNull(fieldMappings.mapping("manager.name.first.uppercase"));
  }

  @Test
  public void getMappingFromCache() throws IOException {
    // Mock here again for verification below and mock addListener()
    ClusterService mockService = mockClusterService(MAPPING);
    ClusterStateListener[] listener = new ClusterStateListener[1]; // Trick to access inside lambda
    doAnswer(
            invocation -> {
              listener[0] = (ClusterStateListener) invocation.getArguments()[0];
              return null;
            })
        .when(mockService)
        .addListener(any());
    LocalClusterState.state().setClusterService(mockService);

    // 1.Actual findMappings be invoked only once
    for (int i = 0; i < 10; i++) {
      LocalClusterState.state().getFieldMappings(new String[] {INDEX_NAME});
    }
    verify(mockService.state().metadata(), times(1))
        .findMappings(eq(new String[] {INDEX_NAME}), any());

    // 2.Fire cluster state change event
    Assert.assertNotNull(listener[0]);
    ClusterChangedEvent mockEvent = mock(ClusterChangedEvent.class);
    when(mockEvent.metadataChanged()).thenReturn(true);
    listener[0].clusterChanged(mockEvent);

    // 3.Cache should be invalidated and call findMapping another time only
    for (int i = 0; i < 5; i++) {
      LocalClusterState.state().getFieldMappings(new String[] {INDEX_NAME});
    }
    verify(mockService.state().metadata(), times(2))
        .findMappings(eq(new String[] {INDEX_NAME}), any());
  }

  @Test
  public void getDefaultValueForQuerySlowLog() {
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    OpenSearchSettings settings = new OpenSearchSettings(clusterSettings);
    assertEquals(Integer.valueOf(2), settings.getSettingValue(Settings.Key.SQL_SLOWLOG));
  }
}
