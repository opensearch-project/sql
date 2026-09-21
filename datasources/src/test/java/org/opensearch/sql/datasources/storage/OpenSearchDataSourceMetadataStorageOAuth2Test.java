/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.storage;

import static org.opensearch.sql.datasources.storage.OpenSearchDataSourceMetadataStorage.DATASOURCE_INDEX_NAME;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.encryptor.Encryptor;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.transport.client.Client;

@ExtendWith(MockitoExtension.class)
public class OpenSearchDataSourceMetadataStorageOAuth2Test {

  private static final String TEST_DATASOURCE_INDEX_NAME = "testDS";

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Client client;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ClusterService clusterService;

  @Mock private Encryptor encryptor;

  @Mock private OpenSearchSettings openSearchSettings;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private SearchResponse searchResponse;

  @Mock private ActionFuture<SearchResponse> searchResponseActionFuture;
  @Mock private ActionFuture<CreateIndexResponse> createIndexResponseActionFuture;
  @Mock private ActionFuture<IndexResponse> indexResponseActionFuture;
  @Mock private IndexResponse indexResponse;
  @Mock private ActionFuture<UpdateResponse> updateResponseActionFuture;
  @Mock private UpdateResponse updateResponse;
  @Mock private SearchHit searchHit;
  @InjectMocks private OpenSearchDataSourceMetadataStorage openSearchDataSourceMetadataStorage;

  @SneakyThrows
  @Test
  public void testGetDataSourceMetadataWithOAuth2() {
    setDataSourcesEnabled(true);
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
    Mockito.when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(21, TotalHits.Relation.EQUAL_TO), 1.0F));
    Mockito.when(searchHit.getSourceAsString()).thenReturn(getOAuth2DataSourceMetadataString());
    Mockito.when(encryptor.decrypt("encryptedClientSecret")).thenReturn("testClientSecret");

    Optional<DataSourceMetadata> dataSourceMetadataOptional =
        openSearchDataSourceMetadataStorage.getDataSourceMetadata(TEST_DATASOURCE_INDEX_NAME);

    Assertions.assertFalse(dataSourceMetadataOptional.isEmpty());
    DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
    Assertions.assertEquals(TEST_DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    Assertions.assertEquals(DataSourceType.PROMETHEUS, dataSourceMetadata.getConnector());
    Assertions.assertEquals(
        "testClientSecret",
        dataSourceMetadata.getProperties().get("prometheus.oauth2.clientSecret"));
    Assertions.assertEquals(
        "testClientId", dataSourceMetadata.getProperties().get("prometheus.oauth2.clientId"));
    Assertions.assertEquals(
        "https://auth.example.com/token",
        dataSourceMetadata.getProperties().get("prometheus.oauth2.tokenUrl"));
    Assertions.assertEquals(
        "oauth2", dataSourceMetadata.getProperties().get("prometheus.auth.type"));
  }

  @Test
  public void testCreateDataSourceMetadataWithOAuth2() {
    setDataSourcesEnabled(true);

    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    Mockito.when(encryptor.encrypt("testClientSecret")).thenReturn("encryptedClientSecret");
    Mockito.when(client.admin().indices().create(ArgumentMatchers.any()))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, DATASOURCE_INDEX_NAME));
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);
    Mockito.when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    Mockito.when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);
    DataSourceMetadata dataSourceMetadata = getOAuth2DataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(dataSourceMetadata);

    // Verify OAuth2 client secret is encrypted
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("testClientSecret");
    Mockito.verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(2)).stashContext();
  }

  @Test
  public void testCreateDataSourceMetadataWithOAuth2WithoutCreatingIndex() {
    setDataSourcesEnabled(true);
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.TRUE);
    Mockito.when(encryptor.encrypt("testClientSecret")).thenReturn("encryptedClientSecret");
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);
    Mockito.when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    Mockito.when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);
    DataSourceMetadata dataSourceMetadata = getOAuth2DataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(dataSourceMetadata);

    // Verify OAuth2 client secret is encrypted
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("testClientSecret");
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();
  }

  @Test
  public void testUpdateDataSourceMetadataWithOAuth2() {
    setDataSourcesEnabled(true);
    Mockito.when(encryptor.encrypt("testClientSecret")).thenReturn("encryptedClientSecret");
    Mockito.when(client.update(ArgumentMatchers.any())).thenReturn(updateResponseActionFuture);
    Mockito.when(updateResponseActionFuture.actionGet()).thenReturn(updateResponse);
    Mockito.when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.UPDATED);
    DataSourceMetadata dataSourceMetadata = getOAuth2DataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.updateDataSourceMetadata(dataSourceMetadata);

    // Verify OAuth2 client secret is encrypted
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("testClientSecret");
    Mockito.verify(client.admin().indices(), Mockito.times(0)).create(ArgumentMatchers.any());
    Mockito.verify(client, Mockito.times(1)).update(ArgumentMatchers.any());
    Mockito.verify(client.threadPool().getThreadContext(), Mockito.times(1)).stashContext();
  }

  @Test
  public void testCreateDataSourceMetadataWithOAuth2AndBasicAuth() {
    setDataSourcesEnabled(true);

    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(Boolean.TRUE);
    // Mock encryption for any string (more flexible for mixed auth scenarios)
    Mockito.when(encryptor.encrypt(ArgumentMatchers.anyString()))
        .thenAnswer(
            invocation -> {
              String input = invocation.getArgument(0);
              return "encrypted" + input;
            });
    Mockito.when(client.index(ArgumentMatchers.any())).thenReturn(indexResponseActionFuture);
    Mockito.when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    Mockito.when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);

    // Create data source with both OAuth2 and Basic Auth properties
    DataSourceMetadata dataSourceMetadata = getMixedAuthDataSourceMetadata();

    this.openSearchDataSourceMetadataStorage.createDataSourceMetadata(dataSourceMetadata);

    // Verify both OAuth2 client secret and Basic Auth password are encrypted
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("testClientSecret");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("testPassword");
    Mockito.verify(encryptor, Mockito.times(1)).encrypt("testUser");
    Mockito.verify(client, Mockito.times(1)).index(ArgumentMatchers.any());
  }

  @Test
  public void testGetDataSourceMetadataWithUndecryptableOAuth2SecretPropagates() {
    // OAuth2 support ships with encryption from the start, so a client secret that will not
    // decrypt means a rotated master key or corrupted document - never a "legacy plaintext"
    // datasource. Swallowing the failure would pass the raw stored value to the token
    // interceptor as the client secret and surface as an unexplained 401 from the IdP, so
    // the exception must propagate.
    setDataSourcesEnabled(true);
    Mockito.when(clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME))
        .thenReturn(true);
    Mockito.when(client.search(ArgumentMatchers.any())).thenReturn(searchResponseActionFuture);
    Mockito.when(searchResponseActionFuture.actionGet()).thenReturn(searchResponse);
    Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
    Mockito.when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(21, TotalHits.Relation.EQUAL_TO), 1.0F));

    Map<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.type", "oauth2");
    properties.put("prometheus.oauth2.clientId", "testClientId");
    properties.put("prometheus.oauth2.clientSecret", "undecryptableSecret");
    properties.put("prometheus.oauth2.tokenUrl", "https://auth.example.com/token");
    properties.put("prometheus.uri", "https://localhost:9090");
    DataSourceMetadata storedDataSource =
        new DataSourceMetadata.Builder()
            .setName("testDS")
            .setProperties(properties)
            .setConnector(DataSourceType.PROMETHEUS)
            .setAllowedRoles(Collections.singletonList("prometheus_access"))
            .build();

    try {
      Mockito.when(searchHit.getSourceAsString()).thenReturn(serialize(storedDataSource));
    } catch (Exception e) {
      Assertions.fail("Failed to serialize test fixture: " + e.getMessage());
    }

    Mockito.when(encryptor.decrypt("undecryptableSecret"))
        .thenThrow(new RuntimeException("Invalid ciphertext"));

    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                openSearchDataSourceMetadataStorage.getDataSourceMetadata(
                    TEST_DATASOURCE_INDEX_NAME));
    Assertions.assertEquals("Invalid ciphertext", exception.getMessage());
  }

  private String getOAuth2DataSourceMetadataString() throws JsonProcessingException {
    Map<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.type", "oauth2");
    properties.put("prometheus.oauth2.clientId", "testClientId");
    properties.put(
        "prometheus.oauth2.clientSecret",
        "encryptedClientSecret"); // This would be encrypted in storage
    properties.put("prometheus.oauth2.tokenUrl", "https://auth.example.com/token");
    properties.put("prometheus.uri", "https://localhost:9090");
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata.Builder()
            .setName("testDS")
            .setProperties(properties)
            .setConnector(DataSourceType.PROMETHEUS)
            .setAllowedRoles(Collections.singletonList("prometheus_access"))
            .build();
    return serialize(dataSourceMetadata);
  }

  private DataSourceMetadata getOAuth2DataSourceMetadata() {
    Map<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.type", "oauth2");
    properties.put("prometheus.oauth2.clientId", "testClientId");
    properties.put(
        "prometheus.oauth2.clientSecret", "testClientSecret"); // Plain text before encryption
    properties.put("prometheus.oauth2.tokenUrl", "https://auth.example.com/token");
    properties.put("prometheus.uri", "https://localhost:9090");
    return new DataSourceMetadata.Builder()
        .setName("testDS")
        .setProperties(properties)
        .setConnector(DataSourceType.PROMETHEUS)
        .setAllowedRoles(Collections.singletonList("prometheus_access"))
        .build();
  }

  private DataSourceMetadata getMixedAuthDataSourceMetadata() {
    Map<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.type", "oauth2");
    // OAuth2 properties
    properties.put("prometheus.oauth2.clientId", "testClientId");
    properties.put("prometheus.oauth2.clientSecret", "testClientSecret");
    properties.put("prometheus.oauth2.tokenUrl", "https://auth.example.com/token");
    // Basic Auth properties (for testing mixed scenarios)
    properties.put("prometheus.auth.username", "testUser");
    properties.put("prometheus.auth.password", "testPassword");
    properties.put("prometheus.uri", "https://localhost:9090");
    return new DataSourceMetadata.Builder()
        .setName("testDS")
        .setProperties(properties)
        .setConnector(DataSourceType.PROMETHEUS)
        .setAllowedRoles(Collections.singletonList("prometheus_access"))
        .build();
  }

  private String serialize(DataSourceMetadata dataSourceMetadata) throws JsonProcessingException {
    return getObjectMapper().writeValueAsString(dataSourceMetadata);
  }

  private ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    addSerializerForDataSourceType(mapper);
    return mapper;
  }

  /** It is needed to serialize DataSourceType as string. */
  private void addSerializerForDataSourceType(ObjectMapper mapper) {
    SimpleModule module = new SimpleModule();
    module.addSerializer(DataSourceType.class, getDataSourceTypeSerializer());
    mapper.registerModule(module);
  }

  private StdSerializer<DataSourceType> getDataSourceTypeSerializer() {
    return new StdSerializer<>(DataSourceType.class) {
      @Override
      public void serialize(
          DataSourceType dsType, JsonGenerator jsonGen, SerializerProvider provider)
          throws IOException {
        jsonGen.writeString(dsType.name());
      }
    };
  }

  private void setDataSourcesEnabled(boolean enabled) {
    Mockito.when(
            openSearchSettings.getSettingValue(
                ArgumentMatchers.eq(Settings.Key.DATASOURCES_ENABLED)))
        .thenReturn(enabled);
  }
}
