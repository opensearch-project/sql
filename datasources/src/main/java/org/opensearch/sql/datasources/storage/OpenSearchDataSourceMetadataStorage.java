/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.storage;

import static org.opensearch.sql.datasources.service.DataSourceServiceImpl.DATASOURCE_NAME_REGEX;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.datasources.encryptor.Encryptor;
import org.opensearch.sql.datasources.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.datasources.service.DataSourceMetadataStorage;
import org.opensearch.sql.datasources.utils.XContentParserUtils;

public class OpenSearchDataSourceMetadataStorage implements DataSourceMetadataStorage {

  public static final String DATASOURCE_INDEX_NAME = ".ql-datasources";
  private static final String DATASOURCE_INDEX_MAPPING_FILE_NAME = "datasources-index-mapping.yml";

  private static final Integer DATASOURCE_QUERY_RESULT_SIZE = 10000;
  private static final String DATASOURCE_INDEX_SETTINGS_FILE_NAME
      = "datasources-index-settings.yml";
  private static final Logger LOG = LogManager.getLogger();
  private final Client client;
  private final ClusterService clusterService;
  private final Encryptor encryptor;

  //This field is for concurrent hashmap
  private final ConcurrentHashMap<String, DataSourceMetadata> keyStoreDataSourceHashMap;



  /**
   * This class implements DataSourceMetadataStorage interface
   * using OpenSearch as underlying storage.
   *
   * @param client         opensearch NodeClient.
   * @param clusterService ClusterService.
   * @param encryptor      Encryptor.
   */
  public OpenSearchDataSourceMetadataStorage(Client client, ClusterService clusterService,
                                             Encryptor encryptor,
                                             List<DataSourceMetadata> keyStoreMetadataList) {
    this.client = client;
    this.clusterService = clusterService;
    this.encryptor = encryptor;
    this.keyStoreDataSourceHashMap = new ConcurrentHashMap<>();
    keyStoreMetadataList.forEach(metadata
        -> keyStoreDataSourceHashMap.put(metadata.getName(), metadata));
  }

  @Override
  public List<DataSourceMetadata> getDataSourceMetadata() {
    if (!this.clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME)) {
      createDataSourcesIndex();
    }
    if (!this.keyStoreDataSourceHashMap.isEmpty()) {
      storeKeyStoreMetadataList();
    }
    return searchInDataSourcesIndex(QueryBuilders.matchAllQuery());
  }

  @Override
  public Optional<DataSourceMetadata> getDataSourceMetadata(String datasourceName) {
    if (!this.clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME)) {
      createDataSourcesIndex();
    }
    if (!this.keyStoreDataSourceHashMap.isEmpty()) {
      storeKeyStoreMetadataList();
    }
    return searchInDataStore(datasourceName);
  }

  @Override
  public void createDataSourceMetadata(DataSourceMetadata dataSourceMetadata) {
    if (!this.clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME)) {
      createDataSourcesIndex();
    }
    if (!this.keyStoreDataSourceHashMap.isEmpty()) {
      storeKeyStoreMetadataList();
    }
    storeMetadata(dataSourceMetadata);
  }

  @Override
  public void updateDataSourceMetadata(DataSourceMetadata dataSourceMetadata) {
    encryptDecryptAuthenticationData(dataSourceMetadata, true);
    UpdateRequest updateRequest
        = new UpdateRequest(DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    UpdateResponse updateResponse;
    try (ThreadContext.StoredContext storedContext = client.threadPool().getThreadContext()
        .stashContext()) {
      updateRequest.doc(XContentParserUtils.convertToXContent(dataSourceMetadata));
      updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
      ActionFuture<UpdateResponse> updateResponseActionFuture
          = client.update(updateRequest);
      updateResponse = updateResponseActionFuture.actionGet();
    } catch (DocumentMissingException exception) {
      throw new DataSourceNotFoundException("Datasource with name: "
          + dataSourceMetadata.getName() + " doesn't exist");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
      LOG.debug("DatasourceMetadata : {}  successfully updated", dataSourceMetadata.getName());
    } else {
      throw new RuntimeException("Saving dataSource metadata information failed with result : "
          + updateResponse.getResult().getLowercase());
    }
  }

  @Override
  public void deleteDataSourceMetadata(String datasourceName) {
    DeleteRequest deleteRequest = new DeleteRequest(DATASOURCE_INDEX_NAME);
    deleteRequest.id(datasourceName);
    ActionFuture<DeleteResponse> deleteResponseActionFuture;
    try (ThreadContext.StoredContext storedContext = client.threadPool().getThreadContext()
        .stashContext()) {
      deleteResponseActionFuture = client.delete(deleteRequest);
    }
    DeleteResponse deleteResponse = deleteResponseActionFuture.actionGet();
    if (deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)) {
      LOG.debug("DatasourceMetadata : {}  successfully deleted", datasourceName);
    } else if (deleteResponse.getResult().equals(DocWriteResponse.Result.NOT_FOUND)) {
      throw new DataSourceNotFoundException("Datasource with name: "
          + datasourceName + " doesn't exist");
    } else {
      throw new RuntimeException("Deleting dataSource metadata information failed with result : "
          + deleteResponse.getResult().getLowercase());
    }
  }

  private Optional<DataSourceMetadata> searchInDataStore(String datasourceName) {
    return searchInDataSourcesIndex(QueryBuilders.termQuery("name", datasourceName))
        .stream()
        .findFirst()
        .map(x -> {
          this.encryptDecryptAuthenticationData(x, false);
          return x;
        });
  }

  private void storeMetadata(DataSourceMetadata dataSourceMetadata) {
    encryptDecryptAuthenticationData(dataSourceMetadata, true);
    IndexRequest indexRequest = new IndexRequest(DATASOURCE_INDEX_NAME);
    indexRequest.id(dataSourceMetadata.getName());
    indexRequest.opType(DocWriteRequest.OpType.CREATE);
    indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
    ActionFuture<IndexResponse> indexResponseActionFuture;
    IndexResponse indexResponse;
    try (ThreadContext.StoredContext ignored = client.threadPool().getThreadContext()
        .stashContext()) {
      indexRequest.source(XContentParserUtils.convertToXContent(dataSourceMetadata));
      indexResponseActionFuture = client.index(indexRequest);
      indexResponse = indexResponseActionFuture.actionGet();
    } catch (VersionConflictEngineException exception) {
      throw new IllegalArgumentException("A datasource already exists with name: "
          + dataSourceMetadata.getName());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
      LOG.debug("DatasourceMetadata : {}  successfully created", dataSourceMetadata.getName());
    } else {
      throw new RuntimeException("Saving dataSource metadata information failed with result : "
          + indexResponse.getResult().getLowercase());
    }
  }

  private void createDataSourcesIndex() {
    try {
      InputStream mappingFileStream = OpenSearchDataSourceMetadataStorage.class.getClassLoader()
          .getResourceAsStream(DATASOURCE_INDEX_MAPPING_FILE_NAME);
      InputStream settingsFileStream = OpenSearchDataSourceMetadataStorage.class.getClassLoader()
          .getResourceAsStream(DATASOURCE_INDEX_SETTINGS_FILE_NAME);
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(DATASOURCE_INDEX_NAME);
      createIndexRequest.mapping(IOUtils.toString(mappingFileStream, StandardCharsets.UTF_8),
              XContentType.YAML)
          .settings(IOUtils.toString(settingsFileStream, StandardCharsets.UTF_8),
              XContentType.YAML);
      ActionFuture<CreateIndexResponse> createIndexResponseActionFuture;
      try (ThreadContext.StoredContext ignored = client.threadPool().getThreadContext()
          .stashContext()) {
        createIndexResponseActionFuture = client.admin().indices().create(createIndexRequest);
      }
      CreateIndexResponse createIndexResponse = createIndexResponseActionFuture.actionGet();
      if (createIndexResponse.isAcknowledged()) {
        LOG.info("Index: {} creation Acknowledged", DATASOURCE_INDEX_NAME);
      } else {
        throw new RuntimeException(
            "Index creation is not acknowledged.");
      }
    } catch (Throwable e) {
      throw new RuntimeException(
          "Internal server error while creating" + DATASOURCE_INDEX_NAME + " index:: "
              + e.getMessage());
    }
  }

  private List<DataSourceMetadata> searchInDataSourcesIndex(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(DATASOURCE_INDEX_NAME);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchSourceBuilder.size(DATASOURCE_QUERY_RESULT_SIZE);
    searchRequest.source(searchSourceBuilder);
    ActionFuture<SearchResponse> searchResponseActionFuture;
    try (ThreadContext.StoredContext ignored = client.threadPool().getThreadContext()
        .stashContext()) {
      searchResponseActionFuture = client.search(searchRequest);
    }
    SearchResponse searchResponse = searchResponseActionFuture.actionGet();
    if (searchResponse.status().getStatus() != 200) {
      throw new RuntimeException("Fetching dataSource metadata information failed with status : "
          + searchResponse.status());
    } else {
      List<DataSourceMetadata> list = new ArrayList<>();
      for (SearchHit searchHit : searchResponse.getHits().getHits()) {
        String sourceAsString = searchHit.getSourceAsString();
        DataSourceMetadata dataSourceMetadata;
        try {
          dataSourceMetadata = XContentParserUtils.toDataSourceMetadata(sourceAsString);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        list.add(dataSourceMetadata);
      }
      return list;
    }
  }

  @SuppressWarnings("missingswitchdefault")
  private void encryptDecryptAuthenticationData(DataSourceMetadata dataSourceMetadata,
                                                              Boolean isEncryption) {
    Map<String, String> propertiesMap = dataSourceMetadata.getProperties();
    Optional<AuthenticationType> authTypeOptional
        = propertiesMap.keySet().stream().filter(s -> s.endsWith("auth.type"))
        .findFirst()
        .map(propertiesMap::get)
        .map(AuthenticationType::get);
    if (authTypeOptional.isPresent()) {
      switch (authTypeOptional.get()) {
        case BASICAUTH:
          handleBasicAuthPropertiesEncryptionDecryption(propertiesMap, isEncryption);
          break;
        case AWSSIGV4AUTH:
          handleSigV4PropertiesEncryptionDecryption(propertiesMap, isEncryption);
          break;
      }
    }
  }

  private void handleBasicAuthPropertiesEncryptionDecryption(Map<String, String> propertiesMap,
                                                             Boolean isEncryption) {
    ArrayList<String> list = new ArrayList<>();
    propertiesMap.keySet().stream()
        .filter(s -> s.endsWith("auth.username"))
        .findFirst()
        .ifPresent(list::add);
    propertiesMap.keySet().stream()
        .filter(s -> s.endsWith("auth.password"))
        .findFirst()
        .ifPresent(list::add);
    encryptOrDecrypt(propertiesMap, isEncryption, list);
  }

  private void encryptOrDecrypt(Map<String, String> propertiesMap, Boolean isEncryption,
                                List<String> keyIdentifiers) {
    for (String key : keyIdentifiers) {
      if (isEncryption) {
        propertiesMap.put(key,
            this.encryptor.encrypt(propertiesMap.get(key)));
      } else {
        propertiesMap.put(key,
            this.encryptor.decrypt(propertiesMap.get(key)));
      }
    }
  }

  private void handleSigV4PropertiesEncryptionDecryption(Map<String, String> propertiesMap,
                                                         Boolean isEncryption) {
    ArrayList<String> list = new ArrayList<>();
    propertiesMap.keySet().stream()
        .filter(s -> s.endsWith("auth.access_key"))
        .findFirst()
        .ifPresent(list::add);
    propertiesMap.keySet().stream()
        .filter(s -> s.endsWith("auth.secret_key"))
        .findFirst()
        .ifPresent(list::add);
    encryptOrDecrypt(propertiesMap, isEncryption, list);
  }

  private void storeKeyStoreMetadataList() {
    for (String dataSourceName : this.keyStoreDataSourceHashMap.keySet()) {
      try {
        DataSourceMetadata dataSourceMetadata = this.keyStoreDataSourceHashMap.get(dataSourceName);
        validateDataSourceMetaData(dataSourceMetadata);
        storeMetadata(dataSourceMetadata);
      } catch (Throwable e) {
        LOG.error("Datasource  {} creation from keystore failed. "
                + "Please try manually creating using create api", dataSourceName);
      }
    }
    this.keyStoreDataSourceHashMap.clear();
  }

  /**
   * This can be moved to a different validator class when we introduce more connectors.
   *
   * @param metadata {@link DataSourceMetadata}.
   */
  private void validateDataSourceMetaData(DataSourceMetadata metadata) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(metadata.getName()),
        "Missing Name Field from a DataSource. Name is a required parameter.");
    Preconditions.checkArgument(
        metadata.getName().matches(DATASOURCE_NAME_REGEX),
        StringUtils.format(
            "DataSource Name: %s contains illegal characters. Allowed characters: a-zA-Z0-9_-*@.",
            metadata.getName()));
    Preconditions.checkArgument(
        !Objects.isNull(metadata.getProperties()),
        "Missing properties field in catalog configuration. Properties are required parameters.");
  }

}