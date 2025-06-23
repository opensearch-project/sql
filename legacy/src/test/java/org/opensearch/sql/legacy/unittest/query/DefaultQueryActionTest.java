/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.query;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.util.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.script.Script;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.domain.KVValue;
import org.opensearch.sql.legacy.domain.MethodField;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.Format;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.request.SqlRequest;

public class DefaultQueryActionTest {

  private DefaultQueryAction queryAction;

  private Client mockClient;

  private Select mockSelect;

  private SearchRequestBuilder mockRequestBuilder;

  @Before
  public void initDefaultQueryAction() {

    mockClient = mock(Client.class);
    mockSelect = mock(Select.class);
    mockRequestBuilder = mock(SearchRequestBuilder.class);

    List<Field> fields = new LinkedList<>();
    fields.add(new Field("balance", "bbb"));

    doReturn(fields).when(mockSelect).getFields();
    doReturn(null)
        .when(mockRequestBuilder)
        .setFetchSource(any(String[].class), any(String[].class));
    doReturn(null).when(mockRequestBuilder).addScriptField(anyString(), any(Script.class));

    queryAction = new DefaultQueryAction(mockClient, mockSelect);
    queryAction.initialize(mockRequestBuilder);
  }

  @After
  public void cleanup() {
    LocalClusterState.state(null);
  }

  @Test
  public void scriptFieldWithTwoParams() throws SqlParseException {

    List<Field> fields = new LinkedList<>();
    fields.add(createScriptField("script1", "doc['balance'] * 2", false, true, false));

    queryAction.setFields(fields);

    final Optional<List<String>> fieldNames = queryAction.getFieldNames();
    Assert.assertTrue("Field names have not been set", fieldNames.isPresent());
    Assert.assertThat(fieldNames.get().size(), equalTo(1));
    Assert.assertThat(fieldNames.get().get(0), equalTo("script1"));

    Mockito.verify(mockRequestBuilder).addScriptField(eq("script1"), any(Script.class));
  }

  @Test
  public void scriptFieldWithThreeParams() throws SqlParseException {

    List<Field> fields = new LinkedList<>();
    fields.add(createScriptField("script1", "doc['balance'] * 2", true, true, false));

    queryAction.setFields(fields);

    final Optional<List<String>> fieldNames = queryAction.getFieldNames();
    Assert.assertTrue("Field names have not been set", fieldNames.isPresent());
    Assert.assertThat(fieldNames.get().size(), equalTo(1));
    Assert.assertThat(fieldNames.get().get(0), equalTo("script1"));

    Mockito.verify(mockRequestBuilder).addScriptField(eq("script1"), any(Script.class));
  }

  @Test(expected = SqlParseException.class)
  public void scriptFieldWithLessThanTwoParams() throws SqlParseException {

    List<Field> fields = new LinkedList<>();
    fields.add(createScriptField("script1", "doc['balance'] * 2", false, false, false));

    queryAction.setFields(fields);
  }

  @Test
  public void scriptFieldWithMoreThanThreeParams() throws SqlParseException {

    List<Field> fields = new LinkedList<>();
    fields.add(createScriptField("script1", "doc['balance'] * 2", false, true, true));

    queryAction.setFields(fields);
  }

  @Test
  public void testIfScrollShouldBeOpenWithDifferentFormats() {
    int settingFetchSize = 500;
    TimeValue timeValue = new TimeValue(120000);
    int limit = 2300;
    mockLocalClusterStateAndInitializeMetrics(timeValue);

    doReturn(limit).when(mockSelect).getRowCount();
    doReturn(mockRequestBuilder).when(mockRequestBuilder).setSize(settingFetchSize);
    SqlRequest mockSqlRequest = mock(SqlRequest.class);
    doReturn(settingFetchSize).when(mockSqlRequest).fetchSize();
    queryAction.setSqlRequest(mockSqlRequest);

    Format[] formats = new Format[] {Format.CSV, Format.RAW, Format.JSON, Format.TABLE};
    for (Format format : formats) {
      queryAction.setFormat(format);
      queryAction.checkAndSetScroll();
    }

    Mockito.verify(mockRequestBuilder, times(4)).setSize(limit);
    Mockito.verify(mockRequestBuilder, never()).setScroll(any(TimeValue.class));

    queryAction.setFormat(Format.JDBC);
    queryAction.checkAndSetScroll();
    Mockito.verify(mockRequestBuilder).setSize(settingFetchSize);
    Mockito.verify(mockRequestBuilder).addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC);
    Mockito.verify(mockRequestBuilder, never()).setScroll(timeValue);

    // Verify setScroll when SQL_PAGINATION_API_SEARCH_AFTER is set to false
    mockLocalClusterStateAndIntializeMetricsForScroll(timeValue);
    queryAction.checkAndSetScroll();
    Mockito.verify(mockRequestBuilder).setScroll(timeValue);
  }

  @Test
  public void testIfScrollShouldBeOpen() {
    int settingFetchSize = 500;
    TimeValue timeValue = new TimeValue(120000);
    int limit = 2300;

    doReturn(limit).when(mockSelect).getRowCount();
    doReturn(mockRequestBuilder).when(mockRequestBuilder).setSize(settingFetchSize);
    SqlRequest mockSqlRequest = mock(SqlRequest.class);
    doReturn(settingFetchSize).when(mockSqlRequest).fetchSize();
    queryAction.setSqlRequest(mockSqlRequest);
    queryAction.setFormat(Format.JDBC);

    mockLocalClusterStateAndInitializeMetrics(timeValue);
    queryAction.checkAndSetScroll();
    Mockito.verify(mockRequestBuilder).setSize(settingFetchSize);
    Mockito.verify(mockRequestBuilder).addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC);
    Mockito.verify(mockRequestBuilder, never()).setScroll(timeValue);

    // Verify setScroll when SQL_PAGINATION_API_SEARCH_AFTER is set to false
    mockLocalClusterStateAndIntializeMetricsForScroll(timeValue);
    queryAction.checkAndSetScroll();
    Mockito.verify(mockRequestBuilder).setScroll(timeValue);
  }

  @Test
  public void testIfScrollShouldBeOpenWithDifferentFetchSize() {
    TimeValue timeValue = new TimeValue(120000);
    int limit = 2300;
    mockLocalClusterStateAndInitializeMetrics(timeValue);

    doReturn(limit).when(mockSelect).getRowCount();
    SqlRequest mockSqlRequest = mock(SqlRequest.class);
    queryAction.setSqlRequest(mockSqlRequest);
    queryAction.setFormat(Format.JDBC);

    int[] fetchSizes = new int[] {0, -10};
    for (int fetch : fetchSizes) {
      doReturn(fetch).when(mockSqlRequest).fetchSize();
      queryAction.checkAndSetScroll();
    }
    Mockito.verify(mockRequestBuilder, times(2)).setSize(limit);
    Mockito.verify(mockRequestBuilder, never()).setScroll(timeValue);

    int userFetchSize = 20;
    doReturn(userFetchSize).when(mockSqlRequest).fetchSize();
    doReturn(mockRequestBuilder).when(mockRequestBuilder).setSize(userFetchSize);
    queryAction.checkAndSetScroll();
    Mockito.verify(mockRequestBuilder).setSize(20);
    Mockito.verify(mockRequestBuilder).addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC);
    Mockito.verify(mockRequestBuilder, never()).setScroll(timeValue);

    // Verify setScroll when SQL_PAGINATION_API_SEARCH_AFTER is set to false
    mockLocalClusterStateAndIntializeMetricsForScroll(timeValue);
    queryAction.checkAndSetScroll();
    Mockito.verify(mockRequestBuilder).setScroll(timeValue);
  }

  @Test
  public void testIfScrollShouldBeOpenWithDifferentValidFetchSizeAndLimit() {
    TimeValue timeValue = new TimeValue(120000);
    mockLocalClusterStateAndInitializeMetrics(timeValue);

    int limit = 2300;
    doReturn(limit).when(mockSelect).getRowCount();
    SqlRequest mockSqlRequest = mock(SqlRequest.class);

    /** fetchSize <= LIMIT - open scroll */
    int userFetchSize = 1500;
    doReturn(userFetchSize).when(mockSqlRequest).fetchSize();
    doReturn(mockRequestBuilder).when(mockRequestBuilder).setSize(userFetchSize);
    queryAction.setSqlRequest(mockSqlRequest);
    queryAction.setFormat(Format.JDBC);

    queryAction.checkAndSetScroll();
    Mockito.verify(mockRequestBuilder).setSize(userFetchSize);
    Mockito.verify(mockRequestBuilder).addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC);
    // Skip setScroll when SQL_PAGINATION_API_SEARCH_AFTER is set to false
    Mockito.verify(mockRequestBuilder, never()).setScroll(timeValue);

    /** fetchSize > LIMIT - no scroll */
    userFetchSize = 5000;
    doReturn(userFetchSize).when(mockSqlRequest).fetchSize();
    mockRequestBuilder = mock(SearchRequestBuilder.class);
    queryAction.initialize(mockRequestBuilder);
    queryAction.checkAndSetScroll();
    Mockito.verify(mockRequestBuilder).setSize(limit);
    Mockito.verify(mockRequestBuilder, never()).setScroll(timeValue);

    // Verify setScroll when SQL_PAGINATION_API_SEARCH_AFTER is set to false
    mockLocalClusterStateAndIntializeMetricsForScroll(timeValue);
    /** fetchSize <= LIMIT - open scroll */
    userFetchSize = 1500;
    doReturn(userFetchSize).when(mockSqlRequest).fetchSize();
    queryAction.checkAndSetScroll();
    Mockito.verify(mockRequestBuilder).setScroll(timeValue);
  }

  private void mockLocalClusterStateAndInitializeMetrics(TimeValue time) {
    LocalClusterState mockLocalClusterState = mock(LocalClusterState.class);
    LocalClusterState.state(mockLocalClusterState);
    doReturn(time).when(mockLocalClusterState).getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    doReturn(3600L)
        .when(mockLocalClusterState)
        .getSettingValue(Settings.Key.METRICS_ROLLING_WINDOW);
    doReturn(2L).when(mockLocalClusterState).getSettingValue(Settings.Key.METRICS_ROLLING_INTERVAL);
    doReturn(true)
        .when(mockLocalClusterState)
        .getSettingValue(Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER);

    Metrics.getInstance().registerDefaultMetrics();
  }

  private void mockLocalClusterStateAndIntializeMetricsForScroll(TimeValue time) {
    LocalClusterState mockLocalClusterState = mock(LocalClusterState.class);
    LocalClusterState.state(mockLocalClusterState);
    doReturn(time).when(mockLocalClusterState).getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    doReturn(3600L)
        .when(mockLocalClusterState)
        .getSettingValue(Settings.Key.METRICS_ROLLING_WINDOW);
    doReturn(2L).when(mockLocalClusterState).getSettingValue(Settings.Key.METRICS_ROLLING_INTERVAL);
    doReturn(false)
        .when(mockLocalClusterState)
        .getSettingValue(Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER);

    Metrics.getInstance().registerDefaultMetrics();
  }

  private Field createScriptField(
      final String name,
      final String script,
      final boolean addScriptLanguage,
      final boolean addScriptParam,
      final boolean addRedundantParam) {

    final List<KVValue> params = new ArrayList<>();

    params.add(new KVValue("alias", name));
    if (addScriptLanguage) {
      params.add(new KVValue("painless"));
    }
    if (addScriptParam) {
      params.add(new KVValue(script));
    }
    if (addRedundantParam) {
      params.add(new KVValue("Fail the test"));
    }

    return new MethodField("script", params, null, null);
  }
}
