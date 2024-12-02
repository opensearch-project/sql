/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex.SingleRowQuery;

@ExtendWith(MockitoExtension.class)
class SingleRowQueryTest {

  @Mock private OpenSearchClient openSearchClient;
  @Mock private NodeClient nodeClient;
  @Mock private ActionFuture<SearchResponse> searchFuture;
  @Mock private SearchResponse response;
  @Mock private SearchHits searchHits;
  @Mock private SearchHit hit;
  @Captor private ArgumentCaptor<SearchRequest> searchRequestCaptor;

  private SingleRowQuery singleRowQuery;

  @BeforeEach
  public void beforeEach() {
    when(openSearchClient.getNodeClient()).thenReturn(nodeClient);
    singleRowQuery = new SingleRowQuery(openSearchClient);
  }

  @AfterEach
  public void shouldUseExactlyOneSearch() {
    verify(nodeClient, times(1)).search(any());
  }

  @Test
  void shouldReturnNullWhenRowDoesNotExist() {
    Map<String, Object> predicates = Map.of("column_name", "value 1");
    Set<String> projection = Set.of("returned column name");
    mockSearch(new SearchHit[0]);

    Map<String, Object> row = singleRowQuery.executeQuery("index_name", predicates, projection);

    assertThat(row, nullValue());
  }

  @Test
  void shouldThrowExceptionWhenMoreThanOneRowIsFound() {
    Map<String, Object> predicates = Map.of("column_name", "value 1");
    Set<String> projection = Set.of("returned column name");
    mockSearch(new SearchHit[2]);

    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () -> singleRowQuery.executeQuery("index_name", predicates, projection));

    assertThat(ex.getMessage(), Matchers.containsString("too many hits"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"table_1", "other_table", "yet_another_table"})
  void shouldQueryCorrectTable(String tableName) {
    Map<String, Object> predicates = Map.of("column_name", "value row criteria");
    Set<String> projection = Set.of("returned column name");

    mockSearch(new SearchHit[0]);

    Map<String, Object> row = singleRowQuery.executeQuery(tableName, predicates, projection);

    SearchRequest searchRequest = searchRequestCaptor.getValue();
    assertThat(searchRequest.indices(), equalTo(new String[] {tableName}));
    assertThat(row, nullValue());
  }

  @ParameterizedTest
  @CsvSource({
    "field,value row criteria,fetched_column_name_one",
    "column,another value row criteria,another_fetched_column_name",
    "attribute,yet another value row criteria,third_fetched_column_name",
    "regular_name,one_2_three,this_is_the_fetched_column_name",
    "extra_ordinary_column_name,I am an expected value,my_name_is_the_fetched_column",
    "nice_column_name,last value row criteria,my_nice_fetched_column",
  })
  public void shouldBuildOpenSearchQueryForOnePredicate(
      String columnName, String valuePredicate, String projection) {
    Map<String, Object> predicates = Map.of(columnName, valuePredicate);
    mockSearch(new SearchHit[0]);

    Map<String, Object> row =
        singleRowQuery.executeQuery("index_name", predicates, Set.of(projection));

    SearchRequest searchRequest = searchRequestCaptor.getValue();
    BoolQueryBuilder filterQueryForSinglePredicate = new BoolQueryBuilder();
    filterQueryForSinglePredicate.should(new TermQueryBuilder(columnName, valuePredicate));
    filterQueryForSinglePredicate.should(new MatchQueryBuilder(columnName, valuePredicate));
    filterQueryForSinglePredicate.minimumShouldMatch(1);
    BoolQueryBuilder expectedQuery = new BoolQueryBuilder();
    expectedQuery.filter(filterQueryForSinglePredicate);

    assertThat(searchRequest.source().query(), equalTo(expectedQuery));
    assertThat(searchRequest.source().size(), equalTo(2));
    assertThat(searchRequest.source().fetchSource().includes(), equalTo(new String[] {projection}));
    assertThat(searchRequest.source().fetchSource().excludes(), emptyArray());
    assertThat(row, nullValue());
  }

  @ParameterizedTest
  @CsvSource({
    "columnName1,columnName2,columnName3",
    "columnName4,columnName5,columnName6",
    "columnName,columnName8,columnName9",
    "extraOrdinaryOne,eXtraOrdinaryTwo,extraOrdinaryThree"
  })
  void shouldFetchVariousColumns(String columnOne, String columnTwo, String columnThree) {
    Map<String, Object> predicates = Map.of("find_only_row", "with_value_abc");
    mockSearch(new SearchHit[0]);

    Map<String, Object> row =
        singleRowQuery.executeQuery(
            "index_name", predicates, Set.of(columnOne, columnTwo, columnThree));

    assertThat(row, nullValue());
  }

  @ParameterizedTest
  @MethodSource("variousPredicates")
  void shouldUseComplexPredicate(Map<String, Object> predicates) {
    Set<String> projection = Set.of("returned column name");
    mockSearch(new SearchHit[0]);

    Map<String, Object> row = singleRowQuery.executeQuery("index_name", predicates, projection);

    SearchRequest searchRequest = searchRequestCaptor.getValue();
    BoolQueryBuilder expectedQuery = new BoolQueryBuilder();
    predicates.entrySet().stream()
        .map(
            entry -> {
              String columnName = entry.getKey();
              Object value = entry.getValue();
              BoolQueryBuilder filterQueryForSinglePredicate = new BoolQueryBuilder();
              filterQueryForSinglePredicate.should(new TermQueryBuilder(columnName, value));
              filterQueryForSinglePredicate.should(new MatchQueryBuilder(columnName, value));
              filterQueryForSinglePredicate.minimumShouldMatch(1);
              return filterQueryForSinglePredicate;
            })
        .forEach(expectedQuery::filter);

    assertThat(searchRequest.source().query(), equalTo(expectedQuery));
    assertThat(row, nullValue());
  }

  static Stream<Arguments> variousPredicates() {
    return Stream.of(
        Arguments.of(Map.of("column_name_1", "value row criteria_12")),
        Arguments.of(
            Map.of(
                "column_name_2", "value row criteria_23", "another_column_5", "another value_8")),
        Arguments.of(
            Map.of(
                "column_name_3",
                "value row criteria_34",
                "another_column_6",
                "another value_8",
                "yet_another_column_11",
                "yet another value_13")),
        Arguments.of(
            Map.of(
                "column_name_4",
                "value row criteria_45",
                "another_column_7",
                "another value_10",
                "yet_another_column_12",
                "yet another value_14",
                "extra_column_15",
                "extra value_16")));
  }

  @Test
  public void shouldReturnRow() {
    Map<String, Object> predicates = Map.of("column_name", "value row criteria");
    Set<String> projection = Set.of("returned column name");
    Map<String, Object> searchResult =
        Map.of("column_name", "value row criteria", "returned column name", "value 2");
    mockSearch(searchResult);

    Map<String, Object> row = singleRowQuery.executeQuery("index_name", predicates, projection);

    assertThat(row, equalTo(searchResult));
  }

  @Test
  public void shouldTreatProjectionAsOptionalParameter() {
    Map<String, Object> predicates = Map.of("column_name", "value row criteria");
    Set<String> projection = null;
    Map<String, Object> searchResult =
        Map.of("column_name", "value row criteria", "returned column name", "value 2");
    mockSearch(searchResult);

    Map<String, Object> row = singleRowQuery.executeQuery("index_name", predicates, projection);

    assertThat(row, equalTo(searchResult));
  }

  private void mockSearch(Map<String, Object> searchResult) {
    when(hit.getSourceAsMap()).thenReturn(searchResult);
    mockSearch(new SearchHit[] {hit});
  }

  private void mockSearch(SearchHit[] searchResult) {
    when(nodeClient.search(searchRequestCaptor.capture())).thenReturn(searchFuture);
    when(searchFuture.actionGet()).thenReturn(response);
    when(response.getHits()).thenReturn(searchHits);
    when(searchHits.getHits()).thenReturn(searchResult);
  }
}
