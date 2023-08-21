package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;

@ExtendWith(MockitoExtension.class)
class PushDownQueryBuilderTest {
  @Test
  void default_implementations() {
    var sample =
        new PushDownQueryBuilder() {
          @Override
          public OpenSearchRequestBuilder build() {
            return null;
          }
        };
    assertAll(
        () -> assertFalse(sample.pushDownFilter(mock(LogicalFilter.class))),
        () -> assertFalse(sample.pushDownProject(mock(LogicalProject.class))),
        () -> assertFalse(sample.pushDownHighlight(mock(LogicalHighlight.class))),
        () -> assertFalse(sample.pushDownSort(mock(LogicalSort.class))),
        () -> assertFalse(sample.pushDownNested(mock(LogicalNested.class))),
        () -> assertFalse(sample.pushDownLimit(mock(LogicalLimit.class))),
        () -> assertFalse(sample.pushDownPageSize(mock(LogicalPaginate.class))));
  }
}
