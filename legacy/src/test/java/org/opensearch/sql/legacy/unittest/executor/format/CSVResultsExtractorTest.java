/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.executor.format;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.opensearch.sql.legacy.executor.csv.CSVResult;
import org.opensearch.sql.legacy.executor.csv.CSVResultsExtractor;
import org.opensearch.sql.legacy.executor.csv.CsvExtractorException;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;

public class CSVResultsExtractorTest {
  private final CSVResultsExtractor csvResultsExtractor = new CSVResultsExtractor(false, false);

  @Test
  public void extractResultsFromBindingTupleListShouldPass() throws CsvExtractorException {
    CSVResult csvResult =
        csv(
            Arrays.asList(
                BindingTuple.from(ImmutableMap.of("age", 31, "gender", "m")),
                BindingTuple.from(ImmutableMap.of("age", 31, "gender", "f")),
                BindingTuple.from(ImmutableMap.of("age", 39, "gender", "m")),
                BindingTuple.from(ImmutableMap.of("age", 39, "gender", "f"))),
            Arrays.asList("age", "gender"));

    assertThat(csvResult.getHeaders(), contains("age", "gender"));
    assertThat(csvResult.getLines(), contains("31,m", "31,f", "39,m", "39,f"));
  }

  private CSVResult csv(List<BindingTuple> bindingTupleList, List<String> fieldNames)
      throws CsvExtractorException {
    return csvResultsExtractor.extractResults(bindingTupleList, false, ",", fieldNames);
  }
}
