/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.test.rest.yaml.ClientYamlTestCandidate;
import org.opensearch.test.rest.yaml.OpenSearchClientYamlSuiteTestCase;

public class RestHandlerClientYamlTestSuiteIT extends OpenSearchClientYamlSuiteTestCase {

  public RestHandlerClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
    super(testCandidate);
  }

  @ParametersFactory
  public static Iterable<Object[]> parameters() throws Exception {
    return OpenSearchClientYamlSuiteTestCase.createParameters();
  }
}
