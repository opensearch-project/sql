/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.opensearch.sql.datasource.model.DataSourceType;

@AllArgsConstructor
public class GrammarElementValidatorProvider {

  private final Map<DataSourceType, GrammarElementValidator> validatorMap;
  private final GrammarElementValidator defaultValidator;

  public GrammarElementValidator getValidatorForDatasource(DataSourceType dataSourceType) {
    return validatorMap.getOrDefault(dataSourceType, defaultValidator);
  }
}
