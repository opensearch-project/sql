/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

/**
 * Custom SQL validator for PPL queries.
 *
 * <p>This validator extends Calcite's default SqlValidatorImpl to provide PPL-specific validation
 * behavior. Currently, it uses the default implementation but can be extended in the future to add
 * PPL-specific validation rules.
 */
public class PplValidator extends SqlValidatorImpl {
  /**
   * Creates a PPL validator.
   *
   * @param opTab Operator table containing PPL operators
   * @param catalogReader Catalog reader for accessing schema information
   * @param typeFactory Type factory for creating type information
   * @param config Validator configuration
   */
  protected PplValidator(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      Config config) {
    super(opTab, catalogReader, typeFactory, config);
  }
}
