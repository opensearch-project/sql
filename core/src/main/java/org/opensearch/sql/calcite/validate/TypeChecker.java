/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.tools.FrameworkConfig;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

/**
 * Utility class for creating and configuring SqlValidator instances for PPL validation.
 *
 * <p>This class provides factory methods to create validators with custom type coercion rules and
 * PPL-specific operator tables.
 */
public class TypeChecker {

  /**
   * Creates a SqlValidator configured for PPL validation.
   *
   * @param statement Calcite server statement
   * @param config Framework configuration
   * @param operatorTable SQL operator table to use for validation
   * @return configured SqlValidator instance
   */
  public static SqlValidator getValidator(
      CalciteServerStatement statement, FrameworkConfig config, SqlOperatorTable operatorTable) {
    SchemaPlus defaultSchema = config.getDefaultSchema();

    final CalcitePrepare.Context prepareContext = statement.createPrepareContext();
    final CalciteSchema schema =
        defaultSchema != null ? CalciteSchema.from(defaultSchema) : prepareContext.getRootSchema();
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            schema.root(),
            schema.path(null),
            OpenSearchTypeFactory.TYPE_FACTORY,
            prepareContext.config());
    SqlValidator.Config validatorConfig =
        SqlValidator.Config.DEFAULT
            .withTypeCoercionRules(getTypeCoercionRule())
            .withTypeCoercionFactory(TypeChecker::createTypeCoercion)
            // Use lenient conformance for PPL compatibility
            .withConformance(OpenSearchSparkSqlDialect.DEFAULT.getConformance())
            // Use Spark SQL's NULL collation (NULLs sorted LOW/FIRST)
            .withDefaultNullCollation(NullCollation.LOW);
    return new PplValidator(
        operatorTable, catalogReader, OpenSearchTypeFactory.TYPE_FACTORY, validatorConfig);
  }

  /**
   * Gets the type coercion rules for PPL.
   *
   * @return SqlTypeCoercionRule instance
   */
  public static SqlTypeCoercionRule getTypeCoercionRule() {
    return PplTypeCoercionRule.instance();
  }

  /**
   * Creates a custom TypeCoercion instance for PPL. This can be used as a TypeCoercionFactory.
   *
   * @param typeFactory the type factory
   * @param validator the SQL validator
   * @return custom PplTypeCoercion instance
   */
  public static TypeCoercion createTypeCoercion(
      RelDataTypeFactory typeFactory, SqlValidator validator) {
    return new PplTypeCoercion(typeFactory, validator);
  }
}
