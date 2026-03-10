/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static org.opensearch.sql.calcite.validate.ValidationUtils.createUDTWithAttributes;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.tools.FrameworkConfig;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * Custom SQL validator for PPL queries.
 *
 * <p>This validator extends Calcite's default SqlValidatorImpl to provide PPL-specific validation
 * behavior. Currently, it uses the default implementation but can be extended in the future to add
 * PPL-specific validation rules.
 */
public class PplValidator extends SqlValidatorImpl {
  /**
   * Tracks whether the current deriveType call is at the top level (true) or a recursive call
   * (false). Top-level calls return user-defined types, while recursive calls return SQL types for
   * internal validation.
   */
  private boolean top;

  /**
   * Creates a SqlValidator configured for PPL validation.
   *
   * @param frameworkConfig Framework configuration
   * @param operatorTable SQL operator table to use for validation
   * @param typeFactory Type factory for creating data types
   * @param validatorConfig Validator configuration settings
   * @return configured SqlValidator instance
   */
  public static PplValidator create(
      FrameworkConfig frameworkConfig,
      SqlOperatorTable operatorTable,
      RelDataTypeFactory typeFactory,
      SqlValidator.Config validatorConfig) {
    SchemaPlus defaultSchema =
        Objects.requireNonNull(frameworkConfig.getDefaultSchema(), "defaultSchema");

    final CalciteSchema schema = CalciteSchema.from(defaultSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            schema.root(), schema.path(null), typeFactory, CalciteConnectionConfig.DEFAULT);
    return new PplValidator(operatorTable, catalogReader, typeFactory, validatorConfig);
  }

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
    top = true;
  }

  /**
   * Overrides the deriveType method to map user-defined types (UDTs) to SqlTypes so that they can
   * be validated
   */
  @Override
  public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
    // The type has to be sql type during type derivation & validation
    boolean original = top;
    top = false;
    RelDataType type = super.deriveType(scope, expr);
    top = original;
    if (top) {
      return sqlTypeToUserDefinedType(type);
    }
    return userDefinedTypeToSqlType(type);
  }

  @Override
  public @Nullable RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
    RelDataType type = super.getValidatedNodeTypeIfKnown(node);
    return sqlTypeToUserDefinedType(type);
  }

  /**
   * Disable nullary call to not confuse with field reference.
   *
   * <p>It was originally designed for function calls that have no arguments and require no
   * parentheses (for example "CURRENT_USER"). However, PPL does not have such use cases. Besides,
   * as nullary calls are resolved before field reference, this will make field references with name
   * like USER, LOCALTIME to function calls in an unwanted but subtle way.
   *
   * @see SqlValidatorImpl.Expander#visit(SqlIdentifier)
   */
  @Override
  public @Nullable SqlCall makeNullaryCall(SqlIdentifier id) {
    return null;
  }

  private RelDataType userDefinedTypeToSqlType(RelDataType type) {
    return convertType(
        type,
        t -> {
          if (OpenSearchTypeUtil.isUserDefinedType(t)) {
            AbstractExprRelDataType<?> exprType = (AbstractExprRelDataType<?>) t;
            ExprType udtType = exprType.getExprType();
            OpenSearchTypeFactory typeFactory = (OpenSearchTypeFactory) this.getTypeFactory();
            return switch (udtType) {
              case ExprCoreType.TIMESTAMP ->
                  typeFactory.createSqlType(SqlTypeName.TIMESTAMP, t.isNullable());
              case ExprCoreType.TIME -> typeFactory.createSqlType(SqlTypeName.TIME, t.isNullable());
              case ExprCoreType.DATE -> typeFactory.createSqlType(SqlTypeName.DATE, t.isNullable());
              case ExprCoreType.BINARY ->
                  typeFactory.createSqlType(SqlTypeName.BINARY, t.isNullable());
              case ExprCoreType.IP -> UserDefinedFunctionUtils.NULLABLE_IP_UDT;
              default -> t;
            };
          }
          return t;
        });
  }

  private RelDataType sqlTypeToUserDefinedType(RelDataType type) {
    return convertType(
        type,
        t -> {
          OpenSearchTypeFactory typeFactory = (OpenSearchTypeFactory) this.getTypeFactory();
          return switch (t.getSqlTypeName()) {
            case TIMESTAMP -> createUDTWithAttributes(typeFactory, t, ExprUDT.EXPR_TIMESTAMP);
            case TIME -> createUDTWithAttributes(typeFactory, t, ExprUDT.EXPR_TIME);
            case DATE -> createUDTWithAttributes(typeFactory, t, ExprUDT.EXPR_DATE);
            case BINARY -> createUDTWithAttributes(typeFactory, t, ExprUDT.EXPR_BINARY);
            default -> t;
          };
        });
  }

  private RelDataType convertType(RelDataType type, Function<RelDataType, RelDataType> convert) {
    if (type == null) return null;

    if (type instanceof RelRecordType recordType) {
      List<RelDataType> subTypes =
          recordType.getFieldList().stream().map(RelDataTypeField::getType).map(convert).toList();
      return typeFactory.createTypeWithNullability(
          typeFactory.createStructType(subTypes, recordType.getFieldNames()),
          recordType.isNullable());
    }
    if (type instanceof ArraySqlType arrayType) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createArrayType(convert.apply(arrayType.getComponentType()), -1),
          arrayType.isNullable());
    }
    if (type instanceof MapSqlType mapType) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createMapType(
              convert.apply(mapType.getKeyType()), convert.apply(mapType.getValueType())),
          mapType.isNullable());
    }
    if (type instanceof MultisetSqlType multisetType) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createMultisetType(convert.apply(multisetType.getComponentType()), -1),
          multisetType.isNullable());
    }

    return convert.apply(type);
  }
}
