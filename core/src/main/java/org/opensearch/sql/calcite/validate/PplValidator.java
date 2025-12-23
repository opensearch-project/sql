/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static org.opensearch.sql.calcite.validate.ValidationUtils.createUDTWithAttributes;

import java.util.List;
import java.util.function.Function;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
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
   * Create a PPL-specific SQL validator configured with the given catalog, operator table, type factory, and validator settings.
   *
   * @param statement       Calcite server statement used to build a prepare context and resolve the root schema
   * @param frameworkConfig Framework configuration providing the default schema
   * @param operatorTable   SQL operator table to use for validation
   * @param typeFactory     type factory for constructing SQL types and catalog reader
   * @param validatorConfig validator configuration to pass to the created PPL validator
   * @return                a configured PplValidator instance
   */
  public static PplValidator create(
      CalciteServerStatement statement,
      FrameworkConfig frameworkConfig,
      SqlOperatorTable operatorTable,
      RelDataTypeFactory typeFactory,
      SqlValidator.Config validatorConfig) {
    SchemaPlus defaultSchema = frameworkConfig.getDefaultSchema();

    final CalcitePrepare.Context prepareContext = statement.createPrepareContext();
    final CalciteSchema schema =
        defaultSchema != null ? CalciteSchema.from(defaultSchema) : prepareContext.getRootSchema();
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            schema.root(), schema.path(null), typeFactory, prepareContext.config());
    return new PplValidator(operatorTable, catalogReader, typeFactory, validatorConfig);
  }

  /**
   * Constructs a PPL-specific SqlValidator with the given operator table, catalog reader, type factory, and configuration,
   * and initializes the validator to treat subsequent type derivations as top-level.
   *
   * @param opTab operator table containing PPL operators
   * @param catalogReader catalog reader used to resolve schema and object information
   * @param typeFactory factory used to create and manipulate SQL type instances
   * @param config validator configuration options
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
   * Derives the type of a SQL expression while mapping between SQL types and OpenSearch user-defined types (UDTs) for PPL validation.
   *
   * @param scope the validation scope in which the expression is resolved
   * @param expr  the SQL expression whose type is being derived
   * @return the derived RelDataType; at top-level derivations this is converted to a user-defined type, otherwise it is converted to a SQL type used for internal validation
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

  /**
   * Get the validated type for a SQL node, mapped to OpenSearch user-defined types.
   *
   * @param node the SQL node whose validated type to retrieve
   * @return the node's validated {@code RelDataType} with SQL types converted to OpenSearch user-defined types, or {@code null} if the type is unknown
   */
  @Override
  public @Nullable RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
    RelDataType type = super.getValidatedNodeTypeIfKnown(node);
    return sqlTypeToUserDefinedType(type);
  }

  /**
   * Disables nullary calls to avoid treating identifiers like field references as function calls.
   *
   * <p>PPL does not use functions without parentheses (nullary functions). Allowing nullary calls
   * would cause identifiers such as USER or LOCALTIME to be resolved as function calls before being
   * treated as field references.
   *
   * @see SqlValidatorImpl.Expander#visit(SqlIdentifier)
   * @return always {@code null}
   */
  @Override
  public @Nullable SqlCall makeNullaryCall(SqlIdentifier id) {
    return null;
  }

  /**
   * Converts OpenSearch user-defined RelDataType instances to their equivalent SQL types.
   *
   * <p>Maps UDTs for timestamp, time, date, and binary to the corresponding SQL types preserving
   * nullability; maps the OpenSearch IP UDT to the nullable IP UDT constant; returns the input
   * type unchanged if no mapping applies.
   *
   * @param type the input RelDataType, possibly a user-defined type
   * @return the corresponding SQL RelDataType or the original type if no conversion is required
   */
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

  /**
   * Convert SQL types to OpenSearch user-defined types (UDTs) where applicable.
   *
   * <p>Recursively traverses the input type and replaces SQL TIMESTAMP, TIME, DATE, and BINARY
   * types with corresponding OpenSearch UDTs that carry the original type's attributes; all other
   * types are preserved.
   *
   * @param type the input type to convert; may be a complex/compound RelDataType
   * @return a RelDataType with SQL temporal and binary types mapped to OpenSearch UDTs, or the
   *         original type structure if no mappings apply
   */
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

  /**
   * Recursively applies a conversion function to a RelDataType and its nested component types, preserving structure and nullability.
   *
   * <p>For record types, each field type is converted and a new struct is created with the original field
   * names and the record's nullability. For array, map, and multiset types, their component/key/value
   * types are converted and the original collection nullability is preserved. For other types, the
   * conversion function is applied directly.
   *
   * @param type the input type to convert; may be {@code null}
   * @param convert function that maps a RelDataType to another RelDataType
   * @return the converted RelDataType with the same composite structure and nullability as {@code type},
   *     or {@code null} if {@code type} is {@code null}
   */
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