/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.externalize.RelEnumTypes;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsertKeyword;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * An extension to {@link RelJson} to allow serialization & deserialization of UDTs
 *
 * <p>It replicates a lot of methods from {@link RelJson} because we can not override some private
 * methods and because we cannot create an instance of ExtendedRelJson with the custom values for
 * private final fields not included in public constructors of RelJson. For example, there is no
 * public constructor with inputTranslator and operatorTable. As a result, every method that make
 * use of these private fields has to be copied in ExtendedRelJson.
 */
public class ExtendedRelJson extends RelJson {
  private final JsonBuilder jsonBuilder;
  private final InputTranslator inputTranslator;
  private final SqlOperatorTable operatorTable;

  /**
   * Registry of enum classes that can be serialized to JSON, replicated from {@link RelEnumTypes}
   * as toEnum(String) method is package private
   */
  private static final ImmutableMap<String, Enum<?>> ENUM_BY_NAME;

  static {
    // Build a mapping from enum constants to the enum instances, same as RelEnumTypes
    final ImmutableMap.Builder<String, Enum<?>> enumByName = ImmutableMap.builder();
    registerEnum(enumByName, JoinConditionType.class);
    registerEnum(enumByName, JoinType.class);
    registerEnum(enumByName, RexUnknownAs.class);
    registerEnum(enumByName, SqlExplain.Depth.class);
    registerEnum(enumByName, SqlExplainFormat.class);
    registerEnum(enumByName, SqlExplainLevel.class);
    registerEnum(enumByName, SqlInsertKeyword.class);
    registerEnum(enumByName, SqlJsonConstructorNullClause.class);
    registerEnum(enumByName, SqlJsonQueryWrapperBehavior.class);
    registerEnum(enumByName, SqlJsonValueEmptyOrErrorBehavior.class);
    registerEnum(enumByName, SqlMatchRecognize.AfterOption.class);
    registerEnum(enumByName, SqlSelectKeyword.class);
    registerEnum(enumByName, SqlTrimFunction.Flag.class);
    registerEnum(enumByName, TimeUnitRange.class);
    registerEnum(enumByName, TableModify.Operation.class);
    ENUM_BY_NAME = enumByName.build();
  }

  private static void registerEnum(
      ImmutableMap.Builder<String, Enum<?>> builder, Class<? extends Enum<?>> enumClass) {
    for (Enum<?> enumConstant : enumClass.getEnumConstants()) {
      builder.put(enumConstant.name(), enumConstant);
    }
  }

  private ExtendedRelJson(JsonBuilder jsonBuilder) {
    super(jsonBuilder);
    this.jsonBuilder = jsonBuilder;
    this.inputTranslator = null;
    this.operatorTable = SqlStdOperatorTable.instance();
  }

  private ExtendedRelJson(
      @Nullable JsonBuilder jsonBuilder,
      InputTranslator inputTranslator,
      SqlOperatorTable operatorTable) {
    super(jsonBuilder);
    this.jsonBuilder = jsonBuilder;
    this.inputTranslator = requireNonNull(inputTranslator, "inputTranslator");
    this.operatorTable = requireNonNull(operatorTable, "operatorTable");
  }

  /** Creates a ExtendedRelJson. */
  public static ExtendedRelJson create(JsonBuilder jsonBuilder) {
    return new ExtendedRelJson(jsonBuilder);
  }

  @Override
  public RelJson withInputTranslator(InputTranslator inputTranslator) {
    if (inputTranslator == this.inputTranslator) {
      return this;
    }
    return new ExtendedRelJson(jsonBuilder, inputTranslator, operatorTable);
  }

  @Override
  public RelJson withOperatorTable(SqlOperatorTable operatorTable) {
    if (operatorTable == this.operatorTable) {
      return this;
    }
    return new ExtendedRelJson(jsonBuilder, inputTranslator, operatorTable);
  }

  @Override
  public @Nullable Object toJson(@Nullable Object value) {
    if (value instanceof RelDataTypeField) {
      return toJson((RelDataTypeField) value);
    } else if (value instanceof RelDataType) {
      return toJson((RelDataType) value);
    }
    return super.toJson(value);
  }

  // Copied from RelJson since many overrides of toJson are private, thus not overridable.
  @Override
  public Object toJson(RexNode node) {
    final Map<String, @Nullable Object> map;
    switch (node.getKind()) {
      case DYNAMIC_PARAM:
        map = jsonBuilder().map();
        final RexDynamicParam rexDynamicParam = (RexDynamicParam) node;
        final RelDataType rdpType = rexDynamicParam.getType();
        map.put("dynamicParam", rexDynamicParam.getIndex());
        map.put("type", toJson(rdpType));
        return map;
      case FIELD_ACCESS:
        map = jsonBuilder().map();
        final RexFieldAccess fieldAccess = (RexFieldAccess) node;
        map.put("field", fieldAccess.getField().getName());
        map.put("expr", toJson(fieldAccess.getReferenceExpr()));
        return map;
      case LITERAL:
        final RexLiteral literal = (RexLiteral) node;
        final Object value = literal.getValue3();
        map = jsonBuilder().map();
        //noinspection rawtypes
        map.put(
            "literal", value instanceof Enum ? RelEnumTypes.fromEnum((Enum) value) : toJson(value));
        map.put("type", toJson(node.getType()));
        return map;
      case INPUT_REF:
        // reduce copy when possible
        // noinspection unchecked cast
        map = (Map<String, @Nullable Object>) super.toJson(node);
        return map;
      case LOCAL_REF:
        map = jsonBuilder().map();
        map.put("input", ((RexSlot) node).getIndex());
        map.put("name", ((RexSlot) node).getName());
        map.put("type", toJson(node.getType()));
        return map;
      case CORREL_VARIABLE:
        map = jsonBuilder().map();
        map.put("correl", ((RexCorrelVariable) node).getName());
        map.put("type", toJson(node.getType()));
        return map;
      default:
        if (node instanceof RexCall) {
          final RexCall call = (RexCall) node;
          map = jsonBuilder().map();
          map.put("op", toJson(call.getOperator()));
          final List<@Nullable Object> list = jsonBuilder().list();
          for (RexNode operand : call.getOperands()) {
            list.add(toJson(operand));
          }
          map.put("operands", list);
          switch (node.getKind()) {
            case MINUS:
            case CAST:
            case SAFE_CAST:
              map.put("type", toJson(node.getType()));
              break;
            default:
              break;
          }
          if (call.getOperator() instanceof SqlFunction) {
            if (((SqlFunction) call.getOperator()).getFunctionType().isUserDefined()) {
              SqlOperator op = call.getOperator();
              map.put("class", op.getClass().getName());
              map.put("type", toJson(node.getType()));
              map.put("deterministic", op.isDeterministic());
              map.put("dynamic", op.isDynamicFunction());
            }
          }
          if (call instanceof RexOver) {
            RexOver over = (RexOver) call;
            map.put("distinct", over.isDistinct());
            map.put("type", toJson(node.getType()));
            map.put("window", toJson(over.getWindow()));
          }
          return map;
        }
        throw new UnsupportedOperationException("unknown rex " + node);
    }
  }

  // Copied from RelJson as its private but used in toJson(RexNode)
  private Object toJson(RelDataTypeField node) {
    final Map<String, @Nullable Object> map;
    if (node.getType().isStruct()) {
      map = jsonBuilder().map();
      map.put("fields", toJson(node.getType()));
      map.put("nullable", node.getType().isNullable());
    } else {
      //noinspection unchecked
      map = (Map<String, @Nullable Object>) toJson(node.getType());
    }
    map.put("name", node.getName());
    return map;
  }

  /** Modifies behavior for AbstractExprRelDataType instances, delegates to RelJson otherwise. */
  private Object toJson(RelDataType node) {
    final Map<String, @Nullable Object> map = jsonBuilder().map();
    if (node.isStruct()) {
      final List<@Nullable Object> list = jsonBuilder().list();
      for (RelDataTypeField field : node.getFieldList()) {
        list.add(toJson(field));
      }
      map.put("fields", list);
      map.put("nullable", node.isNullable());
    } else {
      // For UDT like EXPR_TIMESTAMP, we additionally save its UDT info as a tag.
      if (node instanceof AbstractExprRelDataType) {
        map.put("udt", ((AbstractExprRelDataType<?>) node).getUdt().name());
      }
      map.put("type", node.getSqlTypeName().name());
      map.put("nullable", node.isNullable());
      if (node.getComponentType() != null) {
        map.put("component", toJson(node.getComponentType()));
      }
      RelDataType keyType = node.getKeyType();
      if (keyType != null) {
        map.put("key", toJson(keyType));
      }
      RelDataType valueType = node.getValueType();
      if (valueType != null) {
        map.put("value", toJson(valueType));
      }
      if (node.getSqlTypeName().allowsPrec()) {
        map.put("precision", node.getPrecision());
      }
      if (node.getSqlTypeName().allowsScale()) {
        map.put("scale", node.getScale());
      }
    }
    return map;
  }

  // Copied from RelJson since its private but used in toJson(RexNode)
  private Map<String, @Nullable Object> toJson(SqlOperator operator) {
    // User-defined operators are not yet handled.
    Map<String, @Nullable Object> map = jsonBuilder().map();
    map.put("name", operator.getName());
    map.put("kind", operator.kind.toString());
    map.put("syntax", operator.getSyntax().toString());
    return map;
  }

  // Copied from RelJson since its private but used in toJson(RexNode)
  private Object toJson(RexWindow window) {
    final Map<String, @Nullable Object> map = jsonBuilder().map();
    if (!window.partitionKeys.isEmpty()) {
      map.put("partition", toJson(window.partitionKeys));
    }
    if (!window.orderKeys.isEmpty()) {
      map.put("order", toJson(window.orderKeys));
    }
    if (window.getLowerBound() == null) {
      // No ROWS or RANGE clause
    } else if (window.getUpperBound() == null) {
      if (window.isRows()) {
        map.put("rows-lower", toJson(window.getLowerBound()));
      } else {
        map.put("range-lower", toJson(window.getLowerBound()));
      }
    } else {
      if (window.isRows()) {
        map.put("rows-lower", toJson(window.getLowerBound()));
        map.put("rows-upper", toJson(window.getUpperBound()));
      } else {
        map.put("range-lower", toJson(window.getLowerBound()));
        map.put("range-upper", toJson(window.getUpperBound()));
      }
    }
    return map;
  }

  /**
   * Reconstruct a RelDataType from a json map. It overrides {@link
   * RelJson#toType(RelDataTypeFactory, Object)} to handle the reconstruction of user-defined types
   * (UDTs).
   */
  @Override
  public RelDataType toType(RelDataTypeFactory typeFactory, Object o) {
    if (o instanceof Map
        && ((Map<?, ?>) o).containsKey("udt")
        && typeFactory instanceof OpenSearchTypeFactory) {
      // Reconstruct UDT from its udt tag
      Object udtName = ((Map<?, ?>) o).get("udt");
      OpenSearchTypeFactory.ExprUDT udt = OpenSearchTypeFactory.ExprUDT.valueOf((String) udtName);
      // View IP as string to avoid using a value of customized java type in the script.
      if (udt == ExprUDT.EXPR_IP) return super.toType(typeFactory, o);
      RelDataType type = ((OpenSearchTypeFactory) typeFactory).createUDT(udt);
      boolean nullable = (Boolean) ((Map<?, ?>) o).get("nullable");
      return typeFactory.createTypeWithNullability(type, nullable);
    }
    return super.toType(typeFactory, o);
  }

  /**
   * Reconstruct a RexNode from a Json object using the provided cluster context.
   *
   * <p>This method overrides {@link RelJson#toRex(RelOptCluster, Object)} to use our custom {@code
   * toRex(RelInput, Object)} implementation which supports UDT deserialization and uses our own
   * {@code inputTranslator} and {@code operatorTable} instances.
   */
  @Override
  public RexNode toRex(RelOptCluster cluster, Object o) {
    RelInput input = new RelInputForCluster(cluster);
    return toRex(input, o);
  }

  // Copied from RelJson for the access of custom inputTranslator and operatorTable
  @SuppressWarnings({"rawtypes", "unchecked"})
  @PolyNull
  RexNode toRex(RelInput relInput, @PolyNull Object o) {
    final RelOptCluster cluster = relInput.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (o == null) {
      return null;
      // Support JSON deserializing of non-default Map classes such as gson LinkedHashMap
    } else if (Map.class.isAssignableFrom(o.getClass())) {
      final Map<String, @Nullable Object> map = (Map) o;
      final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      if (map.containsKey("op")) {
        final Map<String, @Nullable Object> opMap = get(map, "op");
        if (map.containsKey("class")) {
          opMap.put("class", get(map, "class"));
        }
        final List operands = get(map, "operands");
        final List<RexNode> rexOperands = toRexList(relInput, operands);
        final Object jsonType = map.get("type");
        final Map window = (Map) map.get("window");
        if (window != null) {
          final SqlAggFunction operator = requireNonNull(toAggregation(opMap), "operator");
          final RelDataType type = toType(typeFactory, requireNonNull(jsonType, "jsonType"));
          List<RexNode> partitionKeys = new ArrayList<>();
          Object partition = window.get("partition");
          if (partition != null) {
            partitionKeys = toRexList(relInput, (List) partition);
          }
          List<RexFieldCollation> orderKeys = new ArrayList<>();
          if (window.containsKey("order")) {
            addRexFieldCollationList(orderKeys, relInput, (List) window.get("order"));
          }
          final RexWindowBound lowerBound;
          final RexWindowBound upperBound;
          final boolean physical;
          if (window.get("rows-lower") != null) {
            lowerBound = toRexWindowBound(relInput, (Map) window.get("rows-lower"));
            upperBound = toRexWindowBound(relInput, (Map) window.get("rows-upper"));
            physical = true;
          } else if (window.get("range-lower") != null) {
            lowerBound = toRexWindowBound(relInput, (Map) window.get("range-lower"));
            upperBound = toRexWindowBound(relInput, (Map) window.get("range-upper"));
            physical = false;
          } else {
            // No ROWS or RANGE clause
            // Note: lower and upper bounds are non-nullable, so this branch is not reachable
            lowerBound = null;
            upperBound = null;
            physical = false;
          }
          final RexWindowExclusion exclude;
          if (window.get("exclude") != null) {
            exclude = toRexWindowExclusion((Map) window.get("exclude"));
          } else {
            exclude = RexWindowExclusion.EXCLUDE_NO_OTHER;
          }
          final boolean distinct = get(map, "distinct");
          return rexBuilder.makeOver(
              type,
              operator,
              rexOperands,
              partitionKeys,
              ImmutableList.copyOf(orderKeys),
              requireNonNull(lowerBound, "lowerBound"),
              requireNonNull(upperBound, "upperBound"),
              requireNonNull(exclude, "exclude"),
              physical,
              true,
              false,
              distinct,
              false);
        } else {
          final SqlOperator operator = requireNonNull(toOp(opMap), "operator");
          final RelDataType type;
          if (jsonType != null) {
            type = toType(typeFactory, jsonType);
          } else {
            type = rexBuilder.deriveReturnType(operator, rexOperands);
          }
          return rexBuilder.makeCall(type, operator, rexOperands);
        }
      }
      final Integer input = (Integer) map.get("input");
      if (input != null) {
        return inputTranslator.translateInput(this, input, map, relInput);
      }
      final String field = (String) map.get("field");
      if (field != null) {
        final Object jsonExpr = get(map, "expr");
        final RexNode expr = toRex(relInput, jsonExpr);
        return rexBuilder.makeFieldAccess(expr, field, true);
      }
      final String correl = (String) map.get("correl");
      if (correl != null) {
        final Object jsonType = get(map, "type");
        RelDataType type = toType(typeFactory, jsonType);
        return rexBuilder.makeCorrel(type, new CorrelationId(correl));
      }
      if (map.containsKey("literal")) {
        Object literal = map.get("literal");
        if (literal == null) {
          final RelDataType type = toType(typeFactory, get(map, "type"));
          return rexBuilder.makeNullLiteral(type);
        }
        if (!map.containsKey("type")) {
          // In previous versions, type was not specified for all literals.
          // To keep backwards compatibility, if type is not specified
          // we just interpret the literal
          return toRex(relInput, literal);
        }
        final RelDataType type = toType(typeFactory, get(map, "type"));
        if (literal instanceof Map && ((Map<?, ?>) literal).containsKey("rangeSet")) {
          Sarg sarg = sargFromJson((Map) literal, type);
          return rexBuilder.makeSearchArgumentLiteral(sarg, type);
        }
        if (type.getSqlTypeName() == SqlTypeName.SYMBOL) {
          literal = toEnum((String) literal);
        }
        return rexBuilder.makeLiteral(literal, type);
      }
      if (map.containsKey("sargLiteral")) {
        Object sargObject = map.get("sargLiteral");
        if (sargObject == null) {
          final RelDataType type = toType(typeFactory, get(map, "type"));
          return rexBuilder.makeNullLiteral(type);
        }
        final RelDataType type = toType(typeFactory, get(map, "type"));
        Sarg sarg = sargFromJson((Map) sargObject, type);
        return rexBuilder.makeSearchArgumentLiteral(sarg, type);
      }
      if (map.containsKey("dynamicParam")) {
        final Object dynamicParamObject = requireNonNull(map.get("dynamicParam"));
        final Integer index = (Integer) dynamicParamObject;
        final RelDataType type = toType(typeFactory, get(map, "type"));
        return rexBuilder.makeDynamicParam(type, index);
      }
      throw new UnsupportedOperationException("cannot convert to rex " + o);
    } else if (o instanceof Boolean) {
      return rexBuilder.makeLiteral((Boolean) o);
    } else if (o instanceof String) {
      return rexBuilder.makeLiteral((String) o);
    } else if (o instanceof Number) {
      final Number number = (Number) o;
      if (number instanceof Double || number instanceof Float) {
        return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(number.doubleValue()));
      } else {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(number.longValue()));
      }
    } else {
      throw new UnsupportedOperationException("cannot convert to rex " + o);
    }
  }

  private JsonBuilder jsonBuilder() {
    return requireNonNull(jsonBuilder, "jsonBuilder");
  }

  // Copied from RelJson because it's private but used in toRex(RelInput, Object)
  @SuppressWarnings("unchecked")
  private static <T extends Object> T get(Map<String, ? extends @Nullable Object> map, String key) {
    return (T) requireNonNull(map.get(key), () -> "entry for key " + key);
  }

  // Copied from RelJson for the usage of custom operatorTable
  @Nullable SqlOperator toOp(Map<String, ? extends @Nullable Object> map) {
    // in case different operator has the same kind, check with both name and kind.
    String name = get(map, "name");
    String kind = get(map, "kind");
    String syntax = get(map, "syntax");
    SqlKind sqlKind = SqlKind.valueOf(kind);
    SqlSyntax sqlSyntax = SqlSyntax.valueOf(syntax);
    List<SqlOperator> operators = new ArrayList<>();
    operatorTable.lookupOperatorOverloads(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        null,
        sqlSyntax,
        operators,
        SqlNameMatchers.liberal());
    for (SqlOperator operator : operators) {
      if (operator.kind == sqlKind) {
        return operator;
      }
    }
    String class_ = (String) map.get("class");
    if (class_ != null) {
      return AvaticaUtils.instantiatePlugin(SqlOperator.class, class_);
    }
    throw RESOURCE.noOperator(name, kind, syntax).ex();
  }

  // Copied from RelJson for the usage of custom operatorTable
  @Nullable SqlAggFunction toAggregation(Map<String, ? extends @Nullable Object> map) {
    return (SqlAggFunction) toOp(map);
  }

  // Copied from RelJson because it's private but used in toRex(RelInput, Object)
  private List<RexNode> toRexList(RelInput relInput, List operands) {
    final List<RexNode> list = new ArrayList<>();
    for (Object operand : operands) {
      list.add(toRex(relInput, operand));
    }
    return list;
  }

  // Copied from RelJson because it's private but used in toRex(RelInput, Object)
  private void addRexFieldCollationList(
      List<RexFieldCollation> list, RelInput relInput, @Nullable List<Map<String, Object>> order) {
    if (order == null) {
      return;
    }

    for (Map<String, Object> o : order) {
      RexNode expr = requireNonNull(toRex(relInput, o.get("expr")), "expr");
      Set<SqlKind> directions = new HashSet<>();
      if (RelFieldCollation.Direction.valueOf(get(o, "direction"))
          == RelFieldCollation.Direction.DESCENDING) {
        directions.add(SqlKind.DESCENDING);
      }
      if (RelFieldCollation.NullDirection.valueOf(get(o, "null-direction"))
          == RelFieldCollation.NullDirection.FIRST) {
        directions.add(SqlKind.NULLS_FIRST);
      } else {
        directions.add(SqlKind.NULLS_LAST);
      }
      list.add(new RexFieldCollation(expr, directions));
    }
  }

  /**
   * Converts a string to an enum value. Replicated from RelEnumTypes.toEnum() since it's
   * package-private.
   */
  @SuppressWarnings("unchecked")
  private static <E extends Enum<E>> E toEnum(String name) {
    return (E) requireNonNull(ENUM_BY_NAME.get(name), () -> "No enum registered for name: " + name);
  }

  // Copied from RelJson because it's private but used in toRex(RelInput, Object)
  private @Nullable RexWindowBound toRexWindowBound(
      RelInput relInput, @Nullable Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    final String type = get(map, "type");
    switch (type) {
      case "CURRENT_ROW":
        return RexWindowBounds.CURRENT_ROW;
      case "UNBOUNDED_PRECEDING":
        return RexWindowBounds.UNBOUNDED_PRECEDING;
      case "UNBOUNDED_FOLLOWING":
        return RexWindowBounds.UNBOUNDED_FOLLOWING;
      case "PRECEDING":
        return RexWindowBounds.preceding(toRex(relInput, get(map, "offset")));
      case "FOLLOWING":
        return RexWindowBounds.following(toRex(relInput, get(map, "offset")));
      default:
        throw new UnsupportedOperationException("cannot convert " + type + " to rex window bound");
    }
  }

  // Copied from RelJson because it's private but used in toRex(RelInput, Object)
  private static @Nullable RexWindowExclusion toRexWindowExclusion(
      @Nullable Map<String, Object> map) {
    if (map == null) {
      return null;
    }
    final String type = get(map, "type");
    switch (type) {
      case "CURRENT_ROW":
        return RexWindowExclusion.EXCLUDE_CURRENT_ROW;
      case "GROUP":
        return RexWindowExclusion.EXCLUDE_GROUP;
      case "TIES":
        return RexWindowExclusion.EXCLUDE_TIES;
      case "NO OTHERS":
        return RexWindowExclusion.EXCLUDE_NO_OTHER;
      default:
        throw new UnsupportedOperationException(
            "cannot convert " + type + " to rex window exclusion");
    }
  }

  /**
   * Special context from which a relational expression can be initialized, reading from a
   * serialized form of the relational expression.
   *
   * <p>Contains only a cluster and an empty list of inputs; most methods throw {@link
   * UnsupportedOperationException}.
   *
   * <p>Replicated from {@link RelJson}
   */
  private static class RelInputForCluster implements RelInput {
    private final RelOptCluster cluster;

    RelInputForCluster(RelOptCluster cluster) {
      this.cluster = cluster;
    }

    @Override
    public RelOptCluster getCluster() {
      return cluster;
    }

    @Override
    public RelTraitSet getTraitSet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelOptTable getTable(String table) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelNode getInput() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<RelNode> getInputs() {
      return ImmutableList.of();
    }

    @Override
    public @Nullable RexNode getExpression(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableBitSet getBitSet(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable List<ImmutableBitSet> getBitSetList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<AggregateCall> getAggregateCalls(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable Object get(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable String getString(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <E extends Enum<E>> @Nullable E getEnum(String tag, Class<E> enumClass) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable List<RexNode> getExpressionList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable List<String> getStringList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable List<Integer> getIntegerList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable List<List<Integer>> getIntegerListList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelDataType getRowType(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelDataType getRowType(String expressionsTag, String fieldsTag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelCollation getCollation() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelDistribution getDistribution() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableList<ImmutableList<RexLiteral>> getTuples(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(String tag, boolean default_) {
      throw new UnsupportedOperationException();
    }
  }

  static RexNode translateInput(
      RelJson relJson, int input, Map<String, @Nullable Object> map, RelInput relInput) {
    throw new UnsupportedOperationException(
        "There shouldn't be any RexInputRef in the serialized RexNode.");
  }
}
