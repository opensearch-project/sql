/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.multikv;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Internal UDF backing the {@code multikv} command. Parses the table text of one event into an
 * array of serialized per-row records (see {@link MultikvParser}). The array is then exploded into
 * one row per table data row (via the mvexpand uncollect/correlate primitive), after which {@link
 * MultikvExtractFunctionImpl} pulls named cell values out of each record.
 *
 * <p>Signature: {@code MULTIKV_SPLIT(rawText, forceHeaderInt, noHeaderBool, filterJoined)} where
 * {@code forceHeaderInt} is the 1-based forced header line or a value &lt;= 0 for auto, and {@code
 * filterJoined} is the filter terms joined by {@link MultikvParser#FS} (empty for none). Returns
 * {@code array<varchar>}.
 */
public class MultikvSplitFunctionImpl extends ImplementorUDF {

  public MultikvSplitFunctionImpl() {
    super(new MultikvSplitImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
      return createArrayType(
          typeFactory,
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR), true),
          true);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static Object eval(Object... args) {
    if (args.length < 1 || args[0] == null) {
      return Collections.emptyList();
    }
    String raw = (String) args[0];
    int forceHeader = (args.length > 1 && args[1] != null) ? ((Number) args[1]).intValue() : -1;
    boolean noHeader = args.length > 2 && Boolean.TRUE.equals(args[2]);
    String filterJoined = (args.length > 3 && args[3] != null) ? (String) args[3] : "";
    List<String> filterTerms =
        filterJoined.isEmpty()
            ? Collections.emptyList()
            : Arrays.asList(
                filterJoined.split(java.util.regex.Pattern.quote(MultikvParser.FS), -1));
    return MultikvParser.parse(raw, forceHeader, noHeader, filterTerms);
  }

  public static class MultikvSplitImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(MultikvSplitFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }
}
