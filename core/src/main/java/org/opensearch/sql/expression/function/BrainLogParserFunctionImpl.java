/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.common.patterns.BrainLogParser;

public class BrainLogParserFunctionImpl extends ImplementorUDF {
  protected BrainLogParserFunctionImpl() {
    super(new BrainLogParserImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR;
  }

  public static class BrainLogParserImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      assert call.getOperands().size() == 2 : "BRAIN_LOG_PARSER should have 2 arguments";
      assert translatedOperands.size() == 2 : "BRAIN_LOG_PARSER should have 2 arguments";

      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(
                      BrainLogParserFunctionImpl.class, "eval", String.class, Object.class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  @Strict
  public static Object eval(
      @Parameter(name = "field") String field, @Parameter(name = "aggObject") Object aggObject) {
    BrainLogParser.GroupTokenAggregationInfo aggResult =
        (BrainLogParser.GroupTokenAggregationInfo) aggObject;
    List<String> parsedTokens =
        BrainLogParser.parseLogPattern(
            aggResult.getMessageToProcessedLogMap().get(field),
            aggResult.getTokenFreqMap(),
            aggResult.getGroupTokenSetMap(),
            aggResult.getLogIdGroupCandidateMap(),
            aggResult.getVariableCountThreshold());
    return String.join(" ", parsedTokens);
  }
}
