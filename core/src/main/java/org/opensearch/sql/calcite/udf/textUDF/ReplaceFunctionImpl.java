package org.opensearch.sql.calcite.udf.textUDF;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.expression.function.ImplementorUDF;

// We don't use calcite built in replace since it uses replace instead of replaceAll
public class ReplaceFunctionImpl extends ImplementorUDF {
  public ReplaceFunctionImpl() {
    super(new ReplaceImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  public static class ReplaceImplementor implements NotNullImplementor {
    /**
     * Implements a call with assumption that all the null-checking is implemented by caller.
     *
     * @param translator translator to implement the code
     * @param call call to implement
     * @param translatedOperands arguments of a call
     * @return expression that implements given call
     */
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(ReplaceImplementor.class, "replace", translatedOperands);
    }

    public static String replace(String str, String oldStr, String newStr) {
      return str.replace(oldStr, newStr);
    }
  }
}
