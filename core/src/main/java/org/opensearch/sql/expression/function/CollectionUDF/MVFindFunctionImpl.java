/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * MVFIND function implementation that finds the index of the first element in a multivalue array
 * that matches a regular expression.
 *
 * <p>Usage: mvfind(array, regex)
 *
 * <p>Returns the 0-based index of the first array element matching the regex pattern, or NULL if no
 * match is found.
 *
 * <p>Example: mvfind(array('apple', 'banana', 'apricot'), 'ban.*') returns 1
 */
public class MVFindFunctionImpl extends ImplementorUDF {
  public MVFindFunctionImpl() {
    super(new MVFindImplementor(), NullPolicy.ALL);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.INTEGER_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER));
  }

  public static class MVFindImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(MVFindFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  /**
   * Evaluates the mvfind function.
   *
   * @param args args[0] is the array (List<Object>), args[1] is the regex pattern (String)
   * @return The 0-based index of the first matching element, or null if no match
   */
  public static Object eval(Object... args) {
    if (args == null || args.length < 2) {
      return null;
    }

    List<Object> array = (List<Object>) args[0];
    String regex = (String) args[1];

    if (array == null || regex == null) {
      return null;
    }

    try {
      Pattern pattern = Pattern.compile(regex);
      for (int i = 0; i < array.size(); i++) {
        Object element = array.get(i);
        if (element != null) {
          String strValue = element.toString();
          if (pattern.matcher(strValue).find()) {
            return i; // Return 0-based index
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error in mvfind function: " + e.getMessage(), e);
    }

    return null; // No match found
  }
}
