/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import java.util.List;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.utils.Util;

/** Created by Razma Tazz on 14/04/2016. */
public class ChildrenType {
  public String field;
  public String childType;
  public Where where;
  private boolean simple;

  public boolean tryFillFromExpr(SQLExpr expr) throws SqlParseException {
    if (!(expr instanceof SQLMethodInvokeExpr)) {
      return false;
    }
    SQLMethodInvokeExpr method = (SQLMethodInvokeExpr) expr;

    String methodName = method.getMethodName();

    if (!methodName.toLowerCase().equals("children")) {
      return false;
    }

    List<SQLExpr> parameters = method.getParameters();

    if (parameters.size() != 2) {
      throw new SqlParseException(
          "on children object only allowed 2 parameters (type, field)/(type, conditions...) ");
    }

    String type = Util.extendedToString(parameters.get(0));
    this.childType = type;

    SQLExpr secondParameter = parameters.get(1);
    if (secondParameter instanceof SQLTextLiteralExpr
        || secondParameter instanceof SQLIdentifierExpr
        || secondParameter instanceof SQLPropertyExpr) {
      this.field = Util.extendedToString(secondParameter);
      this.simple = true;
    } else {
      Where where = Where.newInstance();
      new WhereParser(new SqlParser()).parseWhere(secondParameter, where);
      if (where.getWheres().size() == 0) {
        throw new SqlParseException("Failed to parse filter condition");
      }
      this.where = where;
      simple = false;
    }

    return true;
  }

  public boolean isSimple() {
    return simple;
  }
}
