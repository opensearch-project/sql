/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.multi;

import com.alibaba.druid.sql.ast.statement.SQLUnionOperator;
import org.opensearch.sql.legacy.domain.Select;

/** Created by Eliran on 19/8/2016. */
public class MultiQuerySelect {
  private final SQLUnionOperator operation;
  private final Select firstSelect;
  private final Select secondSelect;

  public MultiQuerySelect(SQLUnionOperator operation, Select firstSelect, Select secondSelect) {
    this.operation = operation;
    this.firstSelect = firstSelect;
    this.secondSelect = secondSelect;
  }

  public SQLUnionOperator getOperation() {
    return operation;
  }

  public Select getFirstSelect() {
    return firstSelect;
  }

  public Select getSecondSelect() {
    return secondSelect;
  }
}
