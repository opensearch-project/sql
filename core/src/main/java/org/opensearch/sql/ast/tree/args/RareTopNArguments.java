/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree.args;

import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.jetbrains.annotations.TestOnly;
import org.opensearch.sql.ast.expression.Argument;

@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class RareTopNArguments {
  private int noOfResults = 10;
  private String countField = "count";
  private boolean showCount = true;
  private String percentField = "percent";
  private boolean showPerc = false;
  private boolean useOther = false;

  public RareTopNArguments(List<Argument> arguments) {
    for (Argument arg : arguments) {
      switch (arg.getArgName()) {
        case "noOfResults":
          noOfResults = (int) arg.getValue().getValue();
          break;
        case "countField":
          countField = (String) arg.getValue().getValue();
          if (countField.isBlank()) {
            throw new IllegalArgumentException("Illegal count field in top/rare: cannot be blank");
          }
          break;
        case "showCount":
          showCount = (boolean) arg.getValue().getValue();
          break;
        case "percentField":
          percentField = (String) arg.getValue().getValue();
          if (countField.isBlank()) {
            throw new IllegalArgumentException(
                "Illegal percent field in top/rare: cannot be blank");
          }
          break;
        case "showPerc":
          showPerc = (boolean) arg.getValue().getValue();
          break;
        case "useOther":
          useOther = (boolean) arg.getValue().getValue();
          break;
        default:
          throw new IllegalArgumentException("unknown argument for rare/top: " + arg.getArgName());
      }
    }
  }

  public String renderOptions() {
    StringBuilder options = new StringBuilder();
    if (showCount) {
      options.append("countfield='").append(countField).append("' ");
    } else {
      options.append("showcount=false ");
    }
    if (showPerc) {
      options.append("percfield='").append(percentField).append("' ");
    } else {
      options.append("showperc=false ");
    }
    if (useOther) {
      options.append("useother=true ");
    }
    return options.toString();
  }

  @TestOnly
  public List<Argument> asExprList() {
    return exprList(
        argument("noOfResults", intLiteral(10)),
        argument("countField", stringLiteral("count")),
        argument("showCount", booleanLiteral(true)),
        argument("percentField", stringLiteral("percent")),
        argument("showPerc", booleanLiteral(false)),
        argument("useOther", booleanLiteral(false)));
  }
}
