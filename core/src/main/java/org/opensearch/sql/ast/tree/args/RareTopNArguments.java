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
  public static final String NUMBER_RESULTS = "noOfResults";
  public static final String COUNT_FIELD = "countField";
  public static final String SHOW_COUNT = "showCount";
  public static final String PERCENT_FIELD = "percentField";
  public static final String SHOW_PERCENT = "showPerc";
  public static final String USE_OTHER = "useOther";

  private int noOfResults = 10;
  private String countField = "count";
  private boolean showCount = true;
  private String percentField = "percent";
  private boolean showPerc = false;
  private boolean useOther = false;

  public RareTopNArguments(List<Argument> arguments) {
    // handle `percent=whatever showperc=false` (though I'm not sure if it's ever useful to do so)
    boolean isShowPercOverridden = false;

    for (Argument arg : arguments) {
      switch (arg.getArgName()) {
        case NUMBER_RESULTS:
          noOfResults = (int) arg.getValue().getValue();
          if (noOfResults < 0) {
            throw new IllegalArgumentException(
                "Illegal number of results requested for top/rare: must be non-negative");
          }
          break;
        case COUNT_FIELD:
          countField = (String) arg.getValue().getValue();
          if (countField.isBlank()) {
            throw new IllegalArgumentException("Illegal count field in top/rare: cannot be blank");
          }
          break;
        case SHOW_COUNT:
          showCount = (boolean) arg.getValue().getValue();
          break;
        case PERCENT_FIELD:
          percentField = (String) arg.getValue().getValue();
          if (percentField.isBlank()) {
            throw new IllegalArgumentException(
                "Illegal percent field in top/rare: cannot be blank");
          }
          if (!isShowPercOverridden) {
            showPerc = true;
          }
          break;
        case SHOW_PERCENT:
          showPerc = (boolean) arg.getValue().getValue();
          isShowPercOverridden = true;
          break;
        case USE_OTHER:
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
