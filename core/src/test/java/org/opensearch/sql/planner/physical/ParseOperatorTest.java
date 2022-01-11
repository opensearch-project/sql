package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;

public class ParseOperatorTest extends PhysicalPlanTestBase {
  private static final String regex =
      "\\b(?<city>[A-Za-z\\s]+),\\s(?<state>[A-Z]{2,2}):\\s(?<areaCode>[0-9]{3,3})\\b";
  private static final Pattern pattern = Pattern.compile(regex);

  @Test
  public void parse() {
    Map<String, String> groups = ImmutableMap.of("ipMatched", "");
    PhysicalPlan plan = new ParseOperator(new TestScan(), DSL.ref("ip", STRING),
        "(?<ipMatched>^(\\d{1,3}\\.){3}\\d{1,3}$)", groups);
    List<ExprValue> results = execute(plan);
    assertEquals(5, results.size());
    results.forEach(result -> assertEquals(result.tupleValue().get("ip"),
        result.tupleValue().get("ipMatched")));
  }

}
