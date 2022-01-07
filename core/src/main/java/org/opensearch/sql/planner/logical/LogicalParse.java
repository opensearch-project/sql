package org.opensearch.sql.planner.logical;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;

/**
 * Logical Parse Command.
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalParse extends LogicalPlan {

  @Getter
  private final Expression expression;

  @Getter
  private final String pattern;

  @Getter
  private final Map<String, String> groups;

  private static final Map<String, ExprType> TYPE_MAP;

  private static final Pattern GROUP_PATTERN;

  static {
    TYPE_MAP = new ImmutableMap.Builder<String, ExprType>()
            .put("", STRING) // default type is string
            .put(STRING.typeName(), STRING)
            .put(BYTE.typeName(), BYTE)
            .put(SHORT.typeName(), SHORT)
            .put(INTEGER.typeName(), INTEGER)
            .put(LONG.typeName(), LONG)
            .put(FLOAT.typeName(), FLOAT)
            .put(DOUBLE.typeName(), DOUBLE)
            .put(BOOLEAN.typeName(), BOOLEAN)
            .put(TIME.typeName(), TIME)
            .put(DATE.typeName(), DATE)
            .put(TIMESTAMP.typeName(), TIMESTAMP)
            .put(DATETIME.typeName(), DATETIME)
            .build();
    GROUP_PATTERN = Pattern.compile(String.format("\\(\\?<([a-zA-Z][a-zA-Z0-9]*?)(%s)>",
            TYPE_MAP.keySet().stream().collect(Collectors.joining("|"))));
  }

  /**
   * Constructor of LogicalParse.
   */
  public LogicalParse(LogicalPlan child, Expression expression, String pattern) {
    super(Collections.singletonList(child));
    this.expression = expression;
    this.pattern = pattern;
    this.groups = getNamedGroupCandidates(pattern);
  }

  /**
   * Converts raw string to ExprType.
   * @param type string from regex group name
   * @return ExprType
   */
  public static ExprType typeStrToExprType(String type) {
    return TYPE_MAP.get(type);
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitParse(this, context);
  }

  private static Map<String, String> getNamedGroupCandidates(String pattern) {
    Map<String, String> namedGroups = new HashMap<>();
    Matcher m = GROUP_PATTERN.matcher(pattern);
    while (m.find()) {
      namedGroups.put(m.group(1), m.group(2));
    }
    return namedGroups;
  }
}
