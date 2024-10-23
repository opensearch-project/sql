package org.opensearch.sql.expression.ip;


import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.common.grok.Converter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.planner.physical.collector.Rounding;

import java.util.List;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

@Getter
@ToString
@EqualsAndHashCode
public class GeoipExpression implements Expression {
    private final Expression datasource;
    private final Expression ipAddress;
    private final String[] properties;

    public GeoipExpression(Expression datasource, Expression ipAddress, String properties) {
        this.datasource = datasource;
        this.ipAddress = ipAddress;
        this.properties = properties.split(",");
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
        Rounding<?> rounding =
                Rounding.createRounding(this); // TODO: will integrate with WindowAssigner
        return rounding.round(field.valueOf(valueEnv));
    }

    @Override
    public ExprType type() {
        if (this.properties.length <= 1) {
            return STRING;
        } else {
            return ARRAY;
        }
    }

    @Override
    public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
        return visitor.visitNode(this, context);
    }
}
