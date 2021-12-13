package org.opensearch.sql.protocol.response;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

import java.util.Collection;
import java.util.Iterator;

@RequiredArgsConstructor
public class PlanQueryResult implements Iterable<Object> {

    @Getter
    private final ExecutionEngine.Schema schema;

    /**
     * Results which are collection of expression.
     */
    private final Collection<ExprValue> exprValues;


    /**
     * size of results.
     *
     * @return size of results
     */
    public int size() {
        return exprValues.size();
    }

    @Override
    public Iterator<Object> iterator() {
        return exprValues.stream()
                .map(ExprValue::value)
                .iterator();
    }
}
