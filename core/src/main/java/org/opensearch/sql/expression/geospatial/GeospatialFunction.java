package org.opensearch.sql.expression.geospatial;

import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;

import lombok.Builder;
import lombok.experimental.UtilityClass;
import org.opensearch.geospatial.spl
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionName;

@UtilityClass
public class GeospatialFunction {
    /**
     * Register Geospatial Functions.
     *
     * @param repository {@link BuiltinFunctionRepository}.
     */
    public void register(BuiltinFunctionRepository repository) {
        repository.register(geoip());
    }

    private DefaultFunctionResolver geoipIplocation(FunctionName functionName) {
        return define(
            functionName,
            impl(
                ,
                STRING,
                STRING
            )
        );
    }

    private DefaultFunctionResolver geoip() {
        return geoipIplocation(BuiltinFunctionName.GEOIP.getName());
    }

    private DefaultFunctionResolver iplocation() {
        return geoipIplocation(BuiltinFunctionName.IPLOCATION.getName());
    }
}
