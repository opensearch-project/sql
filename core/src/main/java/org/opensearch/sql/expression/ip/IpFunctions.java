package org.opensearch.sql.expression.ip;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.expression.function.*;

import java.util.Arrays;

import static org.opensearch.sql.data.type.ExprCoreType.*;
import static org.opensearch.sql.expression.function.FunctionDSL.define;

/**
 * Utility class that defines and registers IP functions.
 */
@UtilityClass
public class IpFunctions {

    /**
     * Registers all IP functions with the given built-in function repository.
     */
    public void register(BuiltinFunctionRepository repository) {
        repository.register(cidr());
    }

    private DefaultFunctionResolver cidr() {

        FunctionName name = BuiltinFunctionName.CIDR.getName();
        FunctionSignature signature = new FunctionSignature(name, Arrays.asList(STRING, STRING));

        return define(name, funcName -> Pair.of(signature,(properties, arguments) -> new CidrExpression(arguments)));
    }
}
