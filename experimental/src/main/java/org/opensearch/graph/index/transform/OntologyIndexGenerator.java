package org.opensearch.graph.index.transform;

import javaslang.Tuple2;

import java.util.List;

public interface OntologyIndexGenerator {
    List<Tuple2<String, Boolean>> generateMappings();

    class IndexSchemaConfig {
        public static final String ID = "id";
        public static final String TYPE = "type";
        public static final String PROPERTIES = "properties";
        public static final String NESTED = "nested";
        public static final String CHILD = "child";
        public static final String EMBEDDED = "embedded";

    }
    class ProjectionConfigs {
        public static final String PROJECTION = "projection";
        public static final String TAG = "tag";
        public static final String QUERY_ID = "queryId";
        public static final String CURSOR_ID = "cursorId";
        public static final String EXECUTION_TIME = "timestamp";

    }

    class EdgeSchemaConfig {
        public static String DIRECTION = "direction";

        public static String SOURCE = "entityA";//formally was source
        public static String SOURCE_ID = "entityA.id";//formally was source.id
        public static String SOURCE_TYPE = "entityA.type";//formally was source.type
        public static String SOURCE_NAME = "entityA.name";//formally was source.name

        public static String DEST = "entityB";//formally was target
        public static String DEST_ID = "entityB.id";//formally was target.id
        public static String DEST_TYPE = "entityB.type";//formally was target.type
        public static String DEST_NAME = "entityB.name";//formally was target.name
    }
}
