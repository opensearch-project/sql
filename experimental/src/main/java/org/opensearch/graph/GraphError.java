package org.opensearch.graph;


import java.io.PrintWriter;
import java.io.StringWriter;

public class GraphError {
    private String errorCode;
    private String errorDescription;

    public GraphError() {
    }

    public GraphError(String errorCode, Throwable e) {
        this.errorCode = errorCode;
        StringWriter sw = new StringWriter();
        if (e != null) {
            e.printStackTrace(new PrintWriter(sw));
            //todo check is in debug mode
            e.printStackTrace();
            this.errorDescription = e.getMessage() != null ? e.getMessage() : sw.toString();
        }
    }

    public GraphError(String errorCode, String errorDescription) {
        this.errorCode = errorCode;
        this.errorDescription = errorDescription;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public void setErrorDescription(String errorDescription) {
        this.errorDescription = errorDescription;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorDescription() {
        return errorDescription;
    }

    @Override
    public String toString() {
        return "GraphError{" +
                "errorCode='" + errorCode + '\'' +
                ", errorDescription='" + errorDescription + '\'' +
                '}';
    }


    public static class GraphErrorException extends RuntimeException {
        private final GraphError error;

        public GraphErrorException(String message, GraphError error) {
            super(message);
            this.error = error;
        }

        public GraphErrorException(String error, String description) {
            super();
            this.error = new GraphError(error, description);
        }

        public GraphErrorException(GraphError error) {
            super();
            this.error = error;
        }

        public GraphErrorException(String message, Throwable cause, GraphError error) {
            super(message, cause);
            this.error = error;
        }

        public GraphErrorException(String message, Throwable cause) {
            super(message, cause);
            this.error = new GraphError(message, cause);
        }

        public GraphError getError() {
            return error;
        }

        @Override
        public String toString() {
            return "GraphErrorException{" +
                    "error=" + error +
                    '}';
        }
    }
}
