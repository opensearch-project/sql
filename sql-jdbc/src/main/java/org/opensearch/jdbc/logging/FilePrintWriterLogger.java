/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.logging;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FilePrintWriterLogger extends PrintWriterLogger {

    public FilePrintWriterLogger(String filePath, LogLevel logLevel, Layout layout) throws IOException {
        super(new PrintWriter(
                Files.newBufferedWriter(
                        Paths.get("").resolve(filePath),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND), true), logLevel, layout);
    }

    @Override
    public void close() {
        super.close();
        printWriter.close();
    }
}
