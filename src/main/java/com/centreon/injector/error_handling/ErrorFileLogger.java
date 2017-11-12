package com.centreon.injector.error_handling;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class ErrorFileLogger {

    private final PrintWriter printWriter;

    public ErrorFileLogger(String errorFile) throws IOException {
        this.printWriter = new PrintWriter(new FileWriter(errorFile));
    }

    public void writeLine(String line) {
        this.printWriter.println(line);
    }

    public void close() {
        if (printWriter != null) {
            printWriter.flush();
            printWriter.close();
        }
    }
}
