package com.centreon.injector.error_handling;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * <br/>
 * Class to report row from input file that couldn't be inserted into DSE
 * <br/>
 * <br/>
 *
 * The <strong>{@code close()}</strong> method will be called by the Spring application container because we declared
 *  <strong>{@code @Bean(destroyMethod = "close")}</strong> in the configuration class
 */
public class ErrorFileLogger {

    private final PrintWriter printWriter;

    public ErrorFileLogger(String errorFile) throws IOException {
        this.printWriter = new PrintWriter(new FileWriter(errorFile));
    }

    public void writeLine(String line) {
        this.printWriter.println(line);
        printWriter.flush();
    }

    public void close() {
        if (printWriter != null) {
            printWriter.close();
        }
    }
}
