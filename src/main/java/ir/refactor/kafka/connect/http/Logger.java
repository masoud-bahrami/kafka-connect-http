//-----------------------------------------------------------------------
// <copyright file="Logger.java" Project="ir.refactor.kafka.connect.rest.http Project">
//     Copyright (C) Author <Masoud Bahrami>. <http://www.Refactor.Ir>
//     Github repo <https://github.com/masoud-bahrami/kafka-connect-http>
// </copyright>
//-----------------------------------------------------------------------

package ir.refactor.kafka.connect.http;

public class Logger {
    private static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger("RestAPISourceTask.InfoLogging");

    public static void logInfo(String message) {
        LOGGER.info(message);
    }
}
