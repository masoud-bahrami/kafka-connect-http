//-----------------------------------------------------------------------
// <copyright file="ConfigService.java" Project="ir.refactor.kafka.connect.rest.http Project">
//     Copyright (C) Author <Masoud Bahrami>. <http://www.Refactor.Ir>
//     Github repo <https://github.com/masoud-bahrami/kafka-connect-http>
// </copyright>
//-----------------------------------------------------------------------
package ir.refactor.kafka.connect.http;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConfigService {
    public static void storeLastFedPoint(String configFileName, Integer from) {
        Logger.logInfo("Starting ConfigService.storeLastFedPoint...");
        Writer out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(configFileName), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            Logger.logInfo("At ConfigService.storeLastFedPoint. UnsupportedEncodingException =" + e.getMessage());

        } catch (FileNotFoundException e) {
            Logger.logInfo("At ConfigService.storeLastFedPoint. FileNotFoundException =" + e.getMessage());
        }
        try {
            try {
                out.write(from.toString());
            } catch (IOException e) {
                Logger.logInfo("At ConfigService.storeLastFedPoint. IOException =" + e.getMessage());
            }
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                Logger.logInfo("At ConfigService.storeLastFedPoint. Closing config file. IOException =" + e.getMessage());

            }
        }
        Logger.logInfo("End ConfigService.storeLastFedPoint...");
    }

    public static Integer getLastFeedPoint(String configFileName) throws IOException {
        Logger.logInfo("Start ConfigService.getLastFeedPoint() ....");

        String content;

        byte[] bytes = Files.readAllBytes(Paths.get(configFileName));
        content = new String(bytes);

        Integer lastFedPoint = Integer.parseInt(content);
        Logger.logInfo("At ConfigService.getLastFeedPoint lastFedPoint = " + lastFedPoint);
        Logger.logInfo("End ConfigService.getLastFeedPoint ....");
        return lastFedPoint;
    }

    public static void createConfigFileIfNotExist(String configFileName) {
        Logger.logInfo("Start ConfigService.createConfigFileIfNotExist ....");

        try {
            Logger.logInfo("At ConfigService.createConfigFileIfNotExist. Create a new File() object with parameter ".concat(configFileName));
            File f = new File(configFileName);
            if (!f.exists()) {
                Logger.logInfo("At ConfigService.createConfigFileIfNotExist, config file not exist. Trying to create a new config file. File name is ".concat(configFileName));
                f.createNewFile();

            } else {
                Logger.logInfo("At ConfigService.createConfigFileIfNotExist, Config File already exists");
            }
        } catch (Exception e) {
            Logger.logInfo("At ConfigService.createConfigFileIfNotExist, creating config file failed. " + e.getMessage());
        }
        Logger.logInfo("End ConfigService.createConfigFileIfNotExist");
    }
}
