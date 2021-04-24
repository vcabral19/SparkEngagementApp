package com.jspark.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SparkJobVariables {

    public final Properties properties;

    public SparkJobVariables(){
        this.properties = new Properties();
        String propertiesFileName = "config.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertiesFileName);
        try{
            this.properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getEnvVarOrProperty(String envVarName){
        String systemEnvVar = System.getenv(envVarName);
        if (systemEnvVar == null){
            systemEnvVar = properties.getProperty(envVarName);
        }
        return systemEnvVar;
    }
}
