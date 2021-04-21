package com.jspark;

import com.jspark.schema.UserRegistrationSchema;
import com.jspark.generic.transformations.SchemaTransformations;
import org.apache.spark.sql.*;


import static org.apache.spark.sql.functions.col;


import java.util.logging.Level;
import java.util.logging.Logger;

public class EventsProcessor {
    private static final Logger LOGGER = Logger.getLogger(EventsProcessor.class.getName() );

    public static void main(String[] args){

        SparkSession spark = SparkSession.builder()
                .appName("FirstWeekEngagement").master("local[*]").getOrCreate();
        //TODO get executors variable and etc as env var and set dev as fallback

        //Encoder<UserRegistrationSchema> userRegistrationSchemaEncoder = Encoders.bean(UserRegistrationSchema.class);

        Dataset<Row> events = spark.read().json("data/raw/dataset.json");

        Dataset<Row> registeredEventsDf = events
                .filter(col("event").equalTo("registered"));

        registeredEventsDf = SchemaTransformations.convertSchema(registeredEventsDf, UserRegistrationSchema.userRegistrationSchema);

        Dataset<Row> appLoadedEventsDf = events
                .filter(col("event").equalTo("app_loaded"));




        Dataset<Row> app_loaded = events.filter(col("event").equalTo("app_loaded"));

        registeredEventsDf.printSchema();
        registeredEventsDf.show(10, false);

        long registeredCount = registeredEventsDf.count();
        //long appLoadedCount = app_loaded.count();
        LOGGER.log(Level.INFO, "registered events found: " + registeredCount);
        //LOGGER.log(Level.INFO, "app_loaded events found: " + appLoadedCount);
        System.out.println("print registeredCount: " + registeredCount);
        //System.out.println("print appLoadedCount: " + appLoadedCount);


        //TODO write this datasets as Parquet files with the right columns and datatypes!

    }
}
