package com.jspark;

import com.jspark.ingestion.DataSource;
import com.jspark.ingestion.ApplicationLoadingIngestion;
import com.jspark.ingestion.UserRegistrationIngestion;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.util.logging.Level;
import java.util.logging.Logger;

public class EventsProcessor {
    private static final Logger LOGGER = Logger.getLogger(EventsProcessor.class.getName() );

    public static void main(String[] args){

        SparkSession spark = SparkSession.builder()
                .appName("FirstWeekEngagement").master("local[*]")
                .getOrCreate();
        //TODO get executors variable and etc as env var and set dev as fallback

        Dataset<Row> eventSource = DataSource.getDataFromSource(spark);

        Dataset<Row> registeredEvents = UserRegistrationIngestion.runUserRegistrationIngestion(eventSource);

        Dataset<Row> appLoadedEvents = ApplicationLoadingIngestion.runApplicationLoadingIngestion(eventSource);


        registeredEvents.printSchema();
        registeredEvents.show(10, false);

        appLoadedEvents.printSchema();
        appLoadedEvents.show(10, false);

        long registeredCount = registeredEvents.count();
        long appLoadedCount = appLoadedEvents.count();
        LOGGER.log(Level.INFO, "registered events found: " + registeredCount);
        LOGGER.log(Level.INFO, "app_loaded events found: " + appLoadedCount);
        System.out.println("print registeredCount: " + registeredCount);
        System.out.println("print appLoadedCount: " + appLoadedCount);

        //TODO modularize me
        registeredEvents
                .withColumn("year", date_format(col("timestamp"), "y"))
                .withColumn("month", date_format(col("timestamp"), "M"))
                .withColumn("day", date_format(col("timestamp"), "d"))
                .write()
                .partitionBy("year","month","day")
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(System.getProperty("user.dir") + "/data/processed/registered");

    }
}
