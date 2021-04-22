package com.jspark;

import org.apache.spark.sql.*;

public class AnalalyticsFirstWeekEngamentStep {

    public static void run(){
        //TODO please organize me for god sake

        SparkSession spark = SparkSession.builder()
                .appName("FirstWeekEngagement").master("local[*]")
                .getOrCreate();
        System.out.println("To be implemented soon!");

        Dataset<Row> registeredEvents = spark.read().parquet(System.getProperty("user.dir") + "/data/processed/registered");
        Dataset<Row> appLoadedEvents = spark.read().parquet(System.getProperty("user.dir") + "/data/processed/app_loaded");

        registeredEvents.printSchema();
        long numEvents = registeredEvents.count();
        System.out.println("Total number of events -> " + numEvents);

        //TODO turn me into a method
        registeredEvents = registeredEvents.withColumn("weekofyear", functions.weekofyear(registeredEvents.col("timestamp")))
                .withColumn("dayofweek", functions.date_format(registeredEvents.col("timestamp"), "E"));

        registeredEvents = registeredEvents.withColumn("is_last_day_of_the_week",
                functions.when(registeredEvents.col("dayofweek").equalTo("Sun"), true)
                        .otherwise(false));

        appLoadedEvents = appLoadedEvents.withColumn("weekofyear", functions.weekofyear(appLoadedEvents.col("timestamp")))
                .withColumn("dayofweek", functions.date_format(appLoadedEvents.col("timestamp"), "E"));

        Dataset<Row> allEvents = registeredEvents.as("registered").join(appLoadedEvents.as("loaded"),
                registeredEvents.col("initiator_id").equalTo(appLoadedEvents.col("initiator_id"))
                .and(functions.when(
                        registeredEvents.col("is_last_day_of_the_week"),
                        registeredEvents.col("weekofyear").equalTo(appLoadedEvents.col("weekofyear").plus(1))
                        )
                        .otherwise(registeredEvents.col("weekofyear").equalTo(appLoadedEvents.col("weekofyear")))
                )
                );

        allEvents.printSchema();
        allEvents.show(20, false);

        long countAllEvents = allEvents.count();
        long countAppLoadedEvents = appLoadedEvents.count();
        System.out.println("All events count -> " + countAllEvents);
        System.out.println("App loaded events count -> " + countAppLoadedEvents);

    }
}
