package com.jspark;

import com.jspark.analytics.FirstWeekEngagementMetric;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AnalalyticsFirstWeekEngamentStep {


    public static void run(){
        SparkSession spark = SparkSession.builder()
                .appName("FirstWeekEngagement").master("local[*]")
                .getOrCreate();
        System.out.println("To be implemented soon!");

        Dataset<Row> registeredEvents = spark.read().parquet(System.getProperty("user.dir") + "/data/processed/registered");
        Dataset<Row> appLoadedEvents = spark.read().parquet(System.getProperty("user.dir") + "/data/processed/app_loaded");

        registeredEvents = FirstWeekEngagementMetric.createProcessableDateColumns(registeredEvents);
        appLoadedEvents = FirstWeekEngagementMetric.createProcessableDateColumns(appLoadedEvents);


        Dataset<Row> allAppLoadedEventsAfterAWeekOfRegistration = registeredEvents.as("registered").join(appLoadedEvents.as("loaded"),
                registeredEvents.col("initiator_id").equalTo(appLoadedEvents.col("initiator_id"))
                .and(FirstWeekEngagementMetric.getEventsBetweenTomorrowAndNextSunday(registeredEvents, appLoadedEvents)
                )
        ).drop(appLoadedEvents.col("initiator_id"));


        double resultFraction = FirstWeekEngagementMetric.calculateFirstWeekFractionOfActiveUsers(allAppLoadedEventsAfterAWeekOfRegistration);
        FirstWeekEngagementMetric.printResult(resultFraction);

    }
}
