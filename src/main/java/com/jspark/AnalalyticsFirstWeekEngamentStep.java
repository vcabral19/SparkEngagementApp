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

        //TODO read me from datasource
        Dataset<Row> registeredEvents = spark.read().parquet(System.getProperty("user.dir") + "/data/processed/registered");
        Dataset<Row> appLoadedEvents = spark.read().parquet(System.getProperty("user.dir") + "/data/processed/app_loaded");

        registeredEvents = FirstWeekEngagementMetric.createProcessableDateColumns(registeredEvents);
        appLoadedEvents = FirstWeekEngagementMetric.createProcessableDateColumns(appLoadedEvents);

        Dataset<Row> appLoadedEventsAfterAWeekOfRegistration = FirstWeekEngagementMetric.joinApplyingCalendarDateConditions(registeredEvents, appLoadedEvents);

        double resultFraction = FirstWeekEngagementMetric.calculateFirstWeekFractionOfActiveUsers(appLoadedEventsAfterAWeekOfRegistration);
        FirstWeekEngagementMetric.printResult(resultFraction);

    }
}
