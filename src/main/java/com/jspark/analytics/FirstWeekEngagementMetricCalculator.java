package com.jspark.analytics;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.text.DecimalFormat;

public class FirstWeekEngagementMetricCalculator {

    public static Dataset<Row> createProcessableDateColumns(Dataset<Row> datasetWithTimestamp){
        return datasetWithTimestamp
                .withColumn("dayofweek", functions.date_format(datasetWithTimestamp.col("timestamp"), "E"))
                .withColumn("day", functions.date_trunc("DD", datasetWithTimestamp.col("timestamp")))
                .withColumn("tomorrow", functions.date_add(functions.date_trunc("DD", datasetWithTimestamp.col("timestamp")), 1))
                .withColumn("endOfWeek", functions.next_day(datasetWithTimestamp.col("timestamp"), "Sunday"));
    }

   public static Column getEventsBetweenTomorrowAndNextSunday(Dataset<Row> registeredEvents, Dataset<Row> appLoadedEvents){
        return appLoadedEvents.col("day").between(registeredEvents.col("tomorrow"), registeredEvents.col("endOfWeek"));
    }

    public static Dataset<Row> joinApplyingCalendarDateConditions(Dataset<Row> registeredEvents, Dataset<Row> appLoadedEvents){
        return registeredEvents.as("registered").join(appLoadedEvents.as("loaded"),
                registeredEvents.col("initiator_id").equalTo(appLoadedEvents.col("initiator_id"))
                        .and(getEventsBetweenTomorrowAndNextSunday(registeredEvents, appLoadedEvents)
                        )
        ).drop(appLoadedEvents.col("initiator_id"));
    }

    public static double calculateFirstWeekFractionOfActiveUsers(Dataset<Row> registeredEvents, Dataset<Row> appLoadedEventsAfterAWeekOfRegistration){
        long countAllRegisteredEvents = registeredEvents.count();
        long countUserWhoLoadedAppAtLeastOnceAfterAWeek = appLoadedEventsAfterAWeekOfRegistration.select(
                registeredEvents.col("initiator_id")
        ).distinct().count();
        return (double) countUserWhoLoadedAppAtLeastOnceAfterAWeek / countAllRegisteredEvents;
    }

    public static void printResult(double calculatedMetric){
        String printableResult = new DecimalFormat("#.###").format(calculatedMetric * 100);
        System.out.println("Fraction of users who loaded the application at least once during the calendar week" +
                " after the registration -> " + printableResult + "%");
    }
}
