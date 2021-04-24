package com.jspark.steps;

import com.jspark.analytics.FirstWeekEngagementMetricCalculator;
import com.jspark.read.DataFormats;
import com.jspark.read.LocalFileSystemDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AnalyticsFirstWeekEngagementStep implements StepInterface {
    private static final String registeredProcessedEventsPath = System.getProperty("user.dir") + "/data/processed/registered";
    private static final String appLoadedProcessedEventsPath = System.getProperty("user.dir") + "/data/processed/app_loaded";
    private static final DataFormats dataFormat = DataFormats.PARQUET;

    public AnalyticsFirstWeekEngagementStep(){};

    public void run(){
        SparkSession spark = SparkSession.builder()
                .appName("FirstWeekEngagement").master("local[*]")
                .getOrCreate();

        LocalFileSystemDataSource localFileSystemDataReader = new LocalFileSystemDataSource(spark);
        Dataset<Row> registeredEvents = localFileSystemDataReader.getDataFromSource(dataFormat, registeredProcessedEventsPath);
        Dataset<Row> appLoadedEvents = localFileSystemDataReader.getDataFromSource(dataFormat, appLoadedProcessedEventsPath);

        registeredEvents = FirstWeekEngagementMetricCalculator.createProcessableDateColumns(registeredEvents);
        appLoadedEvents = FirstWeekEngagementMetricCalculator.createProcessableDateColumns(appLoadedEvents);

        Dataset<Row> appLoadedEventsAfterAWeekOfRegistration = FirstWeekEngagementMetricCalculator
                .joinApplyingCalendarDateConditions(registeredEvents, appLoadedEvents);

        double resultFraction = FirstWeekEngagementMetricCalculator
                .calculateFirstWeekFractionOfActiveUsers(appLoadedEventsAfterAWeekOfRegistration);

        FirstWeekEngagementMetricCalculator.printResult(resultFraction);

    }
}
