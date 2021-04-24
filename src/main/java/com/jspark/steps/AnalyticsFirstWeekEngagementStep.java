package com.jspark.steps;

import com.jspark.analytics.FirstWeekEngagementMetricCalculator;
import com.jspark.read.DataFormats;
import com.jspark.read.LocalFileSystemDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AnalyticsFirstWeekEngagementStep implements StepInterface {
    private final String registeredProcessedEventsPath;
    private final String appLoadedProcessedEventsPath;
    private final String sparkMasterConfig;
    private final DataFormats dataFormat = DataFormats.PARQUET;

    public AnalyticsFirstWeekEngagementStep(String registeredPath, String appLoadedPath, String sparkMasterConfig){
        this.registeredProcessedEventsPath = registeredPath;
        this.appLoadedProcessedEventsPath = appLoadedPath;
        this.sparkMasterConfig = sparkMasterConfig;
    }

    public void run(){
        SparkSession spark = SparkSession.builder()
                .appName("FirstWeekEngagement").master(sparkMasterConfig)
                .getOrCreate();

        LocalFileSystemDataSource localFileSystemDataReader = new LocalFileSystemDataSource(spark);
        Dataset<Row> registeredEvents = localFileSystemDataReader.getDataFromSource(dataFormat, registeredProcessedEventsPath);
        Dataset<Row> appLoadedEvents = localFileSystemDataReader.getDataFromSource(dataFormat, appLoadedProcessedEventsPath);

        registeredEvents = FirstWeekEngagementMetricCalculator.createProcessableDateColumns(registeredEvents);
        appLoadedEvents = FirstWeekEngagementMetricCalculator.createProcessableDateColumns(appLoadedEvents);

        Dataset<Row> appLoadedEventsAfterAWeekOfRegistration = FirstWeekEngagementMetricCalculator
                .joinApplyingCalendarDateConditions(registeredEvents, appLoadedEvents);

        double resultFraction = FirstWeekEngagementMetricCalculator
                .calculateFirstWeekFractionOfActiveUsers(registeredEvents, appLoadedEventsAfterAWeekOfRegistration);

        FirstWeekEngagementMetricCalculator.printResult(resultFraction);

    }
}
