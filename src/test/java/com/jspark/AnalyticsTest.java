package com.jspark;

import com.jspark.analytics.FirstWeekEngagementMetricCalculator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AnalyticsTest extends SessionContext{
    private final String dataRegisteredPath = System.getProperty("user.dir") + "/src/test/dummy_processed/registered";
    private final String dataAppLoadedPath = System.getProperty("user.dir") + "/src/test/dummy_processed/app_loaded";

    private Dataset<Row> dataRegistered;
    private Dataset<Row> dataAppLoaded;

    @BeforeEach
    public void readDummyRegisteredData(){
        this.dataRegistered = this.spark.read().parquet(dataRegisteredPath);
    }

    @BeforeEach
    public void readDummyAppLoadedData(){
        this.dataAppLoaded = this.spark.read().parquet(dataAppLoadedPath);
    }

    @Test
    @DisplayName("Checking for FirstWeekEngagementMetricCalculator logic")
    public void testFirstWeekEngagementMetricCalculator(){
        Dataset<Row> dataRegistered = FirstWeekEngagementMetricCalculator.createProcessableDateColumns(this.dataRegistered);
        Dataset<Row> dataAppLoaded = FirstWeekEngagementMetricCalculator.createProcessableDateColumns(this.dataAppLoaded);

        dataRegistered.show();
        dataRegistered.printSchema();
        dataAppLoaded.show();
        dataAppLoaded.printSchema();


        Dataset<Row> appLoadedEventsAfterAWeekOfRegistration = FirstWeekEngagementMetricCalculator
                .joinApplyingCalendarDateConditions(dataRegistered, dataAppLoaded);

        appLoadedEventsAfterAWeekOfRegistration.show();
        appLoadedEventsAfterAWeekOfRegistration.printSchema();

        double resultFraction = FirstWeekEngagementMetricCalculator
                .calculateFirstWeekFractionOfActiveUsers(dataRegistered, appLoadedEventsAfterAWeekOfRegistration);


        List<Row> expectedIds = appLoadedEventsAfterAWeekOfRegistration.select("initiator_id").distinct().collectAsList();
        assertEquals(RowFactory.create(5), expectedIds.get(0));
        assertEquals(RowFactory.create(2), expectedIds.get(1));
        assertEquals(0.4, resultFraction);

        FirstWeekEngagementMetricCalculator.printResult(resultFraction);
    }
}
