package com.jspark;

import com.jspark.ingestion.ApplicationLoadingIngestion;
import com.jspark.ingestion.DataSource;
import com.jspark.ingestion.UserRegistrationIngestion;
import com.jspark.write.DataWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EventsIngestorStep {
    public static void run(){

        SparkSession spark = SparkSession.builder()
                .appName("EventsIngestor").master("local[*]")
                .getOrCreate();
        //TODO get executors variable and etc as env var and set dev as fallback

        Dataset<Row> eventSource = DataSource.getDataFromSource(spark);

        Dataset<Row> registeredEvents = UserRegistrationIngestion.runUserRegistrationIngestion(eventSource);

        Dataset<Row> appLoadedEvents = ApplicationLoadingIngestion.runApplicationLoadingIngestion(eventSource);

        registeredEvents.printSchema();

        appLoadedEvents.printSchema();

        //TODO move the path logic from out of here
        DataWriter.saveEventsAsParquet(registeredEvents, System.getProperty("user.dir") + "/data/processed/registered");
        DataWriter.saveEventsAsParquet(appLoadedEvents, System.getProperty("user.dir") + "/data/processed/app_loaded");
    }
}
