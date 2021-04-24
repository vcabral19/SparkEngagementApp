package com.jspark.steps;

import com.jspark.ingestion.ApplicationLoadingIngestion;
import com.jspark.ingestion.UserRegistrationIngestion;
import com.jspark.read.LocalFileSystemDataSource;
import com.jspark.schema.ApplicationLoadingSchema;
import com.jspark.schema.UserRegistrationSchema;
import com.jspark.read.DataFormats;

import com.jspark.write.DataWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class EventsIngestorStep implements StepInterface {
    private static final String path = "data/raw/dataset.json";
    private static final DataFormats dataFormat = DataFormats.JSON;
    private static final StructType userRegistrationSchema = UserRegistrationSchema.getSchema();
    private static final StructType applicationLoadingSchema = ApplicationLoadingSchema.getSchema();

    public EventsIngestorStep(){};

    public void run(){
        SparkSession spark = SparkSession.builder()
                .appName("EventsIngestor").master("local[*]")
                .getOrCreate();
        //TODO get executors variable and etc as env var and set dev as fallback

        LocalFileSystemDataSource localFileSystemDataReader = new LocalFileSystemDataSource(spark);

        Dataset<Row> eventSource = localFileSystemDataReader.getDataFromSource(dataFormat, path);

        UserRegistrationIngestion userIngestion = new UserRegistrationIngestion(eventSource, userRegistrationSchema);
        Dataset<Row> registeredEvents = userIngestion.runIngestion();

        ApplicationLoadingIngestion applicationIngestion = new ApplicationLoadingIngestion(eventSource, applicationLoadingSchema);
        Dataset<Row> appLoadedEvents = applicationIngestion.runIngestion();

        registeredEvents.printSchema();

        appLoadedEvents.printSchema();

        //TODO move the path logic from out of here
        DataWriter.saveEventsAsParquet(registeredEvents, System.getProperty("user.dir") + "/data/processed/registered");
        DataWriter.saveEventsAsParquet(appLoadedEvents, System.getProperty("user.dir") + "/data/processed/app_loaded");
    }
}
