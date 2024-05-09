package com.jspark.steps;

import com.jspark.ingestion.ApplicationLoadingIngestion;
import com.jspark.ingestion.IngestionInterface;
import com.jspark.ingestion.UserRegistrationIngestion;
import com.jspark.read.LocalFileSystemDataSource;
import com.jspark.schema.EventsSourceSchema;
import com.jspark.schema.ApplicationLoadingSchema;
import com.jspark.schema.UserRegistrationSchema;
import com.jspark.read.DataFormats;

import com.jspark.write.DataWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class EventsIngestorStep implements StepInterface {
    private final String sourcePath;
    private final String registeredWritePath;
    private final String appLoadedWritePath;
    private final String sparkMasterConfig;
    private final DataFormats dataFormat = DataFormats.JSON;
    private final StructType eventsSourceSchema = EventsSourceSchema.getSchema();
    private final StructType userRegistrationSchema = UserRegistrationSchema.getSchema();
    private final StructType applicationLoadingSchema = ApplicationLoadingSchema.getSchema();

    public EventsIngestorStep(String sourcePath, String registeredPath, String appLoadedPath, String sparkMasterConfig){
        this.sourcePath = sourcePath;
        this.registeredWritePath = registeredPath;
        this.appLoadedWritePath = appLoadedPath;
        this.sparkMasterConfig = sparkMasterConfig;
    }

    public void run(){
        SparkSession spark = SparkSession.builder()
                .appName("EventsIngestor").master(sparkMasterConfig)
                .getOrCreate();
        spark.conf().set("spark.sql.session.timeZone", "UTC");

        LocalFileSystemDataSource localFileSystemDataReader = new LocalFileSystemDataSource(spark);

        Dataset<Row> eventSource = localFileSystemDataReader.getDataFromSource(dataFormat, sourcePath, eventsSourceSchema);


        IngestionInterface userIngestion = new UserRegistrationIngestion(eventSource, userRegistrationSchema);
        Dataset<Row> registeredEvents = userIngestion.runIngestion();

        IngestionInterface applicationIngestion = new ApplicationLoadingIngestion(eventSource, applicationLoadingSchema);
        Dataset<Row> appLoadedEvents = applicationIngestion.runIngestion();

        registeredEvents.printSchema();

        appLoadedEvents.printSchema();

        DataWriter.saveEventsAsParquet(registeredEvents, registeredWritePath);
        DataWriter.saveEventsAsParquet(appLoadedEvents, appLoadedWritePath);
    }
}
