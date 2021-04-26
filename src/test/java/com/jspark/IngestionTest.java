package com.jspark;

import com.jspark.write.DataWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.jspark.ingestion.UserRegistrationIngestion;
import com.jspark.schema.UserRegistrationSchema;
import com.jspark.ingestion.ApplicationLoadingIngestion;
import com.jspark.schema.ApplicationLoadingSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IngestionTest extends SessionContext {

    private final String dataSourcePath = System.getProperty("user.dir") + "/src/test/dummy_raw/dummy_source_data.json";
    private Dataset<Row> dataSource;

    @BeforeEach
    public void readDummyDataSource(){
        this.dataSource = this.spark.read().json(dataSourcePath);
    }

    @Test
    @DisplayName("Checking Registered Event Ingestion")
    public void testRegisteredEventsIngestion(){

        StructType schema = UserRegistrationSchema.getSchema();
        UserRegistrationIngestion ingestion = new UserRegistrationIngestion(this.dataSource, schema);
        Dataset<Row> dfResult = ingestion.runIngestion();


        String expectedPath = System.getProperty("user.dir") + "/src/test/dummy_processed/registered/";
        DataWriter.saveEventsAsParquet(dfResult, expectedPath);


        Dataset<Row> expectedDf = this.spark.read().parquet(expectedPath);


        assertEquals(0, dfResult.except(expectedDf).count());
        assertEquals(schema, expectedDf.schema());

    }

    @Test
    @DisplayName("Checking Application Load Event Ingestion")
    public void testAppLoadedEventsIngestion(){

        StructType schema = ApplicationLoadingSchema.getSchema();
        ApplicationLoadingIngestion ingestion = new ApplicationLoadingIngestion(this.dataSource, schema);
        Dataset<Row> dfResult = ingestion.runIngestion();


        String expectedPath = System.getProperty("user.dir") + "/src/test/dummy_processed/app_loaded/";
        DataWriter.saveEventsAsParquet(dfResult, expectedPath);


        Dataset<Row> expectedDf = this.spark.read().parquet(expectedPath);


        assertEquals(0, dfResult.except(expectedDf).count());
        assertEquals(schema, expectedDf.schema());

    }
}
