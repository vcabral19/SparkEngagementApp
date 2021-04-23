package com.jspark.ingestion;

import com.jspark.generic.transformations.SchemaTransformations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class ApplicationLoadingIngestion implements IngestionInterface {

    private final StructType schema;
    private final Dataset<Row> eventSource;

    public ApplicationLoadingIngestion(Dataset<Row> eventSource, StructType schema){
        this.eventSource = eventSource;
        this.schema = schema;
    }

    public Dataset<Row> runIngestion() {
        Dataset<Row> appLoadedEvents = eventSource
                .filter(col("event").equalTo("app_loaded"));
        return SchemaTransformations.convertSchema(appLoadedEvents, schema);
    }
}
