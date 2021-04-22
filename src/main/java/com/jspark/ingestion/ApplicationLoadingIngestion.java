package com.jspark.ingestion;

import com.jspark.generic.transformations.SchemaTransformations;
import com.jspark.schema.ApplicationLoadingSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class ApplicationLoadingIngestion {

    public static Dataset<Row> runApplicationLoadingIngestion(Dataset<Row> eventSource) {
        Dataset<Row> appLoadedEvents = eventSource
                .filter(col("event").equalTo("app_loaded"));
        return SchemaTransformations.convertSchema(appLoadedEvents, ApplicationLoadingSchema.getApplicationLoadingSchema());
    }
}
