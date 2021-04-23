package com.jspark.ingestion;

import com.jspark.generic.transformations.SchemaTransformations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class UserRegistrationIngestion implements IngestionInterface{

    private final StructType schema;
    private final Dataset<Row> eventSource;

    public UserRegistrationIngestion(Dataset<Row> eventSource, StructType schema){
        this.eventSource = eventSource;
        this.schema = schema;
    }

    @Override
    public Dataset<Row> runIngestion() {
        Dataset<Row> registeredEvents = eventSource
                .filter(col("event").equalTo("registered"));
        return SchemaTransformations.convertSchema(registeredEvents, schema);
    }
}
