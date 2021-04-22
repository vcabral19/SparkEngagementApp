package com.jspark.ingestion;

import com.jspark.generic.transformations.SchemaTransformations;
import com.jspark.schema.UserRegistrationSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class UserRegistrationIngestion {

    public static Dataset<Row> runUserRegistrationIngestion(Dataset<Row> eventSource) {
        Dataset<Row> registeredEvents = eventSource
                .filter(col("event").equalTo("registered"));
        return SchemaTransformations.convertSchema(registeredEvents, UserRegistrationSchema.getUserRegistrationSchema());
    }
}
