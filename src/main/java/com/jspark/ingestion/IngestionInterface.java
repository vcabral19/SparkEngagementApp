package com.jspark.ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface IngestionInterface {
    public Dataset<Row> runIngestion();
}
