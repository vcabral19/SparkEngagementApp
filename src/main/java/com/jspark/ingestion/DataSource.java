package com.jspark.ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSource {

    private static final String dataSourcePath = "data/raw/dataset.json";

    public static Dataset<Row> getDataFromSource(SparkSession spark){
        return spark.read().json(dataSourcePath);
    }
}
