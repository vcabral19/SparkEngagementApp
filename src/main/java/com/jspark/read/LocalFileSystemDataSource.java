package com.jspark.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class LocalFileSystemDataSource implements DataSourceInterface {

    private final SparkSession spark;


    public LocalFileSystemDataSource(SparkSession spark){
        this.spark = spark;

    }

    public Dataset<Row> getDataFromSource(DataFormats dataFormat, String dataSourcePath){
        switch (dataFormat) {
            case JSON:
                return this.spark.read().json(dataSourcePath);
            case PARQUET:
                return this.spark.read().parquet(dataSourcePath);
            default:
                throw new IllegalArgumentException(dataFormat + " is an invalid or not implemented Data Format");
        }
    }

    public Dataset<Row> getDataFromSource(DataFormats dataFormat, String dataSourcePath, StructType schema){
        switch (dataFormat) {
            case JSON:
                return this.spark.read().schema(schema).json(dataSourcePath);
            case PARQUET:
                return this.spark.read().schema(schema).parquet(dataSourcePath);
            default:
                throw new IllegalArgumentException(dataFormat + " is an invalid or not implemented Data Format");
        }
    }
}
