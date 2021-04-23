package com.jspark.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LocalFileSystemDataSource {

    private final SparkSession spark;
    private final DataFormats dataFormat;
    private final String dataSourcePath;

    public LocalFileSystemDataSource(SparkSession spark, DataFormats dataFormat, String path){
        this.spark = spark;
        this.dataFormat = dataFormat;
        this.dataSourcePath = path;
    }

    public Dataset<Row> getDataFromSource(){
        switch (this.dataFormat) {
            case JSON:
                return this.spark.read().json(this.dataSourcePath);
            case PARQUET:
                return this.spark.read().parquet(this.dataSourcePath);
            default:
                throw new IllegalArgumentException(this.dataFormat + " is an invalid  or not implemented Data Format");
        }
    }
}
