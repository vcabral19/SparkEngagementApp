package com.jspark.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class DataWriter {

    public static void saveEventsAsPartitionedByDateParquet(Dataset<Row> dataToBePersisted, String path){
        dataToBePersisted
                .withColumn("year", date_format(col("timestamp"), "y"))
                .withColumn("month", date_format(col("timestamp"), "M"))
                .withColumn("day", date_format(col("timestamp"), "d"))
                .write()
                .partitionBy("year","month","day")
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(path);
    }
}
