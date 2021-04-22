package com.jspark.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;


public class DataWriter {
    public static void saveEventsAsParquet(Dataset<Row> dataToBePersisted, String path){
        dataToBePersisted
                .write()
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(path);
    }
}
