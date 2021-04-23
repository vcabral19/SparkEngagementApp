package com.jspark.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;


public class DataWriter {
    //TODO make this an object (constructor) and maybe build some interface
    public static void saveEventsAsParquet(Dataset<Row> dataToBePersisted, String path){
        dataToBePersisted
                .write()
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(path);
    }
}
