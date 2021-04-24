package com.jspark.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public interface DataSourceInterface {
    Dataset<Row>getDataFromSource(DataFormats dataFormat, String dataSourcePath);
}
