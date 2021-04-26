package com.jspark;


import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class SessionContext {

    protected SparkSession spark;

    @BeforeEach
    public void setUpSparkSession() throws Exception {
        this.spark = SparkSession.builder()
                .appName("SparkTests").master("local[*]")
                .getOrCreate();
        spark.conf().set("spark.sql.session.timeZone", "UTC");
    }

    @AfterEach
    public void stopSparkSession() throws Exception {
        this.spark.stop();
    }

}
