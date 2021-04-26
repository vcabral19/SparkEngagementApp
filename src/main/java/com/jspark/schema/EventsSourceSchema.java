package com.jspark.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class EventsSourceSchema {

    private final static StructField[] fields = {
            DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
            DataTypes.createStructField("initiator_id", DataTypes.LongType, true),
            DataTypes.createStructField("event", DataTypes.StringType, true),
            DataTypes.createStructField("device_type", DataTypes.StringType, true),
            DataTypes.createStructField("browser_version", DataTypes.StringType, true),
            DataTypes.createStructField("campaign", DataTypes.StringType, true),
            DataTypes.createStructField("channel", DataTypes.StringType, true)
    };
    private final static StructType eventsSourceSchema = DataTypes.createStructType(fields);

    public static StructType getSchema(){
        return eventsSourceSchema;
    }
}
