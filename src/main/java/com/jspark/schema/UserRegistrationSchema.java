package com.jspark.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class UserRegistrationSchema {

    private final static StructField[] fields = {
            DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
            DataTypes.createStructField("initiator_id", DataTypes.LongType, true),
            DataTypes.createStructField("channel", DataTypes.StringType, true)
    };
    private final static StructType userRegistrationSchema = DataTypes.createStructType(fields);

    public static StructType getSchema(){
        return userRegistrationSchema;
    }

}
