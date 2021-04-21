package com.jspark.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.time.Instant;



public class UserRegistrationSchema implements Serializable {

    static StructField[] fields = {
            DataTypes.createStructField("timestamp", DataTypes.TimestampType, false),
            DataTypes.createStructField("initiator_id", DataTypes.LongType, false),
            DataTypes.createStructField("channel", DataTypes.StringType, true)
    };
    public static StructType userRegistrationSchema = DataTypes.createStructType(fields);


    //TODO delete that all
    private Instant timestamp; //= ZonedDateTime.parse("2020-01-08T06:21:14.000Z");
    private long initiator_id; // = 3074457347135400447L;
    private String channel; //= "invite";

    public Instant getTimeStamp(){
        return timestamp;
    }

    public void setTimesStamp(String timestamp){
        this.timestamp = Instant.parse(timestamp);
    }

    public long getInitiadorId(){
        return initiator_id;
    }

    public void setInitiadorID(long initiator_id){
        this.initiator_id = initiator_id;
    }

    public String getChannel(){
        return channel;
    }

    public void setChannel(String channel){
        this.channel = channel;
    }
}
