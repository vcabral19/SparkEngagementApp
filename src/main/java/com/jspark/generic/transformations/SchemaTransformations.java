package com.jspark.generic.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class SchemaTransformations {

    public static Seq<String> getColumnsSeq(StructType schema) {
        List<String> inputList = Arrays.asList(schema.fieldNames());
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public static Dataset<Row> convertSchema(Dataset<Row> dataset, StructType targetSchema){
        Iterator<StructField> fieldIterator = JavaConverters.asJavaIterator(targetSchema.iterator());

        while(fieldIterator.hasNext()){
            StructField field = fieldIterator.next();
            String columnName = field.name();
            DataType targetType = field.dataType();
            dataset = dataset.withColumn(columnName, dataset.col(columnName).cast(targetType));
        }
        return dataset.selectExpr(getColumnsSeq(targetSchema));
    }

}
