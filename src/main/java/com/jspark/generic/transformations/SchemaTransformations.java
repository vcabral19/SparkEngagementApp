package com.jspark.generic.transformations;

import com.jspark.schema.UserRegistrationSchema;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SchemaTransformations {
    //TODO delete me
    private static List<Column> getColumnsAsList(StructType schema){
        List<Column> columnsAsList = new ArrayList<>();
        for (String column : schema.fieldNames()){
            columnsAsList.add(col(column));
        }
        return columnsAsList;
    }

    public static Seq<String> getColumnsSeq(StructType schema) {
        List<String> inputList = Arrays.asList(schema.fieldNames());
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public static Dataset<Row> convertSchema(Dataset<Row> dataset, StructType targetSchema){
        Iterator<StructField> fieldIterator = JavaConversions.asJavaIterator(targetSchema.iterator());

        while(fieldIterator.hasNext()){
            StructField field = fieldIterator.next();
            String columnName = field.name();
            DataType targetType = field.dataType();
            dataset = dataset.withColumn(columnName, dataset.col(columnName).cast(targetType));
        }
        dataset = dataset.selectExpr(getColumnsSeq(targetSchema));
        return dataset;
    }

}
