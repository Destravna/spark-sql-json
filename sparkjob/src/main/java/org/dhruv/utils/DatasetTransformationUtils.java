package org.dhruv.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

public class DatasetTransformationUtils {
    public static Dataset<Row> castColumnsToString(Dataset<Row> df){
        for(String column : df.columns()){
            df = df.withColumn(column, col(column).cast("string"));
        }
        return df;
    }
}
