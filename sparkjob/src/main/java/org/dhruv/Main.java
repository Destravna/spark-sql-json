package org.dhruv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dhruv.exceptions.InvalidFieldValue;
import org.dhruv.exceptions.InvalidJsonInput;
import org.dhruv.extraction.Extract;
import org.dhruv.parse.JsonInput;
import org.dhruv.utils.QueryAttribute;
import org.dhruv.validators.TradeOnListedValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;




public class Main {
private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, InvalidJsonInput, InvalidFieldValue {
        // System.out.println(log.isDebugEnabled());
        if(args.length < 1){
            log.error("Json file location not given");
            System.exit(1);
        }
        String filePath = args[0];
        log.info("File path {}", filePath);
        // JsonInput jsonInput = new JsonInput(filePath);\
        Extract extract = new Extract(filePath);
        extract.execute();
        
        
        // TradeOnListedValidator.validateAttributes();
      
        // for(Field field: tradeOnListedSchema.getFields()){
        //     System.out.print("\"" + field.name() + "\", ");
        // }
        // SparkSession sparkSession = SparkSession.builder().master("spark://dhruv-VMware-Virtual-Platform:7077").appName("app").enableHiveSupport().getOrCreate();
        // // sparkSession.sql("show tables IN default").show();
        // Dataset<Row> df = sparkSession.sql("select * from trade_otc");
        // df.show();
        // df.write().format("com.databricks.spark.xml").save("/home/dhruv/projects/sample-spark-job/output"); //if submit then works 


       
       
        // df = sparkSession.sql("select * from employee");
        // df.show();
        // System.out.println("Hello world!");
    }
}