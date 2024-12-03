package org.dhruv.extraction;

import static org.apache.spark.sql.functions.some;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dhruv.exceptions.InvalidFieldValue;
import org.dhruv.exceptions.InvalidJsonInput;
import org.dhruv.parse.JsonInput;
import org.dhruv.utils.DatasetTransformationUtils;
import org.dhruv.utils.QueryAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Extract {
    private static final Logger logger = LoggerFactory.getLogger(Extract.class);
    private static final String db = "default";

    private String extractName;
    private String outputPath;
    private String head = "SELECT";
    private String fields = "";
    // private String extractQuery;
    private String tail = "where storageday between";
    private String from;
    private String tableName;
    private String outputFormat;
    private JsonInput jsonInput;


    private String getQuery(){
        return head + " " + fields + " " + from + " " + tail + " "; 
    }

    private void setExtractName(){
        extractName = jsonInput.getExtractName();
    }

    public void execute(){
        String query = getQuery();
        logger.info("extract query : {} ", query);
        SparkSession spark = SparkSession.builder().master("spark://dhruv-VMware-Virtual-Platform:7077").appName(extractName).enableHiveSupport().getOrCreate();
        Dataset<Row> df = spark.sql(query);
        df.show();
        writeOutput(df);
    }

    private void writeOutput(Dataset<Row> df){
        if(outputFormat.equals("csv")){
            df = DatasetTransformationUtils.castColumnsToString(df);
        }
        LocalDate date = LocalDate.now();
        System.out.println(date);
        String finalPath = outputPath + "_" + extractName + "_" + date.toString();
        df.write().mode("overwrite").format(outputFormat).save(finalPath);
    }

    public Extract(String jsonFile) throws FileNotFoundException, IOException, InvalidJsonInput, InvalidFieldValue {
        jsonInput = new JsonInput(jsonFile);
        setFields();
        setTableName();
        setFrom();
        setTail();
        setOutputPath();
        setOutputFormat();
        setExtractName();
    }

    private void setFields() {
        if (jsonInput.isGetAllAttribues()) {
            logger.error("getAllAttributes true, fields set to *");
            fields = " * ";
        } else {
            List<QueryAttribute> queryAttributes = jsonInput.toQueryAttributeList();
            for (QueryAttribute queryAttribute : queryAttributes) {
                // System.out.println(queryAttribute.getCondition());
                if (queryAttribute.getCondition().length() > 0) {
                    String queryFiled = String.format((" %s(%s) AS %s,"), queryAttribute.getCondition(),
                            queryAttribute.getKey(), queryAttribute.getAlias());
                    fields += queryFiled;
                } else {
                    String queryField = String.format(" %s AS %s,", queryAttribute.getKey(), queryAttribute.getAlias());
                    fields += queryField;
                }

            }
            fields = fields.substring(0, fields.length() - 1); // removing the extra comma at the end
            logger.info("fields : {}", fields);
        }

    }

    private void setTableName(){
        if(jsonInput.getModel().equals("otc")) tableName = "trade_otc";
        else tableName = "trade_otc"; //change in future
        logger.info("table name : {}", tableName);
    }

    private void setTail(){
        tail += String.format(" '%s' and '%s' ", jsonInput.getDateFrom(), jsonInput.getDateTo());
    }

    private void setFrom(){
        from = String.format(" from %s ", tableName);
    }


    private void setOutputPath() {
        outputPath = jsonInput.getOutputPath();
    }

    private void setOutputFormat(){
        outputFormat = jsonInput.getOutputFormat();
    }

    public String getOutputPath() {
        return outputPath;
    }

    

}