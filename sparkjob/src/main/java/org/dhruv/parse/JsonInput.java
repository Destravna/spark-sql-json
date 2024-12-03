package org.dhruv.parse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.dhruv.exceptions.InvalidFieldValue;
import org.dhruv.exceptions.InvalidJsonInput;
import org.dhruv.utils.QueryAttribute;
import org.dhruv.validators.TradeOnListedValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.Gson;


import lombok.Getter;
import lombok.NoArgsConstructor;


@Getter
@NoArgsConstructor
public class JsonInput {
    private static final Logger logger = LoggerFactory.getLogger(JsonInput.class);
    private static final String[] validOutputFormats = {"csv", "json", "text", "parquet", "xml"};

    private JsonObject inputJsonObject;
    private String model; //otc or listed
    private String outputPath;
    private LocalDate dateFrom;
    private LocalDate dateTo;
    private JsonObject attributesInfo;
    private String outputFormat;
    private boolean getAllAttribues;
    private String extractName;

    public JsonInput(String jsonPath) throws FileNotFoundException, IOException, InvalidJsonInput, InvalidFieldValue {
        logger.info("input path : {}",   jsonPath);
        String inputJsonString = new String(Files.readAllBytes(Paths.get(jsonPath)));
        logger.info("inputJsonString: {}", inputJsonString);
        Gson gson = new Gson();
        // inputJsonObject = gson.fromJson(inputJsonString, JsonObject.class); 

        inputJsonObject = JsonParser.parseString(inputJsonString).getAsJsonObject();
        setExtractName();
        setAttributesInfo();
        setGetAllAtrributes();
        setModel();
        setDateFrom();
        setDateTo();
        validateDates();
        setOutputFormat();
        setOutputPath();
        validateAttributes();
    }

    private void setExtractName(){
        if(inputJsonObject.has("extractName")){
            extractName = inputJsonObject.get("extractName").getAsString();
            logger.info("extractName : {}", extractName);
        }
    }

    public boolean isGetAllAttributes(){
        return getAllAttribues;
    }


    private void setAttributesInfo() throws InvalidJsonInput{
        if(inputJsonObject.has("attributes")){
            attributesInfo = inputJsonObject.get("attributes").getAsJsonObject();
            logger.info("attributes info: {}", attributesInfo);
        }
        else throw new InvalidJsonInput("could not find attributes field in the given jsonInput");
    }

    private void setGetAllAtrributes(){
        if(attributesInfo.has("allAttributes")){
            // System.out.println("_________________getAll: " + attributesInfo.get("allAttributes").toString());
            getAllAttribues = attributesInfo.get("allAttributes").toString().equals("\"true\""); // escape because for some reason true is read as "true"(along with quotes), usually happens for child objects 
            logger.info("getAllAttributes : {}", getAllAttribues);
        }
        else logger.error("allAttributes property not found in json");
    }

    private void setModel() throws InvalidJsonInput, InvalidFieldValue{
        if(inputJsonObject.has("model")){
            model = inputJsonObject.get("model").getAsString();
            validateModel();
            logger.info("model : {}", this.model);
        }
        else throw new InvalidJsonInput("could not find \"model\" field in the given jsonInput");
    }

    private void setDateFrom() throws InvalidJsonInput {
        if(inputJsonObject.has("dateFrom")){
            dateFrom = LocalDate.parse(inputJsonObject.get("dateFrom").getAsString());
            logger.info("dateFrom : {}", dateFrom);
        }
        else throw new InvalidJsonInput("could not find \"dateFrom\" field in the given jsonInput");
    }

    private void setDateTo() throws InvalidJsonInput {
        if(inputJsonObject.has("dateTo")){
            dateTo = LocalDate.parse(inputJsonObject.get("dateTo").getAsString());
            logger.info("dateTo : {}", dateFrom);
        }
        else throw new InvalidJsonInput("could not find \"dateTo\" field in the given jsonInput");
    }

    private void setOutputFormat() throws InvalidFieldValue{
        if(inputJsonObject.has("outputFormat")){
            outputFormat = inputJsonObject.get("outputFormat").getAsString();
            validateOutputFormat();
            logger.info("outputFormat : {}", outputFormat);
        }
        else {
            logger.error("outputFormat not found, csv will be used by default");
            outputFormat = "csv";
        }
    }

    private void setOutputPath()  {
        if(inputJsonObject.has("outputPath")){
            outputPath = inputJsonObject.get("outputPath").getAsString();
        }
        else outputPath = "/home/dhruv/projects/sample-spark-job/output";
        logger.info("outputPath : {}", outputPath);
    }


    @Override
    public String toString(){
        return model + " " + dateFrom + " " + dateTo + " " + outputFormat + " " + getAllAttribues + " " + attributesInfo + "\n";
    }


    //validators
    private void validateModel() throws InvalidFieldValue{
        if(!model.equals("OTC") && !model.equals("listed")){
            throw new InvalidFieldValue("model:  " + model);
        }
    }

    private void validateDates() throws InvalidFieldValue{
        if(dateFrom.isAfter(dateTo)){
            throw new InvalidFieldValue("date From is greater than dateTo");
        }
    }

    private void validateOutputFormat() throws InvalidFieldValue{
        if(!(Arrays.asList(validOutputFormats)).contains(outputFormat)){
            throw new InvalidFieldValue("outputFormat");
        }
    }

    private void validateAttributes() throws InvalidJsonInput, InvalidFieldValue{
        if(model.equals("listed")){
            TradeOnListedValidator.validateAttributes(attributesInfo);
        }
    }

    public List<QueryAttribute> toQueryAttributeList(){
        List<QueryAttribute> queryAttributes = new ArrayList<>();
        JsonArray attributesArray = attributesInfo.getAsJsonArray("attributesList");
        System.out.println(attributesArray);
        for(JsonElement attribute : attributesArray){
            QueryAttribute queryAttribute = new QueryAttribute();
            JsonObject attributeObject = attribute.getAsJsonObject();

            //There is only one field here, but couldn't find a better way
            for(String field: attributeObject.keySet()) {
                queryAttribute.setKey(field);
            }

            JsonObject attributeInfo = attributeObject.get(queryAttribute.getKey()).getAsJsonObject();
            // System.out.println(attributeInfo);
            if(attributeInfo.has("alias")){
                queryAttribute.setAlias(attributeInfo.get("alias").getAsString());
            }
            else queryAttribute.setAlias(queryAttribute.getKey());
            if(attributeInfo.has("condition")){
                queryAttribute.setCondition(attributeInfo.get("condition").getAsString());
            }
            else queryAttribute.setCondition("");
            queryAttributes.add(queryAttribute);
            logger.info("{}", queryAttribute);
        }
        logger.info("query attributes {}", queryAttributes);
        return queryAttributes;
        
    } 


    



    
}
