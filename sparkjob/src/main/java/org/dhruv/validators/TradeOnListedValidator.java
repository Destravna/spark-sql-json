package org.dhruv.validators;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.dhruv.exceptions.InvalidFieldValue;
import org.dhruv.exceptions.InvalidJsonInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class TradeOnListedValidator {
    private static final Logger logger = LoggerFactory.getLogger(TradeOnListedValidator.class);
    private static final String[] fields = {"TradeDirection", "tradeType", "IsDestroyed", "redid", "comment", "tradeDate", "tradeValueDate", "lakeStorageTimeStamp", "modelVersion", "nominalAmount", "nominalCurrency", "quantity", "rawStorageTimeStamp", "sourceApplication", "tradeld", "version", "lastUpdateTimeStamp", "product"};
    private static final Set<String> fieldSet = new HashSet<>();
    private static final String schemaString = "{\"type\":\"record\",\"name\":\"trade_otc\",\"namespace\":\"example\",\"fields\":[{\"name\":\"TradeDirection\",\"type\":\"string\",\"doc\":\"Enum: 'buy' or 'sell'\"},{\"name\":\"tradeType\",\"type\":\"string\",\"doc\":\"Enum: 'external', 'internal', 'interco'\"},{\"name\":\"IsDestroyed\",\"type\":\"boolean\",\"doc\":\"Specify data type\"},{\"name\":\"redid\",\"type\":\"string\"},{\"name\":\"comment\",\"type\":\"string\"},{\"name\":\"tradeDate\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"tradeValueDate\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"lakeStorageTimeStamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"modelVersion\",\"type\":\"string\",\"doc\":\"Enum: 'v01', 'v02', 'v03', 'v04', 'v05'\"},{\"name\":\"nominalAmount\",\"type\":\"double\"},{\"name\":\"nominalCurrency\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"double\"},{\"name\":\"rawStorageTimeStamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"sourceApplication\",\"type\":\"string\"},{\"name\":\"tradeld\",\"type\":\"string\"},{\"name\":\"version\",\"type\":\"int\"},{\"name\":\"lastUpdateTimeStamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"product\",\"type\":{\"type\":\"record\",\"name\":\"product\",\"fields\":[{\"name\":\"productid\",\"type\":\"string\"},{\"name\":\"productName\",\"type\":\"string\"},{\"name\":\"maturity\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}]}}]}";
    private static Schema tradeOnListedSchema;

    private static void initFieldSet(){
        fieldSet.addAll(Arrays.stream(fields).map(String::toLowerCase).collect(Collectors.toList()));
        // System.out.println("size + " + fieldSet.size());
        Schema.Parser parser = new Schema.Parser();
        tradeOnListedSchema = parser.parse(schemaString);
        // System.out.println(tradeOnListedSchema);
    }
    
    public static void validateAttributes(JsonObject attributeInfo) throws InvalidJsonInput, InvalidFieldValue{
        if(fieldSet.isEmpty()) initFieldSet();

        JsonArray attributeArray = attributeInfo.getAsJsonArray("attributesList");
        logger.info("attributeArray : {}", attributeArray);

        for(JsonElement attribute: attributeArray){
            JsonObject attributeObject = attribute.getAsJsonObject();
            for(String field : attributeObject.keySet()){
                if(!fieldSet.contains(field.toLowerCase())){
                    throw new InvalidJsonInput(field  + " is not a valid field for the Listed schema");
                }
                else {
                    JsonObject attributeDetails = attributeObject.getAsJsonObject(field);
                    String condition = attributeDetails.get("condition").getAsString();
                    
                    if(!condition.isEmpty()){
                        // System.out.println("condition" + condition);
                        Schema.Field fieldInSchema = tradeOnListedSchema.getFields().stream().filter(f->f.name().equalsIgnoreCase(field)).findFirst().orElse(null);
                        Schema.Type fieldType = fieldInSchema.schema().getType();
                        CondtionValidator.validateTransformation(condition, fieldType.getName());
                    }
                    // System.out.println(attributeDetails);
                }
            }
        }
    }
    
    
}
